// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package importutil

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus/api/schemapb"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

// A struct to hold insert log paths and delta log paths of a segment
type SegmentFilesHolder struct {
	segmentID  int64                        // id of the segment
	fieldFiles map[storage.FieldID][]string // mapping of field id and data file path
	deltaFiles []string                     // a list of delta log file path, typically has only one item
}

// Adapter class to process insertlog/deltalog of a backuped segment
// This class do the following works:
// 1. read insert log of each field, then constructs map[storage.FieldID]storage.FieldData in memory.
// 2. read delta log to remove deleted entities(TimeStampField is used to apply or skip the operation).
// 3. split data according to shard number
// 4. call the callFlushFunc function to flush data into new segment if data size reaches segmentSize.
type BinlogAdapter struct {
	collectionSchema *schemapb.CollectionSchema // collection schema
	chunkManager     storage.ChunkManager       // storage interfaces to read binlog files
	callFlushFunc    ImportFlushFunc            // call back function to flush segment
	shardNum         int32                      // sharding number of the collection
	segmentSize      int64                      // maximum size of a segment(unit:byte)
	maxTotalSize     int64                      // maximum size of in-memory segments(unit:byte)
	primaryKey       storage.FieldID            // id of primary key
	primaryType      schemapb.DataType          // data type of primary key

	// a timestamp to define the end point of restore, data after this point will be ignored
	// set this value to 0, all the data will be ignored
	// set this value to math.MaxUint64, all the data will be imported
	tsEndPoint uint64
}

func NewBinlogAdapter(collectionSchema *schemapb.CollectionSchema,
	shardNum int32,
	segmentSize int64,
	maxTotalSize int64,
	chunkManager storage.ChunkManager,
	flushFunc ImportFlushFunc,
	tsEndPoint uint64) (*BinlogAdapter, error) {
	if collectionSchema == nil {
		log.Error("Binlog adapter: collection schema is nil")
		return nil, errors.New("collection schema is nil")
	}

	if chunkManager == nil {
		log.Error("Binlog adapter: chunk manager pointer is nil")
		return nil, errors.New("chunk manager pointer is nil")
	}

	if flushFunc == nil {
		log.Error("Binlog adapter: flush function is nil")
		return nil, errors.New("flush function is nil")
	}

	adapter := &BinlogAdapter{
		collectionSchema: collectionSchema,
		chunkManager:     chunkManager,
		callFlushFunc:    flushFunc,
		shardNum:         shardNum,
		segmentSize:      segmentSize,
		maxTotalSize:     maxTotalSize,
		tsEndPoint:       tsEndPoint,
	}

	// amend the segment size to avoid portential OOM risk
	if adapter.segmentSize > MaxSegmentSizeInMemory {
		adapter.segmentSize = MaxSegmentSizeInMemory
	}

	// find out the primary key ID and its data type
	adapter.primaryKey = -1
	for i := 0; i < len(collectionSchema.Fields); i++ {
		schema := collectionSchema.Fields[i]
		if schema.GetIsPrimaryKey() {
			adapter.primaryKey = schema.GetFieldID()
			adapter.primaryType = schema.GetDataType()
			break
		}
	}
	// primary key not found
	if adapter.primaryKey == -1 {
		log.Error("Binlog adapter: collection schema has no primary key")
		return nil, errors.New("collection schema has no primary key")
	}

	return adapter, nil
}

func (p *BinlogAdapter) Read(segmentHolder *SegmentFilesHolder) error {
	if segmentHolder == nil {
		log.Error("Binlog adapter: segment files holder is nil")
		return errors.New("segment files holder is nil")
	}

	log.Info("Binlog adapter: read segment", zap.Int64("segmentID", segmentHolder.segmentID))

	// step 1: verify the file count by collection schema
	err := p.verify(segmentHolder)
	if err != nil {
		return err
	}

	// step 2: read the delta log to prepare delete list, and combine lists into one dict
	intDeletedList, strDeletedList, err := p.readDeltalogs(segmentHolder)
	if err != nil {
		return err
	}

	// step 3: read binlog files batch by batch
	// Assume the collection has 2 fields: a and b
	// a has these binlog files: a_1, a_2, a_3 ...
	// b has these binlog files: b_1, b_2, b_3 ...
	// Then first round read a_1 and b_1, second round read a_2 and b_2, etc...
	// deleted list will be used to remove deleted entities
	// if accumulate data exceed segmentSize, call callFlushFunc to generate new segment
	batchCount := 0
	for _, files := range segmentHolder.fieldFiles {
		batchCount = len(files)
		break
	}

	// prepare FieldData list
	segmentsData := make([]map[storage.FieldID]storage.FieldData, 0, p.shardNum)
	for i := 0; i < int(p.shardNum); i++ {
		segmentData := initSegmentData(p.collectionSchema)
		if segmentData == nil {
			log.Error("Binlog adapter: failed to initialize FieldData list")
			return errors.New("failed to initialize FieldData list")
		}
		segmentsData = append(segmentsData, segmentData)
	}

	// read binlog files batch by batch
	for i := 0; i < batchCount; i++ {
		// batchFiles excludes the primary key field and the timestamp field
		// timestamp field is used to compare the tsEndPoint to skip some rows, no need to pass old timestamp to new segment.
		// once a new segment generated, the timestamp field will be re-generated, too.
		batchFiles := make(map[storage.FieldID]string)
		for fieldID, files := range segmentHolder.fieldFiles {
			if fieldID == p.primaryKey || fieldID == common.TimeStampField {
				continue
			}
			batchFiles[fieldID] = files[i]
		}
		log.Info("Binlog adapter: batch files to read", zap.Any("batchFiles", batchFiles))

		// read primary keys firstly
		primaryLog := segmentHolder.fieldFiles[p.primaryKey][i] // no need to check existence, already verified
		log.Info("Binlog adapter: prepare to read primary key binglog", zap.Int64("pk", p.primaryKey), zap.String("logPath", primaryLog))
		intList, strList, err := p.readPrimaryKeys(primaryLog)
		if err != nil {
			return err
		}

		// read timestamps list
		timestampLog := segmentHolder.fieldFiles[common.TimeStampField][i] // no need to check existence, already verified
		log.Info("Binlog adapter: prepare to read timestamp binglog", zap.Any("logPath", timestampLog))
		timestampList, err := p.readTimestamp(timestampLog)
		if err != nil {
			return err
		}

		var shardList []int32
		if p.primaryType == schemapb.DataType_Int64 {
			// calculate a shard num list by primary keys and deleted entities
			shardList, err = p.getShardingListByPrimaryInt64(intList, timestampList, segmentsData, intDeletedList)
			if err != nil {
				return err
			}
		} else if p.primaryType == schemapb.DataType_VarChar {
			// calculate a shard num list by primary keys and deleted entities
			shardList, err = p.getShardingListByPrimaryVarchar(strList, timestampList, segmentsData, strDeletedList)
			if err != nil {
				return err
			}
		} else {
			log.Error("Binlog adapter: unknow primary key type", zap.Int("type", int(p.primaryType)))
			return errors.New("unknow primary key type")
		}

		// if shardList is empty, that means all the primary keys have been deleted(or skipped), no need to read other files
		if len(shardList) == 0 {
			continue
		}

		// read other insert logs and use the shardList to do sharding
		for fieldID, file := range batchFiles {
			err = p.readInsertlog(fieldID, file, segmentsData, shardList)
			if err != nil {
				return err
			}
		}

		// flush segment whose size exceed segmentSize
		err = p.tryFlushSegments(segmentsData, false)
		if err != nil {
			return err
		}
	}

	// finally, force to flush
	return p.tryFlushSegments(segmentsData, true)
}

// This method verify the schema and binlog files
//  1. each field must has binlog file
//  2. binlog file count of each field must be equal
//  3. the collectionSchema doesn't contain TimeStampField and RowIDField since the import_wrapper excludes them,
//     but the segmentHolder.fieldFiles need to contains the two fields.
func (p *BinlogAdapter) verify(segmentHolder *SegmentFilesHolder) error {
	if segmentHolder == nil {
		log.Error("Binlog adapter: segment files holder is nil")
		return errors.New("segment files holder is nil")
	}

	firstFieldFileCount := 0
	//  each field must has binlog file
	for i := 0; i < len(p.collectionSchema.Fields); i++ {
		schema := p.collectionSchema.Fields[i]

		files, ok := segmentHolder.fieldFiles[schema.FieldID]
		if !ok {
			log.Error("Binlog adapter: a field has no binlog file", zap.Int64("fieldID", schema.FieldID))
			return errors.New("the field " + strconv.Itoa(int(schema.FieldID)) + " has no binlog file")
		}

		if i == 0 {
			firstFieldFileCount = len(files)
		}
	}

	// the segmentHolder.fieldFiles need to contains RowIDField
	_, ok := segmentHolder.fieldFiles[common.RowIDField]
	if !ok {
		log.Error("Binlog adapter: the binlog files of RowIDField is missed")
		return errors.New("the binlog files of RowIDField is missed")
	}

	// the segmentHolder.fieldFiles need to contains TimeStampField
	_, ok = segmentHolder.fieldFiles[common.TimeStampField]
	if !ok {
		log.Error("Binlog adapter: the binlog files of TimeStampField is missed")
		return errors.New("the binlog files of TimeStampField is missed")
	}

	// binlog file count of each field must be equal
	for _, files := range segmentHolder.fieldFiles {
		if firstFieldFileCount != len(files) {
			log.Error("Binlog adapter: file count of each field must be equal", zap.Int("firstFieldFileCount", firstFieldFileCount))
			return errors.New("binlog file count of each field must be equal")
		}
	}

	return nil
}

// This method read data from deltalog, and convert to a dict
// The deltalog data is a list, to improve performance of next step, we convert it to a dict,
// key is the deleted ID, value is operation timestamp which is used to apply or skip the delete operation.
func (p *BinlogAdapter) readDeltalogs(segmentHolder *SegmentFilesHolder) (map[int64]uint64, map[string]uint64, error) {
	deleteLogs, err := p.decodeDeleteLogs(segmentHolder)
	if err != nil {
		return nil, nil, err
	}

	if len(deleteLogs) == 0 {
		log.Info("Binlog adapter: no deletion for segment", zap.Int64("segmentID", segmentHolder.segmentID))
		return nil, nil, nil // no deletion
	}

	if p.primaryType == schemapb.DataType_Int64 {
		deletedIDDict := make(map[int64]uint64)
		for _, deleteLog := range deleteLogs {
			deletedIDDict[deleteLog.Pk.GetValue().(int64)] = deleteLog.Ts
		}
		log.Info("Binlog adapter: count of deleted entities", zap.Int("deletedCount", len(deletedIDDict)))
		return deletedIDDict, nil, nil
	} else if p.primaryType == schemapb.DataType_VarChar {
		deletedIDDict := make(map[string]uint64)
		for _, deleteLog := range deleteLogs {
			deletedIDDict[deleteLog.Pk.GetValue().(string)] = deleteLog.Ts
		}
		log.Info("Binlog adapter: count of deleted entities", zap.Int("deletedCount", len(deletedIDDict)))
		return nil, deletedIDDict, nil
	} else {
		log.Error("Binlog adapter: primary key is neither int64 nor varchar")
		return nil, nil, errors.New("primary key is neither int64 nor varchar")
	}
}

// Decode string array(read from delta log) to storage.DeleteLog array
func (p *BinlogAdapter) decodeDeleteLogs(segmentHolder *SegmentFilesHolder) ([]*storage.DeleteLog, error) {
	// step 1: read all delta logs to construct a string array, each string is marshaled from storage.DeleteLog
	stringArray := make([]string, 0)
	for _, deltalog := range segmentHolder.deltaFiles {
		deltaStrings, err := p.readDeltalog(deltalog)
		if err != nil {
			return nil, err
		}
		stringArray = append(stringArray, deltaStrings...)
	}

	if len(stringArray) == 0 {
		return nil, nil // no delete log, return directly
	}

	// print out the first deletion information for diagnose purpose
	log.Info("Binlog adapter: total deletion count", zap.Int("count", len(stringArray)), zap.String("firstDeletion", stringArray[0]))

	// step 2: decode each string to a storage.DeleteLog object
	deleteLogs := make([]*storage.DeleteLog, 0)
	for i := 0; i < len(stringArray); i++ {
		deleteLog, err := p.decodeDeleteLog(stringArray[i])
		if err != nil {
			return nil, err
		}

		// ignore deletions whose timestamp is larger than the tsEndPoint
		if deleteLog.Ts <= p.tsEndPoint {
			deleteLogs = append(deleteLogs, deleteLog)
		}
	}
	log.Info("Binlog adapter: deletion count after filtering", zap.Int("count", len(deleteLogs)))

	// step 3: verify the current collection primary key type and the delete logs data type
	for i := 0; i < len(deleteLogs); i++ {
		if deleteLogs[i].PkType != int64(p.primaryType) {
			log.Error("Binlog adapter: delta log data type is not equal to collection's primary key data type",
				zap.Int64("deltaDataType", deleteLogs[i].PkType),
				zap.Int64("pkDataType", int64(p.primaryType)))
			return nil, errors.New("delta log data type is not equal to collection's primary key data type")
		}
	}

	return deleteLogs, nil
}

// Decode a string to storage.DeleteLog
// Note: the following code is mainly come from data_codec.go, I suppose the code can compatible with old version 2.0
func (p *BinlogAdapter) decodeDeleteLog(deltaStr string) (*storage.DeleteLog, error) {
	deleteLog := &storage.DeleteLog{}
	if err := json.Unmarshal([]byte(deltaStr), deleteLog); err != nil {
		// compatible with versions that only support int64 type primary keys
		// compatible with fmt.Sprintf("%d,%d", pk, ts)
		// compatible error info (unmarshal err invalid character ',' after top-level value)
		splits := strings.Split(deltaStr, ",")
		if len(splits) != 2 {
			log.Error("Binlog adapter: the format of deletion string is incorrect", zap.String("deltaStr", deltaStr))
			return nil, fmt.Errorf("the format of deletion string is incorrect, %v can not be split", deltaStr)
		}
		pk, err := strconv.ParseInt(splits[0], 10, 64)
		if err != nil {
			log.Error("Binlog adapter: failed to parse primary key of deletion string from old version",
				zap.String("deltaStr", deltaStr), zap.Error(err))
			return nil, err
		}
		deleteLog.Pk = &storage.Int64PrimaryKey{
			Value: pk,
		}
		deleteLog.PkType = int64(schemapb.DataType_Int64)
		deleteLog.Ts, err = strconv.ParseUint(splits[1], 10, 64)
		if err != nil {
			log.Error("Binlog adapter: failed to parse timestamp of deletion string from old version",
				zap.String("deltaStr", deltaStr), zap.Error(err))
			return nil, err
		}
	}

	return deleteLog, nil
}

// Each delta log data type is varchar, marshaled from an array of storage.DeleteLog objects.
func (p *BinlogAdapter) readDeltalog(logPath string) ([]string, error) {
	// open the delta log file
	binlogFile, err := NewBinlogFile(p.chunkManager)
	if err != nil {
		log.Error("Binlog adapter: failed to initialize binlog file", zap.String("logPath", logPath), zap.Error(err))
		return nil, err
	}

	err = binlogFile.Open(logPath)
	if err != nil {
		log.Error("Binlog adapter: failed to open delta log", zap.String("logPath", logPath), zap.Error(err))
		return nil, err
	}
	defer binlogFile.Close()

	// delta log type is varchar, return a string array(marshaled from an array of storage.DeleteLog objects)
	data, err := binlogFile.ReadVarchar()
	if err != nil {
		log.Error("Binlog adapter: failed to read delta log", zap.String("logPath", logPath), zap.Error(err))
		return nil, err
	}
	log.Info("Binlog adapter: successfully read deltalog", zap.Int("deleteCount", len(data)))

	return data, nil
}

// This method read data from int64 field, currently we use it to read the timestamp field.
func (p *BinlogAdapter) readTimestamp(logPath string) ([]int64, error) {
	// open the log file
	binlogFile, err := NewBinlogFile(p.chunkManager)
	if err != nil {
		log.Error("Binlog adapter: failed to initialize binlog file", zap.String("logPath", logPath), zap.Error(err))
		return nil, err
	}

	err = binlogFile.Open(logPath)
	if err != nil {
		log.Error("Binlog adapter: failed to open timestamp log file", zap.String("logPath", logPath))
		return nil, err
	}
	defer binlogFile.Close()

	// read int64 data
	int64List, err := binlogFile.ReadInt64()
	if err != nil {
		log.Error("Binlog adapter: failed to read timestamp data from log file", zap.String("logPath", logPath))
		return nil, err
	}

	log.Info("Binlog adapter: read timestamp from log file", zap.Int("tsCount", len(int64List)))

	return int64List, nil
}

// This method read primary keys from insert log.
func (p *BinlogAdapter) readPrimaryKeys(logPath string) ([]int64, []string, error) {
	// open the delta log file
	binlogFile, err := NewBinlogFile(p.chunkManager)
	if err != nil {
		log.Error("Binlog adapter: failed to initialize binlog file", zap.String("logPath", logPath), zap.Error(err))
		return nil, nil, err
	}

	err = binlogFile.Open(logPath)
	if err != nil {
		log.Error("Binlog adapter: failed to open primary key binlog", zap.String("logPath", logPath))
		return nil, nil, err
	}
	defer binlogFile.Close()

	// primary key can be int64 or varchar, we need to handle the two cases
	if p.primaryType == schemapb.DataType_Int64 {
		idList, err := binlogFile.ReadInt64()
		if err != nil {
			log.Error("Binlog adapter: failed to read int64 primary key from binlog", zap.String("logPath", logPath), zap.Error(err))
			return nil, nil, err
		}
		log.Info("Binlog adapter: succeed to read int64 primary key binlog", zap.Int("len", len(idList)))
		return idList, nil, nil
	} else if p.primaryType == schemapb.DataType_VarChar {
		idList, err := binlogFile.ReadVarchar()
		if err != nil {
			log.Error("Binlog adapter: failed to read varchar primary key from binlog", zap.String("logPath", logPath), zap.Error(err))
			return nil, nil, err
		}
		log.Info("Binlog adapter: succeed to read varchar primary key binlog", zap.Int("len", len(idList)))
		return nil, idList, nil
	} else {
		log.Error("Binlog adapter: primary key is neither int64 nor varchar")
		return nil, nil, errors.New("primary key is neither int64 nor varchar")
	}
}

// This method generate a shard id list by primary key(int64) list and deleted list.
// For example, an insert log has 10 rows, the no.3 and no.7 has been deleted, shardNum=2, the shardList could be:
// [0, 1, -1, 1, 0, 1, -1, 1, 0, 1]
// Compare timestampList with tsEndPoint to skip some rows.
func (p *BinlogAdapter) getShardingListByPrimaryInt64(primaryKeys []int64,
	timestampList []int64,
	memoryData []map[storage.FieldID]storage.FieldData,
	intDeletedList map[int64]uint64) ([]int32, error) {
	if len(timestampList) != len(primaryKeys) {
		log.Error("Binlog adapter: primary key length is not equal to timestamp list length",
			zap.Int("primaryKeysLen", len(primaryKeys)), zap.Int("timestampLen", len(timestampList)))
		return nil, errors.New("primary key length is not equal to timestamp list length")
	}

	log.Info("Binlog adapter: building shard list", zap.Int("pkLen", len(primaryKeys)), zap.Int("tsLen", len(timestampList)))

	actualDeleted := 0
	excluded := 0
	shardList := make([]int32, 0, len(primaryKeys))
	for i, key := range primaryKeys {
		// if this entity's timestamp is greater than the tsEndPoint, set shardID = -1 to skip this entity
		// timestamp is stored as int64 type in log file, actually it is uint64, compare with uint64
		ts := timestampList[i]
		if uint64(ts) > p.tsEndPoint {
			shardList = append(shardList, -1)
			excluded++
			continue
		}

		_, deleted := intDeletedList[key]
		// if the key exists in intDeletedList, that means this entity has been deleted
		if deleted {
			shardList = append(shardList, -1) // this entity has been deleted, set shardID = -1 and skip this entity
			actualDeleted++
		} else {
			hash, _ := typeutil.Hash32Int64(key)
			shardID := hash % uint32(p.shardNum)
			fields := memoryData[shardID] // initSegmentData() can ensure the existence, no need to check bound here
			field := fields[p.primaryKey] // initSegmentData() can ensure the existence, no need to check here

			// append the entity to primary key's FieldData
			field.(*storage.Int64FieldData).Data = append(field.(*storage.Int64FieldData).Data, key)
			field.(*storage.Int64FieldData).NumRows[0]++

			shardList = append(shardList, int32(shardID))
		}
	}
	log.Info("Binlog adapter: succeed to calculate a shard list", zap.Int("actualDeleted", actualDeleted),
		zap.Int("excluded", excluded), zap.Int("len", len(shardList)))

	return shardList, nil
}

// This method generate a shard id list by primary key(varchar) list and deleted list.
// For example, an insert log has 10 rows, the no.3 and no.7 has been deleted, shardNum=2, the shardList could be:
// [0, 1, -1, 1, 0, 1, -1, 1, 0, 1]
func (p *BinlogAdapter) getShardingListByPrimaryVarchar(primaryKeys []string,
	timestampList []int64,
	memoryData []map[storage.FieldID]storage.FieldData,
	strDeletedList map[string]uint64) ([]int32, error) {
	if len(timestampList) != len(primaryKeys) {
		log.Error("Binlog adapter: primary key length is not equal to timestamp list length",
			zap.Int("primaryKeysLen", len(primaryKeys)), zap.Int("timestampLen", len(timestampList)))
		return nil, errors.New("primary key length is not equal to timestamp list length")
	}

	log.Info("Binlog adapter: building shard list", zap.Int("pkLen", len(primaryKeys)), zap.Int("tsLen", len(timestampList)))

	actualDeleted := 0
	excluded := 0
	shardList := make([]int32, 0, len(primaryKeys))
	for i, key := range primaryKeys {
		// if this entity's timestamp is greater than the tsEndPoint, set shardID = -1 to skip this entity
		// timestamp is stored as int64 type in log file, actually it is uint64, compare with uint64
		ts := timestampList[i]
		if uint64(ts) > p.tsEndPoint {
			shardList = append(shardList, -1)
			excluded++
			continue
		}

		_, deleted := strDeletedList[key]
		// if exists in strDeletedList, that means this entity has been deleted
		if deleted {
			shardList = append(shardList, -1) // this entity has been deleted, set shardID = -1 and skip this entity
			actualDeleted++
		} else {
			hash := typeutil.HashString2Uint32(key)
			shardID := hash % uint32(p.shardNum)
			fields := memoryData[shardID] // initSegmentData() can ensure the existence, no need to check bound here
			field := fields[p.primaryKey] // initSegmentData() can ensure the existence, no need to check existence here

			// append the entity to primary key's FieldData
			field.(*storage.StringFieldData).Data = append(field.(*storage.StringFieldData).Data, key)
			field.(*storage.StringFieldData).NumRows[0]++

			shardList = append(shardList, int32(shardID))
		}
	}
	log.Info("Binlog adapter: succeed to calculate a shard list", zap.Int("actualDeleted", actualDeleted),
		zap.Int("excluded", excluded), zap.Int("len", len(shardList)))

	return shardList, nil
}

// This method read an insert log, and split the data into different shards according to a shard list
// The shardList is a list to tell which row belong to which shard, returned by getShardingListByPrimaryXXX()
// For deleted rows, we say its shard id is -1.
// For example, an insert log has 10 rows, the no.3 and no.7 has been deleted, shardNum=2, the shardList could be:
// [0, 1, -1, 1, 0, 1, -1, 1, 0, 1]
// This method put each row into different FieldData according to its shard id and field id,
// so, the no.1, no.5, no.9 will be put into shard_0
// the no.2, no.4, no.6, no.8, no.10 will be put into shard_1
// Note: the row count of insert log need to be equal to length of shardList
func (p *BinlogAdapter) readInsertlog(fieldID storage.FieldID, logPath string,
	memoryData []map[storage.FieldID]storage.FieldData, shardList []int32) error {
	// open the insert log file
	binlogFile, err := NewBinlogFile(p.chunkManager)
	if err != nil {
		log.Error("Binlog adapter: failed to initialize binlog file", zap.String("logPath", logPath), zap.Error(err))
		return err
	}

	err = binlogFile.Open(logPath)
	if err != nil {
		log.Error("Binlog adapter: failed to open insert log", zap.String("logPath", logPath), zap.Error(err))
		return err
	}
	defer binlogFile.Close()

	// read data according to data type
	switch binlogFile.DataType() {
	case schemapb.DataType_Bool:
		data, err := binlogFile.ReadBool()
		if err != nil {
			return err
		}

		err = p.dispatchBoolToShards(data, memoryData, shardList, fieldID)
		if err != nil {
			return err
		}
	case schemapb.DataType_Int8:
		data, err := binlogFile.ReadInt8()
		if err != nil {
			return err
		}

		err = p.dispatchInt8ToShards(data, memoryData, shardList, fieldID)
		if err != nil {
			return err
		}
	case schemapb.DataType_Int16:
		data, err := binlogFile.ReadInt16()
		if err != nil {
			return err
		}

		err = p.dispatchInt16ToShards(data, memoryData, shardList, fieldID)
		if err != nil {
			return err
		}
	case schemapb.DataType_Int32:
		data, err := binlogFile.ReadInt32()
		if err != nil {
			return err
		}

		err = p.dispatchInt32ToShards(data, memoryData, shardList, fieldID)
		if err != nil {
			return err
		}
	case schemapb.DataType_Int64:
		data, err := binlogFile.ReadInt64()
		if err != nil {
			return err
		}

		err = p.dispatchInt64ToShards(data, memoryData, shardList, fieldID)
		if err != nil {
			return err
		}
	case schemapb.DataType_Float:
		data, err := binlogFile.ReadFloat()
		if err != nil {
			return err
		}

		err = p.dispatchFloatToShards(data, memoryData, shardList, fieldID)
		if err != nil {
			return err
		}
	case schemapb.DataType_Double:
		data, err := binlogFile.ReadDouble()
		if err != nil {
			return err
		}

		err = p.dispatchDoubleToShards(data, memoryData, shardList, fieldID)
		if err != nil {
			return err
		}
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		data, err := binlogFile.ReadVarchar()
		if err != nil {
			return err
		}

		err = p.dispatchVarcharToShards(data, memoryData, shardList, fieldID)
		if err != nil {
			return err
		}
	case schemapb.DataType_BinaryVector:
		data, dim, err := binlogFile.ReadBinaryVector()
		if err != nil {
			return err
		}

		err = p.dispatchBinaryVecToShards(data, dim, memoryData, shardList, fieldID)
		if err != nil {
			return err
		}
	case schemapb.DataType_FloatVector:
		data, dim, err := binlogFile.ReadFloatVector()
		if err != nil {
			return err
		}

		err = p.dispatchFloatVecToShards(data, dim, memoryData, shardList, fieldID)
		if err != nil {
			return err
		}
	default:
		return errors.New("unsupported data type")
	}
	log.Info("Binlog adapter: read data into shard list", zap.Int("dataType", int(binlogFile.DataType())), zap.Int("shardLen", len(shardList)))

	return nil
}

func (p *BinlogAdapter) dispatchBoolToShards(data []bool, memoryData []map[storage.FieldID]storage.FieldData,
	shardList []int32, fieldID storage.FieldID) error {
	// verify row count
	if len(data) != len(shardList) {
		log.Error("Binlog adapter: bool field row count is not equal to primary key", zap.Int("dataLen", len(data)), zap.Int("shardLen", len(shardList)))
		return errors.New("bool field row count is not equal to primary key")
	}

	// dispatch entities acoording to shard list
	for i, val := range data {
		shardID := shardList[i]
		if shardID < 0 {
			continue // this entity has been deleted or excluded by timestamp
		}

		fields := memoryData[shardID] // initSegmentData() can ensure the existence, no need to check bound here
		field := fields[fieldID]      // initSegmentData() can ensure the existence, no need to check existence here
		field.(*storage.BoolFieldData).Data = append(field.(*storage.BoolFieldData).Data, val)
		field.(*storage.BoolFieldData).NumRows[0]++
	}

	return nil
}

func (p *BinlogAdapter) dispatchInt8ToShards(data []int8, memoryData []map[storage.FieldID]storage.FieldData,
	shardList []int32, fieldID storage.FieldID) error {
	// verify row count
	if len(data) != len(shardList) {
		log.Error("Binlog adapter: int8 field row count is not equal to primary key", zap.Int("dataLen", len(data)), zap.Int("shardLen", len(shardList)))
		return errors.New("int8 field row count is not equal to primary key")
	}

	// dispatch entity acoording to shard list
	for i, val := range data {
		shardID := shardList[i]
		if shardID < 0 {
			continue // this entity has been deleted or excluded by timestamp
		}

		fields := memoryData[shardID] // initSegmentData() can ensure the existence, no need to check bound here
		field := fields[fieldID]      // initSegmentData() can ensure the existence, no need to check existence here
		field.(*storage.Int8FieldData).Data = append(field.(*storage.Int8FieldData).Data, val)
		field.(*storage.Int8FieldData).NumRows[0]++
	}

	return nil
}

func (p *BinlogAdapter) dispatchInt16ToShards(data []int16, memoryData []map[storage.FieldID]storage.FieldData,
	shardList []int32, fieldID storage.FieldID) error {
	// verify row count
	if len(data) != len(shardList) {
		log.Error("Binlog adapter: int16 field row count is not equal to primary key", zap.Int("dataLen", len(data)), zap.Int("shardLen", len(shardList)))
		return errors.New("int16 field row count is not equal to primary key")
	}

	// dispatch entities acoording to shard list
	for i, val := range data {
		shardID := shardList[i]
		if shardID < 0 {
			continue // this entity has been deleted or excluded by timestamp
		}

		fields := memoryData[shardID] // initSegmentData() can ensure the existence, no need to check bound here
		field := fields[fieldID]      // initSegmentData() can ensure the existence, no need to check existence here
		field.(*storage.Int16FieldData).Data = append(field.(*storage.Int16FieldData).Data, val)
		field.(*storage.Int16FieldData).NumRows[0]++
	}

	return nil
}

func (p *BinlogAdapter) dispatchInt32ToShards(data []int32, memoryData []map[storage.FieldID]storage.FieldData,
	shardList []int32, fieldID storage.FieldID) error {
	// verify row count
	if len(data) != len(shardList) {
		log.Error("Binlog adapter: int32 field row count is not equal to primary key", zap.Int("dataLen", len(data)), zap.Int("shardLen", len(shardList)))
		return errors.New("int32 field row count is not equal to primary key")
	}

	// dispatch entities acoording to shard list
	for i, val := range data {
		shardID := shardList[i]
		if shardID < 0 {
			continue // this entity has been deleted or excluded by timestamp
		}

		fields := memoryData[shardID] // initSegmentData() can ensure the existence, no need to check bound here
		field := fields[fieldID]      // initSegmentData() can ensure the existence, no need to check existence here
		field.(*storage.Int32FieldData).Data = append(field.(*storage.Int32FieldData).Data, val)
		field.(*storage.Int32FieldData).NumRows[0]++
	}

	return nil
}

func (p *BinlogAdapter) dispatchInt64ToShards(data []int64, memoryData []map[storage.FieldID]storage.FieldData,
	shardList []int32, fieldID storage.FieldID) error {
	// verify row count
	if len(data) != len(shardList) {
		log.Error("Binlog adapter: int64 field row count is not equal to primary key", zap.Int("dataLen", len(data)), zap.Int("shardLen", len(shardList)))
		return errors.New("int64 field row count is not equal to primary key")
	}

	// dispatch entities acoording to shard list
	for i, val := range data {
		shardID := shardList[i]
		if shardID < 0 {
			continue // this entity has been deleted or excluded by timestamp
		}

		fields := memoryData[shardID] // initSegmentData() can ensure the existence, no need to check bound here
		field := fields[fieldID]      // initSegmentData() can ensure the existence, no need to check existence here
		field.(*storage.Int64FieldData).Data = append(field.(*storage.Int64FieldData).Data, val)
		field.(*storage.Int64FieldData).NumRows[0]++
	}

	return nil
}

func (p *BinlogAdapter) dispatchFloatToShards(data []float32, memoryData []map[storage.FieldID]storage.FieldData,
	shardList []int32, fieldID storage.FieldID) error {
	// verify row count
	if len(data) != len(shardList) {
		log.Error("Binlog adapter: float field row count is not equal to primary key", zap.Int("dataLen", len(data)), zap.Int("shardLen", len(shardList)))
		return errors.New("float field row count is not equal to primary key")
	}

	// dispatch entities acoording to shard list
	for i, val := range data {
		shardID := shardList[i]
		if shardID < 0 {
			continue // this entity has been deleted or excluded by timestamp
		}

		fields := memoryData[shardID] // initSegmentData() can ensure the existence, no need to check bound here
		field := fields[fieldID]      // initSegmentData() can ensure the existence, no need to check existence here
		field.(*storage.FloatFieldData).Data = append(field.(*storage.FloatFieldData).Data, val)
		field.(*storage.FloatFieldData).NumRows[0]++
	}

	return nil
}

func (p *BinlogAdapter) dispatchDoubleToShards(data []float64, memoryData []map[storage.FieldID]storage.FieldData,
	shardList []int32, fieldID storage.FieldID) error {
	// verify row count
	if len(data) != len(shardList) {
		log.Error("Binlog adapter: double field row count is not equal to primary key", zap.Int("dataLen", len(data)), zap.Int("shardLen", len(shardList)))
		return errors.New("double field row count is not equal to primary key")
	}

	// dispatch entities acoording to shard list
	for i, val := range data {
		shardID := shardList[i]
		if shardID < 0 {
			continue // this entity has been deleted or excluded by timestamp
		}

		fields := memoryData[shardID] // initSegmentData() can ensure the existence, no need to check bound here
		field := fields[fieldID]      // initSegmentData() can ensure the existence, no need to check existence here
		field.(*storage.DoubleFieldData).Data = append(field.(*storage.DoubleFieldData).Data, val)
		field.(*storage.DoubleFieldData).NumRows[0]++
	}

	return nil
}

func (p *BinlogAdapter) dispatchVarcharToShards(data []string, memoryData []map[storage.FieldID]storage.FieldData,
	shardList []int32, fieldID storage.FieldID) error {
	// verify row count
	if len(data) != len(shardList) {
		log.Error("Binlog adapter: varchar field row count is not equal to primary key", zap.Int("dataLen", len(data)), zap.Int("shardLen", len(shardList)))
		return errors.New("varchar field row count is not equal to primary key")
	}

	// dispatch entities acoording to shard list
	for i, val := range data {
		shardID := shardList[i]
		if shardID < 0 {
			continue // this entity has been deleted or excluded by timestamp
		}

		fields := memoryData[shardID] // initSegmentData() can ensure the existence, no need to check bound here
		field := fields[fieldID]      // initSegmentData() can ensure the existence, no need to check existence here
		field.(*storage.StringFieldData).Data = append(field.(*storage.StringFieldData).Data, val)
		field.(*storage.StringFieldData).NumRows[0]++
	}

	return nil
}

func (p *BinlogAdapter) dispatchBinaryVecToShards(data []byte, dim int, memoryData []map[storage.FieldID]storage.FieldData,
	shardList []int32, fieldID storage.FieldID) error {
	// verify row count
	bytesPerVector := dim / 8
	count := len(data) / bytesPerVector
	if count != len(shardList) {
		log.Error("Binlog adapter: binary vector field row count is not equal to primary key", zap.Int("dataLen", count), zap.Int("shardLen", len(shardList)))
		return errors.New("binary vector field row count is not equal to primary key")
	}

	// dispatch entities acoording to shard list
	for i := 0; i < count; i++ {
		shardID := shardList[i]
		if shardID < 0 {
			continue // this entity has been deleted or excluded by timestamp
		}

		fields := memoryData[shardID] // initSegmentData() can ensure the existence, no need to check bound here
		field := fields[fieldID]      // initSegmentData() can ensure the existence, no need to check existence here
		binVecField := field.(*storage.BinaryVectorFieldData)
		if binVecField == nil {
			log.Error("Binlog adapter: the in-memory field is not a binary vector field")
			return errors.New("the in-memory field is not a binary vector field")
		}
		if binVecField.Dim != dim {
			log.Error("Binlog adapter: binary vector dimension mismatch", zap.Int("sourceDim", dim), zap.Int("schemaDim", binVecField.Dim))
			return errors.New("binary vector dimension mismatch")
		}
		for j := 0; j < bytesPerVector; j++ {
			val := data[bytesPerVector*i+j]

			binVecField.Data = append(binVecField.Data, val)
		}
		binVecField.NumRows[0]++
	}

	return nil
}

func (p *BinlogAdapter) dispatchFloatVecToShards(data []float32, dim int, memoryData []map[storage.FieldID]storage.FieldData,
	shardList []int32, fieldID storage.FieldID) error {
	// verify row count
	count := len(data) / dim
	if count != len(shardList) {
		log.Error("Binlog adapter: float vector field row count is not equal to primary key", zap.Int("dataLen", count), zap.Int("shardLen", len(shardList)))
		return errors.New("float vector field row count is not equal to primary key")
	}

	// dispatch entities acoording to shard list
	for i := 0; i < count; i++ {
		shardID := shardList[i]
		if shardID < 0 {
			continue // this entity has been deleted or excluded by timestamp
		}

		fields := memoryData[shardID] // initSegmentData() can ensure the existence, no need to check bound here
		field := fields[fieldID]      // initSegmentData() can ensure the existence, no need to check existence here
		floatVecField := field.(*storage.FloatVectorFieldData)
		if floatVecField == nil {
			log.Error("Binlog adapter: the in-memory field is not a float vector field")
			return errors.New("the in-memory field is not a float vector field")
		}
		if floatVecField.Dim != dim {
			log.Error("Binlog adapter: float vector dimension mismatch", zap.Int("sourceDim", dim), zap.Int("schemaDim", floatVecField.Dim))
			return errors.New("float vector dimension mismatch")
		}
		for j := 0; j < dim; j++ {
			val := data[dim*i+j]
			floatVecField.Data = append(floatVecField.Data, val)
		}
		floatVecField.NumRows[0]++
	}

	return nil
}

// This method do the two things:
// 1. if accumulate data of a segment exceed segmentSize, call callFlushFunc to generate new segment
// 2. if total accumulate data exceed maxTotalSize, call callFlushFUnc to flush the biggest segment
func (p *BinlogAdapter) tryFlushSegments(segmentsData []map[storage.FieldID]storage.FieldData, force bool) error {
	totalSize := 0
	biggestSize := 0
	biggestItem := -1

	// 1. if accumulate data of a segment exceed segmentSize, call callFlushFunc to generate new segment
	for i := 0; i < len(segmentsData); i++ {
		segmentData := segmentsData[i]
		// Note: even rowCount is 0, the size is still non-zero
		size := 0
		rowCount := 0
		for _, fieldData := range segmentData {
			size += fieldData.GetMemorySize()
			rowCount = fieldData.RowNum()
		}

		// force to flush, called at the end of Read()
		if force && rowCount > 0 {
			err := p.callFlushFunc(segmentData, i)
			if err != nil {
				log.Error("Binlog adapter: failed to force flush segment data", zap.Int("shardID", i))
				return err
			}
			log.Info("Binlog adapter: force flush", zap.Int("rowCount", rowCount), zap.Int("size", size), zap.Int("shardID", i))

			segmentsData[i] = initSegmentData(p.collectionSchema)
			if segmentsData[i] == nil {
				log.Error("Binlog adapter: failed to initialize FieldData list")
				return errors.New("failed to initialize FieldData list")
			}
			continue
		}

		// if segment size is larger than predefined segmentSize, flush to create a new segment
		// initialize a new FieldData list for next round batch read
		if size > int(p.segmentSize) && rowCount > 0 {
			err := p.callFlushFunc(segmentData, i)
			if err != nil {
				log.Error("Binlog adapter: failed to flush segment data", zap.Int("shardID", i))
				return err
			}
			log.Info("Binlog adapter: segment size exceed limit and flush", zap.Int("rowCount", rowCount), zap.Int("size", size), zap.Int("shardID", i))

			segmentsData[i] = initSegmentData(p.collectionSchema)
			if segmentsData[i] == nil {
				log.Error("Binlog adapter: failed to initialize FieldData list")
				return errors.New("failed to initialize FieldData list")
			}
			continue
		}

		// calculate the total size(ignore the flushed segments)
		// find out the biggest segment for the step 2
		totalSize += size
		if size > biggestSize {
			biggestSize = size
			biggestItem = i
		}
	}

	// 2. if total accumulate data exceed maxTotalSize, call callFlushFUnc to flush the biggest segment
	if totalSize > int(p.maxTotalSize) && biggestItem >= 0 {
		segmentData := segmentsData[biggestItem]
		size := 0
		rowCount := 0
		for _, fieldData := range segmentData {
			size += fieldData.GetMemorySize()
			rowCount = fieldData.RowNum()
		}

		if rowCount > 0 {
			err := p.callFlushFunc(segmentData, biggestItem)
			if err != nil {
				log.Error("Binlog adapter: failed to flush biggest segment data", zap.Int("shardID", biggestItem))
				return err
			}
			log.Info("Binlog adapter: total size exceed limit and flush", zap.Int("rowCount", rowCount),
				zap.Int("size", size), zap.Int("totalSize", totalSize), zap.Int("shardID", biggestItem))

			segmentsData[biggestItem] = initSegmentData(p.collectionSchema)
			if segmentsData[biggestItem] == nil {
				log.Error("Binlog adapter: failed to initialize FieldData list")
				return errors.New("failed to initialize FieldData list")
			}
		}
	}

	return nil
}
