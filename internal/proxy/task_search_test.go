package proxy

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/types"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/api/milvuspb"
	"github.com/milvus-io/milvus/api/schemapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"

	"github.com/milvus-io/milvus/internal/util/distance"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	testShardsNum = int32(2)
)

func TestSearchTask_PostExecute(t *testing.T) {
	t.Run("Test empty result", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		qt := &searchTask{
			ctx:       ctx,
			Condition: NewTaskCondition(context.TODO()),
			SearchRequest: &internalpb.SearchRequest{
				Base: &commonpb.MsgBase{
					MsgType:  commonpb.MsgType_Search,
					SourceID: Params.ProxyCfg.GetNodeID(),
				},
			},
			request: nil,
			qc:      nil,
			tr:      timerecord.NewTimeRecorder("search"),

			resultBuf:       make(chan *internalpb.SearchResults, 10),
			toReduceResults: make([]*internalpb.SearchResults, 0),
		}
		// no result
		qt.resultBuf <- &internalpb.SearchResults{}

		err := qt.PostExecute(context.TODO())
		assert.NoError(t, err)
		assert.Equal(t, qt.result.Status.ErrorCode, commonpb.ErrorCode_Success)
	})
}

func createColl(t *testing.T, name string, rc types.RootCoord) {
	schema := constructCollectionSchema(testInt64Field, testFloatVecField, testVecDim, name)
	marshaledSchema, err := proto.Marshal(schema)
	require.NoError(t, err)
	ctx := context.TODO()

	createColT := &createCollectionTask{
		Condition: NewTaskCondition(context.TODO()),
		CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
			CollectionName: name,
			Schema:         marshaledSchema,
			ShardsNum:      testShardsNum,
		},
		ctx:       ctx,
		rootCoord: rc,
	}

	require.NoError(t, createColT.OnEnqueue())
	require.NoError(t, createColT.PreExecute(ctx))
	require.NoError(t, createColT.Execute(ctx))
	require.NoError(t, createColT.PostExecute(ctx))
}

func getValidSearchParams() []*commonpb.KeyValuePair {
	return []*commonpb.KeyValuePair{
		{
			Key:   AnnsFieldKey,
			Value: testFloatVecField,
		},
		{
			Key:   TopKKey,
			Value: "10",
		},
		{
			Key:   MetricTypeKey,
			Value: distance.L2,
		},
		{
			Key:   SearchParamsKey,
			Value: `{"nprobe": 10}`,
		},
		{
			Key:   RoundDecimalKey,
			Value: "-1",
		}}
}

func TestSearchTask_PreExecute(t *testing.T) {
	var err error

	Params.InitOnce()
	var (
		rc  = NewRootCoordMock()
		qc  = NewQueryCoordMock()
		ctx = context.TODO()

		collectionName = t.Name() + funcutil.GenRandomStr()
	)

	err = rc.Start()
	defer rc.Stop()
	require.NoError(t, err)
	mgr := newShardClientMgr()
	err = InitMetaCache(ctx, rc, qc, mgr)
	require.NoError(t, err)

	err = qc.Start()
	defer qc.Stop()
	require.NoError(t, err)

	getSearchTask := func(t *testing.T, collName string) *searchTask {
		task := &searchTask{
			ctx:           ctx,
			SearchRequest: &internalpb.SearchRequest{},
			request: &milvuspb.SearchRequest{
				CollectionName: collName,
			},
			qc: qc,
			tr: timerecord.NewTimeRecorder("test-search"),
		}
		require.NoError(t, task.OnEnqueue())
		return task
	}

	t.Run("collection not exist", func(t *testing.T) {
		task := getSearchTask(t, collectionName)
		err = task.PreExecute(ctx)
		assert.Error(t, err)
	})

	t.Run("invalid collection name", func(t *testing.T) {
		task := getSearchTask(t, collectionName)
		createColl(t, collectionName, rc)

		invalidCollNameTests := []struct {
			inCollName  string
			description string
		}{
			{"$", "invalid collection name $"},
			{"0", "invalid collection name 0"},
		}

		for _, test := range invalidCollNameTests {
			t.Run(test.description, func(t *testing.T) {
				task.request.CollectionName = test.inCollName
				assert.Error(t, task.PreExecute(context.TODO()))
			})
		}
	})

	t.Run("invalid partition names", func(t *testing.T) {
		task := getSearchTask(t, collectionName)
		createColl(t, collectionName, rc)

		invalidCollNameTests := []struct {
			inPartNames []string
			description string
		}{
			{[]string{"$"}, "invalid partition name $"},
			{[]string{"0"}, "invalid collection name 0"},
			{[]string{"default", "$"}, "invalid empty partition name"},
		}

		for _, test := range invalidCollNameTests {
			t.Run(test.description, func(t *testing.T) {
				task.request.PartitionNames = test.inPartNames
				assert.Error(t, task.PreExecute(context.TODO()))
			})
		}
	})

	t.Run("test checkIfLoaded error", func(t *testing.T) {
		collName := "test_checkIfLoaded_error" + funcutil.GenRandomStr()
		createColl(t, collName, rc)
		_, err := globalMetaCache.GetCollectionID(context.TODO(), collName)
		require.NoError(t, err)
		task := getSearchTask(t, collName)
		task.collectionName = collName

		t.Run("show collection status unexpected error", func(t *testing.T) {
			qc.SetShowCollectionsFunc(func(ctx context.Context, request *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
				return &querypb.ShowCollectionsResponse{
					Status: &commonpb.Status{
						ErrorCode: commonpb.ErrorCode_UnexpectedError,
						Reason:    "mock",
					},
				}, nil
			})

			assert.Error(t, task.PreExecute(ctx))
			qc.ResetShowCollectionsFunc()
		})

		qc.ResetShowCollectionsFunc()
		qc.ResetShowPartitionsFunc()
	})

	t.Run("search with timeout", func(t *testing.T) {
		collName := "search_with_timeout" + funcutil.GenRandomStr()
		createColl(t, collName, rc)
		collID, err := globalMetaCache.GetCollectionID(context.TODO(), collName)
		require.NoError(t, err)
		status, err := qc.LoadCollection(ctx, &querypb.LoadCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_LoadCollection,
			},
			CollectionID: collID,
		})
		require.NoError(t, err)
		require.Equal(t, commonpb.ErrorCode_Success, status.GetErrorCode())

		task := getSearchTask(t, collName)
		task.request.SearchParams = getValidSearchParams()
		task.request.DslType = commonpb.DslType_BoolExprV1

		ctxTimeout, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		require.Equal(t, typeutil.ZeroTimestamp, task.TimeoutTimestamp)

		task.ctx = ctxTimeout
		assert.NoError(t, task.PreExecute(ctx))
		assert.Greater(t, task.TimeoutTimestamp, typeutil.ZeroTimestamp)

		// field not exist
		task.ctx = context.TODO()
		task.request.OutputFields = []string{testInt64Field + funcutil.GenRandomStr()}
		assert.Error(t, task.PreExecute(ctx))

		// contain vector field
		task.request.OutputFields = []string{testFloatVecField}
		assert.Error(t, task.PreExecute(ctx))
	})
}

func TestSearchTaskV2_Execute(t *testing.T) {
	Params.InitOnce()

	var (
		err error

		rc  = NewRootCoordMock()
		qc  = NewQueryCoordMock()
		ctx = context.TODO()

		collectionName = t.Name() + funcutil.GenRandomStr()
	)

	err = rc.Start()
	require.NoError(t, err)
	defer rc.Stop()
	mgr := newShardClientMgr()
	err = InitMetaCache(ctx, rc, qc, mgr)
	require.NoError(t, err)

	err = qc.Start()
	require.NoError(t, err)
	defer qc.Stop()

	task := &searchTask{
		ctx: ctx,
		SearchRequest: &internalpb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Search,
				Timestamp: uint64(time.Now().UnixNano()),
			},
		},
		request: &milvuspb.SearchRequest{
			CollectionName: collectionName,
		},
		result: &milvuspb.SearchResults{
			Status: &commonpb.Status{},
		},
		qc: qc,
		tr: timerecord.NewTimeRecorder("search"),
	}
	require.NoError(t, task.OnEnqueue())
	createColl(t, collectionName, rc)
}

func genSearchResultData(nq int64, topk int64, ids []int64, scores []float32) *schemapb.SearchResultData {
	return &schemapb.SearchResultData{
		NumQueries: nq,
		TopK:       topk,
		FieldsData: nil,
		Scores:     scores,
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: ids,
				},
			},
		},
		Topks: make([]int64, nq),
	}
}

func TestSearchTask_Ts(t *testing.T) {
	Params.InitOnce()
	task := &searchTask{
		SearchRequest: &internalpb.SearchRequest{},

		tr: timerecord.NewTimeRecorder("test-search"),
	}
	require.NoError(t, task.OnEnqueue())

	ts := Timestamp(time.Now().Nanosecond())
	task.SetTs(ts)
	assert.Equal(t, ts, task.BeginTs())
	assert.Equal(t, ts, task.EndTs())
}

func TestSearchTask_Reduce(t *testing.T) {
	// const (
	//     nq         = 1
	//     topk       = 4
	//     metricType = "L2"
	// )
	// t.Run("case1", func(t *testing.T) {
	//     ids := []int64{1, 2, 3, 4}
	//     scores := []float32{-1.0, -2.0, -3.0, -4.0}
	//     data1 := genSearchResultData(nq, topk, ids, scores)
	//     data2 := genSearchResultData(nq, topk, ids, scores)
	//     dataArray := make([]*schemapb.SearchResultData, 0)
	//     dataArray = append(dataArray, data1)
	//     dataArray = append(dataArray, data2)
	//     res, err := reduceSearchResultData(dataArray, nq, topk, metricType)
	//     assert.Nil(t, err)
	//     assert.Equal(t, ids, res.Results.Ids.GetIntId().Data)
	//     assert.Equal(t, []float32{1.0, 2.0, 3.0, 4.0}, res.Results.Scores)
	// })
	// t.Run("case2", func(t *testing.T) {
	//     ids1 := []int64{1, 2, 3, 4}
	//     scores1 := []float32{-1.0, -2.0, -3.0, -4.0}
	//     ids2 := []int64{5, 1, 3, 4}
	//     scores2 := []float32{-1.0, -1.0, -3.0, -4.0}
	//     data1 := genSearchResultData(nq, topk, ids1, scores1)
	//     data2 := genSearchResultData(nq, topk, ids2, scores2)
	//     dataArray := make([]*schemapb.SearchResultData, 0)
	//     dataArray = append(dataArray, data1)
	//     dataArray = append(dataArray, data2)
	//     res, err := reduceSearchResultData(dataArray, nq, topk, metricType)
	//     assert.Nil(t, err)
	//     assert.ElementsMatch(t, []int64{1, 5, 2, 3}, res.Results.Ids.GetIntId().Data)
	// })
}

func TestSearchTaskWithInvalidRoundDecimal(t *testing.T) {
	// var err error
	//
	// Params.Init()
	// Params.ProxyCfg.SearchResultChannelNames = []string{funcutil.GenRandomStr()}
	//
	// rc := NewRootCoordMock()
	// rc.Start()
	// defer rc.Stop()
	//
	// ctx := context.Background()
	//
	// err = InitMetaCache(ctx, rc)
	// assert.NoError(t, err)
	//
	// shardsNum := int32(2)
	// prefix := "TestSearchTaskV2_all"
	// collectionName := prefix + funcutil.GenRandomStr()
	//
	// dim := 128
	// expr := fmt.Sprintf("%s > 0", testInt64Field)
	// nq := 10
	// topk := 10
	// roundDecimal := 7
	// nprobe := 10
	//
	// fieldName2Types := map[string]schemapb.DataType{
	//     testBoolField:     schemapb.DataType_Bool,
	//     testInt32Field:    schemapb.DataType_Int32,
	//     testInt64Field:    schemapb.DataType_Int64,
	//     testFloatField:    schemapb.DataType_Float,
	//     testDoubleField:   schemapb.DataType_Double,
	//     testFloatVecField: schemapb.DataType_FloatVector,
	// }
	// if enableMultipleVectorFields {
	//     fieldName2Types[testBinaryVecField] = schemapb.DataType_BinaryVector
	// }
	// schema := constructCollectionSchemaByDataType(collectionName, fieldName2Types, testInt64Field, false)
	// marshaledSchema, err := proto.Marshal(schema)
	// assert.NoError(t, err)
	//
	// createColT := &createCollectionTask{
	//     Condition: NewTaskCondition(ctx),
	//     CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
	//         Base:           nil,
	//         CollectionName: collectionName,
	//         Schema:         marshaledSchema,
	//         ShardsNum:      shardsNum,
	//     },
	//     ctx:       ctx,
	//     rootCoord: rc,
	//     result:    nil,
	//     schema:    nil,
	// }
	//
	// assert.NoError(t, createColT.OnEnqueue())
	// assert.NoError(t, createColT.PreExecute(ctx))
	// assert.NoError(t, createColT.Execute(ctx))
	// assert.NoError(t, createColT.PostExecute(ctx))
	//
	// dmlChannelsFunc := getDmlChannelsFunc(ctx, rc)
	// query := newMockGetChannelsService()
	// factory := newSimpleMockMsgStreamFactory()
	//
	// collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	// assert.NoError(t, err)
	//
	// qc := NewQueryCoordMock()
	// qc.Start()
	// defer qc.Stop()
	// status, err := qc.LoadCollection(ctx, &querypb.LoadCollectionRequest{
	//     Base: &commonpb.MsgBase{
	//         MsgType:   commonpb.MsgType_LoadCollection,
	//         MsgID:     0,
	//         Timestamp: 0,
	//         SourceID:  Params.ProxyCfg.GetNodeID(),
	//     },
	//     DbID:         0,
	//     CollectionID: collectionID,
	//     Schema:       nil,
	// })
	// assert.NoError(t, err)
	// assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	//
	// req := constructSearchRequest("", collectionName,
	//     expr,
	//     testFloatVecField,
	//     nq, dim, nprobe, topk, roundDecimal)
	//
	// task := &searchTaskV2{
	//     Condition: NewTaskCondition(ctx),
	//     SearchRequest: &internalpb.SearchRequest{
	//         Base: &commonpb.MsgBase{
	//             MsgType:   commonpb.MsgType_Search,
	//             MsgID:     0,
	//             Timestamp: 0,
	//             SourceID:  Params.ProxyCfg.GetNodeID(),
	//         },
	//         ResultChannelID:    strconv.FormatInt(Params.ProxyCfg.GetNodeID(), 10),
	//         DbID:               0,
	//         CollectionID:       0,
	//         PartitionIDs:       nil,
	//         Dsl:                "",
	//         PlaceholderGroup:   nil,
	//         DslType:            0,
	//         SerializedExprPlan: nil,
	//         OutputFieldsId:     nil,
	//         TravelTimestamp:    0,
	//         GuaranteeTimestamp: 0,
	//     },
	//     ctx:       ctx,
	//     resultBuf: make(chan *internalpb.SearchResults, 10),
	//     result:    nil,
	//     request:   req,
	//     qc:        qc,
	//     tr:        timerecord.NewTimeRecorder("search"),
	// }
	//
	// // simple mock for query node
	// // TODO(dragondriver): should we replace this mock using RocksMq or MemMsgStream?
	//
	//
	// var wg sync.WaitGroup
	// wg.Add(1)
	// consumeCtx, cancel := context.WithCancel(ctx)
	// go func() {
	//     defer wg.Done()
	//     for {
	//         select {
	//         case <-consumeCtx.Done():
	//             return
	//         case pack, ok := <-stream.Chan():
	//             assert.True(t, ok)
	//             if pack == nil {
	//                 continue
	//             }
	//
	//             for _, msg := range pack.Msgs {
	//                 _, ok := msg.(*msgstream.SearchMsg)
	//                 assert.True(t, ok)
	//                 // TODO(dragondriver): construct result according to the request
	//
	//                 constructSearchResulstData := func() *schemapb.SearchResultData {
	//                     resultData := &schemapb.SearchResultData{
	//                         NumQueries: int64(nq),
	//                         TopK:       int64(topk),
	//                         Scores:     make([]float32, nq*topk),
	//                         Ids: &schemapb.IDs{
	//                             IdField: &schemapb.IDs_IntId{
	//                                 IntId: &schemapb.LongArray{
	//                                     Data: make([]int64, nq*topk),
	//                                 },
	//                             },
	//                         },
	//                         Topks: make([]int64, nq),
	//                     }
	//
	//                     fieldID := common.StartOfUserFieldID
	//                     for fieldName, dataType := range fieldName2Types {
	//                         resultData.FieldsData = append(resultData.FieldsData, generateFieldData(dataType, fieldName, int64(fieldID), nq*topk))
	//                         fieldID++
	//                     }
	//
	//                     for i := 0; i < nq; i++ {
	//                         for j := 0; j < topk; j++ {
	//                             offset := i*topk + j
	//                             score := float32(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()) // increasingly
	//                             id := int64(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	//                             resultData.Scores[offset] = score
	//                             resultData.Ids.IdField.(*schemapb.IDs_IntId).IntId.Data[offset] = id
	//                         }
	//                         resultData.Topks[i] = int64(topk)
	//                     }
	//
	//                     return resultData
	//                 }
	//
	//                 result1 := &internalpb.SearchResults{
	//                     Base: &commonpb.MsgBase{
	//                         MsgType:   commonpb.MsgType_SearchResult,
	//                         MsgID:     0,
	//                         Timestamp: 0,
	//                         SourceID:  0,
	//                     },
	//                     Status: &commonpb.Status{
	//                         ErrorCode: commonpb.ErrorCode_Success,
	//                         Reason:    "",
	//                     },
	//                     ResultChannelID:          "",
	//                     MetricType:               distance.L2,
	//                     NumQueries:               int64(nq),
	//                     TopK:                     int64(topk),
	//                     SealedSegmentIDsSearched: nil,
	//                     ChannelIDsSearched:       nil,
	//                     GlobalSealedSegmentIDs:   nil,
	//                     SlicedBlob:               nil,
	//                     SlicedNumCount:           1,
	//                     SlicedOffset:             0,
	//                 }
	//                 resultData := constructSearchResulstData()
	//                 sliceBlob, err := proto.Marshal(resultData)
	//                 assert.NoError(t, err)
	//                 result1.SlicedBlob = sliceBlob
	//
	//                 // result2.SliceBlob = nil, will be skipped in decode stage
	//                 result2 := &internalpb.SearchResults{
	//                     Base: &commonpb.MsgBase{
	//                         MsgType:   commonpb.MsgType_SearchResult,
	//                         MsgID:     0,
	//                         Timestamp: 0,
	//                         SourceID:  0,
	//                     },
	//                     Status: &commonpb.Status{
	//                         ErrorCode: commonpb.ErrorCode_Success,
	//                         Reason:    "",
	//                     },
	//                     ResultChannelID:          "",
	//                     MetricType:               distance.L2,
	//                     NumQueries:               int64(nq),
	//                     TopK:                     int64(topk),
	//                     SealedSegmentIDsSearched: nil,
	//                     ChannelIDsSearched:       nil,
	//                     GlobalSealedSegmentIDs:   nil,
	//                     SlicedBlob:               nil,
	//                     SlicedNumCount:           1,
	//                     SlicedOffset:             0,
	//                 }
	//
	//                 // send search result
	//                 task.resultBuf <- result1
	//                 task.resultBuf <- result2
	//             }
	//         }
	//     }
	// }()
	//
	// assert.NoError(t, task.OnEnqueue())
	// assert.Error(t, task.PreExecute(ctx))
	//
	// cancel()
	// wg.Wait()
}

func TestSearchTaskV2_all(t *testing.T) {
	// var err error
	//
	// Params.Init()
	// Params.ProxyCfg.SearchResultChannelNames = []string{funcutil.GenRandomStr()}
	//
	// rc := NewRootCoordMock()
	// rc.Start()
	// defer rc.Stop()
	//
	// ctx := context.Background()
	//
	// err = InitMetaCache(ctx, rc)
	// assert.NoError(t, err)
	//
	// shardsNum := int32(2)
	// prefix := "TestSearchTaskV2_all"
	// collectionName := prefix + funcutil.GenRandomStr()
	//
	// dim := 128
	// expr := fmt.Sprintf("%s > 0", testInt64Field)
	// nq := 10
	// topk := 10
	// roundDecimal := 3
	// nprobe := 10
	//
	// fieldName2Types := map[string]schemapb.DataType{
	//     testBoolField:     schemapb.DataType_Bool,
	//     testInt32Field:    schemapb.DataType_Int32,
	//     testInt64Field:    schemapb.DataType_Int64,
	//     testFloatField:    schemapb.DataType_Float,
	//     testDoubleField:   schemapb.DataType_Double,
	//     testFloatVecField: schemapb.DataType_FloatVector,
	// }
	// if enableMultipleVectorFields {
	//     fieldName2Types[testBinaryVecField] = schemapb.DataType_BinaryVector
	// }
	//
	// schema := constructCollectionSchemaByDataType(collectionName, fieldName2Types, testInt64Field, false)
	// marshaledSchema, err := proto.Marshal(schema)
	// assert.NoError(t, err)
	//
	// createColT := &createCollectionTask{
	//     Condition: NewTaskCondition(ctx),
	//     CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
	//         Base:           nil,
	//         CollectionName: collectionName,
	//         Schema:         marshaledSchema,
	//         ShardsNum:      shardsNum,
	//     },
	//     ctx:       ctx,
	//     rootCoord: rc,
	//     result:    nil,
	//     schema:    nil,
	// }
	//
	// assert.NoError(t, createColT.OnEnqueue())
	// assert.NoError(t, createColT.PreExecute(ctx))
	// assert.NoError(t, createColT.Execute(ctx))
	// assert.NoError(t, createColT.PostExecute(ctx))
	//
	// dmlChannelsFunc := getDmlChannelsFunc(ctx, rc)
	// query := newMockGetChannelsService()
	// factory := newSimpleMockMsgStreamFactory()
	//
	// collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	// assert.NoError(t, err)
	//
	// qc := NewQueryCoordMock()
	// qc.Start()
	// defer qc.Stop()
	// status, err := qc.LoadCollection(ctx, &querypb.LoadCollectionRequest{
	//     Base: &commonpb.MsgBase{
	//         MsgType:   commonpb.MsgType_LoadCollection,
	//         MsgID:     0,
	//         Timestamp: 0,
	//         SourceID:  Params.ProxyCfg.GetNodeID(),
	//     },
	//     DbID:         0,
	//     CollectionID: collectionID,
	//     Schema:       nil,
	// })
	// assert.NoError(t, err)
	// assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	//
	// req := constructSearchRequest("", collectionName,
	//     expr,
	//     testFloatVecField,
	//     nq, dim, nprobe, topk, roundDecimal)
	//
	// task := &searchTaskV2{
	//     Condition: NewTaskCondition(ctx),
	//     SearchRequest: &internalpb.SearchRequest{
	//         Base: &commonpb.MsgBase{
	//             MsgType:   commonpb.MsgType_Search,
	//             MsgID:     0,
	//             Timestamp: 0,
	//             SourceID:  Params.ProxyCfg.GetNodeID(),
	//         },
	//         ResultChannelID:    strconv.FormatInt(Params.ProxyCfg.GetNodeID(), 10),
	//         DbID:               0,
	//         CollectionID:       0,
	//         PartitionIDs:       nil,
	//         Dsl:                "",
	//         PlaceholderGroup:   nil,
	//         DslType:            0,
	//         SerializedExprPlan: nil,
	//         OutputFieldsId:     nil,
	//         TravelTimestamp:    0,
	//         GuaranteeTimestamp: 0,
	//     },
	//     ctx:       ctx,
	//     resultBuf: make(chan *internalpb.SearchResults, 10),
	//     result:    nil,
	//     request:   req,
	//     qc:        qc,
	//     tr:        timerecord.NewTimeRecorder("search"),
	// }
	//
	// // simple mock for query node
	// // TODO(dragondriver): should we replace this mock using RocksMq or MemMsgStream?
	//
	// var wg sync.WaitGroup
	// wg.Add(1)
	// consumeCtx, cancel := context.WithCancel(ctx)
	// go func() {
	//     defer wg.Done()
	//     for {
	//         select {
	//         case <-consumeCtx.Done():
	//             return
	//         case pack, ok := <-stream.Chan():
	//             assert.True(t, ok)
	//             if pack == nil {
	//                 continue
	//             }
	//
	//             for _, msg := range pack.Msgs {
	//                 _, ok := msg.(*msgstream.SearchMsg)
	//                 assert.True(t, ok)
	//                 // TODO(dragondriver): construct result according to the request
	//
	//                 constructSearchResulstData := func() *schemapb.SearchResultData {
	//                     resultData := &schemapb.SearchResultData{
	//                         NumQueries: int64(nq),
	//                         TopK:       int64(topk),
	//                         Scores:     make([]float32, nq*topk),
	//                         Ids: &schemapb.IDs{
	//                             IdField: &schemapb.IDs_IntId{
	//                                 IntId: &schemapb.LongArray{
	//                                     Data: make([]int64, nq*topk),
	//                                 },
	//                             },
	//                         },
	//                         Topks: make([]int64, nq),
	//                     }
	//
	//                     fieldID := common.StartOfUserFieldID
	//                     for fieldName, dataType := range fieldName2Types {
	//                         resultData.FieldsData = append(resultData.FieldsData, generateFieldData(dataType, fieldName, int64(fieldID), nq*topk))
	//                         fieldID++
	//                     }
	//
	//                     for i := 0; i < nq; i++ {
	//                         for j := 0; j < topk; j++ {
	//                             offset := i*topk + j
	//                             score := float32(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()) // increasingly
	//                             id := int64(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	//                             resultData.Scores[offset] = score
	//                             resultData.Ids.IdField.(*schemapb.IDs_IntId).IntId.Data[offset] = id
	//                         }
	//                         resultData.Topks[i] = int64(topk)
	//                     }
	//
	//                     return resultData
	//                 }
	//
	//                 result1 := &internalpb.SearchResults{
	//                     Base: &commonpb.MsgBase{
	//                         MsgType:   commonpb.MsgType_SearchResult,
	//                         MsgID:     0,
	//                         Timestamp: 0,
	//                         SourceID:  0,
	//                     },
	//                     Status: &commonpb.Status{
	//                         ErrorCode: commonpb.ErrorCode_Success,
	//                         Reason:    "",
	//                     },
	//                     ResultChannelID:          "",
	//                     MetricType:               distance.L2,
	//                     NumQueries:               int64(nq),
	//                     TopK:                     int64(topk),
	//                     SealedSegmentIDsSearched: nil,
	//                     ChannelIDsSearched:       nil,
	//                     GlobalSealedSegmentIDs:   nil,
	//                     SlicedBlob:               nil,
	//                     SlicedNumCount:           1,
	//                     SlicedOffset:             0,
	//                 }
	//                 resultData := constructSearchResulstData()
	//                 sliceBlob, err := proto.Marshal(resultData)
	//                 assert.NoError(t, err)
	//                 result1.SlicedBlob = sliceBlob
	//
	//                 // result2.SliceBlob = nil, will be skipped in decode stage
	//                 result2 := &internalpb.SearchResults{
	//                     Base: &commonpb.MsgBase{
	//                         MsgType:   commonpb.MsgType_SearchResult,
	//                         MsgID:     0,
	//                         Timestamp: 0,
	//                         SourceID:  0,
	//                     },
	//                     Status: &commonpb.Status{
	//                         ErrorCode: commonpb.ErrorCode_Success,
	//                         Reason:    "",
	//                     },
	//                     ResultChannelID:          "",
	//                     MetricType:               distance.L2,
	//                     NumQueries:               int64(nq),
	//                     TopK:                     int64(topk),
	//                     SealedSegmentIDsSearched: nil,
	//                     ChannelIDsSearched:       nil,
	//                     GlobalSealedSegmentIDs:   nil,
	//                     SlicedBlob:               nil,
	//                     SlicedNumCount:           1,
	//                     SlicedOffset:             0,
	//                 }
	//
	//                 // send search result
	//                 task.resultBuf <- result1
	//                 task.resultBuf <- result2
	//             }
	//         }
	//     }
	// }()
	//
	// assert.NoError(t, task.OnEnqueue())
	// assert.NoError(t, task.PreExecute(ctx))
	// assert.NoError(t, task.Execute(ctx))
	// assert.NoError(t, task.PostExecute(ctx))
	//
	// cancel()
	// wg.Wait()
}

func TestSearchTaskV2_7803_reduce(t *testing.T) {
	// var err error
	//
	// Params.Init()
	// Params.ProxyCfg.SearchResultChannelNames = []string{funcutil.GenRandomStr()}
	//
	// rc := NewRootCoordMock()
	// rc.Start()
	// defer rc.Stop()
	//
	// ctx := context.Background()
	//
	// err = InitMetaCache(ctx, rc)
	// assert.NoError(t, err)
	//
	// shardsNum := int32(2)
	// prefix := "TestSearchTaskV2_7803_reduce"
	// collectionName := prefix + funcutil.GenRandomStr()
	// int64Field := "int64"
	// floatVecField := "fvec"
	// dim := 128
	// expr := fmt.Sprintf("%s > 0", int64Field)
	// nq := 10
	// topk := 10
	// roundDecimal := 3
	// nprobe := 10
	//
	// schema := constructCollectionSchema(
	//     int64Field,
	//     floatVecField,
	//     dim,
	//     collectionName)
	// marshaledSchema, err := proto.Marshal(schema)
	// assert.NoError(t, err)
	//
	// createColT := &createCollectionTask{
	//     Condition: NewTaskCondition(ctx),
	//     CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
	//         Base:           nil,
	//         CollectionName: collectionName,
	//         Schema:         marshaledSchema,
	//         ShardsNum:      shardsNum,
	//     },
	//     ctx:       ctx,
	//     rootCoord: rc,
	//     result:    nil,
	//     schema:    nil,
	// }
	//
	// assert.NoError(t, createColT.OnEnqueue())
	// assert.NoError(t, createColT.PreExecute(ctx))
	// assert.NoError(t, createColT.Execute(ctx))
	// assert.NoError(t, createColT.PostExecute(ctx))
	//
	// dmlChannelsFunc := getDmlChannelsFunc(ctx, rc)
	// query := newMockGetChannelsService()
	// factory := newSimpleMockMsgStreamFactory()
	//
	// collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	// assert.NoError(t, err)
	//
	// qc := NewQueryCoordMock()
	// qc.Start()
	// defer qc.Stop()
	// status, err := qc.LoadCollection(ctx, &querypb.LoadCollectionRequest{
	//     Base: &commonpb.MsgBase{
	//         MsgType:   commonpb.MsgType_LoadCollection,
	//         MsgID:     0,
	//         Timestamp: 0,
	//         SourceID:  Params.ProxyCfg.GetNodeID(),
	//     },
	//     DbID:         0,
	//     CollectionID: collectionID,
	//     Schema:       nil,
	// })
	// assert.NoError(t, err)
	// assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	//
	// req := constructSearchRequest("", collectionName,
	//     expr,
	//     floatVecField,
	//     nq, dim, nprobe, topk, roundDecimal)
	//
	// task := &searchTaskV2{
	//     Condition: NewTaskCondition(ctx),
	//     SearchRequest: &internalpb.SearchRequest{
	//         Base: &commonpb.MsgBase{
	//             MsgType:   commonpb.MsgType_Search,
	//             MsgID:     0,
	//             Timestamp: 0,
	//             SourceID:  Params.ProxyCfg.GetNodeID(),
	//         },
	//         ResultChannelID:    strconv.FormatInt(Params.ProxyCfg.GetNodeID(), 10),
	//         DbID:               0,
	//         CollectionID:       0,
	//         PartitionIDs:       nil,
	//         Dsl:                "",
	//         PlaceholderGroup:   nil,
	//         DslType:            0,
	//         SerializedExprPlan: nil,
	//         OutputFieldsId:     nil,
	//         TravelTimestamp:    0,
	//         GuaranteeTimestamp: 0,
	//     },
	//     ctx:       ctx,
	//     resultBuf: make(chan *internalpb.SearchResults, 10),
	//     result:    nil,
	//     request:   req,
	//     qc:        qc,
	//     tr:        timerecord.NewTimeRecorder("search"),
	// }
	//
	// // simple mock for query node
	// // TODO(dragondriver): should we replace this mock using RocksMq or MemMsgStream?
	//
	// var wg sync.WaitGroup
	// wg.Add(1)
	// consumeCtx, cancel := context.WithCancel(ctx)
	// go func() {
	//     defer wg.Done()
	//     for {
	//         select {
	//         case <-consumeCtx.Done():
	//             return
	//         case pack, ok := <-stream.Chan():
	//             assert.True(t, ok)
	//             if pack == nil {
	//                 continue
	//             }
	//
	//             for _, msg := range pack.Msgs {
	//                 _, ok := msg.(*msgstream.SearchMsg)
	//                 assert.True(t, ok)
	//                 // TODO(dragondriver): construct result according to the request
	//
	//                 constructSearchResulstData := func(invalidNum int) *schemapb.SearchResultData {
	//                     resultData := &schemapb.SearchResultData{
	//                         NumQueries: int64(nq),
	//                         TopK:       int64(topk),
	//                         FieldsData: nil,
	//                         Scores:     make([]float32, nq*topk),
	//                         Ids: &schemapb.IDs{
	//                             IdField: &schemapb.IDs_IntId{
	//                                 IntId: &schemapb.LongArray{
	//                                     Data: make([]int64, nq*topk),
	//                                 },
	//                             },
	//                         },
	//                         Topks: make([]int64, nq),
	//                     }
	//
	//                     for i := 0; i < nq; i++ {
	//                         for j := 0; j < topk; j++ {
	//                             offset := i*topk + j
	//                             if j >= invalidNum {
	//                                 resultData.Scores[offset] = minFloat32
	//                                 resultData.Ids.IdField.(*schemapb.IDs_IntId).IntId.Data[offset] = -1
	//                             } else {
	//                                 score := float32(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()) // increasingly
	//                                 id := int64(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	//                                 resultData.Scores[offset] = score
	//                                 resultData.Ids.IdField.(*schemapb.IDs_IntId).IntId.Data[offset] = id
	//                             }
	//                         }
	//                         resultData.Topks[i] = int64(topk)
	//                     }
	//
	//                     return resultData
	//                 }
	//
	//                 result1 := &internalpb.SearchResults{
	//                     Base: &commonpb.MsgBase{
	//                         MsgType:   commonpb.MsgType_SearchResult,
	//                         MsgID:     0,
	//                         Timestamp: 0,
	//                         SourceID:  0,
	//                     },
	//                     Status: &commonpb.Status{
	//                         ErrorCode: commonpb.ErrorCode_Success,
	//                         Reason:    "",
	//                     },
	//                     ResultChannelID:          "",
	//                     MetricType:               distance.L2,
	//                     NumQueries:               int64(nq),
	//                     TopK:                     int64(topk),
	//                     SealedSegmentIDsSearched: nil,
	//                     ChannelIDsSearched:       nil,
	//                     GlobalSealedSegmentIDs:   nil,
	//                     SlicedBlob:               nil,
	//                     SlicedNumCount:           1,
	//                     SlicedOffset:             0,
	//                 }
	//                 resultData := constructSearchResulstData(topk / 2)
	//                 sliceBlob, err := proto.Marshal(resultData)
	//                 assert.NoError(t, err)
	//                 result1.SlicedBlob = sliceBlob
	//
	//                 result2 := &internalpb.SearchResults{
	//                     Base: &commonpb.MsgBase{
	//                         MsgType:   commonpb.MsgType_SearchResult,
	//                         MsgID:     0,
	//                         Timestamp: 0,
	//                         SourceID:  0,
	//                     },
	//                     Status: &commonpb.Status{
	//                         ErrorCode: commonpb.ErrorCode_Success,
	//                         Reason:    "",
	//                     },
	//                     ResultChannelID:          "",
	//                     MetricType:               distance.L2,
	//                     NumQueries:               int64(nq),
	//                     TopK:                     int64(topk),
	//                     SealedSegmentIDsSearched: nil,
	//                     ChannelIDsSearched:       nil,
	//                     GlobalSealedSegmentIDs:   nil,
	//                     SlicedBlob:               nil,
	//                     SlicedNumCount:           1,
	//                     SlicedOffset:             0,
	//                 }
	//                 resultData2 := constructSearchResulstData(topk - topk/2)
	//                 sliceBlob2, err := proto.Marshal(resultData2)
	//                 assert.NoError(t, err)
	//                 result2.SlicedBlob = sliceBlob2
	//
	//                 // send search result
	//                 task.resultBuf <- result1
	//                 task.resultBuf <- result2
	//             }
	//         }
	//     }
	// }()
	//
	// assert.NoError(t, task.OnEnqueue())
	// assert.NoError(t, task.PreExecute(ctx))
	// assert.NoError(t, task.Execute(ctx))
	// assert.NoError(t, task.PostExecute(ctx))
	//
	// cancel()
	// wg.Wait()
}

func Test_checkSearchResultData(t *testing.T) {
	type args struct {
		data *schemapb.SearchResultData
		nq   int64
		topk int64
	}
	tests := []struct {
		description string
		wantErr     bool

		args args
	}{
		{"data.NumQueries != nq", true,
			args{
				data: &schemapb.SearchResultData{NumQueries: 100},
				nq:   10,
			}},
		{"data.TopK != topk", true,
			args{
				data: &schemapb.SearchResultData{NumQueries: 1, TopK: 1},
				nq:   1,
				topk: 10,
			}},
		{"size of IntId != NumQueries * TopK", true,
			args{
				data: &schemapb.SearchResultData{
					NumQueries: 1,
					TopK:       1,
					Ids: &schemapb.IDs{
						IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
				},
				nq:   1,
				topk: 1,
			}},
		{"size of StrID != NumQueries * TopK", true,
			args{
				data: &schemapb.SearchResultData{
					NumQueries: 1,
					TopK:       1,
					Ids: &schemapb.IDs{
						IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"1", "2"}}}},
				},
				nq:   1,
				topk: 1,
			}},
		{"size of score != nq * topK", true,
			args{
				data: &schemapb.SearchResultData{
					NumQueries: 1,
					TopK:       1,
					Ids: &schemapb.IDs{
						IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
					Scores: []float32{0.99, 0.98},
				},
				nq:   1,
				topk: 1,
			}},
		{"correct params", false,
			args{
				data: &schemapb.SearchResultData{
					NumQueries: 1,
					TopK:       1,
					Ids: &schemapb.IDs{
						IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
					Scores: []float32{0.99}},
				nq:   1,
				topk: 1,
			}},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			err := checkSearchResultData(test.args.data, test.args.nq, test.args.topk)

			if test.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestTaskSearch_selectHighestScoreIndex(t *testing.T) {
	t.Run("Integer ID", func(t *testing.T) {
		type args struct {
			subSearchResultData []*schemapb.SearchResultData
			subSearchNqOffset   [][]int64
			cursors             []int64
			topk                int64
			nq                  int64
		}
		tests := []struct {
			description string
			args        args

			expectedIdx     []int
			expectedDataIdx []int
		}{
			{
				description: "reduce 2 subSearchResultData",
				args: args{
					subSearchResultData: []*schemapb.SearchResultData{
						{
							Ids: &schemapb.IDs{
								IdField: &schemapb.IDs_IntId{
									IntId: &schemapb.LongArray{
										Data: []int64{11, 9, 8, 5, 3, 1},
									},
								},
							},
							Scores: []float32{1.1, 0.9, 0.8, 0.5, 0.3, 0.1},
							Topks:  []int64{2, 2, 2},
						},
						{
							Ids: &schemapb.IDs{
								IdField: &schemapb.IDs_IntId{
									IntId: &schemapb.LongArray{
										Data: []int64{12, 10, 7, 6, 4, 2},
									},
								},
							},
							Scores: []float32{1.2, 1.0, 0.7, 0.5, 0.4, 0.2},
							Topks:  []int64{2, 2, 2},
						},
					},
					subSearchNqOffset: [][]int64{{0, 2, 4}, {0, 2, 4}},
					cursors:           []int64{0, 0},
					topk:              2,
					nq:                3,
				},
				expectedIdx:     []int{1, 0, 1},
				expectedDataIdx: []int{0, 2, 4},
			},
		}
		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				for nqNum := int64(0); nqNum < test.args.nq; nqNum++ {
					idx, dataIdx := selectHighestScoreIndex(test.args.subSearchResultData, test.args.subSearchNqOffset, test.args.cursors, nqNum)
					assert.Equal(t, test.expectedIdx[nqNum], idx)
					assert.Equal(t, test.expectedDataIdx[nqNum], int(dataIdx))
				}
			})
		}
	})

	t.Run("String ID", func(t *testing.T) {
		type args struct {
			subSearchResultData []*schemapb.SearchResultData
			subSearchNqOffset   [][]int64
			cursors             []int64
			topk                int64
			nq                  int64
		}
		tests := []struct {
			description string
			args        args

			expectedIdx     []int
			expectedDataIdx []int
		}{
			{
				description: "reduce 2 subSearchResultData",
				args: args{
					subSearchResultData: []*schemapb.SearchResultData{
						{
							Ids: &schemapb.IDs{
								IdField: &schemapb.IDs_StrId{
									StrId: &schemapb.StringArray{
										Data: []string{"11", "9", "8", "5", "3", "1"},
									},
								},
							},
							Scores: []float32{1.1, 0.9, 0.8, 0.5, 0.3, 0.1},
							Topks:  []int64{2, 2, 2},
						},
						{
							Ids: &schemapb.IDs{
								IdField: &schemapb.IDs_StrId{
									StrId: &schemapb.StringArray{
										Data: []string{"12", "10", "7", "6", "4", "2"},
									},
								},
							},
							Scores: []float32{1.2, 1.0, 0.7, 0.5, 0.4, 0.2},
							Topks:  []int64{2, 2, 2},
						},
					},
					subSearchNqOffset: [][]int64{{0, 2, 4}, {0, 2, 4}},
					cursors:           []int64{0, 0},
					topk:              2,
					nq:                3,
				},
				expectedIdx:     []int{1, 0, 1},
				expectedDataIdx: []int{0, 2, 4},
			},
		}
		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				for nqNum := int64(0); nqNum < test.args.nq; nqNum++ {
					idx, dataIdx := selectHighestScoreIndex(test.args.subSearchResultData, test.args.subSearchNqOffset, test.args.cursors, nqNum)
					assert.Equal(t, test.expectedIdx[nqNum], idx)
					assert.Equal(t, test.expectedDataIdx[nqNum], int(dataIdx))
				}
			})
		}
	})
}

func TestTaskSearch_reduceSearchResultData(t *testing.T) {
	var (
		topk int64 = 5
		nq   int64 = 2
	)

	data := [][]int64{
		{10, 9, 8, 7, 6, 5, 4, 3, 2, 1},
		{20, 19, 18, 17, 16, 15, 14, 13, 12, 11},
		{30, 29, 28, 27, 26, 25, 24, 23, 22, 21},
		{40, 39, 38, 37, 36, 35, 34, 33, 32, 31},
		{50, 49, 48, 47, 46, 45, 44, 43, 42, 41},
	}

	score := [][]float32{
		{10, 9, 8, 7, 6, 5, 4, 3, 2, 1},
		{20, 19, 18, 17, 16, 15, 14, 13, 12, 11},
		{30, 29, 28, 27, 26, 25, 24, 23, 22, 21},
		{40, 39, 38, 37, 36, 35, 34, 33, 32, 31},
		{50, 49, 48, 47, 46, 45, 44, 43, 42, 41},
	}

	resultScore := []float32{-50, -49, -48, -47, -46, -45, -44, -43, -42, -41}

	t.Run("Offset limit", func(t *testing.T) {
		tests := []struct {
			description string
			offset      int64
			limit       int64

			outScore []float32
			outData  []int64
		}{
			{"offset 0, limit 5", 0, 5,
				[]float32{-50, -49, -48, -47, -46, -45, -44, -43, -42, -41},
				[]int64{50, 49, 48, 47, 46, 45, 44, 43, 42, 41}},
			{"offset 1, limit 4", 1, 4,
				[]float32{-49, -48, -47, -46, -44, -43, -42, -41},
				[]int64{49, 48, 47, 46, 44, 43, 42, 41}},
			{"offset 2, limit 3", 2, 3,
				[]float32{-48, -47, -46, -43, -42, -41},
				[]int64{48, 47, 46, 43, 42, 41}},
			{"offset 3, limit 2", 3, 2,
				[]float32{-47, -46, -42, -41},
				[]int64{47, 46, 42, 41}},
			{"offset 4, limit 1", 4, 1,
				[]float32{-46, -41},
				[]int64{46, 41}},
		}

		var results []*schemapb.SearchResultData
		for i := range data {
			r := getSearchResultData(nq, topk)

			r.Ids.IdField = &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: data[i]}}
			r.Scores = score[i]
			r.Topks = []int64{5, 5}

			results = append(results, r)
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				reduced, err := reduceSearchResultData(context.TODO(), results, nq, topk, distance.L2, schemapb.DataType_Int64, test.offset)
				assert.NoError(t, err)
				assert.Equal(t, test.outData, reduced.GetResults().GetIds().GetIntId().GetData())
				assert.Equal(t, []int64{test.limit, test.limit}, reduced.GetResults().GetTopks())
				assert.Equal(t, test.limit, reduced.GetResults().GetTopK())
				assert.InDeltaSlice(t, test.outScore, reduced.GetResults().GetScores(), 10e-8)
			})
		}

		lessThanLimitTests := []struct {
			description string
			offset      int64
			limit       int64

			outLimit int64
			outScore []float32
			outData  []int64
		}{
			{"offset 0, limit 6", 0, 6, 5,
				[]float32{-50, -49, -48, -47, -46, -45, -44, -43, -42, -41},
				[]int64{50, 49, 48, 47, 46, 45, 44, 43, 42, 41}},
			{"offset 1, limit 5", 1, 5, 4,
				[]float32{-49, -48, -47, -46, -44, -43, -42, -41},
				[]int64{49, 48, 47, 46, 44, 43, 42, 41}},
			{"offset 2, limit 4", 2, 4, 3,
				[]float32{-48, -47, -46, -43, -42, -41},
				[]int64{48, 47, 46, 43, 42, 41}},
			{"offset 3, limit 3", 3, 3, 2,
				[]float32{-47, -46, -42, -41},
				[]int64{47, 46, 42, 41}},
			{"offset 4, limit 2", 4, 2, 1,
				[]float32{-46, -41},
				[]int64{46, 41}},
			{"offset 5, limit 1", 5, 1, 0,
				[]float32{},
				[]int64{}},
		}

		for _, test := range lessThanLimitTests {
			t.Run(test.description, func(t *testing.T) {
				reduced, err := reduceSearchResultData(context.TODO(), results, nq, topk, distance.L2, schemapb.DataType_Int64, test.offset)
				assert.NoError(t, err)
				assert.Equal(t, test.outData, reduced.GetResults().GetIds().GetIntId().GetData())
				assert.Equal(t, []int64{test.outLimit, test.outLimit}, reduced.GetResults().GetTopks())
				assert.Equal(t, test.outLimit, reduced.GetResults().GetTopK())
				assert.InDeltaSlice(t, test.outScore, reduced.GetResults().GetScores(), 10e-8)
			})
		}
	})

	t.Run("Int64 ID", func(t *testing.T) {
		resultData := []int64{50, 49, 48, 47, 46, 45, 44, 43, 42, 41}

		var results []*schemapb.SearchResultData
		for i := range data {
			r := getSearchResultData(nq, topk)

			r.Ids.IdField = &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: data[i]}}
			r.Scores = score[i]
			r.Topks = []int64{5, 5}

			results = append(results, r)
		}

		reduced, err := reduceSearchResultData(context.TODO(), results, nq, topk, distance.L2, schemapb.DataType_Int64, 0)

		assert.NoError(t, err)
		assert.Equal(t, resultData, reduced.GetResults().GetIds().GetIntId().GetData())
		assert.Equal(t, []int64{5, 5}, reduced.GetResults().GetTopks())
		assert.Equal(t, int64(5), reduced.GetResults().GetTopK())
		assert.InDeltaSlice(t, resultScore, reduced.GetResults().GetScores(), 10e-8)
	})

	t.Run("String ID", func(t *testing.T) {
		resultData := []string{"50", "49", "48", "47", "46", "45", "44", "43", "42", "41"}

		var results []*schemapb.SearchResultData
		for i := range data {
			r := getSearchResultData(nq, topk)

			var strData []string
			for _, d := range data[i] {
				strData = append(strData, strconv.FormatInt(d, 10))
			}
			r.Ids.IdField = &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: strData}}
			r.Scores = score[i]
			r.Topks = []int64{5, 5}

			results = append(results, r)
		}

		reduced, err := reduceSearchResultData(context.TODO(), results, nq, topk, distance.L2, schemapb.DataType_VarChar, 0)

		assert.NoError(t, err)
		assert.Equal(t, resultData, reduced.GetResults().GetIds().GetStrId().GetData())
		assert.Equal(t, []int64{5, 5}, reduced.GetResults().GetTopks())
		assert.Equal(t, int64(5), reduced.GetResults().GetTopK())
		assert.InDeltaSlice(t, resultScore, reduced.GetResults().GetScores(), 10e-8)
	})
}

func Test_checkIfLoaded(t *testing.T) {
	t.Run("failed to get collection info", func(t *testing.T) {
		cache := newMockCache()
		cache.setGetInfoFunc(func(ctx context.Context, collectionName string) (*collectionInfo, error) {
			return nil, errors.New("mock")
		})
		globalMetaCache = cache
		var qc types.QueryCoord
		_, err := checkIfLoaded(context.Background(), qc, "test", []UniqueID{})
		assert.Error(t, err)
	})

	t.Run("collection loaded", func(t *testing.T) {
		cache := newMockCache()
		cache.setGetInfoFunc(func(ctx context.Context, collectionName string) (*collectionInfo, error) {
			return &collectionInfo{isLoaded: true}, nil
		})
		globalMetaCache = cache
		var qc types.QueryCoord
		loaded, err := checkIfLoaded(context.Background(), qc, "test", []UniqueID{})
		assert.NoError(t, err)
		assert.True(t, loaded)
	})

	t.Run("show partitions failed", func(t *testing.T) {
		cache := newMockCache()
		cache.setGetInfoFunc(func(ctx context.Context, collectionName string) (*collectionInfo, error) {
			return &collectionInfo{isLoaded: false}, nil
		})
		globalMetaCache = cache
		qc := NewQueryCoordMock()
		qc.SetShowPartitionsFunc(func(ctx context.Context, request *querypb.ShowPartitionsRequest) (*querypb.ShowPartitionsResponse, error) {
			return nil, errors.New("mock")
		})
		_, err := checkIfLoaded(context.Background(), qc, "test", []UniqueID{1, 2})
		assert.Error(t, err)
	})

	t.Run("show partitions but didn't success", func(t *testing.T) {
		cache := newMockCache()
		cache.setGetInfoFunc(func(ctx context.Context, collectionName string) (*collectionInfo, error) {
			return &collectionInfo{isLoaded: false}, nil
		})
		globalMetaCache = cache
		qc := NewQueryCoordMock()
		qc.SetShowPartitionsFunc(func(ctx context.Context, request *querypb.ShowPartitionsRequest) (*querypb.ShowPartitionsResponse, error) {
			return &querypb.ShowPartitionsResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_CollectionNotExists}}, nil
		})
		_, err := checkIfLoaded(context.Background(), qc, "test", []UniqueID{1, 2})
		assert.Error(t, err)
	})

	t.Run("partitions loaded", func(t *testing.T) {
		cache := newMockCache()
		cache.setGetInfoFunc(func(ctx context.Context, collectionName string) (*collectionInfo, error) {
			return &collectionInfo{isLoaded: false}, nil
		})
		globalMetaCache = cache
		qc := NewQueryCoordMock()
		qc.SetShowPartitionsFunc(func(ctx context.Context, request *querypb.ShowPartitionsRequest) (*querypb.ShowPartitionsResponse, error) {
			return &querypb.ShowPartitionsResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, InMemoryPercentages: []int64{100, 100}}, nil
		})
		loaded, err := checkIfLoaded(context.Background(), qc, "test", []UniqueID{1, 2})
		assert.NoError(t, err)
		assert.True(t, loaded)
	})

	t.Run("partitions loaded, some patitions not fully loaded", func(t *testing.T) {
		cache := newMockCache()
		cache.setGetInfoFunc(func(ctx context.Context, collectionName string) (*collectionInfo, error) {
			return &collectionInfo{isLoaded: false}, nil
		})
		globalMetaCache = cache
		qc := NewQueryCoordMock()
		qc.SetShowPartitionsFunc(func(ctx context.Context, request *querypb.ShowPartitionsRequest) (*querypb.ShowPartitionsResponse, error) {
			return &querypb.ShowPartitionsResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, InMemoryPercentages: []int64{100, 50}}, nil
		})
		loaded, err := checkIfLoaded(context.Background(), qc, "test", []UniqueID{1, 2})
		assert.NoError(t, err)
		assert.False(t, loaded)
	})

	t.Run("no specified partitions, show partitions failed", func(t *testing.T) {
		cache := newMockCache()
		cache.setGetInfoFunc(func(ctx context.Context, collectionName string) (*collectionInfo, error) {
			return &collectionInfo{isLoaded: false}, nil
		})
		globalMetaCache = cache
		qc := NewQueryCoordMock()
		qc.SetShowPartitionsFunc(func(ctx context.Context, request *querypb.ShowPartitionsRequest) (*querypb.ShowPartitionsResponse, error) {
			return nil, errors.New("mock")
		})
		_, err := checkIfLoaded(context.Background(), qc, "test", []UniqueID{1, 2})
		assert.Error(t, err)
	})

	t.Run("no specified partitions, show partitions but didn't succeed", func(t *testing.T) {
		cache := newMockCache()
		cache.setGetInfoFunc(func(ctx context.Context, collectionName string) (*collectionInfo, error) {
			return &collectionInfo{isLoaded: false}, nil
		})
		globalMetaCache = cache
		qc := NewQueryCoordMock()
		qc.SetShowPartitionsFunc(func(ctx context.Context, request *querypb.ShowPartitionsRequest) (*querypb.ShowPartitionsResponse, error) {
			return &querypb.ShowPartitionsResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_CollectionNotExists}}, nil
		})
		_, err := checkIfLoaded(context.Background(), qc, "test", []UniqueID{1, 2})
		assert.Error(t, err)
	})

	t.Run("not fully loaded", func(t *testing.T) {
		cache := newMockCache()
		cache.setGetInfoFunc(func(ctx context.Context, collectionName string) (*collectionInfo, error) {
			return &collectionInfo{isLoaded: false}, nil
		})
		globalMetaCache = cache
		qc := NewQueryCoordMock()
		qc.SetShowPartitionsFunc(func(ctx context.Context, request *querypb.ShowPartitionsRequest) (*querypb.ShowPartitionsResponse, error) {
			return &querypb.ShowPartitionsResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, PartitionIDs: []UniqueID{1, 2}}, nil
		})
		loaded, err := checkIfLoaded(context.Background(), qc, "test", []UniqueID{})
		assert.NoError(t, err)
		assert.False(t, loaded)
	})

	t.Run("not loaded", func(t *testing.T) {
		cache := newMockCache()
		cache.setGetInfoFunc(func(ctx context.Context, collectionName string) (*collectionInfo, error) {
			return &collectionInfo{isLoaded: false}, nil
		})
		globalMetaCache = cache
		qc := NewQueryCoordMock()
		qc.SetShowPartitionsFunc(func(ctx context.Context, request *querypb.ShowPartitionsRequest) (*querypb.ShowPartitionsResponse, error) {
			return &querypb.ShowPartitionsResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, PartitionIDs: []UniqueID{}}, nil
		})
		loaded, err := checkIfLoaded(context.Background(), qc, "test", []UniqueID{})
		assert.NoError(t, err)
		assert.False(t, loaded)
	})
}

func TestSearchTask_ErrExecute(t *testing.T) {
	Params.Init()

	var (
		err error
		ctx = context.TODO()

		rc = NewRootCoordMock()
		qc = NewQueryCoordMock(withValidShardLeaders())
		qn = &QueryNodeMock{}

		shardsNum      = int32(2)
		collectionName = t.Name() + funcutil.GenRandomStr()
		errPolicy      = func(context.Context, *shardClientMgr, func(context.Context, int64, types.QueryNode, []string) error, map[string][]nodeInfo) error {
			return fmt.Errorf("fake error")
		}
	)

	mockCreator := func(ctx context.Context, address string) (types.QueryNode, error) {
		return qn, nil
	}

	mgr := newShardClientMgr(withShardClientCreator(mockCreator))

	rc.Start()
	defer rc.Stop()
	qc.Start()
	defer qc.Stop()

	err = InitMetaCache(ctx, rc, qc, mgr)
	assert.NoError(t, err)

	fieldName2Types := map[string]schemapb.DataType{
		testBoolField:     schemapb.DataType_Bool,
		testInt32Field:    schemapb.DataType_Int32,
		testInt64Field:    schemapb.DataType_Int64,
		testFloatField:    schemapb.DataType_Float,
		testDoubleField:   schemapb.DataType_Double,
		testFloatVecField: schemapb.DataType_FloatVector,
	}
	if enableMultipleVectorFields {
		fieldName2Types[testBinaryVecField] = schemapb.DataType_BinaryVector
	}

	schema := constructCollectionSchemaByDataType(collectionName, fieldName2Types, testInt64Field, false)
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	createColT := &createCollectionTask{
		Condition: NewTaskCondition(ctx),
		CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
			CollectionName: collectionName,
			Schema:         marshaledSchema,
			ShardsNum:      shardsNum,
		},
		ctx:       ctx,
		rootCoord: rc,
	}

	require.NoError(t, createColT.OnEnqueue())
	require.NoError(t, createColT.PreExecute(ctx))
	require.NoError(t, createColT.Execute(ctx))
	require.NoError(t, createColT.PostExecute(ctx))

	collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	assert.NoError(t, err)

	status, err := qc.LoadCollection(ctx, &querypb.LoadCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_LoadCollection,
			SourceID: Params.ProxyCfg.GetNodeID(),
		},
		CollectionID: collectionID,
	})
	require.NoError(t, err)
	require.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)

	// test begins
	task := &searchTask{
		Condition: NewTaskCondition(ctx),
		SearchRequest: &internalpb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_Retrieve,
				SourceID: Params.ProxyCfg.GetNodeID(),
			},
			CollectionID:   collectionID,
			OutputFieldsId: make([]int64, len(fieldName2Types)),
		},
		ctx: ctx,
		result: &milvuspb.SearchResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
		},
		request: &milvuspb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_Retrieve,
				SourceID: Params.ProxyCfg.GetNodeID(),
			},
			CollectionName: collectionName,
		},
		qc:       qc,
		shardMgr: mgr,
	}
	for i := 0; i < len(fieldName2Types); i++ {
		task.SearchRequest.OutputFieldsId[i] = int64(common.StartOfUserFieldID + i)
	}

	assert.NoError(t, task.OnEnqueue())

	task.ctx = ctx

	assert.NoError(t, task.PreExecute(ctx))

	task.searchShardPolicy = errPolicy
	assert.Error(t, task.Execute(ctx))

	task.searchShardPolicy = mergeRoundRobinPolicy
	qn.searchError = fmt.Errorf("mock error")
	assert.Error(t, task.Execute(ctx))

	qn.searchError = nil
	qn.withSearchResult = &internalpb.SearchResults{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_NotShardLeader,
		},
	}
	assert.Equal(t, task.Execute(ctx), errInvalidShardLeaders)

	qn.withSearchResult = &internalpb.SearchResults{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}
	assert.Error(t, task.Execute(ctx))

	result1 := &internalpb.SearchResults{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}
	qn.withSearchResult = result1
	assert.NoError(t, task.Execute(ctx))
}

func TestTaskSearch_parseQueryInfo(t *testing.T) {
	t.Run("parseQueryInfo no error", func(t *testing.T) {
		var targetOffset int64 = 200

		sp := getValidSearchParams()
		sp = append(sp, &commonpb.KeyValuePair{
			Key:   OffsetKey,
			Value: strconv.FormatInt(targetOffset, 10),
		})

		info, offset, err := parseQueryInfo(sp)
		assert.NoError(t, err)
		assert.NotNil(t, info)
		assert.Equal(t, targetOffset, offset)
	})

	t.Run("parseQueryInfo error", func(t *testing.T) {
		spNoTopk := []*commonpb.KeyValuePair{{
			Key:   AnnsFieldKey,
			Value: testFloatVecField}}

		spInvalidTopk := append(spNoTopk, &commonpb.KeyValuePair{
			Key:   TopKKey,
			Value: "invalid",
		})

		spInvalidTopk65536 := append(spNoTopk, &commonpb.KeyValuePair{
			Key:   TopKKey,
			Value: "65536",
		})

		spNoMetricType := append(spNoTopk, &commonpb.KeyValuePair{
			Key:   TopKKey,
			Value: "10",
		})

		spInvalidTopkPlusOffset := append(spNoTopk, &commonpb.KeyValuePair{
			Key:   OffsetKey,
			Value: "65535",
		})

		spNoSearchParams := append(spNoMetricType, &commonpb.KeyValuePair{
			Key:   MetricTypeKey,
			Value: distance.L2,
		})

		// no roundDecimal is valid
		noRoundDecimal := append(spNoSearchParams, &commonpb.KeyValuePair{
			Key:   SearchParamsKey,
			Value: `{"nprobe": 10}`,
		})

		spInvalidRoundDecimal2 := append(noRoundDecimal, &commonpb.KeyValuePair{
			Key:   RoundDecimalKey,
			Value: "1000",
		})

		spInvalidRoundDecimal := append(noRoundDecimal, &commonpb.KeyValuePair{
			Key:   RoundDecimalKey,
			Value: "invalid",
		})

		spInvalidOffsetNoInt := append(noRoundDecimal, &commonpb.KeyValuePair{
			Key:   OffsetKey,
			Value: "invalid",
		})

		spInvalidOffsetNegative := append(noRoundDecimal, &commonpb.KeyValuePair{
			Key:   OffsetKey,
			Value: "-1",
		})

		spInvalidOffsetTooLarge := append(noRoundDecimal, &commonpb.KeyValuePair{
			Key:   OffsetKey,
			Value: "16386",
		})

		tests := []struct {
			description   string
			invalidParams []*commonpb.KeyValuePair
		}{
			{"No_topk", spNoTopk},
			{"Invalid_topk", spInvalidTopk},
			{"Invalid_topk_65536", spInvalidTopk65536},
			{"Invalid_topk_plus_offset", spInvalidTopkPlusOffset},
			{"No_Metric_type", spNoMetricType},
			{"No_search_params", spNoSearchParams},
			{"Invalid_round_decimal", spInvalidRoundDecimal},
			{"Invalid_round_decimal_1000", spInvalidRoundDecimal2},
			{"Invalid_offset_not_int", spInvalidOffsetNoInt},
			{"Invalid_offset_negative", spInvalidOffsetNegative},
			{"Invalid_offset_too_large", spInvalidOffsetTooLarge},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				info, offset, err := parseQueryInfo(test.invalidParams)
				assert.Error(t, err)
				assert.Nil(t, info)
				assert.Zero(t, offset)

				t.Logf("err=%s", err.Error())
			})
		}
	})
}

func getSearchResultData(nq, topk int64) *schemapb.SearchResultData {
	result := schemapb.SearchResultData{
		NumQueries: nq,
		TopK:       topk,
		Ids:        &schemapb.IDs{},
		Scores:     []float32{},
		Topks:      []int64{},
	}
	return &result
}
