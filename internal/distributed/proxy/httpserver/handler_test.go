package httpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/stretchr/testify/assert"
)

type mockProxyComponent struct {
	// wrap the interface to avoid implement not used func.
	// and to let not implemented call panics
	// implement the method you want to mock
	types.ProxyComponent
}

func (mockProxyComponent) Dummy(ctx context.Context, request *milvuspb.DummyRequest) (*milvuspb.DummyResponse, error) {
	return nil, nil
}

func (mockProxyComponent) Insert(ctx context.Context, request *milvuspb.InsertRequest) (*milvuspb.MutationResult, error) {
	if request.CollectionName == "" {
		return nil, errors.New("body parse err")
	}
	return &milvuspb.MutationResult{Acknowledged: true}, nil
}

func (mockProxyComponent) Delete(ctx context.Context, request *milvuspb.DeleteRequest) (*milvuspb.MutationResult, error) {
	if request.Expr == "" {
		return nil, errors.New("body parse err")
	}
	return &milvuspb.MutationResult{Acknowledged: true}, nil
}

func (mockProxyComponent) Search(ctx context.Context, request *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
	if request.Dsl == "" {
		return nil, errors.New("body parse err")
	}
	return &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			TopK: 10,
		},
	}, nil
}

func (mockProxyComponent) Query(ctx context.Context, request *milvuspb.QueryRequest) (*milvuspb.QueryResults, error) {
	if request.Expr == "" {
		return nil, errors.New("body parse err")
	}
	return &milvuspb.QueryResults{
		CollectionName: "test",
	}, nil
}

func (mockProxyComponent) Flush(ctx context.Context, request *milvuspb.FlushRequest) (*milvuspb.FlushResponse, error) {
	if len(request.CollectionNames) < 1 {
		return nil, errors.New("body parse err")
	}
	return &milvuspb.FlushResponse{}, nil
}

func (mockProxyComponent) CalcDistance(ctx context.Context, request *milvuspb.CalcDistanceRequest) (*milvuspb.CalcDistanceResults, error) {
	if len(request.Params) < 1 {
		return nil, errors.New("body parse err")
	}
	return &milvuspb.CalcDistanceResults{}, nil
}

func TestHandlers(t *testing.T) {
	mockProxy := &mockProxyComponent{}
	h := NewHandlers(mockProxy)
	testEngine := gin.New()
	h.RegisterRoutesTo(testEngine)

	t.Run("handleGetHealth default json ok", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, w.Body.Bytes(), []byte(`{"status":"ok"}`))
	})
	t.Run("handleGetHealth accept yaml ok", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		req.Header = http.Header{
			"Accept": []string{binding.MIMEYAML},
		}
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, w.Body.Bytes(), []byte("status: ok\n"))
	})
	t.Run("handlePostDummy parsejson failed 400", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/dummy", nil)
		req.Header = http.Header{
			"Content-Type": []string{binding.MIMEJSON},
		}
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})
	t.Run("handlePostDummy parseyaml failed 400", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/dummy", nil)
		req.Header = http.Header{
			"Content-Type": []string{binding.MIMEYAML},
		}
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})
	t.Run("handlePostDummy default json ok", func(t *testing.T) {
		bodyBytes := []byte("{}")
		req := httptest.NewRequest(http.MethodPost, "/dummy", bytes.NewReader(bodyBytes))
		req.Header = http.Header{
			"Content-Type": []string{binding.MIMEJSON},
		}
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
	})
	t.Run("handlePostDummy yaml ok", func(t *testing.T) {
		bodyBytes := []byte("---")
		req := httptest.NewRequest(http.MethodPost, "/dummy", bytes.NewReader(bodyBytes))
		req.Header = http.Header{
			"Content-Type": []string{binding.MIMEYAML},
		}
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("handleFlushRequest ok", func(t *testing.T) {
		bodyStruct := milvuspb.FlushRequest{CollectionNames: []string{"c1"}}
		body, _ := json.Marshal(bodyStruct)
		req := httptest.NewRequest(http.MethodPost, "/persist", bytes.NewReader(body))
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, []byte(`{}`), w.Body.Bytes())
	})
	t.Run("handleFlushRequest bad req", func(t *testing.T) {
		body := []byte("bad request")
		req := httptest.NewRequest(http.MethodPost, "/persist", bytes.NewReader(body))
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("handleCalculateDistance ok", func(t *testing.T) {
		bodyStruct := milvuspb.CalcDistanceRequest{
			Params: []*commonpb.KeyValuePair{
				{Key: "key", Value: "val"},
			}}
		body, _ := json.Marshal(bodyStruct)
		req := httptest.NewRequest(http.MethodGet, "/distance", bytes.NewReader(body))
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
	})
	t.Run("handleCalculateDistance bad req", func(t *testing.T) {
		body := []byte("bad request")
		req := httptest.NewRequest(http.MethodGet, "/distance", bytes.NewReader(body))
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})
}

func TestEntitiesHandlers(t *testing.T) {
	mockProxy := &mockProxyComponent{}
	h := NewHandlers(mockProxy)
	testEngine := gin.New()
	h.RegisterRoutesTo(testEngine)

	t.Run("handleInsertRequest ok", func(t *testing.T) {
		bodyStruct := milvuspb.InsertRequest{CollectionName: "c1"}
		body, _ := json.Marshal(bodyStruct)
		req := httptest.NewRequest(http.MethodPost, "/entities", bytes.NewReader(body))
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, []byte(`{"acknowledged":true}`), w.Body.Bytes())
	})
	t.Run("handleInsertRequest bad req", func(t *testing.T) {
		body := []byte("bad request")
		req := httptest.NewRequest(http.MethodPost, "/entities", bytes.NewReader(body))
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})
	t.Run("handleDeleteRequest ok", func(t *testing.T) {
		bodyStruct := milvuspb.DeleteRequest{Expr: "some expr"}
		body, _ := json.Marshal(bodyStruct)
		req := httptest.NewRequest(http.MethodDelete, "/entities", bytes.NewReader(body))
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, []byte(`{"acknowledged":true}`), w.Body.Bytes())
	})
	t.Run("handleDeleteRequest bad req", func(t *testing.T) {
		body := []byte("bad request")
		req := httptest.NewRequest(http.MethodDelete, "/entities", bytes.NewReader(body))
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})
	t.Run("handleSearchRequest ok", func(t *testing.T) {
		bodyStruct := milvuspb.SearchRequest{Dsl: "some dsl"}
		body, _ := json.Marshal(bodyStruct)
		req := httptest.NewRequest(http.MethodGet, "/entities", bytes.NewReader(body))
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, []byte(`{"results":{"top_k":10}}`), w.Body.Bytes())
	})
	t.Run("handleSearchRequest bad req", func(t *testing.T) {
		body := []byte("bad request")
		req := httptest.NewRequest(http.MethodGet, "/entities", bytes.NewReader(body))
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})
	t.Run("handleQueryRequest ok", func(t *testing.T) {
		bodyStruct := milvuspb.QueryRequest{Expr: "some expr"}
		body, _ := json.Marshal(bodyStruct)
		req := httptest.NewRequest(http.MethodGet, "/entities?by_query", bytes.NewReader(body))
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, []byte(`{"collection_name":"test"}`), w.Body.Bytes())
	})
	t.Run("handleQueryRequest bad req", func(t *testing.T) {
		body := []byte("bad request")
		req := httptest.NewRequest(http.MethodGet, "/entities?by_query", bytes.NewReader(body))
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})
}
