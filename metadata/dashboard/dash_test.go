package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/featureform/metadata"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func GetTestGinContext(w *httptest.ResponseRecorder) *gin.Context {
	gin.SetMode(gin.TestMode)
	ctx, _ := gin.CreateTestContext(w)
	ctx.Request = &http.Request{
		Header: make(http.Header),
		URL:    &url.URL{},
	}
	return ctx
}

func MockJsonGet(c *gin.Context, params gin.Params) {
	c.Request.Method = "GET"
	c.Request.Header.Set("Content-Type", "application/json")
	c.Params = params
}

func MockJsonPost(c *gin.Context, params gin.Params) {
	c.Request.Method = "POST"
	c.Request.Header.Set("Content-Type", "application/json")
	tags := TagRequestBody{
		Tags: []string{"test tag 1", "test tag 2"},
	}
	jsonValue, _ := json.Marshal(tags)
	c.Request.Body = io.NopCloser(bytes.NewBuffer(jsonValue))
	c.Params = params
}

func TestVersionMap(t *testing.T) {
	w := httptest.NewRecorder()
	ctx := GetTestGinContext(w)
	MockJsonGet(ctx, nil)
	serv := MetadataServer{}

	version := "stable"
	t.Setenv("FEATUREFORM_VERSION", version)
	serv.GetVersionMap(ctx)

	var data map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &data)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, version, data["version"])
}

func TestPostTags(t *testing.T) {
	w := httptest.NewRecorder()
	ctx := GetTestGinContext(w)
	params := []gin.Param{
		{
			Key:   "resource",
			Value: "transactions",
		},
	}

	MockJsonPost(ctx, params)

	res := metadata.ResourceID{
		Name:    "transactions",
		Variant: "default",
		Type:    metadata.SOURCE_VARIANT,
	}

	resource :=
		&metadata.SourceResource{}

	localStorageProvider := LocalStorageProvider{}
	lookup, _ := localStorageProvider.GetResourceLookup()
	lookup.Set(res, resource)

	logger := zap.NewExample().Sugar()
	client := &metadata.Client{}
	serv := MetadataServer{
		lookup:          lookup,
		client:          client,
		logger:          logger,
		StorageProvider: localStorageProvider,
	}
	serv.PostTags(ctx)

	var data TagResult
	json.Unmarshal(w.Body.Bytes(), &data)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "Post Name", data.Name)
	assert.Equal(t, "Post Variant", data.Variant)
	assert.Equal(t, []string{"test tag 1", "test tag 2"}, data.Tags)
}
