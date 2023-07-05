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

func GetTestGinContext(mockRecorder *httptest.ResponseRecorder) *gin.Context {
	gin.SetMode(gin.TestMode)
	ctx, _ := gin.CreateTestContext(mockRecorder)
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

func MockJsonPost(c *gin.Context, params gin.Params, tagList []string) {
	c.Request.Method = "POST"
	c.Request.Header.Set("Content-Type", "application/json")
	tags := TagRequestBody{
		Tags: tagList,
	}
	jsonValue, _ := json.Marshal(tags)
	c.Request.Body = io.NopCloser(bytes.NewBuffer(jsonValue))
	c.Params = params
}

func TestVersionMap(t *testing.T) {
	mockRecorder := httptest.NewRecorder()
	ctx := GetTestGinContext(mockRecorder)
	MockJsonGet(ctx, nil)
	serv := MetadataServer{}

	version := "stable"
	t.Setenv("FEATUREFORM_VERSION", version)
	serv.GetVersionMap(ctx)

	var data map[string]interface{}
	json.Unmarshal(mockRecorder.Body.Bytes(), &data)

	assert.Equal(t, http.StatusOK, mockRecorder.Code)
	assert.Equal(t, version, data["version"])
}

func TestPostTags(t *testing.T) {
	name := "transactions"
	variant := "default"
	tagList := []string{"test tag 40", "test tag 66"}

	mockRecorder := httptest.NewRecorder()
	ctx := GetTestGinContext(mockRecorder)
	params := []gin.Param{
		{
			Key:   "resource",
			Value: name,
		},
	}

	MockJsonPost(ctx, params, tagList)

	res := metadata.ResourceID{
		Name:    name,
		Variant: variant,
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
	json.Unmarshal(mockRecorder.Body.Bytes(), &data)

	assert.Equal(t, http.StatusOK, mockRecorder.Code)
	assert.Equal(t, name, data.Name)
	assert.Equal(t, variant, data.Variant)
	assert.Equal(t, tagList, data.Tags)
}
