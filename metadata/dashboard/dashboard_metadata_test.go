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
	"github.com/featureform/provider"
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
	getBody := TagGetBody{
		Variant: "default",
	}
	jsonValue, _ := json.Marshal(getBody)
	c.Request.Body = io.NopCloser(bytes.NewBuffer(jsonValue))
}

func MockJsonPost(c *gin.Context, params gin.Params, tagList []string) {
	c.Request.Method = "POST"
	c.Request.Header.Set("Content-Type", "application/json")
	postBody := TagPostBody{
		Tags:    tagList,
		Variant: "default",
	}
	jsonValue, _ := json.Marshal(postBody)
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
	resourceType := "sources"
	tagList := []string{"test tag 40", "test tag 66"}

	mockRecorder := httptest.NewRecorder()
	ctx := GetTestGinContext(mockRecorder)
	params := []gin.Param{
		{
			Key:   "resource",
			Value: name,
		},
		{
			Key:   "type",
			Value: resourceType,
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

func MockGetSourceGet(c *gin.Context, params gin.Params, u url.Values) {
	c.Request.Method = "GET"
	c.Request.Header.Set("Content-Type", "application/json")
	c.Params = params
	c.Request.URL.RawQuery = u.Encode()
}

func TestGetSourceDataReturnsData(t *testing.T) {
	mockRecorder := httptest.NewRecorder()
	ctx := GetTestGinContext(mockRecorder)
	u := url.Values{}
	u.Add("name", "nameParamValue")
	u.Add("variant", "variantParamValue")
	MockGetSourceGet(ctx, nil, u)

	logger := zap.NewExample().Sugar()
	client := &metadata.Client{
		GrpcConn: metadata.MetadataServerMock{},
	}
	serv := MetadataServer{
		client: client,
		logger: logger,
	}

	serv.GetSourceData(ctx)

	iterator := provider.UnitTestIterator{}
	var data SourceDataResponse
	rowValues := []string{"row value", "row value"}
	expectedRows := [][]string{rowValues}

	json.Unmarshal(mockRecorder.Body.Bytes(), &data)
	assert.Equal(t, http.StatusOK, mockRecorder.Code)
	assert.Equal(t, iterator.Columns(), data.Columns)
	assert.Equal(t, expectedRows, data.Rows)
}

func TestGetSourceMissingNameOrVariantErrors(t *testing.T) {
	mockRecorder := httptest.NewRecorder()
	ctx := GetTestGinContext(mockRecorder)
	u := url.Values{}
	u.Add("name", "")    //intentionally blank
	u.Add("variant", "") //intentionally blank
	MockGetSourceGet(ctx, nil, u)

	logger := zap.NewExample().Sugar()
	serv := MetadataServer{
		logger: logger,
	}

	serv.GetSourceData(ctx)

	var actualErrorMsg string
	expectedMsg := "Error 400: Failed to fetch GetSourceData - Could not find the name or variant query parameters"
	json.Unmarshal(mockRecorder.Body.Bytes(), &actualErrorMsg)

	assert.Equal(t, http.StatusBadRequest, mockRecorder.Code)
	assert.Equal(t, expectedMsg, actualErrorMsg)
}
