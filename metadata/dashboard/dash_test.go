package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
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
	c.Set("user_id", 1)
	c.Params = params
}

func TestHelloWorld(t *testing.T) {
	w := httptest.NewRecorder()
	ctx := GetTestGinContext(w)
	params := []gin.Param{
		{
			Key:   "type",
			Value: "sources",
		},
		{
			Key:   "resource",
			Value: "transactions",
		},
	}
	MockJsonGet(ctx, params)

	serv := MetadataServer{}
	serv.TestSample(ctx)

	resp, _ := ioutil.ReadAll(w.Body)
	fmt.Println(resp)
	fmt.Println(string(resp))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, string(resp), "\"TestSample\"")
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
