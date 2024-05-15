package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"testing"

	"github.com/featureform/metadata"
	"github.com/featureform/metadata/search"
	"github.com/featureform/provider"
	sc "github.com/featureform/scheduling"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func GetTestGinContext(mockRecorder *httptest.ResponseRecorder) *gin.Context {
	SearchClient = search.SearchMock{}
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

func MockJsonPost(c *gin.Context, params gin.Params, body map[string]interface{}) {
	c.Request.Method = "POST"
	c.Request.Header.Set("Content-Type", "application/json")
	jsonValue, _ := json.Marshal(body)
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
	postBody := map[string]interface{}{
		"tags":    tagList,
		"variant": "default",
	}
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

	MockJsonPost(ctx, params, postBody)

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
	rowValues := []string{"row string value", "true", "10"}
	expectedRows := [][]string{rowValues, rowValues, rowValues}

	json.Unmarshal(mockRecorder.Body.Bytes(), &data)
	assert.Equal(t, http.StatusOK, mockRecorder.Code)
	assert.Equal(t, iterator.Columns(), data.Columns)
	assert.Equal(t, expectedRows, data.Rows)

	assert.Len(t, data.Stats, 0, "Get Source stats list should be zero")
}

func TestParseStatFile(t *testing.T) {
	//given: a baseline json file
	testCases := []struct {
		name     string
		filepath string
	}{
		{"full stats", "./testdata/mock_stats.json"},
		{"entity_value_stats file", "./testdata/entity_value_stats.json"},
		{"calendar_stats file", "./testdata/calendar_stats.json"},
	}
	for _, currTest := range testCases {
		t.Run(currTest.name, func(t *testing.T) {
			baseFile, _ := os.ReadFile(currTest.filepath)

			//when: we create the source data response obj using the file
			response, _ := ParseStatFile(baseFile)

			var fileData map[string]interface{}
			json.Unmarshal(baseFile, &fileData)

			fileColumns := fileData["columns"].([]interface{})
			fileRows := fileData["rows"].([]interface{})
			fileStats := fileData["stats"].([]interface{})

			//then: the response column names match the baseline json
			assert.Equal(t, len(fileColumns), len(response.Columns))
			for _, fileColumn := range fileColumns {
				assert.Contains(t, response.Columns, fileColumn)
			}

			//then: the response data rows match the baseline json
			assert.Equal(t, len(fileRows), len(response.Rows))
			for fileIndex, fileRow := range fileRows {
				for _, fileRowValue := range fileRow.([]interface{}) {
					assert.Contains(t, response.Rows[fileIndex], fileRowValue)
				}
			}

			//then: the individual stats objects also match the baseline
			assert.Equal(t, len(fileStats), len(response.Stats))
			for fileIndex, fileStat := range fileStats {
				currFileStat := fileStat.(map[string]interface{})
				assert.Equal(t, currFileStat["name"].(string), response.Stats[fileIndex].Name)
				assert.Equal(t, currFileStat["type"].(string), response.Stats[fileIndex].Type)
				stringCats := []string{}
				for _, category := range currFileStat["string_categories"].([]interface{}) {
					stringCats = append(stringCats, category.(string))
				}
				assert.Equal(t, stringCats, response.Stats[fileIndex].StringCategories)
				numCats := [][]int{}
				for _, category := range currFileStat["numeric_categories"].([]interface{}) {
					innerList := []int{}
					for _, numInterface := range category.([]interface{}) {
						innerList = append(innerList, int(numInterface.(float64)))
					}
					numCats = append(numCats, innerList)
				}
				assert.Equal(t, numCats, response.Stats[fileIndex].NumericCategories)
				countInterface := currFileStat["categoryCounts"].([]interface{})
				counts := []int{}
				for _, count := range countInterface {
					counts = append(counts, int(count.(float64)))
				}
				assert.Equal(t, counts, response.Stats[fileIndex].CategoryCounts)
			}
		})
	}

}

func TestGetSourceMissingNameOrVariantParamErrors(t *testing.T) {
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

func TestGetSourceFaultyOrNilGrpcClientPanic(t *testing.T) {
	mockRecorder := httptest.NewRecorder()
	ctx := GetTestGinContext(mockRecorder)
	u := url.Values{}
	u.Add("name", "nameParamValue")
	u.Add("variant", "variantParamValue")
	MockGetSourceGet(ctx, nil, u)

	logger := zap.NewExample().Sugar()
	serv := MetadataServer{
		logger: logger,
	}

	didPanic := func() {
		serv.GetSourceData(ctx)
	}

	assert.Panics(t, didPanic)
}

func TestGetTaskRuns(t *testing.T) {
	mockRecorder := httptest.NewRecorder()
	ctx := GetTestGinContext(mockRecorder)
	body := map[string]interface{}{"Status": "ALL",
		"SearchText": "",
		"SortBy":     "",
	}
	MockJsonPost(ctx, nil, body)

	logger := zap.NewExample().Sugar()
	client := &metadata.Client{}
	serv := MetadataServer{
		client: client,
		logger: logger,
	}
	CreateDummyTaskRuns(10)
	serv.GetTaskRuns(ctx)

	var data []TaskRunResponse
	json.Unmarshal(mockRecorder.Body.Bytes(), &data)

	assert.Equal(t, http.StatusOK, mockRecorder.Code)
	assert.Greater(t, len(data), 2)
}

func TestGetTaskRunsZeroResults(t *testing.T) {
	mockRecorder := httptest.NewRecorder()
	ctx := GetTestGinContext(mockRecorder)
	body := map[string]interface{}{"Status": "DOES NOT EXIST",
		"SearchText": "hahaha",
		"SortBy":     "Nope",
	}
	MockJsonPost(ctx, nil, body)

	logger := zap.NewExample().Sugar()
	client := &metadata.Client{}
	serv := MetadataServer{
		client: client,
		logger: logger,
	}
	serv.GetTaskRuns(ctx)

	var data []TaskRunResponse
	json.Unmarshal(mockRecorder.Body.Bytes(), &data)

	assert.Equal(t, http.StatusOK, mockRecorder.Code)
	assert.Equal(t, 0, len(data))
}

func TestGetTaskRunDetails(t *testing.T) {
	mockRecorder := httptest.NewRecorder()
	ctx := GetTestGinContext(mockRecorder)
	taskRunIdParam := 1
	taskRunId := sc.TaskRunID(taskRunIdParam)
	params := []gin.Param{
		{
			Key:   "taskRunId",
			Value: strconv.Itoa(taskRunIdParam),
		},
	}
	MockJsonGet(ctx, params)

	logger := zap.NewExample().Sugar()
	client := &metadata.Client{}
	serv := MetadataServer{
		client: client,
		logger: logger,
	}
	CreateDummyTaskRuns(50)
	serv.GetTaskRunDetails(ctx)

	var data TaskRunDetailResponse
	json.Unmarshal(mockRecorder.Body.Bytes(), &data)

	assert.Equal(t, http.StatusOK, mockRecorder.Code)
	assert.Equal(t, taskRunId, data.TaskRun.ID)
	assert.NotEmpty(t, data.TaskRun.Name)
	assert.NotEmpty(t, data.TaskRun.Status)
	assert.NotEmpty(t, data.TaskRun.Logs)
	assert.NotEmpty(t, data.OtherRuns)
}

func TestGetTaskRunDetailZeroResults(t *testing.T) {
	mockRecorder := httptest.NewRecorder()
	ctx := GetTestGinContext(mockRecorder)
	taskRunIdParam := -1
	taskRunId := sc.TaskRunID(taskRunIdParam)
	params := []gin.Param{
		{
			Key:   "taskRunId",
			Value: strconv.Itoa(taskRunIdParam),
		},
	}
	MockJsonGet(ctx, params)

	logger := zap.NewExample().Sugar()
	client := &metadata.Client{}
	serv := MetadataServer{
		client: client,
		logger: logger,
	}
	CreateDummyTaskRuns(50)
	serv.GetTaskRunDetails(ctx)

	var data TaskRunDetailResponse
	json.Unmarshal(mockRecorder.Body.Bytes(), &data)

	assert.Equal(t, http.StatusOK, mockRecorder.Code)
	assert.Equal(t, taskRunId, data.TaskRun.ID)
	assert.Empty(t, data.TaskRun.Name)
	assert.Empty(t, data.TaskRun.Status)
	assert.Empty(t, data.OtherRuns)
}

func TestGetTaskRunDetailParamPanic(t *testing.T) {
	mockRecorder := httptest.NewRecorder()
	ctx := GetTestGinContext(mockRecorder)
	taskRunId := "1"
	params := []gin.Param{
		{
			Key:   "bad_key!",
			Value: taskRunId,
		},
	}
	MockJsonGet(ctx, params)

	logger := zap.NewExample().Sugar()
	client := &metadata.Client{}
	serv := MetadataServer{
		client: client,
		logger: logger,
	}
	serv.GetTaskRunDetails(ctx)

	var actualErrorMsg string
	expectedMsg := "Error 400: Failed to fetch GetTaskRunDetails - Could not find the taskRunId parameter"
	_ = json.Unmarshal(mockRecorder.Body.Bytes(), &actualErrorMsg)

	assert.Equal(t, http.StatusBadRequest, mockRecorder.Code)
	assert.Equal(t, expectedMsg, actualErrorMsg)
}

func TestGetTaskRunDetailBadParamTypePanic(t *testing.T) {
	mockRecorder := httptest.NewRecorder()
	ctx := GetTestGinContext(mockRecorder)
	taskRunId := "not_a_int"
	params := []gin.Param{
		{
			Key:   "taskRunId",
			Value: taskRunId,
		},
	}
	MockJsonGet(ctx, params)

	logger := zap.NewExample().Sugar()
	client := &metadata.Client{}
	serv := MetadataServer{
		client: client,
		logger: logger,
	}
	serv.GetTaskRunDetails(ctx)

	var actualErrorMsg string
	expectedMsg := "Error 400: Failed to fetch GetTaskRunDetails - taskRunId is not a number!"
	_ = json.Unmarshal(mockRecorder.Body.Bytes(), &actualErrorMsg)

	assert.Equal(t, http.StatusBadRequest, mockRecorder.Code)
	assert.Equal(t, expectedMsg, actualErrorMsg)
}

func TestGetTriggers(t *testing.T) {
	mockRecorder := httptest.NewRecorder()
	ctx := GetTestGinContext(mockRecorder)
	body := map[string]interface{}{"searchText": "asdf"}
	MockJsonPost(ctx, nil, body)

	logger := zap.NewExample().Sugar()
	client := &metadata.Client{}
	serv := MetadataServer{
		client: client,
		logger: logger,
	}
	serv.GetTriggers(ctx)

	var data []TriggerResponse
	json.Unmarshal(mockRecorder.Body.Bytes(), &data)

	assert.Equal(t, http.StatusOK, mockRecorder.Code)
	assert.Equal(t, len(data), 0)
}

func TestGetTriggerBadBind(t *testing.T) {
	mockRecorder := httptest.NewRecorder()
	ctx := GetTestGinContext(mockRecorder)
	body := map[string]interface{}{"searchText": 101}
	MockJsonPost(ctx, nil, body)

	logger := zap.NewExample().Sugar()
	client := &metadata.Client{}
	serv := MetadataServer{
		client: client,
		logger: logger,
	}
	serv.GetTriggers(ctx)

	var data interface{}
	json.Unmarshal(mockRecorder.Body.Bytes(), &data)

	assert.Equal(t, http.StatusBadRequest, mockRecorder.Code)
	assert.Equal(t, "Error 400: Failed to fetch GetTriggers - Error binding the request body", data)
}

func TestPostTrigger(t *testing.T) {
	mockRecorder := httptest.NewRecorder()
	ctx := GetTestGinContext(mockRecorder)
	body := map[string]interface{}{"triggerName": "testTrigger", "schedule": "*/10 * * * *"}
	MockJsonPost(ctx, nil, body)

	logger := zap.NewExample().Sugar()
	client := &metadata.Client{}
	serv := MetadataServer{
		client: client,
		logger: logger,
	}
	serv.PostTrigger(ctx)

	var data bool
	json.Unmarshal(mockRecorder.Body.Bytes(), &data)

	assert.Equal(t, http.StatusCreated, mockRecorder.Code)
	assert.Equal(t, true, data)
}

func TestPostTriggerBadBind(t *testing.T) {
	mockRecorder := httptest.NewRecorder()
	ctx := GetTestGinContext(mockRecorder)
	body := map[string]interface{}{"triggerName": 1, "schedule": 2}
	MockJsonPost(ctx, nil, body)

	logger := zap.NewExample().Sugar()
	client := &metadata.Client{}
	serv := MetadataServer{
		client: client,
		logger: logger,
	}
	serv.PostTrigger(ctx)

	var data interface{}
	json.Unmarshal(mockRecorder.Body.Bytes(), &data)

	assert.Equal(t, http.StatusBadRequest, mockRecorder.Code)
	assert.Equal(t, "Error 400: Failed to fetch PostTrigger - Error binding the request body", data)
}

func TestGetTriggerDetail(t *testing.T) {
	mockRecorder := httptest.NewRecorder()
	ctx := GetTestGinContext(mockRecorder)
	var id, name, schedule string = "1", "testName", "testSchedule"
	params := []gin.Param{
		{
			Key:   "triggerId",
			Value: id,
		},
	}
	MockJsonGet(ctx, params)

	logger := zap.NewExample().Sugar()
	client := &metadata.Client{}
	serv := MetadataServer{
		client: client,
		logger: logger,
	}

	CreateDummyTestTrigger(id, name, schedule, true)
	serv.GetTriggerDetails(ctx)

	var data TriggerDetailResponse
	json.Unmarshal(mockRecorder.Body.Bytes(), &data)

	assert.Equal(t, http.StatusOK, mockRecorder.Code)
	assert.Equal(t, id, data.Trigger.ID)
	assert.Equal(t, name, data.Trigger.Name)
	assert.Equal(t, schedule, data.Trigger.Schedule)
	assert.NotZero(t, data.Owner)
	assert.NotZero(t, data.Resources)
}

func TestGetTriggerDetailBadBind(t *testing.T) {
	mockRecorder := httptest.NewRecorder()
	ctx := GetTestGinContext(mockRecorder)
	params := []gin.Param{
		{
			Key:   "bad_param",
			Value: "1",
		},
	}
	MockJsonGet(ctx, params)

	logger := zap.NewExample().Sugar()
	client := &metadata.Client{}
	serv := MetadataServer{
		client: client,
		logger: logger,
	}
	serv.GetTriggerDetails(ctx)

	var data string
	json.Unmarshal(mockRecorder.Body.Bytes(), &data)

	assert.Equal(t, http.StatusBadRequest, mockRecorder.Code)
	assert.Equal(t, "Error 400: Failed to fetch GetTriggerDetails - Could not find the triggerId parameter", data)

}

func TestDeleteTrigger(t *testing.T) {
	mockRecorder := httptest.NewRecorder()
	ctx := GetTestGinContext(mockRecorder)
	var id, name, schedule string = uuid.New().String(), "testName", "testSchedule"
	params := []gin.Param{
		{
			Key:   "triggerId",
			Value: id,
		},
	}
	MockJsonGet(ctx, params)

	logger := zap.NewExample().Sugar()
	client := &metadata.Client{}
	serv := MetadataServer{
		client: client,
		logger: logger,
	}

	CreateDummyTestTrigger(id, name, schedule, false)
	serv.DeleteTrigger(ctx)

	var data bool
	json.Unmarshal(mockRecorder.Body.Bytes(), &data)

	assert.Equal(t, http.StatusOK, mockRecorder.Code)
	assert.Equal(t, true, data)
}

func TestDeleteTriggerResourceValidation(t *testing.T) {
	mockRecorder := httptest.NewRecorder()
	ctx := GetTestGinContext(mockRecorder)
	var id, name, schedule string = uuid.New().String(), "testName", "testSchedule"
	params := []gin.Param{
		{
			Key:   "triggerId",
			Value: id,
		},
	}
	MockJsonGet(ctx, params)

	logger := zap.NewExample().Sugar()
	client := &metadata.Client{}
	serv := MetadataServer{
		client: client,
		logger: logger,
	}

	CreateDummyTestTrigger(id, name, schedule, true)
	serv.DeleteTrigger(ctx)

	var data string
	json.Unmarshal(mockRecorder.Body.Bytes(), &data)

	assert.Equal(t, http.StatusBadRequest, mockRecorder.Code)
	assert.Equal(t, "Error 400: Failed to fetch DeleteTrigger - Cannot delete trigger with resource items", data)
}
