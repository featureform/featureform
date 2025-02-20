// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package coordinator

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/featureform/coordinator/spawner"
	"github.com/featureform/coordinator/tasks"
	"github.com/featureform/fferr"
	"github.com/featureform/ffsync"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	"github.com/featureform/provider"
	"github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/provider/types"
	ptypes "github.com/featureform/provider/types"
	s "github.com/featureform/scheduling"
	"github.com/stretchr/testify/mock"
)

func uintID(id uint) ffsync.Uint64OrderedId {
	return ffsync.Uint64OrderedId(id)
}

type MyMockedKey struct {
}

func (k MyMockedKey) Owner() string {
	return ""
}

func (k MyMockedKey) Key() string {
	return ""
}

type MyMockedLocker struct {
	mock.Mock
}

func (l *MyMockedLocker) Lock(ctx context.Context, lock string, wait bool) (ffsync.Key, error) {
	args := l.Called(lock, wait)
	return MyMockedKey{}, args.Error(0)
}

func (l *MyMockedLocker) Unlock(ctx context.Context, key ffsync.Key) error {
	return nil
}
func (l *MyMockedLocker) Close() {

}

type MyMockedTaskClient struct {
	mock.Mock
}

func (m MyMockedTaskClient) CreateRun(name string, id s.TaskID, trigger s.Trigger) (s.TaskRunID, error) {
	//TODO implement me
	panic("implement me")
}

func (m MyMockedTaskClient) SyncUnfinishedRuns() error {
	return nil
}

func (m MyMockedTaskClient) GetUnfinishedRuns() (s.TaskRunList, error) {
	//TODO implement me
	panic("implement me")
}

func (m MyMockedTaskClient) GetTaskByID(id s.TaskID) (s.TaskMetadata, error) {
	//TODO implement me
	panic("implement me")
}

func (m MyMockedTaskClient) WatchForCancel(tid s.TaskID, id s.TaskRunID) (chan s.Status, chan error) {
	args := m.Called(tid, id)
	return args.Get(0).(chan s.Status), args.Get(1).(chan error)
}

func (m MyMockedTaskClient) GetAllRuns() (s.TaskRunList, error) {
	//TODO implement me
	panic("implement me")
}

func (m MyMockedTaskClient) GetRuns(id s.TaskID) (s.TaskRunList, error) {
	//TODO implement me
	panic("implement me")
}

func (m MyMockedTaskClient) GetRun(tid s.TaskID, id s.TaskRunID) (s.TaskRunMetadata, error) {
	args := m.Called(tid, id)
	return args.Get(0).(s.TaskRunMetadata), args.Error(1)
}

func (m MyMockedTaskClient) GetLatestRun(id s.TaskID) (s.TaskRunMetadata, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MyMockedTaskClient) SetRunStatus(tid s.TaskID, rid s.TaskRunID, status s.Status, errMsg error) error {
	args := m.Called(tid, rid, status)
	return args.Error(0)
}

func (m MyMockedTaskClient) AddRunLog(taskID s.TaskID, runID s.TaskRunID, msg string) error {
	//TODO implement me
	panic("implement me")
}

func (m MyMockedTaskClient) SetRunResumeID(taskID s.TaskID, runID s.TaskRunID, resumeID ptypes.ResumeID) error {
	//TODO implement me
	panic("implement me")
}

func (m MyMockedTaskClient) EndRun(tid s.TaskID, rid s.TaskRunID) error {
	args := m.Called(tid, rid)
	return args.Error(0)
}

type MyMockedSpawner struct {
	mock.Mock
}

func NewExecutor(locker ffsync.Locker, client metadata.Client, logger logging.Logger) *Executor {
	return &Executor{
		locker:   &metadata.TaskLocker{Locker: locker},
		metadata: &client,
		spawner:  &spawner.MemoryJobSpawner{},
		logger:   logger,
		config:   ExecutorConfig{DependencyPollInterval: 1 * time.Second},
	}
}

// TestExecutorRunLocked tests that the executor will return nil if the task is already locked.
// If the same run is run twice at the same time, the second attempt should gracefully exit
func TestExecutorRunLocked(t *testing.T) {
	locker := new(MyMockedLocker)
	taskClient := new(MyMockedTaskClient)
	logger := logging.NewTestLogger(t)

	expectedTaskErr := fferr.NewKeyAlreadyLockedError("/tasklock/1", "/tasklock/1", nil)
	locker.On("Lock", "/tasklock/1", false).Return(expectedTaskErr)
	expectedRunErr := fferr.NewKeyAlreadyLockedError("/runlock/1", "/runlock/1", nil)
	locker.On("Lock", "/runlock/1", false).Return(expectedRunErr)

	client := metadata.Client{
		Logger: logger,
		Tasks:  taskClient,
	}
	e := NewExecutor(locker, client, logger)

	err := e.RunTask(s.TaskID(uintID(1)), s.TaskRunID(uintID(1)))
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
}

// TestExecutorRunFetchFailed tests that if the lock is failed to obtain, but it is not because its already locked
// then the error can be recorded.
func TestExecutorRunFetchFailed(t *testing.T) {
	locker := new(MyMockedLocker)
	taskClient := new(MyMockedTaskClient)
	logger := logging.NewTestLogger(t)

	expectedErr := fferr.NewInternalErrorf("Failed to fetch task run")
	locker.On("Lock", "/tasklock/1", false).Return(nil)
	locker.On("Lock", "/runlock/1", false).Return(nil)

	taskClient.On("GetRun", s.TaskID(uintID(1)), s.TaskRunID(uintID(1))).Return(s.TaskRunMetadata{}, expectedErr)

	client := metadata.Client{
		Logger: logger,
		Tasks:  taskClient,
	}
	e := NewExecutor(locker, client, logger)

	err := e.RunTask(s.TaskID(uintID(1)), s.TaskRunID(uintID(1)))
	if !reflect.DeepEqual(err, expectedErr) {
		t.Errorf("Expected error %v, got %v", expectedErr, err)
	}
}

// TestExecutorRunComplete tests that the executor will exit gracefully if the given run is already complete.
func TestExecutorRunComplete(t *testing.T) {
	locker := new(MyMockedLocker)
	taskClient := new(MyMockedTaskClient)
	logger := logging.NewTestLogger(t)

	locker.On("Lock", "/tasklock/1", false).Return(nil)
	locker.On("Lock", "/runlock/1", false).Return(nil)

	type testCase struct {
		Name   string
		Status s.Status
	}

	testCases := []testCase{
		{
			Name:   "Failed",
			Status: s.FAILED,
		},
		{
			Name:   "Cancelled",
			Status: s.CANCELLED,
		},
		{
			Name:   "Ready",
			Status: s.READY,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			// When fetching the run metadata, return the given status
			taskClient.On("GetRun", s.TaskID(uintID(1)), s.TaskRunID(uintID(1))).Return(s.TaskRunMetadata{
				ID:     s.TaskID(uintID(1)),
				Status: tc.Status,
			}, nil)

			client := metadata.Client{
				Logger: logger,
				Tasks:  taskClient,
			}
			e := NewExecutor(locker, client, logger)

			err := e.RunTask(s.TaskID(uintID(1)), s.TaskRunID(uintID(1)))
			if err != nil {
				t.Errorf("Expected no error, got %v", err)
			}
		})
	}
}

func createDag(t *testing.T) s.TaskDAG {
	// Create task DAG where task 1 is dependent on task 2, and task 2 has a run "run 2"
	taskDag, err := s.NewTaskDAG()
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = taskDag.AddDependency(s.TaskID(uintID(1)), s.TaskID(uintID(2)))
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = taskDag.SetTaskRunID(s.TaskID(uintID(2)), s.TaskRunID(uintID(2)))
	if err != nil {
		t.Fatalf(err.Error())
	}
	return taskDag
}

// TestExecutorWaitForFailedDependencies tests behavior when a task has failed dependencies
func TestExecutorWaitForFailedDependencies(t *testing.T) {
	locker := new(MyMockedLocker)
	taskClient := new(MyMockedTaskClient)
	logger := logging.NewTestLogger(t)

	locker.On("Lock", "/tasklock/1", false).Return(nil)
	locker.On("Lock", "/runlock/1", false).Return(nil)

	// Create task DAG where task 1 is dependent on task 2, and task 2 has a run "run 2"
	taskDag := createDag(t)

	target := s.NameVariant{Name: "resname", Variant: "resvariant", ResourceType: "FEATURE_VARIANT"}
	depTarget := s.NameVariant{Name: "resname2", Variant: "resvariant2", ResourceType: "SOURCE_VARIANT"}

	// Set the status of the current run to PENDING
	taskClient.On(
		"GetRun",
		s.TaskID(uintID(1)),
		s.TaskRunID(uintID(1)),
	).Return(
		s.TaskRunMetadata{
			ID:     s.TaskRunID(uintID(1)),
			TaskId: s.TaskID(uintID(1)),
			Dag:    taskDag,
			Status: s.PENDING,
			Target: target,
		}, nil)

	// Get set status of dependent run as PENDING
	taskClient.On(
		"GetRun",
		s.TaskID(uintID(1)),
		s.TaskRunID(uintID(2)),
	).Return(
		s.TaskRunMetadata{
			ID:     s.TaskID(uintID(2)),
			Status: s.PENDING,
			Target: depTarget,
		}, nil).Once()

	// On the second status check for the dependent run, return the status as FAILED
	taskClient.On(
		"GetRun",
		s.TaskID(uintID(1)),
		s.TaskRunID(uintID(2)),
	).Return(
		s.TaskRunMetadata{
			ID:     s.TaskID(uintID(2)),
			Status: s.FAILED,
			Target: depTarget,
		}, nil).Once()

	// Expect the current run to be set to CANCELLED
	taskClient.On(
		"SetRunStatus",
		s.TaskID(uintID(1)),
		s.TaskRunID(uintID(1)),
		s.CANCELLED,
	).Return(nil)

	client := metadata.Client{
		Logger: logger,
		Tasks:  taskClient,
	}
	e := NewExecutor(locker, client, logger)

	err := e.RunTask(s.TaskID(uintID(1)), s.TaskRunID(uintID(1)))
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

// TestExecutorWaitForCancelledDependencies tests behavior when a dependency has been cancelled.
func TestExecutorWaitForCancelledDependencies(t *testing.T) {
	locker := new(MyMockedLocker)
	taskClient := new(MyMockedTaskClient)
	logger := logging.NewTestLogger(t)

	locker.On("Lock", "/tasklock/1", false).Return(nil)
	locker.On("Lock", "/runlock/1", false).Return(nil)

	// Create task DAG where task 1 is dependent on task 2, and task 2 has a run "run 2"
	taskDag := createDag(t)

	target := s.NameVariant{Name: "resname", Variant: "resvariant", ResourceType: "FEATURE_VARIANT"}
	depTarget := s.NameVariant{Name: "resname2", Variant: "resvariant2", ResourceType: "SOURCE_VARIANT"}

	// Set the status of the current run to PENDING
	taskClient.On(
		"GetRun",
		s.TaskID(uintID(1)),
		s.TaskRunID(uintID(1)),
	).Return(
		s.TaskRunMetadata{
			ID:     s.TaskRunID(uintID(1)),
			TaskId: s.TaskID(uintID(1)),
			Dag:    taskDag,
			Status: s.PENDING,
			Target: target,
		}, nil)

	// Get set status of dependent run as PENDING
	taskClient.On(
		"GetRun",
		s.TaskID(uintID(1)),
		s.TaskRunID(uintID(2)),
	).Return(
		s.TaskRunMetadata{
			ID:     s.TaskID(uintID(2)),
			Status: s.PENDING,
			Target: depTarget,
		}, nil).Once()

	// Set status of dependent run to CANCELLED
	taskClient.On(
		"GetRun",
		s.TaskID(uintID(1)),
		s.TaskRunID(uintID(2)),
	).Return(
		s.TaskRunMetadata{
			ID:     s.TaskID(uintID(2)),
			Status: s.CANCELLED,
			Target: depTarget,
		}, nil).Once()

	// Expect status change to be CANCELLED
	taskClient.On(
		"SetRunStatus",
		s.TaskID(uintID(1)),
		s.TaskRunID(uintID(1)),
		s.CANCELLED,
	).Return(nil)

	client := metadata.Client{
		Logger: logger,
		Tasks:  taskClient,
	}
	e := NewExecutor(locker, client, logger)

	err := e.RunTask(s.TaskID(uintID(1)), s.TaskRunID(uintID(1)))
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func setDependenciesToReady(t *testing.T, taskClient *MyMockedTaskClient) {
	// Create task DAG where task 1 is dependent on task 2, and task 2 has a run "run 2"
	taskDag := createDag(t)

	target := s.NameVariant{Name: "resname", Variant: "resvariant", ResourceType: tasks.Noop}
	// Get set status of current run
	taskClient.On(
		"GetRun",
		s.TaskID(uintID(1)),
		s.TaskRunID(uintID(1))).Return(
		s.TaskRunMetadata{
			ID:     s.TaskRunID(uintID(1)),
			TaskId: s.TaskID(uintID(1)),
			Dag:    taskDag,
			Status: s.PENDING,
			Target: target,
		}, nil)

	// Get set status of dependent run as PENDING
	taskClient.On(
		"GetRun",
		s.TaskID(uintID(1)),
		s.TaskRunID(uintID(2)),
	).Return(
		s.TaskRunMetadata{
			ID:     s.TaskID(uintID(2)),
			Status: s.PENDING,
		}, nil).Once()

	// Set status of dependent run to READY
	taskClient.On("GetRun", s.TaskID(uintID(1)), s.TaskRunID(uintID(2))).Return(s.TaskRunMetadata{
		ID:     s.TaskID(uintID(2)),
		Status: s.READY,
	}, nil).Once()

	// Expect status change to be CANCELLED
	taskClient.On(
		"SetRunStatus",
		s.TaskID(uintID(1)),
		s.TaskRunID(uintID(1)),
		s.RUNNING,
	).Return(nil)
}

// TestExecutorCancelTask tests behavior when a task is cancelled.
func TestExecutorCancelTask(t *testing.T) {
	t.Skip("Need to resolve a nil pointer error with the channel")
	locker := new(MyMockedLocker)
	taskClient := new(MyMockedTaskClient)
	logger := logging.NewTestLogger(t)

	locker.On("Lock", "/tasklock/1", false).Return(nil)
	locker.On("Lock", "/runlock/1", false).Return(nil)

	setDependenciesToReady(t, taskClient)

	statusChan := make(chan s.Status)
	errorChan := make(chan error)
	// Set status of dependent run to READY
	taskClient.On(
		"WatchForCancel",
		s.TaskID(uintID(1)),
		s.TaskRunID(uintID(1)),
	).Return(statusChan, errorChan).Once()

	// Expect status change to be CANCELLED
	taskClient.On(
		"SetRunStatus",
		s.TaskID(uintID(1)),
		s.TaskRunID(uintID(1)),
		s.CANCELLED,
	).Return(nil)

	taskClient.On(
		"EndRun",
		s.TaskID(uintID(1)),
		s.TaskRunID(uintID(1)),
	).Return(nil)

	// Send a Cancel signal
	go func() {
		statusChan <- s.CANCELLED
	}()

	client := metadata.Client{
		Logger: logger,
		Tasks:  taskClient,
	}
	e := NewExecutor(locker, client, logger)

	err := e.RunTask(s.TaskID(uintID(1)), s.TaskRunID(uintID(1)))
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

// TestExecutorSucceedTask tests behavior when a task is successfully executed.
func TestExecutorSucceedTask(t *testing.T) {
	locker := new(MyMockedLocker)
	taskClient := new(MyMockedTaskClient)
	logger := logging.NewTestLogger(t)

	locker.On("Lock", "/tasklock/1", false).Return(nil)
	locker.On("Lock", "/runlock/1", false).Return(nil)

	setDependenciesToReady(t, taskClient)

	statusChan := make(chan s.Status)
	errorChan := make(chan error)
	// Set status of dependent run to READY
	taskClient.On("WatchForCancel", s.TaskID(uintID(1)), s.TaskRunID(uintID(1))).Return(statusChan, errorChan).Once()

	// Expect status change
	taskClient.On(
		"SetRunStatus",
		s.TaskID(uintID(1)),
		s.TaskRunID(uintID(1)),
		s.READY,
	).Return(nil)

	taskClient.On(
		"EndRun",
		s.TaskID(uintID(1)),
		s.TaskRunID(uintID(1)),
	).Return(nil)

	client := metadata.Client{
		Logger: logger,
		Tasks:  taskClient,
	}
	e := NewExecutor(locker, client, logger)

	err := e.RunTask(s.TaskID(uintID(1)), s.TaskRunID(uintID(1)))
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func startServ(ctx context.Context, t *testing.T) (*metadata.MetadataServer, string) {
	manager, err := s.NewMemoryTaskMetadataManager(ctx)
	if err != nil {
		panic(err.Error())
	}
	config := &metadata.Config{
		Logger:      logging.GetLoggerFromContext(ctx),
		TaskManager: manager,
	}
	serv, err := metadata.NewMetadataServer(ctx, config)
	if err != nil {
		panic(err)
	}
	// listen on a random port
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	go func() {
		if err := serv.ServeOnListener(lis); err != nil {
			panic(err)
		}
	}()
	return serv, lis.Addr().String()
}

func TestSourceTaskRun(t *testing.T) {
	ctx, logger := logging.NewTestContextAndLogger(t)
	locker, err := ffsync.NewMemoryLocker()
	if err != nil {
		t.Fatalf(err.Error())
	}

	serv, addr := startServ(ctx, t)
	defer serv.Stop()
	client, err := metadata.NewClient(addr, logger)
	if err != nil {
		t.Fatalf(err.Error())
	}

	e := NewExecutor(&locker, *client, logger)

	createSourcePreqResources(t, client)

	err = client.CreateSourceVariant(ctx, metadata.SourceDef{
		Name:    "sourceName",
		Variant: "sourceVariant",
		Definition: metadata.PrimaryDataSource{
			Location: metadata.SQLTable{
				Name: "mockPrimary",
			},
		},
		Owner:    "mockOwner",
		Provider: "mockProvider",
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	runs, err := client.Tasks.GetAllRuns()
	if err != nil {
		t.Fatalf(err.Error())
	}

	if len(runs) != 1 {
		t.Fatalf("Expected 1 run to be created, got: %d", len(runs))
	}

	err = e.RunTask(runs[0].TaskId, runs[0].ID)
	if err != nil {
		t.Fatalf(err.Error())
	}

}

func createSourcePreqResources(t *testing.T, client *metadata.Client) {
	err := client.CreateUser(context.Background(), metadata.UserDef{
		Name: "mockOwner",
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = client.CreateProvider(context.Background(), metadata.ProviderDef{
		Name: "mockProvider",
		Type: pt.MemoryOffline.String(),
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = client.CreateEntity(context.Background(), metadata.EntityDef{
		Name: "mockEntity",
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	runs, err := client.Tasks.GetAllRuns()
	if err != nil {
		t.Fatalf(err.Error())
	}

	if len(runs) != 0 {
		t.Fatalf("Expected 1 run to be created, got: %d", len(runs))
	}
}

func TestLabelTaskRun(t *testing.T) {
	ctx, logger := logging.NewTestContextAndLogger(t)
	locker, err := ffsync.NewMemoryLocker()
	if err != nil {
		t.Fatalf(err.Error())
	}

	serv, addr := startServ(ctx, t)
	defer serv.Stop()
	client, err := metadata.NewClient(addr, logger)
	if err != nil {
		panic(err)
	}

	sourceTaskRun := createPreqLabelResources(t, client)
	t.Log("Source Run:", sourceTaskRun)

	err = client.Tasks.SetRunStatus(sourceTaskRun.TaskId, sourceTaskRun.ID, s.RUNNING, nil)
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = client.Tasks.SetRunStatus(sourceTaskRun.TaskId, sourceTaskRun.ID, s.READY, nil)
	if err != nil {
		t.Fatalf(err.Error())
	}

	e := NewExecutor(&locker, *client, logger)

	err = client.CreateLabelVariant(ctx, metadata.LabelDef{
		Name:     "labelName",
		Variant:  "labelVariant",
		Owner:    "mockOwner",
		Provider: "mockProvider",
		Source:   metadata.NameVariant{Name: "sourceName", Variant: "sourceVariant"},
		Location: metadata.ResourceVariantColumns{
			Entity: "col1",
			Value:  "col2",
			Source: "mockTable",
		},
		Entity: "mockEntity",
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	runs, err := client.Tasks.GetAllRuns()
	if err != nil {
		t.Fatalf(err.Error())
	}

	if len(runs) != 2 {
		t.Fatalf("Expected 2 run to be created, got: %d", len(runs))
	}

	var labelTaskRun s.TaskRunMetadata
	for _, run := range runs {
		if sourceTaskRun.ID.String() != run.ID.String() {
			labelTaskRun = run
		}
	}

	err = e.RunTask(labelTaskRun.TaskId, labelTaskRun.ID)
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func createPreqLabelResources(t *testing.T, client *metadata.Client) s.TaskRunMetadata {
	err := client.CreateUser(context.Background(), metadata.UserDef{
		Name: "mockOwner",
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = client.CreateProvider(context.Background(), metadata.ProviderDef{
		Name: "mockProvider",
		Type: pt.MemoryOffline.String(),
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = client.CreateSourceVariant(context.Background(), metadata.SourceDef{
		Name:    "sourceName",
		Variant: "sourceVariant",
		Definition: metadata.PrimaryDataSource{
			Location: metadata.SQLTable{
				Name: "mockPrimary",
			},
		},
		Owner:    "mockOwner",
		Provider: "mockProvider",
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	source, err := client.GetSourceVariant(context.Background(), metadata.NameVariant{Name: "sourceName", Variant: "sourceVariant"})
	if err != nil {
		t.Fatalf(err.Error())
	}

	sourceProvider, err := source.FetchProvider(client, context.Background())
	if err != nil {
		t.Fatalf(err.Error())
	}

	p, err := provider.Get(pt.Type(sourceProvider.Type()), sourceProvider.SerializedConfig())
	if err != nil {
		t.Fatalf(err.Error())
	}

	store, err := p.AsOfflineStore()
	if err != nil {
		t.Fatalf(err.Error())
	}

	schema := provider.TableSchema{
		Columns: []provider.TableColumn{
			{Name: "col1", ValueType: types.String},
			{Name: "col2", ValueType: types.String},
		},
		SourceTable: "mockTable",
	}

	// Added this because we dont actually run the primary table registration before this test
	tableID := provider.ResourceID{Name: "sourceName", Variant: "sourceVariant", Type: provider.Primary}
	_, err = store.CreatePrimaryTable(tableID, schema)
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = client.CreateEntity(context.Background(), metadata.EntityDef{
		Name: "mockEntity",
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	runs, err := client.Tasks.GetAllRuns()
	if err != nil {
		t.Fatalf(err.Error())
	}

	if len(runs) != 1 {
		t.Fatalf("Expected 1 run to be created, got: %d", len(runs))
	}

	return runs[0]
}

func TestFeatureTaskRun(t *testing.T) {
	ctx, logger := logging.NewTestContextAndLogger(t)
	locker, err := ffsync.NewMemoryLocker()
	if err != nil {
		t.Fatalf(err.Error())
	}

	serv, addr := startServ(ctx, t)
	defer serv.Stop()
	client, err := metadata.NewClient(addr, logger)
	if err != nil {
		panic(err)
	}

	sourceTaskRun := createPreqLabelResources(t, client)
	t.Log("Source Run:", sourceTaskRun)

	e := NewExecutor(&locker, *client, logger)

	err = client.Tasks.SetRunStatus(sourceTaskRun.TaskId, sourceTaskRun.ID, s.RUNNING, nil)
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = client.Tasks.SetRunStatus(sourceTaskRun.TaskId, sourceTaskRun.ID, s.READY, nil)
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = client.CreateFeatureVariant(ctx, metadata.FeatureDef{
		Name:    "featureName",
		Variant: "featureVariant",
		Owner:   "mockOwner",
		Source:  metadata.NameVariant{Name: "sourceName", Variant: "sourceVariant"},
		Location: metadata.ResourceVariantColumns{
			Entity: "col1",
			Value:  "col2",
			Source: "mockTable",
		},
		Entity: "mockEntity",
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	runs, err := client.Tasks.GetAllRuns()
	if err != nil {
		t.Fatalf(err.Error())
	}

	if len(runs) != 2 {
		t.Fatalf("Expected 2 run to be created, got: %d", len(runs))
	}

	var featureTaskRun s.TaskRunMetadata
	for _, run := range runs {
		if sourceTaskRun.ID.String() != run.ID.String() {
			featureTaskRun = run
		}
	}
	err = e.RunTask(featureTaskRun.TaskId, featureTaskRun.ID)
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func TestTrainingSetTaskRun(t *testing.T) {
	ctx, logger := logging.NewTestContextAndLogger(t)
	locker, err := ffsync.NewMemoryLocker()
	if err != nil {
		t.Fatalf(err.Error())
	}

	serv, addr := startServ(ctx, t)
	defer serv.Stop()
	client, err := metadata.NewClient(addr, logger)
	if err != nil {
		panic(err)
	}

	e := NewExecutor(&locker, *client, logger)

	preReqTaskRuns := createPreqTrainingSetResources(t, client)

	for _, run := range preReqTaskRuns {
		err = client.Tasks.SetRunStatus(run.TaskId, run.ID, s.RUNNING, nil)
		if err != nil {
			t.Fatalf(err.Error())
		}
		err = client.Tasks.SetRunStatus(run.TaskId, run.ID, s.READY, nil)
		if err != nil {
			t.Fatalf(err.Error())
		}
	}

	err = client.CreateTrainingSetVariant(ctx, metadata.TrainingSetDef{
		Name:     "trainingSetName",
		Variant:  "trainingSetVariant",
		Owner:    "mockOwner",
		Provider: "mockProvider",
		Label:    metadata.NameVariant{Name: "labelName", Variant: "labelVariant"},
		Features: metadata.NameVariants{
			{Name: "featureName", Variant: "featureVariant"},
		},
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	runs, err := client.Tasks.GetAllRuns()
	if err != nil {
		t.Fatalf(err.Error())
	}

	if len(runs) != 4 {
		t.Fatalf("Expected 4 run to be created, got: %d", len(runs))
	}

	var trainingSetTaskRun s.TaskRunMetadata
	runDiff := difference(runs, preReqTaskRuns)
	if len(runDiff) != 1 {
		t.Fatalf("Expected 1 run to be different, got: %d", len(runDiff))
	}

	trainingSetTaskRun = runDiff[0]
	err = e.RunTask(trainingSetTaskRun.TaskId, trainingSetTaskRun.ID)
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func TestTrainingSetLastRun(t *testing.T) {
	ctx, logger := logging.NewTestContextAndLogger(t)
	locker, err := ffsync.NewMemoryLocker()
	if err != nil {
		t.Fatalf(err.Error())
	}

	serv, addr := startServ(ctx, t)
	defer serv.Stop()
	client, err := metadata.NewClient(addr, logger)
	if err != nil {
		panic(err)
	}

	e := NewExecutor(&locker, *client, logger)

	preReqTaskRuns := createPreqTrainingSetResources(t, client)

	for _, run := range preReqTaskRuns {
		err = client.Tasks.SetRunStatus(run.TaskId, run.ID, s.RUNNING, nil)
		if err != nil {
			t.Fatalf(err.Error())
		}
		err = client.Tasks.SetRunStatus(run.TaskId, run.ID, s.READY, nil)
		if err != nil {
			t.Fatalf(err.Error())
		}
	}

	err = client.CreateTrainingSetVariant(context.Background(), metadata.TrainingSetDef{
		Name:     "trainingSetName",
		Variant:  "trainingSetVariant",
		Owner:    "mockOwner",
		Provider: "mockProvider",
		Label:    metadata.NameVariant{Name: "labelName", Variant: "labelVariant"},
		Features: metadata.NameVariants{
			{Name: "featureName", Variant: "featureVariant"},
		},
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	runs, err := client.Tasks.GetAllRuns()
	if err != nil {
		t.Fatalf(err.Error())
	}

	fmt.Println("Last Success")
	for _, run := range runs {
		fmt.Println("ID", run.ID, run.LastSuccessful)
	}

	if len(runs) != 4 {
		t.Fatalf("Expected 4 run to be created, got: %d", len(runs))
	}

	var trainingSetTaskRun s.TaskRunMetadata
	runDiff := difference(runs, preReqTaskRuns)
	if len(runDiff) != 1 {
		t.Fatalf("Expected 1 run to be different, got: %d", len(runDiff))
	}

	trainingSetTaskRun = runDiff[0]
	err = e.RunTask(trainingSetTaskRun.TaskId, trainingSetTaskRun.ID)
	if err != nil {
		t.Fatalf(err.Error())
	}

	sortByIdAsc := func(s []s.TaskRunMetadata) {
		sort.Slice(s, func(i, j int) bool {
			return s[i].ID.Value().(uint64) < s[j].ID.Value().(uint64)
		})
	}

	sortByIdAsc(runs)

	for _, run := range runs {

		rid, err := client.Tasks.CreateRun("testRun", run.TaskId, nil)
		if err != nil {
			t.Fatalf("failed to create run: %s", err.Error())
		}
		err = e.RunTask(run.TaskId, rid)
		if err != nil {
			t.Fatalf(err.Error())
		}
	}

	runs, err = client.Tasks.GetAllRuns()
	if err != nil {
		t.Fatalf(err.Error())
	}

	expected := map[string]s.TaskRunID{
		"1": nil,
		"2": nil,
		"3": nil,
		"4": nil,
		"5": s.NewIntTaskRunID(1),
		"6": s.NewIntTaskRunID(2),
		"7": s.NewIntTaskRunID(3),
		"8": s.NewIntTaskRunID(4),
	}
	for _, run := range runs {
		if (expected[run.ID.String()] == nil && run.LastSuccessful != nil) || (expected[run.ID.String()] != nil && run.LastSuccessful == nil) {
			t.Errorf("Expected run (%s) to be nil, but got (%s)", run.ID, run.LastSuccessful)
		} else if expected[run.ID.String()] == nil && run.LastSuccessful == nil {
			continue
		} else if expected[run.ID.String()].String() != run.LastSuccessful.String() {
			t.Errorf("Expected run (%s) to have last successful run (%s), but got (%s)", run.ID, expected[run.ID.String()], run.LastSuccessful)
		}
	}
}

func createPreqTrainingSetResources(t *testing.T, client *metadata.Client) []s.TaskRunMetadata {
	err := client.CreateUser(context.Background(), metadata.UserDef{
		Name: "mockOwner",
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = client.CreateProvider(context.Background(), metadata.ProviderDef{
		Name: "mockProvider",
		Type: pt.MemoryOffline.String(),
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	p, err := provider.Get(pt.MemoryOffline, provider_config.SerializedConfig{})
	if err != nil {
		t.Fatalf(err.Error())
	}

	store, err := p.AsOfflineStore()
	if err != nil {
		t.Fatalf(err.Error())
	}

	_, err = store.RegisterResourceFromSourceTable(provider.ResourceID{Name: "labelName", Variant: "labelVariant", Type: provider.Label}, provider.ResourceSchema{})
	if err != nil {
		return nil
	}

	_, err = store.RegisterResourceFromSourceTable(provider.ResourceID{Name: "featureName", Variant: "featureVariant", Type: provider.Feature}, provider.ResourceSchema{})
	if err != nil {
		return nil
	}

	err = client.CreateSourceVariant(context.Background(), metadata.SourceDef{
		Name:    "sourceName",
		Variant: "sourceVariant",
		Definition: metadata.PrimaryDataSource{
			Location: metadata.SQLTable{
				Name: "mockPrimary",
			},
		},
		Owner:    "mockOwner",
		Provider: "mockProvider",
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = client.CreateEntity(context.Background(), metadata.EntityDef{
		Name: "mockEntity",
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = client.CreateFeatureVariant(context.Background(), metadata.FeatureDef{
		Name:    "featureName",
		Variant: "featureVariant",
		Owner:   "mockOwner",
		Source:  metadata.NameVariant{Name: "sourceName", Variant: "sourceVariant"},
		Location: metadata.ResourceVariantColumns{
			Entity: "col1",
			Value:  "col2",
			Source: "mockTable",
		},
		Entity: "mockEntity",
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = client.CreateLabelVariant(context.Background(), metadata.LabelDef{
		Name:     "labelName",
		Variant:  "labelVariant",
		Owner:    "mockOwner",
		Provider: "mockProvider",
		Source:   metadata.NameVariant{Name: "sourceName", Variant: "sourceVariant"},
		Location: metadata.ResourceVariantColumns{
			Entity: "col1",
			Value:  "col2",
			Source: "mockTable",
		},
		Entity: "mockEntity",
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	runs, err := client.Tasks.GetAllRuns()
	if err != nil {
		t.Fatalf(err.Error())
	}

	if len(runs) != 3 {
		t.Fatalf("Expected 3 run to be created, got: %d", len(runs))
	}

	return runs
}

func TestTaskRecovery(t *testing.T) {
	ctx, logger := logging.NewTestContextAndLogger(t)
	locker, err := ffsync.NewMemoryLocker()
	if err != nil {
		t.Fatalf(err.Error())
	}

	serv, addr := startServ(ctx, t)
	defer serv.Stop()
	client, err := metadata.NewClient(addr, logger)
	if err != nil {
		t.Fatalf(err.Error())
	}

	e := NewExecutor(&locker, *client, logger)

	createSourcePreqResources(t, client)

	err = client.CreateSourceVariant(ctx, metadata.SourceDef{
		// Using namevariant make (panic) leverages a conditional in the memory offline store implementation
		// in provider/offline.go
		Name:    "make",
		Variant: "panic",
		Definition: metadata.PrimaryDataSource{
			Location: metadata.SQLTable{
				Name: "mockPrimary",
			},
		},
		Owner:    "mockOwner",
		Provider: "mockProvider",
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	runs, err := client.Tasks.GetAllRuns()
	if err != nil {
		t.Fatalf(err.Error())
	}

	if len(runs) != 1 {
		t.Fatalf("Expected 1 run to be created, got: %d", len(runs))
	}

	err = e.RunTask(runs[0].TaskId, runs[0].ID)
	if err == nil {
		t.Fatalf("A panic should have been thrown and recovered to an error")
	}

}

func difference(a, b []s.TaskRunMetadata) []s.TaskRunMetadata {
	var diff []s.TaskRunMetadata

	for _, itemA := range a {
		found := false
		for _, itemB := range b {
			if itemA.ID.String() == itemB.ID.String() {
				found = true
				break
			}
		}
		if !found {
			diff = append(diff, itemA)
		}
	}

	return diff
}
