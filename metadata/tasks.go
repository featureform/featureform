package metadata

import (
	"fmt"
	"github.com/featureform/ffsync"
	s "github.com/featureform/scheduling"
	"time"
)

type Tasks struct {
	TaskManager s.TaskMetadataManager
	Locker      ffsync.Locker
}

func (t *Tasks) GetTaskByID(id s.TaskID) (s.TaskMetadata, error) {
	return t.TaskManager.GetTaskByID(id)
}

func (t *Tasks) GetAllRuns() (s.TaskRunList, error) {
	return t.TaskManager.GetAllTaskRuns()
}

func (t *Tasks) GetRuns(id s.TaskID) (s.TaskRunList, error) {
	return t.TaskManager.GetTaskRunMetadata(id)
}

func (t *Tasks) GetLatestRun(id s.TaskID) (s.TaskRunMetadata, error) {
	return t.TaskManager.GetLatestRun(id)
}

func (t *Tasks) SetRunStatus(taskID s.TaskID, runID s.TaskRunID, status s.Status, err error) error {
	return t.SetRunStatus(taskID, runID, status, err)
}

func (t *Tasks) AddRunLog(taskID s.TaskID, runID s.TaskRunID, msg string) error {
	return t.TaskManager.AppendRunLog(taskID, runID, msg)
}

func (t *Tasks) EndRun(taskID s.TaskID, runID s.TaskRunID) error {
	return t.TaskManager.SetRunEndTime(runID, taskID, time.Now())
}

func (t *Tasks) LockRun(id s.TaskRunID) (ffsync.Key, error) {
	//Change this to template later
	runKey := fmt.Sprintf("/runlock/%s", id.String())
	return t.Locker.Lock(runKey)
}

func (t *Tasks) UnlockRun(key ffsync.Key) error {
	return t.Locker.Unlock(key)
}
