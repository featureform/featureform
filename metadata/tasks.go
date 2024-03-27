package metadata

import (
	"context"
	"fmt"
	"github.com/featureform/ffsync"
	"github.com/featureform/metadata/proto"
	s "github.com/featureform/scheduling"
	sch "github.com/featureform/scheduling/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	tspb "google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

type Empty sch.Empty

type Tasks struct {
	Logger   *zap.SugaredLogger
	conn     *grpc.ClientConn
	GrpcConn sch.TasksClient
}

func convertProtoTriggerType(trigger interface{}) (s.Trigger, error) {
	switch t := trigger.(type) {
	case *sch.TaskRunMetadata_Apply:
		return s.OnApplyTrigger{TriggerName: t.Apply.Name}, nil
	case *sch.TaskRunMetadata_Schedule:
		return s.ScheduleTrigger{TriggerName: t.Schedule.Name, Schedule: t.Schedule.Schedule}, nil
	default:
		return nil, fmt.Errorf("unimplemented trigger %T", trigger)
	}
}

func convertProtoTarget(target interface{}) (s.TaskTarget, error) {
	switch t := target.(type) {
	case *sch.TaskMetadata_NameVariant:
		return s.NameVariant{
			Name:         t.NameVariant.ResourceID.Resource.Name,
			Variant:      t.NameVariant.ResourceID.Resource.Variant,
			ResourceType: t.NameVariant.ResourceID.ResourceType.String(),
		}, nil
	case *sch.TaskMetadata_Provider:
		return s.Provider{
			Name: t.Provider.Name,
		}, nil
	default:
		return nil, fmt.Errorf("unimplemented proto target %T", t)
	}
}

func wrapProtoTaskMetadata(task *sch.TaskMetadata) (s.TaskMetadata, error) {
	id := ffsync.Uint64OrderedId(task.GetId().Id)
	tid := s.TaskID(&id)

	t, err := convertProtoTarget(task.Target)
	if err != nil {
		return s.TaskMetadata{}, err
	}

	return s.TaskMetadata{
		ID:          tid,
		Name:        task.Name,
		TaskType:    s.TaskType(task.Type),
		Target:      t,
		TargetType:  s.TargetType(task.TargetType),
		DateCreated: task.Created.AsTime(),
	}, nil
}

func wrapProtoTaskRunMetadata(run *sch.TaskRunMetadata) (s.TaskRunMetadata, error) {
	id := ffsync.Uint64OrderedId(run.TaskID.Id)
	rid := s.TaskRunID(&id)
	id = ffsync.Uint64OrderedId(run.RunID.Id)
	tid := s.TaskID(&id)

	t, err := convertProtoTriggerType(run.GetTrigger())
	if err != nil {
		return s.TaskRunMetadata{}, err
	}
	return s.TaskRunMetadata{
		ID:          rid,
		TaskId:      tid,
		Name:        run.Name,
		Trigger:     t,
		TriggerType: s.TriggerType(run.TriggerType),
		Status:      s.Status(run.Status.Status),
		StartTime:   run.StartTime.AsTime(),
		EndTime:     run.EndTime.AsTime(),
		Logs:        run.Logs,
		Error:       run.Status.ErrorMessage,
		ErrorProto:  run.Status.ErrorStatus,
	}, nil
}

func (t *Tasks) GetTaskByID(id s.TaskID) (s.TaskMetadata, error) {
	task, err := t.GrpcConn.GetTaskByID(context.Background(), &sch.TaskID{Id: id.Value().(uint64)})
	if err != nil {
		return s.TaskMetadata{}, err
	}
	metadata, err := wrapProtoTaskMetadata(task)
	if err != nil {
		return s.TaskMetadata{}, err
	}
	return metadata, nil
}

func (t *Tasks) GetAllRuns() (s.TaskRunList, error) {
	runs, err := t.GrpcConn.GetAllRuns(context.Background(), &sch.Empty{})
	if err != nil {
		return s.TaskRunList{}, err
	}

	wrapped := s.TaskRunList{}
	for _, run := range runs.GetRuns() {
		wrappedRun, err := wrapProtoTaskRunMetadata(run)
		if err != nil {
			return nil, err
		}
		wrapped = append(wrapped, wrappedRun)
	}
	return wrapped, nil
}

func (t *Tasks) GetRuns(id s.TaskID) (s.TaskRunList, error) {
	runs, err := t.GrpcConn.GetRuns(context.Background(), &sch.TaskID{Id: id.Value().(uint64)})
	if err != nil {
		return s.TaskRunList{}, err
	}

	wrapped := s.TaskRunList{}
	for _, run := range runs.GetRuns() {
		wrappedRun, err := wrapProtoTaskRunMetadata(run)
		if err != nil {
			return nil, err
		}
		wrapped = append(wrapped, wrappedRun)
	}
	return wrapped, nil
}

func (t *Tasks) GetLatestRun(id s.TaskID) (s.TaskRunMetadata, error) {
	runs, err := t.GetRuns(id)
	if err != nil {
		return s.TaskRunMetadata{}, err
	}

	if len(runs) == 0 {
		return s.TaskRunMetadata{}, fmt.Errorf("No runs")
	}

	latestIdx := 0
	var latestTime time.Time
	for i, run := range runs {
		if i == 0 {
			latestTime = run.StartTime
		} else if latestTime.Before(run.StartTime) {
			latestTime = run.StartTime
			latestIdx = i
		}
	}
	return runs[latestIdx], nil
}

func (t *Tasks) SetRunStatus(taskID s.TaskID, runID s.TaskRunID, status s.Status, errMsg error) error {
	// Fill this in
	msg := ""
	if errMsg != nil {
		msg = errMsg.Error()
	}
	update := &sch.StatusUpdate{
		RunID:  &sch.RunID{Id: runID.Value().(uint64)},
		TaskID: &sch.TaskID{Id: taskID.Value().(uint64)},
		Status: &proto.ResourceStatus{Status: proto.ResourceStatus_Status(status), ErrorMessage: msg},
	}
	_, err := t.GrpcConn.SetRunStatus(context.Background(), update)
	if err != nil {
		return err
	}
	return nil
}

func (t *Tasks) AddRunLog(taskID s.TaskID, runID s.TaskRunID, msg string) error {
	log := &sch.Log{RunID: &sch.RunID{Id: runID.Value().(uint64)}, TaskID: &sch.TaskID{Id: taskID.Value().(uint64)}, Log: msg}
	_, err := t.GrpcConn.AddRunLog(context.Background(), log)
	if err != nil {
		return err
	}
	return nil
}

func (t *Tasks) EndRun(taskID s.TaskID, runID s.TaskRunID) error {
	update := &sch.RunEndTimeUpdate{
		RunID:  &sch.RunID{Id: runID.Value().(uint64)},
		TaskID: &sch.TaskID{Id: taskID.Value().(uint64)},
		End:    tspb.Now(),
	}
	_, err := t.GrpcConn.SetRunEndTime(context.Background(), update)
	if err != nil {
		return err
	}
	return nil
}

type TaskLocker struct {
	Locker ffsync.Locker
}

func (tl *TaskLocker) LockRun(id s.TaskRunID) (ffsync.Key, error) {
	runKey := fmt.Sprintf("/runlock/%s", id.String())
	return tl.Locker.Lock(runKey)
}

func (tl *TaskLocker) UnlockRun(key ffsync.Key) error {
	return tl.Locker.Unlock(key)
}
