package metadata

import (
	"context"
	"github.com/featureform/fferr"
	"github.com/featureform/ffsync"
	"github.com/featureform/metadata/proto"
	s "github.com/featureform/scheduling"
	sch "github.com/featureform/scheduling/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	grpcstatus "google.golang.org/grpc/status"
	tspb "google.golang.org/protobuf/types/known/timestamppb"
	"io"
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
		return nil, fferr.NewUnimplementedErrorf("could not convert trigger type: %T", trigger)
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
		return nil, fferr.NewUnimplementedErrorf("could not convert target proto type: %T", target)
	}
}

func wrapProtoTaskMetadata(task *sch.TaskMetadata) (s.TaskMetadata, error) {
	tid := s.NewTaskRunIdFromString(task.GetId().Id)

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
	rid := s.NewTaskRunIdFromString(run.RunID.String())
	tid := s.NewTaskIdFromString(run.TaskID.String())

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
	task, err := t.GrpcConn.GetTaskByID(context.Background(), &sch.TaskID{Id: id.String()})
	if err != nil {
		return s.TaskMetadata{}, err
	}
	metadata, err := wrapProtoTaskMetadata(task)
	if err != nil {
		return s.TaskMetadata{}, err
	}
	return metadata, nil
}

type runStream interface {
	grpc.ClientStream
}

func (t *Tasks) genericParseRuns(client runStream) (s.TaskRunList, error) {
	runs := s.TaskRunList{}
	for {
		var msg sch.TaskRunMetadata
		err := client.RecvMsg(&msg)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		wrappedRun, err := wrapProtoTaskRunMetadata(&msg)
		if err != nil {
			return s.TaskRunList{}, err
		}
		runs = append(runs, wrappedRun)

	}
	return runs, nil
}

func (t *Tasks) GetAllRuns() (s.TaskRunList, error) {
	client, err := t.GrpcConn.GetAllRuns(context.Background(), &sch.Empty{})
	if err != nil {
		return s.TaskRunList{}, err
	}

	runs, err := t.genericParseRuns(client)
	if err != nil {
		return s.TaskRunList{}, err
	}
	return runs, nil
}

func (t *Tasks) GetRuns(id s.TaskID) (s.TaskRunList, error) {
	client, err := t.GrpcConn.GetRuns(context.Background(), &sch.TaskID{Id: id.String()})
	if err != nil {
		return s.TaskRunList{}, err
	}

	runs, err := t.genericParseRuns(client)
	if err != nil {
		return s.TaskRunList{}, err
	}
	return runs, nil
}

func (t *Tasks) GetLatestRun(id s.TaskID) (s.TaskRunMetadata, error) {
	run, err := t.GrpcConn.GetLatestRun(context.Background(), &sch.TaskID{Id: id.String()})
	if err != nil {
		return s.TaskRunMetadata{}, err
	}
	wrappedRun, err := wrapProtoTaskRunMetadata(run)
	if err != nil {
		return s.TaskRunMetadata{}, err
	}
	return wrappedRun, nil
}

func (t *Tasks) SetRunStatus(taskID s.TaskID, runID s.TaskRunID, status s.Status, errMsg error) error {
	// This is gross
	msg := ""
	if errMsg != nil {
		msg = errMsg.Error()
	}
	errorStatus, ok := grpcstatus.FromError(errMsg)
	errorProto := errorStatus.Proto()
	var errorStatusProto *proto.ErrorStatus
	if ok && errMsg != nil {
		errorStatusProto = &proto.ErrorStatus{Code: errorProto.Code, Message: errorProto.Message, Details: errorProto.Details}
	} else {
		errorStatusProto = nil
	}

	resourceStatus := proto.ResourceStatus{Status: proto.ResourceStatus_Status(status), ErrorMessage: msg, ErrorStatus: errorStatusProto}

	update := &sch.StatusUpdate{
		RunID:  &sch.RunID{Id: runID.String()},
		TaskID: &sch.TaskID{Id: taskID.String()},
		Status: &resourceStatus,
	}

	_, err := t.GrpcConn.SetRunStatus(context.Background(), update)
	if err != nil {
		return err
	}
	return nil
}

func (t *Tasks) AddRunLog(taskID s.TaskID, runID s.TaskRunID, msg string) error {
	log := &sch.Log{RunID: &sch.RunID{Id: runID.String()}, TaskID: &sch.TaskID{Id: taskID.String()}, Log: msg}
	_, err := t.GrpcConn.AddRunLog(context.Background(), log)
	if err != nil {
		return err
	}
	return nil
}

func (t *Tasks) EndRun(taskID s.TaskID, runID s.TaskRunID) error {
	update := &sch.RunEndTimeUpdate{
		RunID:  &sch.RunID{Id: runID.String()},
		TaskID: &sch.TaskID{Id: taskID.String()},
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
	runKey := ExecutorTaskRunLockPath(id)
	return tl.Locker.Lock(runKey)
}

func (tl *TaskLocker) UnlockRun(key ffsync.Key) error {
	return tl.Locker.Unlock(key)
}
