package metadata

import (
	"github.com/featureform/ffsync"
	pb "github.com/featureform/metadata/proto"
	s "github.com/featureform/scheduling"
	"google.golang.org/protobuf/types/known/anypb"
	"reflect"
	"testing"
	"time"
)

func Test_wrapTaskMetadataProto(t *testing.T) {
	id := ffsync.Uint64OrderedId(1)
	tests := []struct {
		name    string
		task    s.TaskMetadata
		wantErr bool
	}{
		{
			"Resource Creation Name Variant",
			s.TaskMetadata{
				ID:         s.TaskID(&id),
				Name:       "Some Name",
				TaskType:   s.ResourceCreation,
				TargetType: s.NameVariantTarget,
				Target: s.NameVariant{
					Name:         "name",
					Variant:      "Variant",
					ResourceType: FEATURE_VARIANT.String(),
				},
				DateCreated: time.Now().UTC(),
			},
			false,
		},
		{
			"Provider",
			s.TaskMetadata{
				ID:         s.TaskID(&id),
				Name:       "Some Name",
				TaskType:   s.HealthCheck,
				TargetType: s.ProviderTarget,
				Target: s.Provider{
					Name: "my provider",
				},
				DateCreated: time.Now().UTC(),
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proto, err := wrapTaskMetadataProto(tt.task)
			if (err != nil) != tt.wantErr {
				t.Errorf("wrapTaskMetadataProto() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			got, err := wrapProtoTaskMetadata(proto)
			if (err != nil) != tt.wantErr {
				t.Errorf("wrapProtoTaskMetadata() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(tt.task, got) {
				t.Errorf("wrapTaskMetadataProto() \ngiven = %#v \n  got = %#v", tt.task, got)
			}
		})
	}
}

func Test_wrapTaskRunMetadataProto(t *testing.T) {
	id := ffsync.Uint64OrderedId(1)
	tests := []struct {
		name    string
		run     s.TaskRunMetadata
		wantErr bool
	}{
		{
			"On Apply",
			s.TaskRunMetadata{
				ID:     s.TaskRunID(&id),
				TaskId: s.TaskID(&id),
				Trigger: s.OnApplyTrigger{
					TriggerName: "trigger_name",
				},
				TriggerType: s.OnApplyTriggerType,
				Status:      s.READY,
				StartTime:   time.Now().UTC(),
				EndTime:     time.Now().AddDate(0, 0, 1).UTC(),
				Logs:        []string{"log1", "log2"},
				Error:       "some string error",
				ErrorProto: &pb.ErrorStatus{
					Code:    1,
					Message: "Some error message",
					Details: []*anypb.Any{{Value: []byte("abcd")}},
				},
			},
			false,
		},
		{
			"Schedule",
			s.TaskRunMetadata{
				ID:     s.TaskRunID(&id),
				TaskId: s.TaskID(&id),
				Trigger: s.ScheduleTrigger{
					TriggerName: "trigger_name",
					Schedule:    "* * * * *",
				},
				TriggerType: s.ScheduleTriggerType,
				Status:      s.READY,
				StartTime:   time.Now().UTC(),
				EndTime:     time.Now().AddDate(0, 0, 1).UTC(),
				Logs:        []string{"log1", "log2"},
				Error:       "some string error",
				ErrorProto: &pb.ErrorStatus{
					Code:    1,
					Message: "Some error message",
					Details: []*anypb.Any{{Value: []byte("abcd")}},
				},
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proto, err := wrapTaskRunMetadataProto(tt.run)
			if (err != nil) != tt.wantErr {
				t.Errorf("wrapTaskRunMetadataProto() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			got, err := wrapProtoTaskRunMetadata(proto)
			if (err != nil) != tt.wantErr {
				t.Errorf("wrapProtoTaskRunMetadata() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(tt.run, got) {
				t.Errorf("wrapTaskRunMetadataProto() \ngiven = %v,\n  got = %v", tt.run, got)
			}
		})
	}
}
