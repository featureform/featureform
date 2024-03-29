package metadata

import (
	"github.com/featureform/scheduling"
	"testing"
)

func TestExecutorTaskRunLockPath(t *testing.T) {
	type args struct {
		id scheduling.TaskRunID
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{"Zero", args{scheduling.NewTaskRunIdFromString(0)}, "/runlock/0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ExecutorTaskRunLockPath(tt.args.id); got != tt.want {
				t.Errorf("ExecutorTaskRunLockPath() = %v, want %v", got, tt.want)
			}
		})
	}
}
