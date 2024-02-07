package scheduling

import (
	"testing"
	"time"
)

func TestSerializeTaskMetadata(t *testing.T) {
	taskTime := time.Now().Format("2006-01-02 15:04:05")
	task1 := TaskMetadata{
		ID:   1,
		Name: "test",
		Type: ResourceCreation,
		Target: Provider{
			Name:       "test",
			TargetType: ProviderTarget,
		},
		Date: taskTime,
	}

	serializeTask1, err := task1.ToJSON()
	if err != nil {
		t.Errorf("failed to serialize task metadata: %v", err)
	}

	deserializeTask1 := TaskMetadata{}
	if err := deserializeTask1.FromJSON(serializeTask1); err != nil {
		t.Errorf("failed to deserialize task metadata: %v", err)
	}

	if !TaskMetadataIsEqual(task1, deserializeTask1) {
		t.Fatalf("Wrong struct values: %v\nExpected: %v", deserializeTask1, task1)
	}

}

func TaskMetadataIsEqual(a, b TaskMetadata) bool {
	return a.ID == b.ID &&
		a.Name == b.Name &&
		a.Type == b.Type &&
		a.Target == b.Target &&
		a.Date == b.Date
}
