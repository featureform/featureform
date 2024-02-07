package scheduling

import (
	"testing"
	"time"
)

func TestSerializeTaskMetadata(t *testing.T) {
	taskTime := time.Now()
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

	if !TaskMetadataIsEqual(deserializeTask1, task1) {
		t.Fatalf("Wrong struct values: %v\nExpected: %v", deserializeTask1, task1)
	}

}

func TaskMetadataIsEqual(output, expected TaskMetadata) bool {
	return output.ID == expected.ID &&
		output.Name == expected.Name &&
		output.Type == expected.Type &&
		output.Target == expected.Target &&
		output.Date == expected.Date.Truncate(0)
}
