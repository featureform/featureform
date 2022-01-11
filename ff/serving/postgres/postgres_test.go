package postgres

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/featureform/serving/dataset"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func TestMinimalConnectionParam(t *testing.T) {
	params := ConnectionParams{
		User:   "abc",
		DBName: "db",
		Host:   "localhost",
	}
	str := params.urlString()
	expected := "postgres://abc:@localhost:5432/db?sslmode=disable"
	if str != expected {
		t.Errorf(
			"Connection Parameters mis-encoded.\nExpected: %s\n Got: %s\n",
			expected, str,
		)
	}
}

func TestMockedDatasetRead(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed to create mock: %s", err)
	}
	defer db.Close()

	mockRows := sqlmock.NewRows([]string{"feat_a", "feat_b", "label"}).
		AddRow(int64(1), float64(1.0), "abc")
	mock.ExpectQuery("SELECT .* FROM \"dataset\"").WillReturnRows(mockRows)

	exp := dataset.NewRow()
	exp.AddFeature(int64(1))
	exp.AddFeature(float64(1.0))
	exp.SetLabel("abc")

	schema := TableSchema{
		Label:    "label",
		Features: []string{"feat_a", "feat_b"},
	}

	logger, err := zap.NewDevelopment()
	if err != nil {
		t.Fatalf("Failed to create logger")
	}
	provider, err := NewProviderWithDB(db, logger.Sugar())
	if err != nil {
		t.Fatalf("Failed to create provider: %s", err)
	}
	key := provider.ToKey("dataset", schema)
	reader, err := provider.GetDatasetReader(key)
	if err != nil {
		t.Fatalf("Failed to get reader: %s", err)
	}

	for reader.Scan() {
		if !proto.Equal(reader.Row().Serialized(), exp.Serialized()) {
			t.Fatalf("Rows don't match")
		}
	}
	if reader.Err() != nil {
		t.Fatalf("Reader error: %s", reader.Err())
	}
}
