package search

import (
	"testing"

	"github.com/featureform/config"
	"github.com/featureform/helpers/postgres"
	"github.com/featureform/helpers/tests"
	"github.com/featureform/logging"
	"github.com/stretchr/testify/require"
)

func TestSearchTablesTrigger(t *testing.T) {
	ctx := logging.NewTestContext(t)
	dbName, cleanup := tests.CreateTestDatabase(ctx, t, config.GetMigrationPath())
	defer cleanup()

	// Create a connection pool to the test database
	pgConfig := tests.GetTestPostgresParams(dbName)
	pool, err := postgres.NewPool(ctx, pgConfig)
	if err != nil {
		t.Fatalf("Failed to create postgres pool: %v", err)
	}
	defer pool.Close()

	// Insert test data into ff_task_metadata
	testValue := `{
		"ResourceType": 9,
		"StorageType": "Resource",
		"Message": "{\"name\":\"my_training_set2\",\"status\":4,\"tags\":{\"tag\":[\"test\",\"qa\"]},\"properties\":{}}",
		"SerializedVersion": 1
	}`

	_, err = pool.Exec(ctx, `
		INSERT INTO ff_task_metadata (key, value) 
		VALUES ($1, $2)`,
		"TRAINING_SET_VARIANT__my_training_set2__2024-12-06t14-51-21",
		testValue,
	)
	require.NoError(t, err)

	// Verify the trigger created the corresponding entry in search_resources
	var (
		id      string
		name    string
		typ     string
		variant string
		tags    []string
	)
	err = pool.QueryRow(ctx, `
		SELECT id, name, type, variant, tags
		FROM search_resources 
		WHERE type = 'TRAINING_SET_VARIANT'`).Scan(&id, &name, &typ, &variant, &tags)
	require.NoError(t, err)

	// Assert the values were correctly parsed and inserted
	require.Equal(t, "my_training_set2", name)
	require.Equal(t, "TRAINING_SET_VARIANT", typ)
	require.Equal(t, "2024-12-06t14-51-21", variant)
	require.Equal(t, []string{"test", "qa"}, tags)

	// Test update by adding a new tag
	updatedValue := `{
		"ResourceType": 9,
		"StorageType": "Resource",
		"Message": "{\"name\":\"my_training_set2\",\"status\":4,\"tags\":{\"tag\":[\"test\",\"qa\",\"new_tag\"]},\"properties\":{}}",
		"SerializedVersion": 1
	}`

	_, err = pool.Exec(ctx, `
		UPDATE ff_task_metadata
		SET value = $1
		WHERE key = $2`,
		updatedValue,
		"TRAINING_SET_VARIANT__my_training_set2__2024-12-06t14-51-21",
	)
	require.NoError(t, err)

	// Check row count in search_resources
	var rowCount int
	err = pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM search_resources
		WHERE type = 'TRAINING_SET_VARIANT'`).Scan(&rowCount)
	require.NoError(t, err)
	require.Equal(t, 1, rowCount, "Expected exactly one row in search_resources")

	// Verify the tags were updated
	err = pool.QueryRow(ctx, `
		SELECT tags
		FROM search_resources 
		WHERE variant = '2024-12-06t14-51-21'`).Scan(&tags)
	require.NoError(t, err)
	require.Equal(t, []string{"test", "qa", "new_tag"}, tags)
}

func TestSearchTablesMultipleVariants(t *testing.T) {
	ctx := logging.NewTestContext(t)
	dbName, cleanup := tests.CreateTestDatabase(ctx, t, config.GetMigrationPath())
	defer cleanup()

	// Create a connection pool to the test database
	pgConfig := tests.GetTestPostgresParams(dbName)
	pool, err := postgres.NewPool(ctx, pgConfig)
	if err != nil {
		t.Fatalf("Failed to create postgres pool: %v", err)
	}
	defer pool.Close()

	// Insert first test data into ff_task_metadata
	firstValue := `{
		"ResourceType": 9,
		"StorageType": "Resource",
		"Message": "{\"name\":\"shared_name\",\"status\":4,\"tags\":{\"tag\":[\"prod\",\"v1\"]},\"properties\":{}}",
		"SerializedVersion": 1
	}`

	_, err = pool.Exec(ctx, `
		INSERT INTO ff_task_metadata (key, value) 
		VALUES ($1, $2)`,
		"test_type__shared_name__variant1",
		firstValue,
	)
	require.NoError(t, err)

	// Insert second test data with same name but different variant
	secondValue := `{
		"ResourceType": 9,
		"StorageType": "Resource",
		"Message": "{\"name\":\"shared_name\",\"status\":4,\"tags\":{\"tag\":[\"staging\",\"v2\"]},\"properties\":{}}",
		"SerializedVersion": 1
	}`

	_, err = pool.Exec(ctx, `
		INSERT INTO ff_task_metadata (key, value) 
		VALUES ($1, $2)`,
		"test_type__shared_name__variant2",
		secondValue,
	)
	require.NoError(t, err)

	// Query search_resources for both entries
	rows, err := pool.Query(ctx, `
		SELECT name, variant, tags
		FROM search_resources 
		WHERE name = 'shared_name'
		ORDER BY variant`)
	require.NoError(t, err)
	defer rows.Close()

	// Collect and verify results
	var results []struct {
		name    string
		variant string
		tags    []string
	}

	for rows.Next() {
		var result struct {
			name    string
			variant string
			tags    []string
		}
		err := rows.Scan(&result.name, &result.variant, &result.tags)
		require.NoError(t, err)
		results = append(results, result)
	}

	// Verify we got both rows
	require.Len(t, results, 2, "Expected two rows with the same name")

	// Verify first row
	require.Equal(t, "shared_name", results[0].name)
	require.Equal(t, "variant1", results[0].variant)
	require.Equal(t, []string{"prod", "v1"}, results[0].tags)

	// Verify second row
	require.Equal(t, "shared_name", results[1].name)
	require.Equal(t, "variant2", results[1].variant)
	require.Equal(t, []string{"staging", "v2"}, results[1].tags)
}
