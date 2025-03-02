// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"fmt"

	"github.com/featureform/fferr"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	pl "github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	ps "github.com/featureform/provider/provider_schema"
	pt "github.com/featureform/provider/provider_type"

	snowflake "github.com/snowflakedb/gosnowflake"

	tsq "github.com/featureform/provider/tsquery"
)

// sqlColumnType is used to specify the column type of a resource value.
type snowflakeColumnType string

const (
	sfInt       snowflakeColumnType = "integer"
	sfNumber    snowflakeColumnType = "NUMBER"
	sfFloat     snowflakeColumnType = "FLOAT"
	sfString    snowflakeColumnType = "varchar"
	sfBool      snowflakeColumnType = "BOOLEAN"
	sfTimestamp snowflakeColumnType = "TIMESTAMP_NTZ"
)

func snowflakeOfflineStoreFactory(config pc.SerializedConfig) (Provider, error) {
	sc := pc.SnowflakeConfig{}
	if err := sc.Deserialize(config); err != nil {
		return nil, err
	}
	queries := snowflakeSQLQueries{}
	queries.setVariableBinding(MySQLBindingStyle)
	connectionString, err := sc.ConnectionString(sc.Database, sc.Schema)
	if err != nil {
		return nil, fferr.NewInternalErrorf("could not get snowflake connection string: %v", err)
	}
	sgConfig := SQLOfflineStoreConfig{
		Config:                  config,
		ConnectionURL:           connectionString,
		Driver:                  "snowflake",
		ProviderType:            pt.SnowflakeOffline,
		QueryImpl:               &queries,
		ConnectionStringBuilder: sc.ConnectionString,
	}

	store, err := NewSQLOfflineStore(sgConfig)
	if err != nil {
		return nil, err
	}

	return &snowflakeOfflineStore{
		store,
		logging.NewLogger("snowflake_offline_store"),
		&queries,
	}, nil
}

type snowflakeOfflineStore struct {
	*sqlOfflineStore
	logger    logging.Logger
	sfQueries *snowflakeSQLQueries
}

func (sf *snowflakeOfflineStore) CreateTransformation(config TransformationConfig, opts ...TransformationOption) error {
	logger := sf.logger.WithResource(logging.SourceVariant, config.TargetTableID.Name, config.TargetTableID.Variant).With("config", config)
	logger.Info("Snowflake offline store creating transformation ...")
	if len(opts) > 0 {
		logger.Errorw("Snowflake off does not support transformation options")
		return fferr.NewInternalErrorf("Snowflake off does not support transformation options")
	}
	tableName, err := sf.sqlOfflineStore.getTransformationTableName(config.TargetTableID)
	if err != nil {
		logger.Errorw("Failed to get transformation table name", "error", err)
		return err
	}
	var snowflakeConfig pc.SnowflakeConfig
	if err := snowflakeConfig.Deserialize(sf.sqlOfflineStore.Config()); err != nil {
		logger.Errorw("Failed to deserialize snowflake config", "error", err)
		return err
	}
	logger.Debugw("Snowflake catalog config for SQL transformation", "catalog", snowflakeConfig.Catalog)
	// Given our API design allows for users to define Dynamic Iceberg Table configurations at the catalog level, it's possible
	// that config.SnowflakeDynamicTableConfig is nil. In this case, we'll create a new empty config to facilitate the merge with the catalog config.
	resConfig := config.ResourceSnowflakeConfig
	if resConfig == nil {
		logger.Debugw("Resource table config for SQL transformation is empty")
		resConfig = &metadata.ResourceSnowflakeConfig{}
	}
	logger.Debugw("Resource table config for SQL transformation before Merge with Snowflake Config", "config", resConfig)
	if err := resConfig.Merge(&snowflakeConfig); err != nil {
		logger.Errorw("Failed to merge dynamic table config", "error", err)
		return err
	}
	logger.Debugw("Resource table config for SQL transformation after Merge with Snowflake Config", "config", resConfig)
	if err := resConfig.Validate(); err != nil {
		logger.Errorw("Failed to validate dynamic table config", "error", err)
		return err
	}
	query := sf.sfQueries.dynamicIcebergTableCreate(tableName, config.Query, *resConfig)
	logger.Debugw("Creating Dynamic Iceberg Table for source", "query", query)
	if _, err := sf.sqlOfflineStore.db.Exec(query); err != nil {
		logger.Errorw("Failed to create dynamic iceberg table", "error", err)
		wrapped := fferr.NewResourceExecutionError(pt.SnowflakeOffline.String(), config.TargetTableID.Name, config.TargetTableID.Variant, fferr.ResourceType(config.TargetTableID.Type.String()), err)
		return sf.handleErr(wrapped, err)
	}
	logger.Info("Successfully created transformation")
	return nil
}

func (sf *snowflakeOfflineStore) UpdateTransformation(config TransformationConfig, opts ...TransformationOption) error {
	sf.logger.Errorw("Snowflake Offline Store does not currently support updating transformations", "config", config, "opts", opts)
	return fferr.NewInternalErrorf("Snowflake Offline Store does not currently support updating transformations")
}

// Snowflake breaks with the pattern of other offline store that create resource tables for labels and features. (Resource tables are intermediate tables
// between the sources on which labels and features are registered and materializations and training sets; they duplicate the data from the source tables
// for the 2 columns provided, that is, entity, value, and timestamp.)
func (sf *snowflakeOfflineStore) RegisterResourceFromSourceTable(id ResourceID, schema ResourceSchema, opts ...ResourceOption) (OfflineTable, error) {
	logger := sf.logger.WithResource(logging.ResourceType(id.Type.String()), id.Name, id.Variant).With("schema", schema)
	ctx := logger.AttachToContext(context.Background())
	if len(opts) > 0 {
		return nil, fferr.NewInvalidArgumentErrorf("snowflake offline store does not currently support resource options")
	}
	missingCols, err := sf.checkSourceContainsResourceColumns(ctx, id, schema, logger, opts...)
	if err != nil {
		return nil, err
	}
	if len(missingCols) > 0 {
		return nil, fferr.NewInvalidArgumentErrorf("source table does not contain expected columns; missing columns: %v", missingCols)
	}
	return nil, nil
}

// checkSourceContainsResourceColumns checks that the source table contains the expected columns for the resource type
// NOTE: due to the fact that we don't change the case of the columns inputted by the user, which is not an issue in the context
// of SQL queries due to the face they're generally case insensitive, we need to ensure the comparison is case insensitive as well.
// All columns in Snowflake are upper case by default, so we convert the expected columns to upper case as well.
func (sf *snowflakeOfflineStore) checkSourceContainsResourceColumns(ctx context.Context, id ResourceID, schema ResourceSchema, logger logging.Logger, opts ...ResourceOption) ([]string, error) {
	missingCols := make([]string, 0)
	if err := id.check(Label, Feature); err != nil {
		logger.Errorw("Failed to validate resource ID", "error", err)
		return missingCols, err
	}
	tblLoc, err := sf.getValidTableLocation(schema.SourceTable)
	if err != nil {
		logger.Errorw("Failed to get valid table location", "error", err)
		return missingCols, err
	}
	logger.Debugw("Source table location", "table_location", tblLoc)
	query, err := sf.query.resourceTableColumns(tblLoc)
	if err != nil {
		logger.Errorw("Failed to create resourceTableColumns query", "error", err)
		return missingCols, err
	}
	logger.Debugw("Query to check resource columns in source table", "query", query)
	expectedColumns, err := schema.ToColumnStringSet(id.Type)
	if err != nil {
		logger.Errorw("Failed to get expected columns", "error", err)
		return missingCols, err
	}
	logger.Debugw("Expected columns in source table", "columns", expectedColumns)
	logger.Info("Checking source table for resource columns ...")
	rows, err := sf.db.Query(query)
	if err != nil {
		logger.Errorw("Failed to query resource table columns", "error", err)
		return missingCols, err
	}
	defer rows.Close()
	actual := make(stringset.StringSet)
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			logger.Errorw("Failed to scan resource table columns", "error", err)
			return missingCols, err
		}
		actual.Add(strings.ToUpper(column))
	}
	if err := rows.Err(); err != nil {
		logger.Errorw("Error iterating over resource table columns", "error", err)
		return missingCols, err
	}
	if !actual.Contains(expectedColumns) {
		diff := expectedColumns.Difference(actual)
		logger.Errorw("Source table does not have expected columns", "diff", diff.List())
		return diff.List(), fferr.NewInvalidArgumentErrorf("source table does not have expected columns: %v", diff.List())
	}
	logger.Info("Successfully checked source table for resource columns")
	return missingCols, nil
}

func (sf *snowflakeOfflineStore) getValidTableLocation(loc pl.Location) (pl.FullyQualifiedObject, error) {
	sqlLoc, isSqlLoc := loc.(*pl.SQLLocation)
	if !isSqlLoc {
		sf.logger.Errorw("Source table is not an SQL location", "location_type", fmt.Sprintf("%T", loc))
		return pl.FullyQualifiedObject{}, fferr.NewInvalidArgumentErrorf("source table is not an SQL location")
	}
	tblLoc := sqlLoc.TableLocation()
	sf.logger.Debugw("Source table location before provider config", "table_location", tblLoc)
	if tblLoc.Database == "" || tblLoc.Schema == "" {
		sf.logger.Warn("Source table location missing database and/or schema; using provider database from config")
		config := pc.SnowflakeConfig{}
		if err := config.Deserialize(sf.sqlOfflineStore.Config()); err != nil {
			sf.logger.Errorw("Failed to deserialize snowflake config", "error", err)
			return pl.FullyQualifiedObject{}, err
		}
		if tblLoc.Database == "" {
			sf.logger.Debugw("Source table location missing database; using provider database from config", "provider_database", config.Database)
			tblLoc.Database = config.Database
		}
		if tblLoc.Schema == "" {
			sf.logger.Debugw("Source table location missing schema; using provider schema from config", "provider_schema", config.Schema)
			tblLoc.Schema = config.Schema
		}
	}
	sf.logger.Debugw("Source table location after provider config", "table_location", tblLoc)
	return tblLoc, nil
}

func (sf *snowflakeOfflineStore) GetResourceTable(id ResourceID) (OfflineTable, error) {
	return nil, fferr.NewInternalErrorf("Snowflake Offline Store does not currently support getting resource tables")
}

func (sf *snowflakeOfflineStore) CreateMaterialization(id ResourceID, opts MaterializationOptions) (Materialization, error) {
	logger := sf.logger.WithResource(logging.FeatureVariant, id.Name, id.Variant)
	if err := id.check(Feature); err != nil {
		logger.Errorw("Failed to validate resource ID", "error", err)
		return nil, err
	}
	var snowflakeConfig pc.SnowflakeConfig
	if err := snowflakeConfig.Deserialize(sf.sqlOfflineStore.Config()); err != nil {
		logger.Errorw("Failed to deserialize snowflake config", "error", err)
		return nil, err
	}
	resConfig := opts.ResourceSnowflakeConfig
	if resConfig == nil {
		logger.Debugw("Resource table config for materialization is empty")
		resConfig = &metadata.ResourceSnowflakeConfig{}
	}
	logger.Debugw("Dynamic Table Config before Merge with Snowflake Config", "config", resConfig)
	if err := resConfig.Merge(&snowflakeConfig); err != nil {
		logger.Errorw("Failed to merge dynamic table config", "error", err)
		return nil, err
	}

	logger.Debugw(("Dynamic Table Config after Merge with Snowflake Config"), "config", resConfig)
	tableName, err := ps.ResourceToTableName(FeatureMaterialization.String(), id.Name, id.Variant)
	if err != nil {
		logger.Errorw("Failed to get materialization table name", "error", err)
		return nil, err
	}
	if err := opts.Schema.Validate(); err != nil {
		logger.Errorw("Failed to validate schema", "error", err)
		return nil, err
	}
	sqlLoc, isSqlLoc := opts.Schema.SourceTable.(*pl.SQLLocation)
	if !isSqlLoc {
		logger.Errorw("Source table is not an SQL location", "location_type", fmt.Sprintf("%T", opts.Schema.SourceTable))
		return nil, fferr.NewInvalidArgumentErrorf("source table is not an SQL location")
	}
	materializationAsQuery := sf.sfQueries.materializationCreateAsQuery(opts.Schema.Entity, opts.Schema.Value, opts.Schema.TS, SanitizeSqlLocation(sqlLoc.TableLocation()))
	if err := resConfig.Validate(); err != nil {
		logger.Errorw("Failed to validate dynamic table config", "error", err)
		return nil, err
	}
	query := sf.sfQueries.dynamicIcebergTableCreate(tableName, materializationAsQuery, *resConfig)
	logger.Debugw("Creating Dynamic Iceberg Table for materialization", "query", query)
	if _, err := sf.sqlOfflineStore.db.Exec(query); err != nil {
		logger.Errorw("Failed to create dynamic iceberg table", "error", err)
		wrapped := fferr.NewResourceExecutionError(pt.SnowflakeOffline.String(), id.Name, id.Variant, fferr.FEATURE_MATERIALIZATION, err)
		return nil, sf.handleErr(wrapped, err)
	}
	logger.Info("Successfully created materialization")
	return &sqlMaterialization{
		id:           MaterializationID(fmt.Sprintf("%s__%s", id.Name, id.Variant)),
		db:           sf.sqlOfflineStore.db,
		tableName:    tableName,
		location:     pl.NewSQLLocation(tableName),
		query:        sf.sfQueries,
		providerType: pt.SnowflakeOffline,
	}, nil
}

func (sf *snowflakeOfflineStore) UpdateMaterialization(id ResourceID, opts MaterializationOptions) (Materialization, error) {
	sf.logger.Errorw("Snowflake Offline Store does not currently support updating materializations", "id", id, "opts", opts)
	return nil, fferr.NewInternalErrorf("Snowflake Offline Store does not currently support updating materializations")
}

func (sf *snowflakeOfflineStore) CreateTrainingSet(def TrainingSetDef) error {
	logger := sf.logger.WithResource(logging.TrainingSetVariant, def.ID.Name, def.ID.Variant).With("training_set_type", def.Type)
	logger.Debugw("Snowflake offline store creating training set ...")
	if err := def.check(); err != nil {
		logger.Errorw("Failed to validate training set definition", "error", err)
		return err
	}
	var snowflakeConfig pc.SnowflakeConfig
	if err := snowflakeConfig.Deserialize(sf.sqlOfflineStore.Config()); err != nil {
		logger.Errorw("Failed to deserialize snowflake config", "error", err)
		return err
	}
	resConfig := def.ResourceSnowflakeConfig
	if resConfig == nil {
		logger.Debugw("Resource table config for training set is empty")
		resConfig = &metadata.ResourceSnowflakeConfig{}
	}
	if err := resConfig.Merge(&snowflakeConfig); err != nil {
		logger.Errorw("Failed to merge dynamic table config", "error", err)
		return err
	}
	tableName, err := sf.sqlOfflineStore.getTrainingSetName(def.ID)
	if err != nil {
		logger.Errorw("Failed to get training set table name", "error", err)
		return err
	}
	tsQuery, err := sf.buildTrainingSetQuery(def)
	if err != nil {
		logger.Errorw("Failed to build training set query", "error", err)
		return err
	}
	var ctaQuery string
	switch def.Type {
	case metadata.DynamicTrainingSet:
		if err := resConfig.Validate(); err != nil {
			logger.Errorw("Failed to validate dynamic table config", "error", err)
			return err
		}
		ctaQuery = sf.sfQueries.dynamicIcebergTableCreate(tableName, tsQuery, *resConfig)
	case metadata.StaticTrainingSet:
		if err := resConfig.Validate(); err != nil {
			logger.Errorw("Failed to validate dynamic table config", "error", err)
			return err
		}
		ctaQuery = sf.sfQueries.staticIcebergTableCreate(tableName, tsQuery, *resConfig)
	case metadata.ViewTrainingSet:
		ctaQuery = sf.sfQueries.viewCreate(tableName, tsQuery)
	default:
		logger.Errorw("Unsupported training set type", "type", def.Type)
		return fferr.NewInternalErrorf("Unsupported training set type: %v", def.Type)
	}
	logger.Debugw("Creating Dynamic Iceberg Table for training set", "query", ctaQuery)
	if _, err := sf.sqlOfflineStore.db.Exec(ctaQuery); err != nil {
		wrapped := fferr.NewResourceExecutionError(pt.SnowflakeOffline.String(), def.ID.Name, def.ID.Variant, fferr.TRAINING_SET_VARIANT, err)
		logger.Errorw("Failed to create dynamic iceberg table", "error", err)
		return sf.handleErr(wrapped, err)
	}
	logger.Info("Successfully created training set")
	return nil
}

func (sf *snowflakeOfflineStore) UpdateTrainingSet(def TrainingSetDef) error {
	sf.logger.Errorw("Snowflake Offline Store does not currently support updating training sets", "def", def)
	return fferr.NewInternalErrorf("Snowflake Offline Store does not currently support updating training sets")
}

// AsOfflineStore returns the OfflineStore interface for the snowflakeOfflineStore.
// NOTE: snowflakeOfflineStore _must_ implement this method for consumers to call the correct methods on the interface
// (i.e. without implementing this method, consumers would be calling into sqlOfflineStore methods instead). Given this,
// DO NOT REMOVE THIS METHOD.
func (sf *snowflakeOfflineStore) AsOfflineStore() (OfflineStore, error) {
	return sf, nil
}

func (sf snowflakeOfflineStore) Delete(location pl.Location) error {
	logger := sf.logger.With("location", location.Location())
	if exists, err := sf.sqlOfflineStore.tableExists(location); err != nil {
		logger.Errorw("Failed to check if table exists", "error", err)
		return err
	} else if !exists {
		logger.Errorw("Table does not exist")
		return fferr.NewDatasetLocationNotFoundError(location.Location(), nil)
	}

	sqlLoc, isSqlLoc := location.(*pl.SQLLocation)
	if !isSqlLoc {
		logger.Errorw("Location is not an SQL location", "location_type", fmt.Sprintf("%T", location))
		return fferr.NewInternalErrorf("location is not an SQL location")
	}

	query := sf.sfQueries.dropTableQuery(*sqlLoc)
	logger.Debugw("Dropping table", "query", query)
	if _, err := sf.db.Exec(query); err != nil {
		logger.Errorw("Failed to drop table", "error", err)
		return sf.handleErr(fferr.NewExecutionError(pt.SnowflakeOffline.String(), err), err)
	}
	logger.Info("Successfully dropped table")
	return nil
}

// handleErr attempts to add the Snowflake query and session IDs to a wrapped error to aid
// in further debugging in the Snowflake UI; note that handleErr will not fail even if the
// returned error isn't an instance of gosnowflake.SnowflakeError or if the query to get
// the session ID fails.
func (sf snowflakeOfflineStore) handleErr(wrapped fferr.Error, execErr error) error {
	sfErr, isSnowflakeErr := execErr.(*snowflake.SnowflakeError)
	if isSnowflakeErr {
		wrapped.AddDetail("query_id", sfErr.QueryID)
	}
	var sessionID string
	r := sf.db.QueryRow("SELECT CURRENT_SESSION()")
	if err := r.Scan(&sessionID); err != nil {
		sf.logger.Errorf("failed to query session ID for err: %v", err)
	} else {
		wrapped.AddDetail("session_id", sessionID)
	}
	return wrapped
}

func (sf snowflakeOfflineStore) buildTrainingSetQuery(def TrainingSetDef) (string, error) {
	sf.logger.Debugw("Building training set query...", "def", def)
	params, err := sf.adaptTsDefToBuilderParams(def)
	if err != nil {
		return "", err
	}
	sf.logger.Debugw("Training set builder params", "params", params)

	queryConfig := tsq.QueryConfig{
		UseAsOfJoin: true,
		QuoteChar:   "\"",
		QuoteTable:  false,
	}
	ts := tsq.NewTrainingSet(queryConfig, params)
	return ts.CompileSQL()
}

func (sf snowflakeOfflineStore) adaptTsDefToBuilderParams(def TrainingSetDef) (tsq.BuilderParams, error) {
	sanitizeTableNameFn := func(loc pl.Location) (string, error) {
		lblLoc, isSQLLocation := loc.(*pl.SQLLocation)
		if !isSQLLocation {
			return "", fferr.NewInternalErrorf("label location is not an SQL location")
		}
		return SanitizeSnowflakeIdentifier(lblLoc.TableLocation()), nil
	}

	return def.ToBuilderParams(sf.logger, sanitizeTableNameFn)
}
