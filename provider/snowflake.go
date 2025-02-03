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

	tsq "github.com/featureform/provider/tsquery"
	snowflake "github.com/snowflakedb/gosnowflake"
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

// RegisterResourceFromSourceTable creates a new table in the database for labels, but not features. Unlike the sqlOfflineStore implementation, which
// creates a resource table for features, there's no need here for the intermediate table between source transformation and materialization table.
func (sf *snowflakeOfflineStore) RegisterResourceFromSourceTable(id ResourceID, schema ResourceSchema, opts ...ResourceOption) (OfflineTable, error) {
	logger := sf.logger.With("id", id, "schema", schema, "opts", opts)
	logger.Info("Snowflake offline store creating resource table...")
	if id.Type == Feature {
		logger.Debugw("Snowflake Offline Store does not support creating resource tables for features")
		return nil, nil
	}
	if err := id.check(Label); err != nil {
		logger.Errorw("Failed to validate resource ID", "error", err)
		return nil, err
	}
	if len(opts) == 0 || opts[0].Type() != SnowflakeDynamicTableResource {
		logger.Errorw("Snowflake Offline Store requires Dynamic Table Resource Options")
		return nil, fferr.NewInternalErrorf("Snowflake Offline Store requires Dynamic Table Resource Options")
	}
	opt, isSnowflakeConfigOpt := opts[0].(*ResourceSnowflakeConfigOption)
	if !isSnowflakeConfigOpt {
		logger.Errorw("Snowflake Offline Store requires Resource Config Options; received %T", opts[0])
		return nil, fferr.NewInternalErrorf("Snowflake Offline Store requires Resource Config Options; received %T", opts[0])
	}
	tableConfig := opt.Config
	if tableConfig == nil {
		logger.Debugw("Resource table config for label resource is empty")
		tableConfig = &metadata.SnowflakeDynamicTableConfig{}
	}
	logger.Debugw("Resource table config for label resource before Merge with Snowflake Config", "config", tableConfig)
	resConfig := &metadata.ResourceSnowflakeConfig{
		DynamicTableConfig: tableConfig,
		Warehouse:          opt.Warehouse,
	}
	var snowflakeConfig pc.SnowflakeConfig
	if err := snowflakeConfig.Deserialize(sf.sqlOfflineStore.Config()); err != nil {
		logger.Errorw("Failed to deserialize snowflake config", "error", err)
		return nil, err
	}
	logger.Debugw("Snowflake catalog config for label resource", "config", snowflakeConfig.Catalog)
	if err := resConfig.Merge(&snowflakeConfig); err != nil {
		logger.Errorw("Failed to merge dynamic table config", "error", err)
		return nil, err
	}
	logger.Debugw("Resource table config for label resource after Merge with Snowflake Config", "config", resConfig)
	if exists, err := sf.sqlOfflineStore.tableExistsForResourceId(id); err != nil {
		logger.Errorw("Failed to check if table exists", "error", err)
		return nil, err
	} else if exists {
		logger.Errorw("Table already exists", "id", id)
		return nil, fferr.NewDatasetAlreadyExistsError(id.Name, id.Variant, nil)
	}
	if err := schema.Validate(); err != nil {
		logger.Errorw("Failed to validate schema", "error", err)
		return nil, err
	}
	resourceAsQuery, err := sf.sfQueries.resourceTableAsQuery(schema)
	if err != nil {
		logger.Errorw("Failed to create resource table as query", "error", err)
		return nil, err
	}
	tableName, err := ps.ResourceToTableName(id.Type.String(), id.Name, id.Variant)
	if err != nil {
		logger.Errorw("Failed to get resource table name", "error", err)
		return nil, err
	}
	if err := resConfig.Validate(); err != nil {
		logger.Errorw("Failed to validate dynamic table config", "error", err)
		return nil, err
	}
	query := sf.sfQueries.dynamicIcebergTableCreate(tableName, resourceAsQuery, *resConfig)
	logger.Debugw("Creating Dynamic Iceberg Table for label variant", "query", query)
	if _, err := sf.sqlOfflineStore.db.Exec(query); err != nil {
		logger.Errorw("Failed to create dynamic iceberg table", "error", err)
		wrapped := fferr.NewResourceExecutionError(pt.SnowflakeOffline.String(), id.Name, id.Variant, fferr.LABEL_VARIANT, err)
		return nil, sf.handleErr(wrapped, err)
	}
	logger.Info("Successfully created resource table")
	return &snowflakeOfflineTable{
		sqlOfflineTable: sqlOfflineTable{
			db:           sf.sqlOfflineStore.db,
			name:         tableName,
			query:        sf.sqlOfflineStore.query,
			providerType: pt.SnowflakeOffline,
		},
		location: pl.NewFullyQualifiedSQLLocation(snowflakeConfig.Database, snowflakeConfig.Schema, tableName),
	}, nil
}

func (sf *snowflakeOfflineStore) GetResourceTable(id ResourceID) (OfflineTable, error) {
	logger := sf.logger.WithResource(logging.LabelVariant, id.Name, id.Variant)
	logger.Info("Snowflake offline store getting resource table...")
	if id.Type == Feature {
		logger.Debugw("Snowflake Offline Store does not support resource tables for features")
		return nil, nil
	}
	if err := id.check(Label); err != nil {
		logger.Errorw("Failed to validate resource ID", "error", err)
		return nil, err
	}
	config := pc.SnowflakeConfig{}
	if err := config.Deserialize(sf.sqlOfflineStore.Config()); err != nil {
		logger.Errorw("Failed to deserialize snowflake config", "error", err)
		return nil, err
	}
	tableName, err := ps.ResourceToTableName(id.Type.String(), id.Name, id.Variant)
	if err != nil {
		logger.Errorw("Failed to get resource table name", "error", err)
		return nil, err
	}
	var schema = config.Schema
	if schema == "" {
		logger.Debug("Schema is empty, defaulting to PUBLIC")
		schema = "PUBLIC"
	}
	loc := pl.NewFullyQualifiedSQLLocation(config.Database, schema, tableName)

	if exists, err := sf.sqlOfflineStore.tableExists(loc); err != nil {
		logger.Errorw("Failed to check if table exists", "error", err)
		return nil, err
	} else if !exists {
		logger.Errorw("Table does not exist", "location", loc.Location())
		return nil, fferr.NewDatasetNotFoundError(id.Name, id.Variant, nil)
	}
	logger.Info("Successfully retrieved resource table")
	return &snowflakeOfflineTable{
		sqlOfflineTable: sqlOfflineTable{
			db:           sf.sqlOfflineStore.db,
			name:         tableName,
			query:        sf.sqlOfflineStore.query,
			providerType: pt.SnowflakeOffline,
		},
		location: loc,
	}, nil
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
