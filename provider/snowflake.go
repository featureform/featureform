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
	_ "github.com/snowflakedb/gosnowflake"
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
	sf.logger.Debugw("Snowflake offline store creating transformation ...", "config", config)
	if len(opts) > 0 {
		sf.logger.Errorw("Snowflake off does not support transformation options")
		return fferr.NewInternalErrorf("Snowflake off does not support transformation options")
	}
	tableName, err := sf.sqlOfflineStore.getTransformationTableName(config.TargetTableID)
	if err != nil {
		sf.logger.Errorw("Failed to get transformation table name", "error", err)
		return err
	}
	var snowflakeConfig pc.SnowflakeConfig
	if err := snowflakeConfig.Deserialize(sf.sqlOfflineStore.Config()); err != nil {
		sf.logger.Errorw("Failed to deserialize snowflake config", "error", err)
		return err
	}
	// Given our API design allows for users to define Dynamic Iceberg Table configurations at the catalog level, it's possible
	// that config.SnowflakeDynamicTableConfig is nil. In this case, we'll create a new empty config to facilitate the merge with the catalog config.
	resConfig := config.ResourceSnowflakeConfig
	if resConfig == nil {
		resConfig = &metadata.ResourceSnowflakeConfig{}
	}
	if err := resConfig.Merge(&snowflakeConfig); err != nil {
		sf.logger.Errorw("Failed to merge dynamic table config", "error", err)
		return err
	}
	query, err := sf.sfQueries.dynamicIcebergTableCreate(tableName, config.Query, *resConfig)
	if err != nil {
		sf.logger.Errorw("Failed to create dynamic iceberg table query", "error", err)
		return err
	}
	sf.logger.Debugw("Creating Dynamic Iceberg Table for source", "query", query)
	if _, err := sf.sqlOfflineStore.db.Exec(query); err != nil {
		sf.logger.Errorw("Failed to create dynamic iceberg table", "error", err)
		return fferr.NewResourceExecutionError(pt.SnowflakeOffline.String(), config.TargetTableID.Name, config.TargetTableID.Variant, fferr.ResourceType(config.TargetTableID.Type.String()), err)
	}
	return nil
}

func (sf *snowflakeOfflineStore) UpdateTransformation(config TransformationConfig, opts ...TransformationOption) error {
	return fferr.NewInternalErrorf("Snowflake Offline Store does not currently support updating transformations")
}

// RegisterResourceFromSourceTable creates a new table in the database for labels, but not features. Unlike the sqlOfflineStore implementation, which
// creates a resource table for features, there's no need here for the intermediate table between source transformation and materialization table.
func (sf *snowflakeOfflineStore) RegisterResourceFromSourceTable(id ResourceID, schema ResourceSchema, opts ...ResourceOption) (OfflineTable, error) {
	sf.logger.Debugw("Snowflake offline store creating resource table...", "id", id, "schema", schema, "opts", opts)
	if id.Type == Feature {
		sf.logger.Debugw("Snowflake Offline Store does not support creating resource tables for features")
		return nil, nil
	}
	if err := id.check(Label); err != nil {
		return nil, err
	}
	if len(opts) == 0 || opts[0].Type() != SnowflakeDynamicTableResource {
		return nil, fferr.NewInternalErrorf("Snowflake Offline Store requires Dynamic Table Resource Options")
	}
	opt, isSnowflakeConfigOpt := opts[0].(*ResourceSnowflakeConfigOption)
	if !isSnowflakeConfigOpt {
		return nil, fferr.NewInternalErrorf("Snowflake Offline Store requires Resource Config Options; received %T", opts[0])
	}
	tableConfig := opt.Config
	if tableConfig == nil {
		tableConfig = &metadata.SnowflakeDynamicTableConfig{}
	}
	resConfig := &metadata.ResourceSnowflakeConfig{
		DynamicTableConfig: tableConfig,
		Warehouse:          opt.Warehouse,
	}
	var snowflakeConfig pc.SnowflakeConfig
	if err := snowflakeConfig.Deserialize(sf.sqlOfflineStore.Config()); err != nil {
		sf.logger.Errorw("Failed to deserialize snowflake config", "error", err)
		return nil, err
	}
	if err := resConfig.Merge(&snowflakeConfig); err != nil {
		sf.logger.Errorw("Failed to merge dynamic table config", "error", err)
		return nil, err
	}
	if exists, err := sf.sqlOfflineStore.tableExistsForResourceId(id); err != nil {
		return nil, err
	} else if exists {
		return nil, fferr.NewDatasetAlreadyExistsError(id.Name, id.Variant, nil)
	}
	if err := schema.Validate(); err != nil {
		sf.logger.Errorw("Failed to validate schema", "error", err)
		return nil, err
	}
	resourceAsQuery, err := sf.sfQueries.resourceTableAsQuery(schema, schema.TS != "")
	if err != nil {
		sf.logger.Errorw("Failed to create resource table as query", "error", err)
		return nil, err
	}
	tableName, err := ps.ResourceToTableName(id.Type.String(), id.Name, id.Variant)
	if err != nil {
		return nil, err
	}
	query, err := sf.sfQueries.dynamicIcebergTableCreate(tableName, resourceAsQuery, *resConfig)
	if err != nil {
		sf.logger.Errorw("Failed to create dynamic iceberg table query", "error", err)
		return nil, err
	}
	sf.logger.Debugw("Creating Dynamic Iceberg Table for label variant", "query", query)
	if _, err := sf.sqlOfflineStore.db.Exec(query); err != nil {
		sf.logger.Errorw("Failed to create dynamic iceberg table", "error", err)
		return nil, fferr.NewResourceExecutionError(pt.SnowflakeOffline.String(), id.Name, id.Variant, fferr.LABEL_VARIANT, err)
	}
	return &snowflakeOfflineTable{
		sqlOfflineTable: sqlOfflineTable{
			db:           sf.sqlOfflineStore.db,
			name:         tableName,
			query:        sf.sqlOfflineStore.query,
			providerType: pt.SnowflakeOffline,
		},
		location: pl.NewSQLLocationWithDBSchemaTable(snowflakeConfig.Database, snowflakeConfig.Schema, tableName),
	}, nil
}

func (sf *snowflakeOfflineStore) GetResourceTable(id ResourceID) (OfflineTable, error) {
	sf.logger.Debugw("Snowflake offline store getting resource table...", "id", id)
	if id.Type == Feature {
		sf.logger.Debugw("Snowflake Offline Store does not support resource tables for features")
		return nil, nil
	}
	if err := id.check(Label); err != nil {
		return nil, err
	}
	config := pc.SnowflakeConfig{}
	if err := config.Deserialize(sf.sqlOfflineStore.Config()); err != nil {
		return nil, err
	}
	tableName, err := ps.ResourceToTableName(id.Type.String(), id.Name, id.Variant)
	if err != nil {
		return nil, err
	}
	var schema = config.Schema
	if schema == "" {
		schema = "PUBLIC"
	}
	loc := pl.NewSQLLocationWithDBSchemaTable(config.Database, schema, tableName)

	if exists, err := sf.sqlOfflineStore.tableExists(loc); err != nil {
		return nil, err
	} else if !exists {
		return nil, fferr.NewDatasetNotFoundError(id.Name, id.Variant, nil)
	}

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
	if err := id.check(Feature); err != nil {
		return nil, err
	}
	var snowflakeConfig pc.SnowflakeConfig
	if err := snowflakeConfig.Deserialize(sf.sqlOfflineStore.Config()); err != nil {
		sf.logger.Errorw("Failed to deserialize snowflake config", "error", err)
		return nil, err
	}
	resConfig := opts.ResourceSnowflakeConfig
	if resConfig == nil {
		resConfig = &metadata.ResourceSnowflakeConfig{}
	}
	sf.logger.Debugw("Dynamic Table Config before Merge with Snowflake Config", "config", resConfig)
	if err := resConfig.Merge(&snowflakeConfig); err != nil {
		sf.logger.Errorw("Failed to merge dynamic table config", "error", err)
		return nil, err
	}
	sf.logger.Debugw(("Dynamic Table Config after Merge with Snowflake Config"), "config", resConfig)
	tableName, err := ps.ResourceToTableName(FeatureMaterialization.String(), id.Name, id.Variant)
	if err != nil {
		return nil, err
	}
	if err := opts.Schema.Validate(); err != nil {
		sf.logger.Errorw("Failed to validate schema", "error", err)
		return nil, err
	}
	sqlLoc, isSqlLoc := opts.Schema.SourceTable.(*pl.SQLLocation)
	if !isSqlLoc {
		return nil, fferr.NewInvalidArgumentErrorf("source table is not an SQL location")
	}
	materializationAsQuery := sf.sfQueries.materializationCreateAsQuery(opts.Schema.Entity, opts.Schema.Value, opts.Schema.TS, SanitizeSqlLocation(sqlLoc.TableLocation()))
	query, err := sf.sfQueries.dynamicIcebergTableCreate(tableName, materializationAsQuery, *resConfig)
	if err != nil {
		sf.logger.Errorw("Failed to create dynamic iceberg table query", "error", err)
		return nil, err
	}
	sf.logger.Debugw("Creating Dynamic Iceberg Table for materialization", "query", query)
	if _, err := sf.sqlOfflineStore.db.Exec(query); err != nil {
		sf.logger.Errorw("Failed to create dynamic iceberg table", "error", err)
		return nil, fferr.NewResourceExecutionError(pt.SnowflakeOffline.String(), id.Name, id.Variant, fferr.FEATURE_MATERIALIZATION, err)
	}
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
	return nil, fferr.NewInternalErrorf("Snowflake Offline Store does not currently support updating materializations")
}

func (sf *snowflakeOfflineStore) CreateTrainingSet(def TrainingSetDef) error {
	if err := def.check(); err != nil {
		return err
	}
	var snowflakeConfig pc.SnowflakeConfig
	if err := snowflakeConfig.Deserialize(sf.sqlOfflineStore.Config()); err != nil {
		sf.logger.Errorw("Failed to deserialize snowflake config", "error", err)
		return err
	}
	resConfig := def.ResourceSnowflakeConfig
	if resConfig == nil {
		resConfig = &metadata.ResourceSnowflakeConfig{}
	}
	if err := resConfig.Merge(&snowflakeConfig); err != nil {
		sf.logger.Errorw("Failed to merge dynamic table config", "error", err)
		return err
	}
	tableName, err := sf.sqlOfflineStore.getTrainingSetName(def.ID)
	if err != nil {
		return err
	}
	snowflakeQueries := sf.sqlOfflineStore.query.(*snowflakeSQLQueries)
	trainingSetAsQuery, err := snowflakeQueries.trainingSetCreateAsQuery(def)
	if err != nil {
		return err
	}
	query, err := snowflakeQueries.dynamicIcebergTableCreate(tableName, trainingSetAsQuery, *resConfig)
	if err != nil {
		sf.logger.Errorw("Failed to create dynamic iceberg table query", "error", err)
		return err
	}
	sf.logger.Debugw("Creating Dynamic Iceberg Table for training set", "query", query)
	if _, err := sf.sqlOfflineStore.db.Exec(query); err != nil {
		sf.logger.Errorw("Failed to create dynamic iceberg table", "error", err)
		return fferr.NewResourceExecutionError(pt.SnowflakeOffline.String(), def.ID.Name, def.ID.Variant, fferr.TRAINING_SET_VARIANT, err)
	}
	return nil
}

func (sf *snowflakeOfflineStore) UpdateTrainingSet(def TrainingSetDef) error {
	return fferr.NewInternalErrorf("Snowflake Offline Store does not currently support updating training sets")
}

// AsOfflineStore returns the OfflineStore interface for the snowflakeOfflineStore.
// NOTE: snowflakeOfflineStore _must_ implement this method for consumers to call the correct methods on the interface
// (i.e. without implementing this method, consumers would be calling into sqlOfflineStore methods instead). Given this,
// DO NOT REMOVE THIS METHOD.
func (sf *snowflakeOfflineStore) AsOfflineStore() (OfflineStore, error) {
	return sf, nil
}
