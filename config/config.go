// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package config

import (
	"crypto/md5"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/featureform/fferr"
	"github.com/featureform/helpers"
	"github.com/featureform/helpers/etcd"
	"github.com/featureform/helpers/postgres"
	"github.com/featureform/logging"
)

// image paths
const (
	PandasBaseImage = "featureformenterprise/k8s_runner"
	WorkerImage     = "featureformenterprise/worker"
)

// script paths
const (
	SparkLocalScriptPath              = "/app/provider/scripts/spark/offline_store_spark_runner.py"
	PythonLocalInitPath               = "/app/provider/scripts/spark/python_packages.sh"
	PythonRemoteInitPath              = "featureform/scripts/spark/python_packages.sh"
	MaterializeNoTimestampQueryPath   = "/app/provider/queries/materialize_no_ts.sql"
	MaterializeWithTimestampQueryPath = "/app/provider/queries/materialize_ts.sql"
)

// Environment variable names for Featureform configuration
const (
	EnvWorkerImage                       = "WORKER_IMAGE"
	EnvPandasRunnerImage                 = "PANDAS_RUNNER_IMAGE"
	EnvSparkLocalScriptPath              = "SPARK_LOCAL_SCRIPT_PATH"
	EnvSkipSparkHealthCheck              = "SKIP_SPARK_HEALTH_CHECK"
	EnvShouldUseDBFS                     = "SHOULD_USE_DBFS"
	EnvDBMigrationPath                   = "DB_MIGRATION_PATH"
	EnvRunGooseMigrationMetadata         = "RUN_GOOSE_MIGRATION_METADATA"
	EnvRunGooseMigrationExecutable       = "RUN_GOOSE_MIGRATION_EXECUTABLE"
	EnvPythonLocalInitPath               = "PYTHON_LOCAL_INIT_PATH"
	EnvPythonRemoteInitPath              = "PYTHON_REMOTE_INIT_PATH"
	EnvMaterializeNoTimestampQueryPath   = "MATERIALIZE_NO_TIMESTAMP_QUERY_PATH"
	EnvMaterializeWithTimestampQueryPath = "MATERIALIZE_WITH_TIMESTAMP_QUERY_PATH"
	EnvFFStateProvider                   = "FF_STATE_PROVIDER"
	EnvSlackChannelId                    = "SLACK_CHANNEL_ID"
	EnvFFInitTimeout                     = "FF_INIT_TIMEOUT"
)

type SparkFileConfigs struct {
	LocalScriptPath      string
	RemoteScriptPath     string
	PythonLocalInitPath  string
	PythonRemoteInitPath string
}

func GetWorkerImage() string {
	return helpers.GetEnv(EnvWorkerImage, WorkerImage)
}

func GetPandasRunnerImage() string {
	return helpers.GetEnv(EnvPandasRunnerImage, PandasBaseImage)
}

func getSparkLocalScriptPath() string {
	return helpers.GetEnv(EnvSparkLocalScriptPath, SparkLocalScriptPath)
}

func ShouldSkipSparkHealthCheck() bool {
	return helpers.GetEnvBool(EnvSkipSparkHealthCheck, false)
}

func ShouldUseDBFS() bool {
	return helpers.GetEnvBool(EnvShouldUseDBFS, false)
}

func GetMigrationPath() string {
	return helpers.GetEnv(EnvDBMigrationPath, "db/migrations")
}

// ShouldRunGooseMigrationMetadata Will determine if our goose migration should run on metadata startup
func ShouldRunGooseMigrationMetadata() bool {
	return helpers.GetEnvBool(EnvRunGooseMigrationMetadata, false)
}

// ShouldRunGooseMigrationExecutable Will determine if our goose migration should run on executable startup
func ShouldRunGooseMigrationExecutable() bool {
	return helpers.GetEnvBool(EnvRunGooseMigrationExecutable, true)
}

func CreateSparkScriptConfig() (SparkFileConfigs, error) {
	remoteScriptPath, err := createSparkRemoteScriptPath()
	if err != nil {
		return SparkFileConfigs{}, err
	}
	return SparkFileConfigs{
		LocalScriptPath:      getSparkLocalScriptPath(),
		RemoteScriptPath:     remoteScriptPath,
		PythonRemoteInitPath: getPythonRemoteInitPath(),
		PythonLocalInitPath:  getPythonLocalInitPath(),
	}, nil
}

// In the event adding the MD5 hash as a suffix to the filename fails, the default ensures the program
// can continue to process transformations and materialization without exceptions
func createSparkRemoteScriptPath() (string, error) {
	// Don't change script if running in a test environment
	if strings.HasSuffix(os.Args[0], ".test") {
		return "featureform/scripts/spark/offline_store_spark_runner.py", nil
	}
	runnerMD5, err := os.ReadFile("/app/provider/scripts/spark/offline_store_spark_runner_md5.txt")
	if err != nil {
		fmt.Printf("failed to read MD5 hash file: %v\nAttempting to read the file from the local filesystem\n", err)
		// TODO remove this hardcoding
		if filename, err := createHashFromFile("./provider/scripts/spark/offline_store_spark_runner.py"); err != nil {
			fmt.Printf("failed to create MD5 hash from file: %v\n", err)
			return "", fmt.Errorf("Could not generate valid MD5 hash for the pyspark file. Exiting...")
		} else {
			return filename, nil
		}
	} else {
		return fmt.Sprintf("featureform/scripts/spark/offline_store_spark_runner_%s.py", string(runnerMD5)), nil
	}
}

func createHashFromFile(file string) (string, error) {
	if pysparkFile, err := os.ReadFile(file); err != nil {
		return "", err
	} else {
		sum := md5.Sum(pysparkFile)
		return fmt.Sprintf("featureform/scripts/spark/offline_store_spark_runner_%x.py", sum), nil
	}
}

func getPythonLocalInitPath() string {
	return helpers.GetEnv(EnvPythonLocalInitPath, PythonLocalInitPath)
}

func getPythonRemoteInitPath() string {
	return helpers.GetEnv(EnvPythonRemoteInitPath, PythonRemoteInitPath)
}

func GetMaterializeNoTimestampQueryPath() string {
	return helpers.GetEnv(EnvMaterializeNoTimestampQueryPath, MaterializeNoTimestampQueryPath)
}

func GetMaterializeWithTimestampQueryPath() string {
	return helpers.GetEnv(EnvMaterializeWithTimestampQueryPath, MaterializeWithTimestampQueryPath)
}

func GetSlackChannelId() string {
	return helpers.GetEnv("SLACK_CHANNEL_ID", "") //no meaningful fallback ID
}

func GetIcebergProxyHost() string {
	return helpers.GetEnv("ICEBERG_PROXY_HOST", "localhost")
}

func GetIcebergProxyPort() string {
	return helpers.GetEnv("ICEBERG_PROXY_PORT", "8086")
}

type StateProviderType string

const (
	StateProviderNIL      StateProviderType = ""
	NoStateProvider       StateProviderType = "memory"
	PostgresStateProvider StateProviderType = "psql"
	EtcdStateProvider     StateProviderType = "etcd"
)

var AllStateProviderTypes = []StateProviderType{
	NoStateProvider, PostgresStateProvider, EtcdStateProvider,
}

var cached *FeatureformApp
var cachedErr error

// Used to make cached a singleton
var parseOnce = &sync.Once{}

func Get(logger logging.Logger) (*FeatureformApp, error) {
	parseOnce.Do(func() {
		logger.Info("Parsing Featureform app config")
		cached, cachedErr = parseFeatureformApp(logger)
	})
	logger.Debug("Returning Featureform app config")
	return cached, cachedErr
}

func parseFeatureformApp(logger logging.Logger) (*FeatureformApp, fferr.Error) {
	cfg := FeatureformApp{}
	if err := parseInitConfig(logger, &cfg); err != nil {
		logger.Errorw("Failed to parse init config", "err", err)
		return nil, err
	}
	if err := parseStateProvider(logger, &cfg); err != nil {
		logger.Errorw("Failed to parse state backend", "err", err)
		return nil, err
	}
	return &cfg, nil
}

func parseInitConfig(logger logging.Logger, cfg *FeatureformApp) fferr.Error {
	defaultTimeout := time.Second * 15
	logger.Debug("Looking up init timeout from env")
	timeout, err := helpers.LookupEnvDuration(EnvFFInitTimeout)
	if _, ok := err.(*helpers.EnvNotFound); ok {
		logger.Infof("ENV %s not set, using default timeout %v", EnvFFInitTimeout, defaultTimeout)
		timeout = defaultTimeout
	} else if err != nil {
		logger.Infof("Unable to parse ENV %s, using default timeout %v", EnvFFInitTimeout, defaultTimeout)
		timeout = defaultTimeout
	}
	cfg.InitTimeout = timeout
	if needsMigration(logger) {
		logger.Debugw("Extending timeout for migration", "timeout", timeout)
		cfg.InitTimeout = timeout + time.Minute
	}
	return nil
}

func needsMigration(logger logging.Logger) bool {
	stateProviderType, err := getStateProvider(logger)
	if err != nil {
		logger.Errorw("Failed to get state provider from env", "err", err)
		return false
	}
	return stateProviderType == PostgresStateProvider &&
		(ShouldRunGooseMigrationExecutable() || ShouldRunGooseMigrationMetadata())
}

func parseStateProvider(logger logging.Logger, cfg *FeatureformApp) fferr.Error {
	logger.Debug("Looking up state provider from env")
	stateProvider, err := getStateProvider(logger)
	if err != nil {
		return fferr.NewInternalErrorf("Failed to get state provider from env")
	}
	stateLogger := logger.With("state-provider", stateProvider)
	stateLogger.Infow("Using state provider from env")
	cfg.StateProviderType = stateProvider
	switch stateProvider {
	case NoStateProvider:
		stateLogger.Debug("Using memory state, nothing to parse")
		return nil
		// Do nothing
	case PostgresStateProvider:
		logger.Debug("Parsing Postgres config from env")
		psqlCfg, err := parsePostgres(stateLogger)
		if err != nil {
			logger.Errorw("Failed to parse postgres config", "err", err)
			return err
		}
		cfg.Postgres = psqlCfg
	case EtcdStateProvider:
		logger.Warn("ETCD state backend is deprecated, switch to PSQL")
		logger.Debug("Parsing Etcd config from env")
		etcdCfg, err := parseEtcd(stateLogger)
		if err != nil {
			logger.Errorw("Failed to parse etcd config", "err", err)
			return err
		}
		cfg.Etcd = etcdCfg
	default:
		stateLogger.Errorw("Invalid state provider")
		return fferr.NewInvalidConfigEnv(
			EnvFFStateProvider, string(stateProvider), AllStateProviderTypes,
		)
	}
	return nil
}

func getStateProvider(logger logging.Logger) (StateProviderType, error) {
	stateProviderStr, has := os.LookupEnv(EnvFFStateProvider)
	if !has {
		err := fferr.NewMissingConfigEnv(EnvFFStateProvider)
		logger.Errorf("%s", err.Error())
		return StateProviderNIL, err
	}
	stateProvider := StateProviderType(stateProviderStr)
	return stateProvider, nil
}

func parsePostgres(logger logging.Logger) (*postgres.Config, fferr.Error) {
	defaultEnvs := map[string]string{
		"PSQL_HOST":     "localhost",
		"PSQL_PORT":     "5432",
		"PSQL_USER":     "postgres",
		"PSQL_PASSWORD": "password",
		"PSQL_DB":       "postgres",
		"PSQL_SSLMODE":  "disable",
	}
	logger.Debugw("Parsing Postgres config from env.")
	envs := fillEnvMap(logger, defaultEnvs)
	cfg := postgres.Config{
		Host:     envs["PSQL_HOST"],
		Port:     envs["PSQL_PORT"],
		User:     envs["PSQL_USER"],
		Password: envs["PSQL_PASSWORD"],
		DBName:   envs["PSQL_DB"],
		SSLMode:  envs["PSQL_SSLMODE"],
	}
	logger.Infow("Postgres config parsed from env", "config", cfg.Redacted())
	return &cfg, nil
}

func parseEtcd(logger logging.Logger) (*etcd.Config, fferr.Error) {
	defaultEnvs := map[string]string{
		"ETCD_HOST":     "localhost",
		"ETCD_PORT":     "2379",
		"ETCD_USERNAME": "",
		"ETCD_PASSWORD": "",
	}
	envs := fillEnvMap(logger, defaultEnvs)
	cfg := etcd.Config{
		Host:     envs["ETCD_HOST"],
		Port:     envs["ETCD_PORT"],
		Username: envs["ETCD_USERNAME"],
		Password: envs["ETCD_PASSWORD"],
	}
	logger.Infow("Etcd config parsed from env", "config", cfg.Redacted())
	return &cfg, nil
}

func fillEnvMap(logger logging.Logger, defaultEnvs map[string]string) map[string]string {
	envs := make(map[string]string)
	for env, defVal := range defaultEnvs {
		envs[env] = getEnvWithDefault(logger, env, defVal)
	}
	return envs
}

func getEnvWithDefault(logger logging.Logger, env, defVal string) string {
	val, has := os.LookupEnv(env)
	if has {
		return val
	} else {
		logger.Infof("Env %s not set, using default value %s.", env, defVal)
		return defVal
	}
}

// TODO(simba) Move all envs into this Config
type FeatureformApp struct {
	// InitTimeout specifies how long the service has to initialize
	InitTimeout time.Duration
	// StateProviderType specifies where app-state is to be stored
	StateProviderType StateProviderType
	// This will only be set when StateProviderType is PostgresStateProvider
	Postgres *postgres.Config
	// This will only be set when StateProviderType is EtcdStateProvider
	Etcd *etcd.Config
}
