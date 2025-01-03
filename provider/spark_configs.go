// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/featureform/filestore"
	"github.com/featureform/helpers/stringset"
	"github.com/featureform/logging"
	pc "github.com/featureform/provider/provider_config"
	pl "github.com/featureform/provider/location"
	"github.com/featureform/provider/types"
)

type sparkCommand struct {
	Script filestore.Filepath
	ScriptArgs []string
	Configs sparkConfigs
}

func (cmd *sparkCommand) AddConfigs(cfgs ...sparkConfig) {
	cmd.Configs = append(cmd.Configs, cfgs...)
}

func (cmd *sparkCommand) Compile() []string {
	return cmd.Configs.CompileCommand(cmd.Script, cmd.ScriptArgs...)
}

type sparkConfigs []sparkConfig

func (cfgs sparkConfigs) CompileCommand(scriptLoc filestore.Filepath, args ...string) []string {
	cmd := []string{
		"spark-submit",
	}
	list := make(sparkFlagsList, len(cfgs))
	for i, cfg := range cfgs {
		list[i] = cfg.SparkFlags()
	}
	native, script := list.SeparateNativeFlags()
	cmd = append(cmd, native.SparkStringFlags()...)
	cmd = append(cmd, scriptLoc.ToURI())
	cmd = append(cmd, args...)
	cmd = append(cmd, script.SparkStringFlags()...)
	return cmd
}

type sparkConfig interface {
	SparkFlags() sparkFlags
}

type sparkFlagsList []sparkFlags

func (flagsList sparkFlagsList) SeparateNativeFlags() (sparkNativeFlags, sparkScriptFlags) {
	allNative := make(sparkNativeFlags, 0)
	allScript := make(sparkScriptFlags, 0)
	for _, flags := range flagsList {
		native, script := flags.SeparateNativeFlags()
		allNative = append(allNative, native...)
		allScript = append(allScript, script...)
	}
	return allNative, allScript
}

func (flagsList sparkFlagsList) SparkStringFlags() []string {
	args := make([]string, 0)
	for _, flags := range flagsList {
		args = append(args, flags.SparkStringFlags()...)
	}
	return args
}

type sparkNativeFlags sparkFlags

func (flags sparkNativeFlags) SparkStringFlags() []string {
	return sparkFlags(flags).SparkStringFlags()
}

type sparkScriptFlags sparkFlags

func (flags sparkScriptFlags) SparkStringFlags() []string {
	return sparkFlags(flags).SparkStringFlags()
}

type sparkFlags []sparkFlagStringer

func (flags sparkFlags) SeparateNativeFlags() (sparkNativeFlags, sparkScriptFlags) {
	native := make(sparkNativeFlags, 0)
	script := make(sparkScriptFlags, 0)
	for _, flag := range flags {
		if flag.IsSparkSubmitNative() {
			native = append(native, flag)
		} else {
			script = append(script, flag)
		}
	}
	return native, script
}

func (flags sparkFlags) SparkStringFlags() []string {
	combinedFlags := flags.combinedFlags()
	args := make([]string, 0, len(combinedFlags)*2)
	for _, flag := range combinedFlags {
		args = append(args, flag.SparkStringFlags()...)
	}
	return args
}

func (flags sparkFlags) combinedFlags() sparkFlags {
	combined := make(sparkFlags, 0)
	for i := 0; i < len(flags); i++ {
		flag := flags[i]
		if flag == nil {
			continue
		}
		for j := i + 1; j < len(flags); j++ {
			other := flags[j]
			if combo := flag.TryCombine(other); combo != nil {
				flag = combo
				flags[j] = nil
			}
		}
		combined = append(combined, flag)
	}
	return combined
}

type sparkFlagStringer interface {
	SparkStringFlags() []string
	// Used to distinguish between script flags and spark-submit flags.
	// spark-submit <native flags> file.py <script flags>
	IsSparkSubmitNative() bool
	// If two flag stringers have the same combine key, they can be
	// combined. This is important in flags like --packages that
	// need to be a comma separated list.
	TryCombine(sparkFlagStringer) sparkFlagStringer
}

// These are flags that are native to the spark-submit command.
// They go between spark-submit and the script name.
// spark-submit <native flags> file.py <script flags>
type sparkSubmitFlag struct {
	Key   string
	Value string
}

func (flag sparkSubmitFlag) SparkStringFlags() []string {
	return []string{
		fmt.Sprintf("--%s", flag.Key),
		flag.Value,
	}
}

func (flag sparkSubmitFlag) IsSparkSubmitNative() bool {
	return true
}

func (flag sparkSubmitFlag) TryCombine(other sparkFlagStringer) sparkFlagStringer {
	return nil
}

type sparkScriptArg struct {
}

// These are flags that are NOT native to spark-submit and are parsed
// directly by our offline runner. They go after the script name.
// spark-submit <native flags> file.py <script flags>
type sparkScriptFlag struct {
	Key   string
	Value string
}

func (flag sparkScriptFlag) SparkStringFlags() []string {
	return []string{
		fmt.Sprintf("--%s", flag.Key),
		flag.Value,
	}
}

func (flag sparkScriptFlag) IsSparkSubmitNative() bool {
	return false
}

func (flag sparkScriptFlag) TryCombine(other sparkFlagStringer) sparkFlagStringer {
	return nil
}

// sparkNativeConfigFlag are passed via --conf to the spark submit
type sparkNativeConfigFlag struct {
	Key   string
	Value string
}

func (config sparkNativeConfigFlag) SparkStringFlags() []string {
	// TODO make sure key/value don't contain =
	return []string{
		"--conf",
		fmt.Sprintf("%s=%s", config.Key, config.Value),
	}
}

func (flag sparkNativeConfigFlag) IsSparkSubmitNative() bool {
	return true
}

func (flag sparkNativeConfigFlag) TryCombine(other sparkFlagStringer) sparkFlagStringer {
	return nil
}

// sparkConfigFlag should be set in Spark via spark.config in
// our PySpark scripts.
type sparkConfigFlag struct {
	Key   string
	Value string
}

func (config sparkConfigFlag) SparkStringFlags() []string {
	// TODO make sure key/value don't contain =
	return []string{
		"--spark_config",
		fmt.Sprintf("\"%s=%s\"", config.Key, config.Value),
	}
}

func (flag sparkConfigFlag) IsSparkSubmitNative() bool {
	return false
}

func (flag sparkConfigFlag) TryCombine(other sparkFlagStringer) sparkFlagStringer {
	return nil
}

// sparkCredFlag are set via --credential flags and are handled directly
// by the script. Typically in a non-Spark specific way.
type sparkCredFlag struct {
	Key   string
	Value string
}

func (flag sparkCredFlag) SparkStringFlags() []string {
	// TODO make sure key/value don't contain =
	return []string{
		"--credential",
		fmt.Sprintf("\"%s=%s\"", flag.Key, flag.Value),
	}
}

func (flag sparkCredFlag) IsSparkSubmitNative() bool {
	return false
}

func (flag sparkCredFlag) TryCombine(other sparkFlagStringer) sparkFlagStringer {
	return nil
}

type sparkPackagesFlag struct {
	Packages []string
}

func (flag sparkPackagesFlag) SparkStringFlags() []string {
	return []string{
		"--packages",
		strings.Join(flag.Packages, ","),
	}
}

func (flag sparkPackagesFlag) IsSparkSubmitNative() bool {
	return true
}

func (this sparkPackagesFlag) TryCombine(other sparkFlagStringer) sparkFlagStringer {
	// All packages should be with the same --packages flag and comma separated
	that, ok := other.(sparkPackagesFlag)
	if !ok {
		return nil
	}
	set := stringset.NewOrdered(this.Packages...)
	duplicates := set.AddAndGetDuplicates(that.Packages...)
	list := set.ToList()
	if len(duplicates) > 0 {
		logging.GlobalLogger.Warnw(
			"Ignoring duplicate packag.",
			"duplicates", duplicates, "full_list", list,
		)
	}
	return sparkPackagesFlag{
		Packages: list,
	}
}

type sparkSourcesFlag struct {
	Sources []pysparkSourceInfo
}

func (flag sparkSourcesFlag) SparkStringFlags() []string {
	serializedSources := make([]string, len(flag.Sources))
	for i, source := range flag.Sources {
		serialized, err := source.Serialize()
		if err != nil {
			// TODO better error handling
			logging.GlobalLogger.Warnw(
				"Failed to serialized source, but will continue.",
				"source", source, "error", err,
			)
		}
		serializedSources[i] = serialized
	}
	flags := []string{"--sources"}
	return append(flags, serializedSources...)
}

func (flag sparkSourcesFlag) IsSparkSubmitNative() bool {
	return false
}

func (this sparkSourcesFlag) TryCombine(other sparkFlagStringer) sparkFlagStringer {
	// All source should be under the same --source flag
	that, ok := other.(sparkSourcesFlag)
	if !ok {
		return nil
	}
	joined := make([]pysparkSourceInfo, 0, len(this.Sources)+len(that.Sources))
	joined = append(joined, this.Sources...)
	joined = append(joined, that.Sources...)
	return sparkSourcesFlag{
		Sources: joined,
	}
}

func (flag sparkSourcesFlag) SparkFlags() sparkFlags {
	return sparkFlags{
		flag,
	}
}

type sparkIncludePyScript struct {
	Path filestore.Filepath
}

func (flag sparkIncludePyScript) SparkFlags() sparkFlags {
	return sparkFlags{
		sparkSubmitFlag{
			"py-files",
			flag.Path.ToURI(),
		},
	}
}

// sqlSubmitParamsURI points at a file containing --sql_query and --sources
// to get around character limits in spark submit APIs.
type sparkSqlSubmitParamsURIFlag struct {
	URI filestore.Filepath
}

func (flag sparkSqlSubmitParamsURIFlag ) SparkFlags() sparkFlags {
	return sparkFlags{
		sparkScriptFlag{
			"submit_params_uri",
			flag.URI.Key(),
		},
	}
}

type sparkSqlQueryFlag struct {
	CleanQuery string
	Sources []pysparkSourceInfo
}

func (flag sparkSqlQueryFlag) SparkFlags() sparkFlags {
	return sparkFlags{
		sparkScriptFlag{
			"sql_query",
			flag.CleanQuery,
		},
		sparkSourcesFlag{
			Sources: flag.Sources,
		},
	}
}

type sparkDataframeQueryFlag struct {
	Code string
	Sources []pysparkSourceInfo
}

func (flag sparkDataframeQueryFlag) SparkFlags() sparkFlags {
	return sparkFlags{
		sparkScriptFlag{
			"code",
			flag.Code,
		},
		sparkSourcesFlag{
			Sources: flag.Sources,
		},
	}
}

type sparkDeployFlag struct {
	Mode types.SparkDeployMode
}

func (flag sparkDeployFlag) SparkFlags() sparkFlags {
	return sparkFlags{
		sparkSubmitFlag{
			"deploy-mode",
			flag.Mode.SparkArg(),
		},
	}
}

type sparkSnowflakeFlags struct {
	Config *pc.SnowflakeConfig
	ExecutorType pc.SparkExecutorType
}

func (args sparkSnowflakeFlags) SparkFlags() sparkFlags {
	if args.Config == nil {
		logging.GlobalLogger.Debug(
			"Not setting spark snowflake flags, snowflake config not set",
		)
		return sparkFlags{}
	}
	flags := sparkFlags {
		sparkCredFlag{
			Key: "sfUrl",
			Value: args.Config.GetBaseURL(),
		},
		sparkCredFlag{
			Key: "sfUser",
			Value: args.Config.Username,
		},
		sparkCredFlag{
			Key: "sfPassword",
			Value: args.Config.Password,
		},
		sparkCredFlag{
			Key: "sfWarehouse",
			Value: args.Config.Warehouse,
		},
	}
	// Databricks has these packages pre-installed.
	if args.ExecutorType == pc.EMR {
		flags = append(flags, sparkPackagesFlag{
			Packages: []string{
				"net.snowflake:snowflake-jdbc:3.13.22",
				"net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4",
			},
		})
	}
	return flags
}

// This is based on very legacy values and aren't tested
type sparkAzureFlags struct {
	AccountName string
	AccountKey string
	ConnectionString string
	ContainerName string
}

func (args sparkAzureFlags) SparkFlags() sparkFlags {
	return sparkFlags{
		sparkConfigFlag{
			Key: fmt.Sprintf("fs.azure.account.key.%s.dfs.core.windows.net", args.AccountName),
			Value: args.AccountKey,
		},
		sparkCredFlag{
			Key:   "azure_connection_string",
			Value: args.ConnectionString,
		},
		sparkCredFlag{
			Key:   "azure_container_name",
			Value: args.ContainerName,
		},
		sparkPackagesFlag{
			Packages: []string{"org.apache.hadoop:hadoop-azure-3.2.0"},
		},
	}
}

// This is based on very legacy values and aren't tested
type sparkGCSFlags struct {
	ProjectID string
	Bucket string
	JSONCreds []byte
}

func (args sparkGCSFlags) SparkFlags() sparkFlags {
	return sparkFlags{
		sparkConfigFlag{
			Key: "fs.gs.impl",
			Value: "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
		},
		sparkConfigFlag{
			Key:   "fs.AbstractFileSystem.gs.impl",
			Value: "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
		},
		sparkConfigFlag{
			Key:   "fs.gs.auth.service.account.enable",
			Value: "true",
		},
		sparkConfigFlag{
			Key:   "fs.gs.auth.type",
			Value: "SERVICE_ACCOUNT_JSON_KEYFILE",
		},
		sparkCredFlag{
			Key:   "gcp_project_id",
			Value: args.ProjectID,
		},
		sparkCredFlag{
			Key:   "gcp_bucket_name",
			Value: args.Bucket,
		},
		sparkCredFlag{
			Key:   "gcp_credentials",
			Value: base64.StdEncoding.EncodeToString(args.JSONCreds),
		},
		sparkPackagesFlag{
			Packages: []string{"com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.0"},
		},
	}
}
type sparkS3Flags struct {
	// AccessKey and SecretKey are optional, if they aren't set we default
	// to assuming a role.
	AccessKey, SecretKey string
	// Region defaults to us-east-1 if not set.
	Region string
	Bucket string
}

func (args sparkS3Flags) SparkFlags() sparkFlags {
	// TODO better arg checking and handling
	if args.Region == "" {
		args.Region = "us-east-1"
	}
	flags := sparkFlags{
		sparkConfigFlag{
			Key:   "spark.hadoop.fs.s3.impl",
			Value: "org.apache.hadoop.fs.s3a.S3AFileSystem",
		},
		sparkCredFlag{
			Key:   "aws_bucket_name",
			Value: args.Bucket,
		},
		sparkCredFlag{
			Key:   "aws_region",
			Value: args.Region,
		},
		sparkConfigFlag{
			Key:   "fs.s3a.endpoint",
			Value: fmt.Sprintf("s3.%s.amazonaws.com", args.Region),
		},
		sparkScriptFlag{
			Key:   "store_type",
			Value: "s3",
		},
	}
	if args.AccessKey != "" && args.SecretKey != "" {
		flags = append(flags,
			sparkConfigFlag{
				Key:   "fs.s3a.aws.credentials.provider",
				Value: "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
			},
			sparkConfigFlag{
				Key:   "fs.s3a.access.key",
				Value: args.AccessKey,
			},
			sparkConfigFlag{
				Key:   "fs.s3a.secret.key",
				Value: args.SecretKey,
			},
			sparkCredFlag{
				Key:   "aws_access_key_id",
				Value: args.AccessKey,
			},
			sparkCredFlag{
				Key:   "aws_secret_key_id",
				Value: args.SecretKey,
			},
		)
	} else {
		flags = append(flags,
			sparkCredFlag{
				Key:   "use_service_account",
				Value: "true",
			},
		)
	}
	return flags
}

type sparkGlueFlags struct {
	fileStoreType   types.FileStoreType
	tableFormatType types.TableFormatType
	Region          string
	Warehouse       string
}

func (args sparkGlueFlags) SparkFlags() sparkFlags {
	if args.fileStoreType != types.S3Type {
		panic("TODO better fail")
	}
	switch args.tableFormatType {
	case types.IcebergType:
		return args.icebergFlags()
	case types.DeltaType:
		return args.deltaFlags()
	default:
		panic("TODO better fail")
	}
}

func (args sparkGlueFlags) icebergFlags() sparkFlags {
	return sparkFlags{
		sparkConfigFlag{
			Key:   "spark.sql.catalog.ff_catalog.io-impl",
			Value: "org.apache.iceberg.aws.s3.S3FileIO",
		},
		sparkConfigFlag{
			Key:   "spark.sql.catalog.ff_catalog.region",
			Value: args.Region,
		},
		sparkConfigFlag{
			Key:   "spark.sql.catalog.ff_catalog",
			Value: "org.apache.iceberg.spark.SparkCatalog",
		},
		sparkConfigFlag{
			Key:   "spark.sql.catalog.ff_catalog.catalog-impl",
			Value: "org.apache.iceberg.aws.glue.GlueCatalog",
		},
		sparkConfigFlag{
			Key:   "spark.sql.catalog.ff_catalog.warehouse",
			Value: args.Warehouse,
		},
	}
}

func (args sparkGlueFlags) deltaFlags() sparkFlags {
	return sparkFlags{
		sparkConfigFlag{
			Key:   "spark.sql.catalog.spark_catalog",
			Value: "org.apache.spark.sql.delta.catalog.DeltaCatalog",
		},
		sparkConfigFlag{
			Key:   "spark.hadoop.hive.metastore.client.factory.class",
			Value: "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
		},
		sparkConfigFlag{
			Key:   "spark.sql.catalogImplementation",
			Value: "hive",
		},
	}
}

type sparkIcebergFlags struct{}

func (args sparkIcebergFlags) SparkFlags() sparkFlags {
	return sparkFlags{
		sparkConfigFlag{
			Key:   "spark.sql.extensions",
			Value: "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
		},
		sparkPackagesFlag{
			Packages: []string{"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"},
		},
	}
}

type sparkDeltaFlags struct{}

func (args sparkDeltaFlags) SparkFlags() sparkFlags {
	return sparkFlags{
		sparkConfigFlag{
			Key:   "spark.sql.extensions",
			Value: "io.delta.sql.DeltaSparkSessionExtension",
		},
	}
}

type sparkKafkaFlags struct{}

func (args sparkKafkaFlags) SparkFlags() sparkFlags {
	return sparkFlags{
		sparkPackagesFlag{
			Packages: []string{
				"org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
				"software.amazon.msk:aws-msk-iam-auth:2.2.0",
			},
		},
	}
}

type directCopyTarget string

const (
	noDirectCopyTarget directCopyTarget = ""
	directCopyDynamo   directCopyTarget = "dynamo"
)

type sparkDirectCopyFlags struct {
	Creds           sparkConfig
	Target          directCopyTarget
	TableName       string
	FeatureName     string
	FeatureVariant  string
	EntityColumn    string
	ValueColumn     string
	TimestampColumn string
}

func (args sparkDirectCopyFlags) SparkFlags() sparkFlags {
	credFlags := args.Creds.SparkFlags()
	copyFlags := sparkFlags{
		sparkScriptFlag{
			Key:   "direct_copy_use_iceberg",
			Value: "true",
		},
		sparkScriptFlag{
			Key:   "direct_copy_target",
			Value: string(args.Target),
		},
		sparkScriptFlag{
			Key:   "direct_copy_table_name",
			Value: args.TableName,
		},
		sparkScriptFlag{
			Key:   "direct_copy_feature_name",
			Value: args.FeatureName,
		},
		sparkScriptFlag{
			Key:   "direct_copy_feature_variant",
			Value: args.FeatureVariant,
		},
		sparkScriptFlag{
			Key:   "direct_copy_entity_column",
			Value: args.EntityColumn,
		},
		sparkScriptFlag{
			Key:   "direct_copy_value_column",
			Value: args.ValueColumn,
		},
	}
	if args.TimestampColumn != "" {
		copyFlags = append(copyFlags, sparkScriptFlag{
			Key:   "direct_copy_timestamp_column",
			Value: args.TimestampColumn,
		})
	}
	return append(credFlags, copyFlags...)
}

type sparkDynamoFlags struct {
	Region    string
	AccessKey string
	SecretKey string
}

func (args sparkDynamoFlags) SparkFlags() sparkFlags {
	return sparkFlags{
		sparkCredFlag{
			Key:   "dynamo_aws_access_key_id",
			Value: args.AccessKey,
		},
		sparkCredFlag{
			Key:   "dynamo_aws_secret_access_key",
			Value: args.SecretKey,
		},
		sparkCredFlag{
			Key:   "dynamo_aws_region",
			Value: args.Region,
		},
	}
}

type sparkJobTypeFlag struct {
	Type JobType
}

func (flag sparkJobTypeFlag) SparkFlags() sparkFlags {
	return sparkFlags{
		sparkScriptFlag{
			Key:   "job_type",
			Value: string(flag.Type),
		},
	}
}

type sparkOutputFlag struct {
	Output pl.Location
}

func (flag sparkOutputFlag) SparkFlags() sparkFlags {
	if flag.Output == nil {
		return sparkFlags{
			// Script expects an output always and needs to be JSON.
			sparkScriptFlag{
				Key:   "output",
				Value: "{}",
			},
		}
	}
	outputStr, err := flag.Output.Serialize()
	if err != nil {
		logging.GlobalLogger.Errorw(
			"Failed to serialize output for spark. Skipping flags.",
			"location", flag.Output,
			"error", err,
		)
		return sparkFlags{
			// Script expects an output always and needs to be JSON.
			sparkScriptFlag{
				Key:   "output",
				Value: "{}",
			},
		}
	}
	return sparkFlags{
		sparkScriptFlag{
			Key:   "output",
			Value: outputStr,
		},
	}
}

// This is a legacy flag to keep the old version of
// materialization working.
type sparkLegacyOutputFormatFlag struct {
	FileType filestore.FileType
}

func (flag sparkLegacyOutputFormatFlag) SparkFlags() sparkFlags {
	switch flag.FileType {
	case filestore.Parquet, filestore.CSV:
		return sparkFlags {
			sparkScriptFlag{
				Key: "output_format",
				Value: string(flag.FileType),
			},
		}
	case filestore.NilFileType:
		// Default to Parquet
		return sparkFlags {
			sparkScriptFlag{
				Key: "output_format",
				Value: string(filestore.Parquet),
			},
		}
	default:
		// Default to Parquet
		logging.GlobalLogger.Warnw(
			"Unsupported file type for output format flag. Default to Parquet.",
			"filetype", flag.FileType,
		)
		return sparkFlags {
			sparkScriptFlag{
				Key: "output_format",
				Value: string(filestore.Parquet),
			},
		}
	}
}

// This is a legacy flag to keep the old version of
// materialization working.
type sparkLegacyIncludeHeadersFlag struct {
	ShouldInclude bool
}

func (flag sparkLegacyIncludeHeadersFlag) SparkFlags() sparkFlags {
	if flag.ShouldInclude {
		// Script defaults to include
		return sparkFlags{}
	} else {
		return sparkFlags{
			sparkScriptFlag{
				Key: "headers",
				Value: "exclude",
			},
		}
	}
}

type sparkMasterFlag struct {
	Master string
}

func (flag sparkMasterFlag) SparkFlags() sparkFlags {
	return sparkFlags{
		sparkSubmitFlag{
			"master",
			flag.Master,
		},
	}
}

type sparkHighMemoryFlags struct{}

func (args sparkHighMemoryFlags) SparkFlags() sparkFlags {
	return sparkFlags{
		sparkNativeConfigFlag{
			Key:   "spark.executor.memory",
			Value: "2g",
		},
		sparkNativeConfigFlag{
			Key:   "spark.driver.memory",
			Value: "2g",
		},
		sparkNativeConfigFlag{
			Key:   "spark.executor.memoryOverhead",
			Value: "1g",
		},
	}
}
