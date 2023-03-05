package metadata

import pc "github.com/featureform/provider/provider_config"

func isValidBigQueryConfigUpdate(sa, sb pc.SerializedConfig) (bool, error) {
	a := pc.BigQueryConfig{}
	b := pc.BigQueryConfig{}
	if err := a.Deserialize(sa); err != nil {
		return false, err
	}
	if err := b.Deserialize(sb); err != nil {
		return false, err
	}
	diff, err := a.DifferingFields(b)
	if err != nil {
		return false, err
	}
	return a.MutableFields().Contains(diff), nil
}

func isValidCassandraConfigUpdate(sa, sb pc.SerializedConfig) (bool, error) {
	a := pc.CassandraConfig{}
	b := pc.CassandraConfig{}
	if err := a.Deserialize(sa); err != nil {
		return false, err
	}
	if err := b.Deserialize(sb); err != nil {
		return false, err
	}
	diff, err := a.DifferingFields(b)
	if err != nil {
		return false, err
	}
	return a.MutableFields().Contains(diff), nil
}

func isValidDynamoConfigUpdate(sa, sb pc.SerializedConfig) (bool, error) {
	a := pc.DynamodbConfig{}
	b := pc.DynamodbConfig{}
	if err := a.Deserialize(sa); err != nil {
		return false, err
	}
	if err := b.Deserialize(sb); err != nil {
		return false, err
	}
	diff, err := a.DifferingFields(b)
	if err != nil {
		return false, err
	}
	return a.MutableFields().Contains(diff), nil
}

func isValidFirestoreConfigUpdate(sa, sb pc.SerializedConfig) (bool, error) {
	a := pc.FirestoreConfig{}
	b := pc.FirestoreConfig{}
	if err := a.Deserialize(sa); err != nil {
		return false, err
	}
	if err := b.Deserialize(sb); err != nil {
		return false, err
	}
	diff, err := a.DifferingFields(b)
	if err != nil {
		return false, err
	}
	return a.MutableFields().Contains(diff), nil
}

func isValidMongoConfigUpdate(sa, sb pc.SerializedConfig) (bool, error) {
	a := pc.MongoDBConfig{}
	b := pc.MongoDBConfig{}
	if err := a.Deserialize(sa); err != nil {
		return false, err
	}
	if err := b.Deserialize(sb); err != nil {
		return false, err
	}
	diff, err := a.DifferingFields(b)
	if err != nil {
		return false, err
	}
	return a.MutableFields().Contains(diff), nil
}

func isValidPostgresConfigUpdate(sa, sb pc.SerializedConfig) (bool, error) {
	a := pc.PostgresConfig{}
	b := pc.PostgresConfig{}
	if err := a.Deserialize(sa); err != nil {
		return false, err
	}
	if err := b.Deserialize(sb); err != nil {
		return false, err
	}
	diff, err := a.DifferingFields(b)
	if err != nil {
		return false, err
	}
	return a.MutableFields().Contains(diff), nil
}

func isValidRedisConfigUpdate(sa, sb pc.SerializedConfig) (bool, error) {
	a := pc.RedisConfig{}
	b := pc.RedisConfig{}
	if err := a.Deserialize(sa); err != nil {
		return false, err
	}
	if err := b.Deserialize(sb); err != nil {
		return false, err
	}
	diff, err := a.DifferingFields(b)
	if err != nil {
		return false, err
	}
	return a.MutableFields().Contains(diff), nil
}

func isValidSnowflakeConfigUpdate(sa, sb pc.SerializedConfig) (bool, error) {
	a := pc.SnowflakeConfig{}
	b := pc.SnowflakeConfig{}
	if err := a.Deserialize(sa); err != nil {
		return false, err
	}
	if err := b.Deserialize(sb); err != nil {
		return false, err
	}
	diff, err := a.DifferingFields(b)
	if err != nil {
		return false, err
	}
	return a.MutableFields().Contains(diff), nil
}

func isValidRedshiftConfigUpdate(sa, sb pc.SerializedConfig) (bool, error) {
	a := pc.RedshiftConfig{}
	b := pc.RedshiftConfig{}
	if err := a.Deserialize(sa); err != nil {
		return false, err
	}
	if err := b.Deserialize(sb); err != nil {
		return false, err
	}
	diff, err := a.DifferingFields(b)
	if err != nil {
		return false, err
	}
	return a.MutableFields().Contains(diff), nil
}

func isValidK8sConfigUpdate(sa, sb pc.SerializedConfig) (bool, error) {
	a := pc.K8sConfig{}
	b := pc.K8sConfig{}
	if err := a.Deserialize(sa); err != nil {
		return false, err
	}
	if err := b.Deserialize(sb); err != nil {
		return false, err
	}
	diff, err := a.DifferingFields(b)
	if err != nil {
		return false, err
	}
	return a.MutableFields().Contains(diff), nil
}

func isValidSparkConfigUpdate(sa, sb pc.SerializedConfig) (bool, error) {
	a := pc.SparkConfig{}
	b := pc.SparkConfig{}
	if err := a.Deserialize(sa); err != nil {
		return false, err
	}
	if err := b.Deserialize(sb); err != nil {
		return false, err
	}
	diff, err := a.DifferingFields(b)
	if err != nil {
		return false, err
	}
	return a.MutableFields().Contains(diff), nil
}
