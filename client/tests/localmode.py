import featureform as ff
import pandas as pd


class TestQuickstart:
    file = './transactions.csv'
    entity = 'CustomerID'
    feature_col = 'TransactionAmount'
    label_col = 'IsFraud'
    training_set_name = 'fraud_training'
    training_set_variant = 'quickstart'
    feature_name = 'avg_transactions'
    feature_variant = 'quickstart'
    name_variant = feature_name + '.' + feature_variant
    entity_value = 'C1410926'
    entity_index = 43653
    feature_value = 5000.0

    def test_training_set(self):
        expected_tset = get_training_set_from_file(self.file, self.entity, self.feature_col, self.label_col,
                                                   self.name_variant)
        client = ff.ServingClient(local=True)
        dataset = client.training_set(self.training_set_name, self.training_set_variant)
        training_dataset = dataset
        for i, feature_batch in enumerate(training_dataset):
            assert feature_batch.features()[0] == expected_tset[i][0]
            assert feature_batch.label() == expected_tset[i][1]

    def test_training_set_repeat(self):
        half_test = get_training_set_from_file(self.file, self.entity, self.feature_col, self.label_col,
                                               self.name_variant)
        expected_tset = half_test + half_test
        client = ff.ServingClient(local=True)
        dataset = client.training_set(self.training_set_name, self.training_set_variant)
        training_dataset = dataset.repeat(1)
        for i, feature_batch in enumerate(training_dataset):
            assert feature_batch.features()[0] == expected_tset[i][0]
            assert feature_batch.label() == expected_tset[i][1]

    def test_training_set_shuffle(self):
        expected_tset = get_training_set_from_file(self.file, self.entity, self.feature_col, self.label_col,
                                                   self.name_variant)
        client = ff.ServingClient(local=True)
        dataset = client.training_set(self.training_set_name, self.training_set_variant)
        training_dataset = dataset.shuffle(1)
        rows = 0
        for feature_batch in training_dataset:
            rows += 1
        assert rows == len(expected_tset)

    def test_training_set_batch(self):
        expected_tset = get_training_set_from_file(self.file, self.entity, self.feature_col, self.label_col,
                                                   self.name_variant)
        client = ff.ServingClient(local=True)
        dataset = client.training_set(self.training_set_name, self.training_set_variant)
        training_dataset = dataset.batch(5)
        for i, feature_batch in enumerate(training_dataset):
            for j, row in enumerate(feature_batch):
                assert row.features()[0] == expected_tset[j + (i * 5)][0]
                assert row.label() == expected_tset[j + (i * 5)][1]

    def test_feature(self):
        client = ff.ServingClient(local=True)
        feature = client.features([(self.feature_name, self.feature_variant)], (self.entity, self.entity_value))
        assert feature == pd.array([self.entity_value])


def get_label(df: pd.DataFrame, entity, label):
    df = df[[entity, label]]
    df.rename(columns={label: 'label'}, inplace=True)
    return df


def get_feature(df: pd.DataFrame, entity, feature_col, name_variant):
    feature = df[[entity, feature_col]]
    feature.rename(columns={feature_col: name_variant}, inplace=True)
    feature.drop_duplicates(subset=[entity, name_variant])
    feature[entity] = feature[entity].astype('string')
    return feature


def run_transformation(df: pd.DataFrame, entity, col):
    df = df[[entity, col]]
    df.set_index(entity, inplace=True)
    training_set = df.groupby(entity)[col].mean()
    df = training_set.to_frame()
    df.reset_index(inplace=True)
    return df


def get_training_set(label: pd.DataFrame, feature: pd.DataFrame, entity):
    training_set_df = label
    training_set_df[entity] = training_set_df[entity].astype('string')
    training_set_df = training_set_df.join(feature.set_index(entity), how="left", on=entity,
                                           lsuffix="_left")
    training_set_df.drop(columns=entity, inplace=True)
    label_col = training_set_df.pop('label')
    training_set_df = training_set_df.assign(label=label_col)
    return training_set_df


def get_training_set_from_file(file, entity, feature_col, label, name_variant):
    df = pd.read_csv(file)
    transformation = run_transformation(df, entity, feature_col)
    feature = get_feature(transformation, entity, feature_col, name_variant)
    label = get_label(df, entity, label)
    training_set_df = get_training_set(label, feature, entity)
    return training_set_df.values.tolist()
