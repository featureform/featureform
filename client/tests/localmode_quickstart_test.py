import os
import shutil
import stat

import featureform as ff
from featureform import local
import pandas as pd
import pytest

class Quickstart:
    file = './transactions.csv'
    entity_col = 'CustomerID'
    entity = 'user'
    feature_col = 'TransactionAmount'
    label_col = 'IsFraud'
    training_set_name = 'fraud_training'
    training_set_variant = 'quickstart'
    feature_name = 'avg_transactions'
    feature_variant = 'quickstart'
    name_variant = f"{feature_name}.{feature_variant}"
    entity_value = 'C1410926'
    entity_index = 43653
    feature_value = 5000.0

    def test_training_set(self):
        expected_tset = get_training_set_from_file(self.file, self.entity_col, self.feature_col, self.label_col,
                                                   self.name_variant)
        client = ff.ServingClient(local=True)
        dataset = client.training_set(self.training_set_name, self.training_set_variant)
        training_dataset = dataset
        for i, feature_batch in enumerate(training_dataset):
            assert feature_batch.features()[0] == [expected_tset[i][0]]
            assert feature_batch.label() == [expected_tset[i][1]]
        

    def test_training_set_repeat(self):
        half_test = get_training_set_from_file(self.file, self.entity_col, self.feature_col, self.label_col,
                                               self.name_variant)
        expected_tset = half_test + half_test
        client = ff.ServingClient(local=True)
        dataset = client.training_set(self.training_set_name, self.training_set_variant)
        training_dataset = dataset.repeat(1)
        for i, feature_batch in enumerate(training_dataset):
            assert feature_batch.features()[0] == [expected_tset[i][0]]
            assert feature_batch.label() == [expected_tset[i][1]]

    def test_training_set_shuffle(self):
        expected_tset = get_training_set_from_file(self.file, self.entity_col, self.feature_col, self.label_col,
                                                   self.name_variant)
        client = ff.ServingClient(local=True)
        dataset = client.training_set(self.training_set_name, self.training_set_variant)
        training_dataset = dataset.shuffle(1)
        rows = 0
        for feature_batch in training_dataset:
            rows += 1
        assert rows == len(expected_tset)

    def test_training_set_batch(self):
        expected_test = get_training_set_from_file(self.file, self.entity_col, self.feature_col, self.label_col,
                                                   self.name_variant)
        client = ff.ServingClient(local=True)
        dataset = client.training_set(self.training_set_name, self.training_set_variant)
        training_dataset = dataset.batch(5)
        for i, feature_batch in enumerate(training_dataset):
            batch_vals = zip(feature_batch.features(), feature_batch.label())
            for j, batch in enumerate(batch_vals):
                features, label = batch
                assert features[0] == expected_test[j + (i * 5)][0]
                assert label == expected_test[j + (i * 5)][1]

    def test_training_set_dataframe(self):
        expected_tset = get_training_set_from_file(self.file, self.entity_col, self.feature_col, self.label_col,
                                                   self.name_variant)
        client = ff.ServingClient(local=True)
        dataset = client.training_set(self.training_set_name, self.training_set_variant).pandas()
        training_dataset = dataset
        for i, feature_batch in enumerate(training_dataset):
            features = feature_batch.iloc[:, :-1]
            label = feature_batch.iloc[:, [-1]]
            assert features.iloc[i, 0] == [expected_tset[i][0]]
            assert labels.iloc[i, 0] == [expected_tset[i][1]]
    
    def test_training_set_dataframe_repeat(self):
        half_test = get_training_set_from_file(self.file, self.entity_col, self.feature_col, self.label_col,
                                               self.name_variant)
        expected_tset = half_test + half_test
        client = ff.ServingClient(local=True)
        dataset = client.training_set(self.training_set_name, self.training_set_variant)
        training_dataset = dataset.repeat(1).pandas()
        for i, feature_batch in enumerate(training_dataset):
            features = feature_batch.iloc[:, :-1]
            labels = feature_batch.iloc[:, [-1]]
            assert features.iloc[i, 0] == [expected_tset[i][0]]
            assert labels.iloc[i, 0] == [expected_tset[i][1]]

    def test_training_set_dataframe_shuffle(self):
        expected_tset = get_training_set_from_file(self.file, self.entity_col, self.feature_col, self.label_col,
                                                   self.name_variant)
        client = ff.ServingClient(local=True)
        dataset = client.training_set(self.training_set_name, self.training_set_variant)
        training_dataset = dataset.shuffle(1).pandas()
        rows = 0
        for feature_batch in training_dataset:
            rows += 1
        assert rows == len(expected_tset)

    def test_training_set_dataframe_batch(self):
        expected_test = get_training_set_from_file(self.file, self.entity_col, self.feature_col, self.label_col,
                                                   self.name_variant)
        client = ff.ServingClient(local=True)
        dataset = client.training_set(self.training_set_name, self.training_set_variant)
        training_dataset = dataset.batch(5).pandas()
        for i, feature_batch in enumerate(training_dataset):
            features = feature_batch.iloc[:, :-1]
            labels = feature_batch.iloc[:, [-1]]
            for i in range(len(features)):
                assert features.iloc[i, 0] == expected_test[j + (i * 5)][0]
                assert labels.iloc[i, 0] == expected_test[j + (i * 5)][1]
        
    def test_feature(self):
        client = ff.ServingClient(local=True)
        feature = client.features([(self.feature_name, self.feature_variant)], {self.entity: self.entity_value})
        assert feature == pd.array([self.entity_value])


    def cleanup(self):
        try:
            client = ff.ServingClient(local=True)
            client.sqldb.close()
            shutil.rmtree('.featureform', onerror=del_rw)
        except:
            print("File Already Removed")

def del_rw(action, name, exc):
    os.chmod(name, stat.S_IWRITE)
    os.remove(name)

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


def get_training_set_from_file(file, entity_col, feature_col, label, name_variant):
    df = pd.read_csv(file)
    transformation = run_transformation(df, entity_col, feature_col)
    feature = get_feature(transformation, entity_col, feature_col, name_variant)
    label = get_label(df, entity_col, label)
    training_set_df = get_training_set(label, feature, entity_col)
    return training_set_df.values.tolist()

class TestCLI:

    def test_setup(self):
        import subprocess

        apply = subprocess.run(['featureform', 'apply', 'client/examples/local_quickstart.py', '--local'])
        print("The exit code was: %d" % apply.returncode)
        assert apply.returncode == 0, f"OUT: {apply.stdout}, ERR: {apply.stderr}"

    Test = Quickstart


class TestResourceClient:
    def test_setup(self):

        transactions = local.register_file(
            name="transactions",
            variant="quickstart",
            description="A dataset of fraudulent transactions",
            path="transactions.csv"
        )

        @local.df_transformation(variant="quickstart",
                                 inputs=[("transactions", "quickstart")])
        def average_user_transaction(transactions):
            """the average transaction amount for a user """
            return transactions.groupby("CustomerID")["TransactionAmount"].mean()

        @local.sql_transformation(variant="quickstart")
        def sql_average_user_transaction():
            """the average transaction amount for a user """
            return "SELECT CustomerID as user_id, avg(TransactionAmount) " \
                "as avg_transaction_amt from {{transactions.kaggle}} GROUP BY user_id"
    
        user = ff.register_entity("user")

        # Register a column from our transformation as a feature
        average_user_transaction.register_resources(
            entity=user,
            entity_column="CustomerID",
            inference_store=local,
            features=[
                {"name": "avg_transactions", "variant": "quickstart", "column": "TransactionAmount", "type": "float32"},
            ],
        )

        sql_average_user_transaction.register_resources(
            entity=user,
            entity_column="CustomerID",
            inference_store=local,
            features=[
                {"name": "avg_transactions", "variant": "sql", "column": "TransactionAmount", "type": "float32"},
            ],
        )

        # Register label from our base Transactions table
        transactions.register_resources(
            entity=user,
            entity_column="CustomerID",
            labels=[
                {"name": "fraudulent", "variant": "quickstart", "column": "IsFraud", "type": "bool"},
            ],
        )

        ff.register_training_set(
            "fraud_training", "quickstart",
            label=("fraudulent", "quickstart"),
            features=[("avg_transactions", "quickstart")],
        )

        client = ff.ResourceClient(local=True)
        client.apply()

    Tests = Quickstart


