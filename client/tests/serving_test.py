import csv
import shutil
import time
from tempfile import NamedTemporaryFile
from unittest import TestCase
import os, stat
import numpy as np

import featureform.resources
import pandas as pd
import pytest
import sys

sys.path.insert(0, 'client/src/')
from featureform import ResourceClient, ServingClient
import serving_cases as cases
import featureform as ff
from datetime import datetime


class TestIndividualFeatures(TestCase):
    def test_process_feature_no_ts(self):
        for name, case in cases.features_no_ts.items():
            with self.subTest(name):
                print("TEST: ", name)
                file_name = create_temp_file(case)
                client = ServingClient(local=True)
                dataframe_mapping = client.process_feature_csv(file_name, case['entity'], case['entity'],
                                                               case['value_col'], [], "test_name_variant", "")
                expected = pd.DataFrame(case['expected'])
                actual = dataframe_mapping[0]
                expected = expected.values.tolist()
                actual = actual.values.tolist()
                client.sqldb.close()
                assert all(elem in expected for elem in actual), \
                    "Expected: {} Got: {}".format(expected, actual)

    def test_process_feature_with_ts(self):
        for name, case in cases.features_with_ts.items():
            with self.subTest(msg=name):
                print("TEST: ", name)
                file_name = create_temp_file(case)
                client = ServingClient(local=True)
                dataframe_mapping = client.process_feature_csv(file_name, case['entity'], case['entity'],
                                                               case['value_col'], [], "test_name_variant",
                                                               case['ts_col'])
                expected = pd.DataFrame(case['expected'])
                actual = dataframe_mapping[0]
                expected = expected.values.tolist()
                actual = actual.values.tolist()
                client.sqldb.close()
                assert all(elem in expected for elem in actual), \
                    "Expected: {} Got: {}".format(expected, actual)

    def test_invalid_entity_col(self):
        case = cases.feature_invalid_entity
        file_name = create_temp_file(case)
        client = ServingClient(local=True)
        with pytest.raises(KeyError) as err:
            client.process_feature_csv(file_name, case['entity'], case['value_col'], case['name'], [],
                                       "test_name_variant", case['ts_col'])
        client.sqldb.close()
        assert "column does not exist" in str(err.value)

    def test_invalid_value_col(self):
        case = cases.feature_invalid_value
        file_name = create_temp_file(case)
        client = ServingClient(local=True)
        with pytest.raises(KeyError) as err:
            client.process_feature_csv(file_name, case['entity'], case['value_col'], case['name'], [],
                                       "test_name_variant", case['ts_col'])
        client.sqldb.close()
        assert "column does not exist" in str(err.value)

    def test_invalid_ts_col(self):
        case = cases.feature_invalid_ts
        file_name = create_temp_file(case)
        client = ServingClient(local=True)
        with pytest.raises(KeyError) as err:
            client.process_feature_csv(file_name, case['entity'], case['value_col'], case['name'], [],
                                       "test_name_variant", case['ts_col'])
        client.sqldb.close()
        assert "column does not exist" in str(err.value)


class TestFeaturesE2E(TestCase):
    def test_features(self):
        for name, case in cases.feature_e2e.items():
            with self.subTest(msg=name):
                print("TEST: ", name)
                file_name = create_temp_file(case)
                res = e2e_features(file_name, case['entity'], case['entity_loc'], case['features'], case['value_cols'],
                                   case['entities'], case['ts_col'])
                expected = case['expected']
                assert all(elem in expected for elem in res), \
                    "Expected: {} Got: {}".format(expected, res)
            retry_delete()

    def test_timestamp_doesnt_exist(self):
        case = {
            'columns': ['entity', 'value'],
            'values': [
                ['a', 1],
                ['b', 2],
                ['c', 3],

            ],
            'value_cols': ['value'],
            'entity': 'entity',
            'entity_loc': 'entity',
            'features': [("avg_transactions", "quickstart")],
            'entities': [{"entity": "a"}, {"entity": "b"}, {"entity": "c"}],
            'expected': [[1], [2], [3]],
            'ts_col': "ts"
        }
        file_name = create_temp_file(case)
        with pytest.raises(KeyError) as err:
            e2e_features(file_name, case['entity'], case['entity_loc'], case['features'], case['value_cols'],
                         case['entities'], case['ts_col'])
        assert "column does not exist" in str(err.value)

    @pytest.fixture(autouse=True)
    def run_before_and_after_tests(tmpdir):
        """Fixture to execute asserts before and after a test is run"""
        # Remove any lingering Databases
        try:
            shutil.rmtree('.featureform', onerror=del_rw)
        except:
            print("File Already Removed")
        yield
        try:
            shutil.rmtree('.featureform', onerror=del_rw)
        except:
            print("File Already Removed")


class TestIndividualLabels(TestCase):
    def test_individual_labels(self):
        for name, case in cases.labels.items():
            with self.subTest(name):
                print("TEST: ", name)
                file_name = create_temp_file(case)
                client = ServingClient(local=True)
                actual = client.process_label_csv(file_name, case['entity_name'], case['entity_col'], case['value_col'],
                                                  case['ts_col'])
                expected = pd.DataFrame(case['expected']).set_index(case['entity_name'])
                pd.testing.assert_frame_equal(actual, expected)

    def test_invalid_entity(self):
        case = {
            'columns': ['entity', 'value', 'ts'],
            'values': [],
            'entity_name': 'entity',
            'entity_col': 'name_dne',
            'value_col': 'value',
            'ts_col': 'ts'
        }
        file_name = create_temp_file(case)
        client = ServingClient(local=True)
        with pytest.raises(KeyError) as err:
            client.process_label_csv(file_name, case['entity_name'], case['entity_col'], case['value_col'],
                                     case['ts_col'])
        assert "column does not exist" in str(err.value)

    def test_invalid_value(self):
        case = {
            'columns': ['entity', 'value', 'ts'],
            'values': [],
            'entity_name': 'entity',
            'entity_col': 'entity',
            'value_col': 'value_dne',
            'ts_col': 'ts'
        }
        file_name = create_temp_file(case)
        client = ServingClient(local=True)
        with pytest.raises(KeyError) as err:
            client.process_label_csv(file_name, case['entity_name'], case['entity_col'], case['value_col'],
                                     case['ts_col'])
        assert "column does not exist" in str(err.value)

    def test_invalid_ts(self):
        case = {
            'columns': ['entity', 'value', 'ts'],
            'values': [],
            'entity_name': 'entity',
            'entity_col': 'entity',
            'value_col': 'value',
            'ts_col': 'ts_dne'
        }
        file_name = create_temp_file(case)
        client = ServingClient(local=True)
        with pytest.raises(KeyError) as err:
            client.process_label_csv(file_name, case['entity_name'], case['entity_col'], case['value_col'],
                                     case['ts_col'])
        assert "column does not exist" in str(err.value)

    @pytest.fixture(autouse=True)
    def run_before_and_after_tests(tmpdir):
        """Fixture to execute asserts before and after a test is run"""
        # Remove any lingering Databases
        try:
            shutil.rmtree('.featureform', onerror=del_rw)
        except:
            print("File Already Removed")
        yield
        try:
            shutil.rmtree('.featureform', onerror=del_rw)
        except:
            print("File Already Removed")


class TestTransformation(TestCase):

    def test_simple(self):
        local = ff.register_local()
        ff.register_user("featureformer").make_default_owner()
        name = 'Simple'
        case = cases.transform[name]
        self.setup(case, name, local)

        @local.df_transformation(variant=name, inputs=[("transactions", name)])
        def transformation(df):
            """ transformation """
            return df

        res = self.run_checks(transformation, name, local)
        np.testing.assert_array_equal(res, np.array([1]))

    def test_simple2(self):
        local = ff.register_local()
        ff.register_user("featureformer").make_default_owner()
        name = 'Simple2'
        case = cases.transform[name]
        self.setup(case, name, local)

        @local.df_transformation(variant=name, inputs=[("transactions", name)])
        def transformation(df):
            """ transformation """
            return df

        res = self.run_checks(transformation, name, local)
        np.testing.assert_array_equal(res, np.array([1]))

    def test_groupby(self):
        local = ff.register_local()
        ff.register_user("featureformer").make_default_owner()
        name = 'GroupBy'
        case = cases.transform[name]
        self.setup(case, name, local)

        @local.df_transformation(variant=name, inputs=[("transactions", name)])
        def transformation(df):
            """ transformation """
            return df.groupby("entity")['values'].mean()

        res = self.run_checks(transformation, name, local)
        np.testing.assert_array_equal(res, np.array([5.5]))

    def test_complex_join(self):
        local = ff.register_local()
        ff.register_user("featureformer").make_default_owner()
        name = 'Complex'
        case = cases.transform[name]
        self.setup(case, name, local)

        @local.df_transformation(variant=name, inputs=[("transactions", name)])
        def transformation1(df):
            """ transformation """
            return df.groupby("entity")['values1'].mean()

        @local.df_transformation(variant=name, inputs=[("transactions", name)])
        def transformation2(df):
            """ transformation """
            return df.groupby("entity")['values2'].mean()

        @local.df_transformation(variant=name, inputs=[("transformation1", name), ("transformation2", name)])
        def transformation3(df1, df2):
            """ transformation """
            df = df1 + df2
            df = df.reset_index().rename(columns={0: "values"})
            return df

        res = self.run_checks(transformation3, name, local)
        np.testing.assert_array_equal(res, np.array([7.5]))

    def setup(self, case, name, local):
        file = create_temp_file(case)

        local.register_file(
            name="transactions",
            variant=name,
            description="dataset 1",
            path=file,
            owner=name
        )

    def run_checks(self, transformation, name, local):
        transformation.register_resources(
            entity="user1",
            entity_column="entity",
            inference_store=local,
            features=[
                {"name": f"feature-{name}", "variant": name, "column": "values", "type": "float32"},
            ],
        )
        client = ff.ResourceClient(local=True)
        client.apply()
        serve = ServingClient(local=True)
        res = serve.features([(f"feature-{name}", name)], {"entity": "a"})
        serve.sqldb.close()
        return res

    @pytest.fixture(autouse=True)
    def run_before_and_after_tests(tmpdir):
        """Fixture to execute asserts before and after a test is run"""
        # Remove any lingering Databases
        try:
            ff.clear_state()
            shutil.rmtree('.featureform', onerror=del_rw)
        except:
            print("File Already Removed")
        yield
        try:
            ff.clear_state()
            shutil.rmtree('.featureform', onerror=del_rw)
        except:
            print("File Already Removed")





class TestTrainingSet(TestCase):
    def _register_feature(self, feature, local, case, index, name):
        file = create_temp_file(feature)
        test_file = local.register_file(
            name=f"table-{name}-{index}",
            variant="v1",
            description="",
            path=file
        )

        test_file.register_resources(
            entity=case['entity'],
            entity_column=case['entity_loc'],
            inference_store=local,
            features=[
                {"name": f"feat-{name}-{index}", "variant": "default", "column": "value", "type": "bool"},
            ],
            timestamp_column=feature['ts_col']
        )
        return file

    def _register_label(self, local, case, name):
        label = case['label']
        file = create_temp_file(label)
        test_file = local.register_file(
            name=f"table-{name}-label",
            variant="v1",
            description="",
            path=file
        )
        test_file.register_resources(
            entity=case['entity'],
            entity_column=case['entity_loc'],
            inference_store=local,
            labels=[
                {"name": f"label-{name}", "variant": "default", "column": 'value', "type": "bool"},
            ],
            timestamp_column=label['ts_col']
        )
        return file

    def test_all(self):
        for name, case in cases.training_set.items():
            with self.subTest(msg=name):
                print("TEST: ", name)
                try:
                    clear_and_reset()
                except Exception as e:
                    print(f"Could Not Reset Database: {e}")
                local = ff.register_local()
                ff.register_user("featureformer").make_default_owner()
                feature_list = []
                for i, feature in enumerate(case['features']):
                    self._register_feature(feature, local, case, i, name)
                    feature_list.append((f"feat-{name}-{i}", "default"))

                self._register_label(local, case, name)

                ff.register_training_set(
                    f"training_set-{name}", "default",
                    label=(f"label-{name}", "default"),
                    features=feature_list
                )

                client = ff.ResourceClient(local=True)
                client.apply()
                serving = ff.ServingClient(local=True)

                tset = serving.training_set(f"training_set-{name}", "default")
                serving.sqldb.close()
                actual_len = 0
                expected_len = len(case['expected'])
                for i, r in enumerate(tset):
                    actual_len += 1
                    actual = r.features() + [r.label()]
                    if actual in case['expected']:
                        case['expected'].remove(actual)
                    else:
                        raise AssertionError(f"{r.features() + [r.label()]} not in  {case['expected']}")
                try:
                    clear_and_reset()
                except Exception as e:
                    print(f"Could Not Reset Database: {e}")
                assert actual_len == expected_len




def clear_and_reset():
    ff.clear_state()
    shutil.rmtree('.featureform', onerror=del_rw)

def del_rw(action, name, exc):
    os.chmod(name, stat.S_IWRITE)
    os.remove(name)


def create_temp_file(test_values):
    file = NamedTemporaryFile(delete=False)
    with open(file.name, 'w') as csvfile:
        writer = csv.writer(csvfile, delimiter=',', quotechar='|')
        writer.writerow(test_values['columns'])
        for row in test_values['values']:
            writer.writerow(row)
        csvfile.close()

    return file.name


def e2e_features(file, entity_name, entity_loc, name_variants, value_cols, entities, ts_col):
    ff = ResourceClient(local=True)
    ff.register_user("featureformer").make_default_owner()
    local = ff.register_local()
    transactions = local.register_file(
        name="transactions",
        variant="quickstart",
        description="A dataset of fraudulent transactions",
        path=file
    )
    entity = ff.register_entity(entity_name)
    for i, variant in enumerate(name_variants):
        transactions.register_resources(
            entity=entity,
            entity_column=entity_loc,
            inference_store=local,
            features=[
                {"name": variant[0], "variant": variant[1], "column": value_cols[i], "type": "float32"},
            ],
            timestamp_column=ts_col
        )
    ff.state().create_all_local()
    client = ServingClient(local=True)
    results = []
    for entity in entities:
        results.append(client.features(name_variants, entity))
    return results


def retry_delete():
    for i in range(0, 100):
        try:
            shutil.rmtree('.featureform', onerror=del_rw)
            print("Table Deleted")
            break
        except Exception:
            print("Could not delete. Retrying...")
            time.sleep(1)
