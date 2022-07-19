from unittest import TestCase

import pandas as pd
from featureform import serving
from tempfile import NamedTemporaryFile
import csv
from localmode_cases import features


class TestLocalClient(TestCase):
    def test_process_feature_csv(self):
        for name, case in features.items():
            with self.subTest(name):
                file_name = create_temp_file(case)
                client = serving.LocalClient()
                feature_dataframes, dataframe_mapping = client.process_feature_csv(file_name, case['entity'], set(), case['value_col'], {}, name, "")
                print("EXTECTED")

                expected = pd.DataFrame(case['expected'])#.reset_index()
                actual = dataframe_mapping[name]#.reset_index()
                print("EXTECTED")
                print(expected)
                print("ACTUAL")
                print(actual)
                pd.testing.assert_frame_equal(expected, actual)



def create_temp_file(test_values):
    file = NamedTemporaryFile(delete=False)
    with open(file.name, 'w') as csvfile:
        writer = csv.writer(csvfile, delimiter=',', quotechar='|')
        writer.writerow(test_values['columns'])
        for row in test_values['values']:
            writer.writerow(row)

    return file.name


