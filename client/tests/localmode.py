import featureform as ff
import pandas as pd


def get_label(df: pd.DataFrame):
    df = df[['CustomerID', 'IsFraud']]
    df.rename(columns={'IsFraud': 'label'}, inplace=True)
    return df


def get_feature(df: pd.DataFrame):
    feature = df[['CustomerID', 'TransactionAmount']]
    name_variant = 'avg_transactions' + "." + 'quickstart'
    feature.rename(columns={'TransactionAmount': name_variant}, inplace=True)
    feature.drop_duplicates(subset=['CustomerID', name_variant])
    feature['CustomerID'] = feature['CustomerID'].astype('string')
    return feature


def run_transformation(df: pd.DataFrame):
    df = df[['CustomerID', 'TransactionAmount']]
    df.set_index('CustomerID', inplace=True)
    training_set = df.groupby("CustomerID")["TransactionAmount"].mean()
    df = training_set.to_frame()
    df.reset_index(inplace=True)
    return df


def get_training_set(label: pd.DataFrame, feature: pd.DataFrame):
    training_set_df = label
    training_set_df['CustomerID'] = training_set_df['CustomerID'].astype('string')
    training_set_df = training_set_df.join(feature.set_index('CustomerID'), how="left", on='CustomerID',
                                           lsuffix="_left")
    training_set_df.drop(columns='CustomerID', inplace=True)
    label_col = training_set_df.pop('label')
    training_set_df = training_set_df.assign(label=label_col)
    return training_set_df


def get_training_set_from_file(file):
    df = pd.read_csv('./transactions.csv')
    transformation = run_transformation(df)
    feature = get_feature(transformation)
    label = get_label(df)
    training_set_df = get_training_set(label, feature)
    return training_set_df.values.tolist()


def test_training_set():
    expected_tset = get_training_set_from_file('./transactions.csv')
    client = ff.ServingLocalClient()
    dataset = client.training_set("fraud_training", "quickstart")
    training_dataset = dataset  # .repeat(2).shuffle(1000).batch(8)
    for i, feature_batch in enumerate(training_dataset):
        assert feature_batch.features()[0] == expected_tset[i][0]
        assert feature_batch.label() == expected_tset[i][1]


def test_training_set_repeat():
    half_test = get_training_set_from_file('./transactions.csv')
    expected_tset = half_test + half_test
    client = ff.ServingLocalClient()
    dataset = client.training_set("fraud_training", "quickstart")
    training_dataset = dataset.repeat(1)
    for i, feature_batch in enumerate(training_dataset):
        assert feature_batch.features()[0] == expected_tset[i][0]
        assert feature_batch.label() == expected_tset[i][1]


def test_training_set_shuffle():
    expected_tset = get_training_set_from_file('./transactions.csv')
    client = ff.ServingLocalClient()
    dataset = client.training_set("fraud_training", "quickstart")
    training_dataset = dataset.shuffle(1)
    rows = 0
    for feature_batch in training_dataset:
        rows += 1
    assert rows == len(expected_tset)

def test_training_set_batch():
    expected_tset = get_training_set_from_file('./transactions.csv')
    client = ff.ServingLocalClient()
    dataset = client.training_set("fraud_training", "quickstart")
    training_dataset = dataset.batch(5)
    for i, feature_batch in enumerate(training_dataset):
        for j, row in enumerate(feature_batch):
            assert row.features()[0] == expected_tset[j+(i*5)][0]
            assert row.label() == expected_tset[j+(i*5)][1]


def test_feature():
    client = ff.ServingLocalClient()
    feature = client.features([("avg_transactions", "quickstart")], ("CustomerID", "C1410926"))
    assert pd.DataFrame(
        data={'CustomerID': ['C1410926'], 'TransactionAmount': [5000.0]},
        index=[43653]).equals(feature)
