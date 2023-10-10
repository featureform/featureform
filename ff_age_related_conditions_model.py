# Competition: https://www.kaggle.com/competitions/icr-identify-age-related-conditions/overview
# Notebook: https://www.kaggle.com/code/gusthema/identifying-age-related-conditions-w-tfdf

import featureform as ff
import tensorflow as tf
import tensorflow_decision_forests as tfdf
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.model_selection import KFold
from featureform import local

# Initialize the Client
client = ff.Client(local=True)

# Register dataset
health_characteristics = local.register_file(
    name="health_characteristics",
    path="data/train.csv",
    description="Health characteristics dataset",
    variant="complete_dataset"
)

@local.df_transformation(variant="complete_dataset",
                         inputs=[("health_characteristics", "complete_dataset")])
def numerical_ej(health_characteristics):
    """ Convert A:0 and B:1 in the EJ column """
    health_characteristics['EJ'] = health_characteristics['EJ'].replace({'A': 0, 'B': 1})
    return health_characteristics

# Has features, label and ID
dataset_df = client.dataframe("health_characteristics", "complete_dataset")

feature_variant_list = []
class User:
    # Define transformations here
    EJ_characteristic = ff.Feature(
        numerical_ej[['Id', "EJ"]], # We can optional include the `timestamp_column` "Timestamp" here
        variant="complete_dataset",
        type=ff.Float32,
        # We can switch this out for an inference store like Redis in production.
        inference_store=local,
    )

    # If you have a few features that cannot be looped through, define them here

    # Define labels here
    disease_label = ff.Label(
        health_characteristics[["Id", "Class"]], variant="complete_dataset", type=ff.Int
    )

# To define all the features in the dataset, we can loop through the columns
for column in dataset_df.columns:
    if column != 'Id' and column != 'Class':
        if column != 'EJ':
            setattr(User, f'{column}_characteristic', ff.Feature(health_characteristics[["Id", column]], variant="complete_dataset", type=ff.Float32))
        feature_variant_list.append((f'{column}_characteristic', 'complete_dataset'))

ff.entity(User)

ff.register_training_set(
    "health_characteristics", "all_features",
    label=("disease_label", "complete_dataset"),
    features=feature_variant_list,
)
client.apply()

# Has features and label
trainingset_df = client.training_set("health_characteristics", "all_features").dataframe()

figure, axis = plt.subplots(3, 2, figsize=(15, 15))
plt.subplots_adjust(hspace=0.25, wspace=0.3)

column_num = 0
for column_name in trainingset_df.columns:
    row = column_num // 2
    col = column_num % 2
    bp = sns.barplot(ax=axis[row, col], x=dataset_df['Id'], y=trainingset_df[column_name])
    bp.set(xticklabels=[])
    axis[row, col].set_title(column_name)
    column_num += 1
    if column_num == 6:
        break
plt.show()

# Creates a GroupKFold with 5 splits
kf = KFold(n_splits=5)

# Create list of ids for the creation of oof dataframe.
ID_LIST = dataset_df.index

# Create a dataframe of required size with zero values.
oof = pd.DataFrame(data=np.zeros((len(ID_LIST),1)), index=ID_LIST)

# Create an empty dictionary to store the models trained for each fold.
models = {}

# Create empty dict to save metrics for the models trained for each fold.
accuracy = {}
cross_entropy = {}

# Measuring and displaying the dataset imbalance
# Calculate the number of negative and positive values in `Class` column
neg, pos = np.bincount(dataset_df['Class'])
# Calculate total samples
total = neg + pos
# Calculate the weight for each label.
weight_for_0 = (1 / neg) * (total / 2.0)
weight_for_1 = (1 / pos) * (total / 2.0)

class_weight = {0: weight_for_0, 1: weight_for_1}

print('Weight for class 0: {:.2f}'.format(weight_for_0))
print('Weight for class 1: {:.2f}'.format(weight_for_1))
print('Examples:\n    Total: {}\n    Positive: {} ({:.2f}% of total)\n'.format(
    total, pos, 100 * pos / total))
# Using class weighting to deal with the imbalance in the dataset


# Train Random Forest model
# Loop through each fold
for i, (train_index, valid_index) in enumerate(kf.split(X=trainingset_df)):
    print('##### Fold',i+1)

    # Fetch values corresponding to the index 
    train_df = trainingset_df.iloc[train_index]
    valid_df = trainingset_df.iloc[valid_index]
    valid_ids = valid_df.index.values
    
    train_ds = tfdf.keras.pd_dataframe_to_tf_dataset(train_df, label="label")
    valid_ds = tfdf.keras.pd_dataframe_to_tf_dataset(valid_df, label="label")

    # Define the model and metrics
    rf = tfdf.keras.RandomForestModel()
    rf.compile(metrics=["accuracy", "binary_crossentropy"]) 
    
    # Train the model
    rf.fit(x=train_ds, class_weight=class_weight)
    
    # Store the model
    models[f"fold_{i+1}"] = rf
    
    # Predict OOF value for validation data
    predict = rf.predict(x=valid_ds)
    
    # Store the predictions in oof dataframe
    oof.loc[valid_ids, 0] = predict.flatten() 
    
    # Evaluate and store the metrics in respective dicts
    evaluation = rf.evaluate(x=valid_ds,return_dict=True)
    accuracy[f"fold_{i+1}"] = evaluation["accuracy"]
    cross_entropy[f"fold_{i+1}"]= evaluation["binary_crossentropy"]

# Display the tree model
tfdf.model_plotter.plot_model_in_colab(models['fold_1'], tree_idx=0, max_depth=3)

# # Visualization of Out of Bag data abd validation data
figure, axis = plt.subplots(3, 2, figsize=(10, 10))
plt.subplots_adjust(hspace=0.5, wspace=0.3)

for i, fold_no in enumerate(models.keys()):
    row = i//2
    col = i % 2
    logs = models[fold_no].make_inspector().training_logs()
    axis[row, col].plot([log.num_trees for log in logs], [log.evaluation.loss for log in logs])
    axis[row, col].set_title(f"Fold {i+1}")
    axis[row, col].set_xlabel('Number of trees')
    axis[row, col].set_ylabel('Loss (out-of-bag)')

axis[2][1].set_visible(False)
plt.show()

# Evaluation metrics for each fold and its average value
average_loss = 0
average_acc = 0

for _model in  models:
    average_loss += cross_entropy[_model]
    average_acc += accuracy[_model]
    print(f"{_model}: acc: {accuracy[_model]:.4f} loss: {cross_entropy[_model]:.4f}")

print(f"\nAverage accuracy: {average_acc/5:.4f}  Average loss: {average_loss/5:.4f}")

# # Test data predictions
# test_df = pd.read_csv('data/test.csv')
# test_ds_pd = test_df
# test_df_columns = test_ds_pd.columns.tolist()
# TEST_FEATURE_COLUMNS = [i for i in FEATURE_COLUMNS \
#                         if i in test_df_columns and i != "Class"]
# test_ds_pd = test_ds_pd[TEST_FEATURE_COLUMNS]
# test_ds = tfdf.keras.pd_dataframe_to_tf_dataset(test_ds_pd)
# predictions = models['fold_1'].predict(test_ds)
# n_predictions= [[round(abs(i-1), 8), i] for i in predictions.ravel()]
# print(n_predictions)
