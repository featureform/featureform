// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

[
  {
    'all-variants': [
      '2024-04-17t14-01-52',
      '2024-04-17t14-59-42',
      '2024-04-22t18-06-43',
      '2024-04-22t18-11-31',
    ],
    type: 'Source',
    'default-variant': '2024-04-22t18-11-31',
    name: 'average_user_transaction',
    variants: {
      '2024-04-17t14-01-52': {
        name: 'average_user_transaction',
        variant: '2024-04-17t14-01-52',
        definition:
          'SELECT CustomerID as user_id, avg(TransactionAmount) as avg_transaction_amt from {{ transactions.2024-04-17t14-01-52 }} GROUP BY user_id',
        owner: 'anthony@featureform.com',
        description: '',
        provider: 'quickstart-postgres',
        created: '2024-04-17T19:01:55.49670116Z',
        status: 'FAILED',
        table: '',
        'training-sets': null,
        features: null,
        labels: null,
        lastUpdated: '1970-01-01T00:00:00Z',
        schedule: '',
        tags: ['tag 1', 'tag 2'],
        properties: {},
        'source-type': 'SQL Transformation',
        error:
          'Resource Failed: required dataset is in a failed state\n\u003e\u003e\u003e resource_type: SOURCE_VARIANT\n\u003e\u003e\u003e resource_status: FAILED\n\u003e\u003e\u003e resource_name: transactions\n\u003e\u003e\u003e resource_variant: 2024-04-17t14-01-52',
        specifications: {},
        inputs: [
          {
            Name: 'transactions',
            Variant: '2024-04-17t14-01-52',
          },
        ],
      },
      '2024-04-17t14-59-42': {
        name: 'average_user_transaction',
        variant: '2024-04-17t14-59-42',
        definition:
          'SELECT CustomerID as user_id, avg(TransactionAmount) as avg_transaction_amt from {{ transactions.2024-04-17t14-59-42 }} GROUP BY user_id',
        owner: 'anthony@featureform.com',
        description: '',
        provider: 'quickstart-postgres2',
        created: '2024-04-17T19:59:45.463277447Z',
        status: 'READY',
        table: '',
        'training-sets': null,
        features: null,
        labels: null,
        lastUpdated: '1970-01-01T00:00:00Z',
        schedule: '',
        tags: ['avg', 'customerId', 'quickstart'],
        properties: {},
        'source-type': 'SQL Transformation',
        error: '',
        specifications: {},
        inputs: [
          {
            Name: 'transactions',
            Variant: '2024-04-17t14-59-42',
          },
        ],
      },
      '2024-04-22t18-06-43': {
        name: 'average_user_transaction',
        variant: '2024-04-22t18-06-43',
        definition:
          'SELECT CustomerID as user_id, avg(TransactionAmount) as avg_transaction_amt from {{ transactions.2024-04-22t18-06-43 }} GROUP BY user_id',
        owner: 'anthony@featureform.com',
        description: '',
        provider: 'postgres-quickstart',
        created: '2024-04-22T23:07:25.021299466Z',
        status: 'READY',
        table: '',
        'training-sets': null,
        features: null,
        labels: null,
        lastUpdated: '1970-01-01T00:00:00Z',
        schedule: '',
        tags: [],
        properties: {},
        'source-type': 'SQL Transformation',
        error: '',
        specifications: {},
        inputs: [
          {
            Name: 'transactions',
            Variant: '2024-04-22t18-06-43',
          },
        ],
      },
      '2024-04-22t18-11-31': {
        name: 'average_user_transaction',
        variant: '2024-04-22t18-11-31',
        definition:
          'SELECT CustomerID as user_id, avg(TransactionAmount) as avg_transaction_amt from {{ transactions.2024-04-22t18-11-31 }} GROUP BY user_id',
        owner: 'anthony@featureform.com',
        description: '',
        provider: 'postgres-quickstart',
        created: '2024-04-22T23:12:34.520135502Z',
        status: 'READY',
        table: '',
        'training-sets': null,
        features: null,
        labels: null,
        lastUpdated: '1970-01-01T00:00:00Z',
        schedule: '',
        tags: [],
        properties: {},
        'source-type': 'SQL Transformation',
        error: '',
        specifications: {},
        inputs: [
          {
            Name: 'transactions',
            Variant: '2024-04-22t18-11-31',
          },
        ],
      },
    },
  },
  {
    'all-variants': [
      '2024-04-29t14-52-59',
      '2024-04-29t14-53-14',
      '2024-04-29t14-54-10',
      '2024-04-29t14-54-29',
    ],
    type: 'Source',
    'default-variant': '2024-04-29t14-54-29',
    name: 'transactions2',
    variants: {
      '2024-04-29t14-52-59': {
        name: 'transactions2',
        variant: '2024-04-29t14-52-59',
        definition: 's3://featureform-internal-sandbox/transactions.csv',
        owner: 'sterling@featureform.com',
        description: '',
        provider: 'spark_provider',
        created: '2024-04-29T21:53:01.761558332Z',
        status: 'READY',
        table: '',
        'training-sets': null,
        features: null,
        labels: null,
        lastUpdated: '1970-01-01T00:00:00Z',
        schedule: '',
        tags: [],
        properties: {},
        'source-type': 'Primary Table',
        error: '',
        specifications: {},
        inputs: [],
      },
      '2024-04-29t14-53-14': {
        name: 'transactions2',
        variant: '2024-04-29t14-53-14',
        definition: 's3://featureform-internal-sandbox/transactions.csv',
        owner: 'sterling@featureform.com',
        description: '',
        provider: 'spark_provider',
        created: '2024-04-29T21:53:16.358833269Z',
        status: 'READY',
        table: '',
        'training-sets': null,
        features: null,
        labels: null,
        lastUpdated: '1970-01-01T00:00:00Z',
        schedule: '',
        tags: [],
        properties: {},
        'source-type': 'Primary Table',
        error: '',
        specifications: {},
        inputs: [],
      },
      '2024-04-29t14-54-10': {
        name: 'transactions2',
        variant: '2024-04-29t14-54-10',
        definition: 's3://featureform-internal-sandbox/transactions.csv',
        owner: 'sterling@featureform.com',
        description: '',
        provider: 'spark_provider',
        created: '2024-04-29T21:54:11.826507827Z',
        status: 'READY',
        table: '',
        'training-sets': null,
        features: null,
        labels: null,
        lastUpdated: '1970-01-01T00:00:00Z',
        schedule: '',
        tags: [],
        properties: {},
        'source-type': 'Primary Table',
        error: '',
        specifications: {},
        inputs: [],
      },
      '2024-04-29t14-54-29': {
        name: 'transactions2',
        variant: '2024-04-29t14-54-29',
        definition: 's3://featureform-internal-sandbox/transactions.csv',
        owner: 'sterling@featureform.com',
        description: '',
        provider: 'spark_provider',
        created: '2024-04-29T21:54:31.392225546Z',
        status: 'READY',
        table: '',
        'training-sets': null,
        features: null,
        labels: null,
        lastUpdated: '1970-01-01T00:00:00Z',
        schedule: '',
        tags: [],
        properties: {},
        'source-type': 'Primary Table',
        error: '',
        specifications: {},
        inputs: [],
      },
    },
  },
  {
    'all-variants': [
      '2024-04-17t14-01-52',
      '2024-04-17t14-59-42',
      '2024-04-22t18-06-43',
      '2024-04-22t18-11-31',
      '2024-04-23t13-41-09',
      'clean',
      '2024-04-29t14-45-26',
      '2024-04-29t14-46-51',
      '2024-04-29t14-49-55',
    ],
    type: 'Source',
    'default-variant': '2024-04-29t14-49-55',
    name: 'transactions',
    variants: {
      '2024-04-17t14-01-52': {
        name: 'transactions',
        variant: '2024-04-17t14-01-52',
        definition: 'transactions',
        owner: 'anthony@featureform.com',
        description: '',
        provider: 'quickstart-postgres',
        created: '2024-04-17T19:01:54.6160144Z',
        status: 'FAILED',
        table: '',
        'training-sets': null,
        features: null,
        labels: null,
        lastUpdated: '1970-01-01T00:00:00Z',
        schedule: '',
        tags: [],
        properties: {},
        'source-type': 'Primary Table',
        error:
          "Dataset Not Found: source table 'transactions' does not exist\n\u003e\u003e\u003e resource_variant: 2024-04-17t14-01-52\n\u003e\u003e\u003e resource_name: transactions",
        specifications: {},
        inputs: [],
      },
      '2024-04-17t14-59-42': {
        name: 'transactions',
        variant: '2024-04-17t14-59-42',
        definition: 'transactions',
        owner: 'anthony@featureform.com',
        description: '',
        provider: 'quickstart-postgres2',
        created: '2024-04-17T19:59:44.690033842Z',
        status: 'READY',
        table: '',
        'training-sets': null,
        features: null,
        labels: null,
        lastUpdated: '1970-01-01T00:00:00Z',
        schedule: '',
        tags: [],
        properties: {},
        'source-type': 'Primary Table',
        error: '',
        specifications: {},
        inputs: [],
      },
      '2024-04-22t18-06-43': {
        name: 'transactions',
        variant: '2024-04-22t18-06-43',
        definition: 'transactions',
        owner: 'anthony@featureform.com',
        description: '',
        provider: 'postgres-quickstart',
        created: '2024-04-22T23:07:24.000493162Z',
        status: 'READY',
        table: '',
        'training-sets': null,
        features: null,
        labels: null,
        lastUpdated: '1970-01-01T00:00:00Z',
        schedule: '',
        tags: [],
        properties: {},
        'source-type': 'Primary Table',
        error: '',
        specifications: {},
        inputs: [],
      },
      '2024-04-22t18-11-31': {
        name: 'transactions',
        variant: '2024-04-22t18-11-31',
        definition: 'transactions',
        owner: 'anthony@featureform.com',
        description: '',
        provider: 'postgres-quickstart',
        created: '2024-04-22T23:12:33.645019702Z',
        status: 'READY',
        table: '',
        'training-sets': null,
        features: null,
        labels: null,
        lastUpdated: '1970-01-01T00:00:00Z',
        schedule: '',
        tags: [],
        properties: {},
        'source-type': 'Primary Table',
        error: '',
        specifications: {},
        inputs: [],
      },
      '2024-04-23t13-41-09': {
        name: 'transactions',
        variant: '2024-04-23t13-41-09',
        definition:
          '@spark.df_transformation(inputs=[raw])\ndef transactions(df):\n    from pyspark.sql import functions as F\n    return df.withColumn("Timestamp", F.to_timestamp(df["Timestamp"]))\n',
        owner: 'simba@featureform.com',
        description: '',
        provider: 'spark_provider',
        created: '2024-04-23T13:42:17.21165255Z',
        status: 'READY',
        table: '',
        'training-sets': null,
        features: null,
        labels: null,
        lastUpdated: '1970-01-01T00:00:00Z',
        schedule: '',
        tags: [],
        properties: {},
        'source-type': 'Dataframe Transformation',
        error: '',
        specifications: {},
        inputs: [
          {
            Name: 'transactions-raw',
            Variant: 'raw',
          },
        ],
      },
      '2024-04-29t14-45-26': {
        name: 'transactions',
        variant: '2024-04-29t14-45-26',
        definition:
          's3://ali-aws-lake-house-iceberg-blog-demo/transactions_short.csv',
        owner: 'sterling@featureform.com',
        description: '',
        provider: 'spark-glue2',
        created: '2024-04-29T21:46:11.262063963Z',
        status: 'READY',
        table: '',
        'training-sets': null,
        features: null,
        labels: null,
        lastUpdated: '1970-01-01T00:00:00Z',
        schedule: '',
        tags: [],
        properties: {},
        'source-type': 'Primary Table',
        error: '',
        specifications: {},
        inputs: [],
      },
      '2024-04-29t14-46-51': {
        name: 'transactions',
        variant: '2024-04-29t14-46-51',
        definition:
          's3://ali-aws-lake-house-iceberg-blog-demo/transactions_short.csv',
        owner: 'sterling@featureform.com',
        description: '',
        provider: 'spark-glue2',
        created: '2024-04-29T21:46:54.430864877Z',
        status: 'READY',
        table: '',
        'training-sets': null,
        features: null,
        labels: null,
        lastUpdated: '1970-01-01T00:00:00Z',
        schedule: '',
        tags: [],
        properties: {},
        'source-type': 'Primary Table',
        error: '',
        specifications: {},
        inputs: [],
      },
      '2024-04-29t14-49-55': {
        name: 'transactions',
        variant: '2024-04-29t14-49-55',
        definition:
          's3://ali-aws-lake-house-iceberg-blog-demo/transactions_short.csv',
        owner: 'sterling@featureform.com',
        description: '',
        provider: 'spark_provider',
        created: '2024-04-29T21:49:59.548339911Z',
        status: 'FAILED',
        table: '',
        'training-sets': null,
        features: null,
        labels: null,
        lastUpdated: '1970-01-01T00:00:00Z',
        schedule: '',
        tags: [],
        properties: {},
        'source-type': 'Primary Table',
        error:
          'Dataset Not Found: s3://ali-aws-lake-house-iceberg-blog-demo/transactions_short.csv\n\u003e\u003e\u003e resource_name: transactions\n\u003e\u003e\u003e resource_variant: 2024-04-29t14-49-55',
        specifications: {},
        inputs: [],
      },
      clean: {
        name: 'transactions',
        variant: 'clean',
        definition:
          '@spark.df_transformation(inputs=[raw], variant="clean")\ndef transactions(df):\n    from pyspark.sql import functions as F\n    return df.withColumn("Timestamp", F.to_timestamp(df["Timestamp"]))\n',
        owner: 'simba@featureform.com',
        description: '',
        provider: 'spark_provider',
        created: '2024-04-23T13:47:48.992212185Z',
        status: 'READY',
        table: '',
        'training-sets': null,
        features: null,
        labels: null,
        lastUpdated: '1970-01-01T00:00:00Z',
        schedule: '',
        tags: [],
        properties: {},
        'source-type': 'Dataframe Transformation',
        error: '',
        specifications: {},
        inputs: [
          {
            Name: 'transactions-raw',
            Variant: '2024-04-23t13-45-35',
          },
        ],
      },
    },
  },
  {
    'all-variants': [
      '2024-04-23t13-48-35',
      '2024-04-23t15-36-30',
      'default-50',
      '2024-04-24t18-04-49',
    ],
    type: 'Source',
    'default-variant': '2024-04-24t18-04-49',
    name: 'perc_balance',
    variants: {
      '2024-04-23t13-48-35': {
        name: 'perc_balance',
        variant: '2024-04-23t13-48-35',
        definition:
          '@spark.df_transformation(inputs=[transactions])\ndef perc_balance(df):\n  """ The transaction amount divided by the customer account balance.\n  It handles NaNs and Inf by defaulting to 100% if the denominator is 0.\n  """\n  from pyspark.sql.functions import col, when\n  return df.withColumn("BalancePercent",\n                       when(col("CustAccountBalance") != 0, col("TransactionAmount") / col("CustAccountBalance")).\n                       otherwise(100.0))\n',
        owner: 'simba@featureform.com',
        description:
          ' The transaction amount divided by the customer account balance.\n  It handles NaNs and Inf by defaulting to 100% if the denominator is 0.\n  ',
        provider: 'spark_provider',
        created: '2024-04-23T13:49:00.675909354Z',
        status: 'READY',
        table: '',
        'training-sets': null,
        features: null,
        labels: null,
        lastUpdated: '1970-01-01T00:00:00Z',
        schedule: '',
        tags: [],
        properties: {},
        'source-type': 'Dataframe Transformation',
        error: '',
        specifications: {},
        inputs: [
          {
            Name: 'transactions',
            Variant: 'clean',
          },
        ],
      },
      '2024-04-23t15-36-30': {
        name: 'perc_balance',
        variant: '2024-04-23t15-36-30',
        definition:
          '@spark.df_transformation(inputs=[transactions])\ndef perc_balance(df):\n  """ The transaction amount divided by the customer account balance.\n  It handles NaNs and Inf by defaulting to 0% if the denominator is 0.\n  """\n  from pyspark.sql.functions import col, when\n  return df.withColumn("BalancePercent",\n                       when(col("CustAccountBalance") != 0, col("TransactionAmount") / col("CustAccountBalance")).\n                       otherwise(100.0))\n',
        owner: 'simba@featureform.com',
        description:
          ' The transaction amount divided by the customer account balance.\n  It handles NaNs and Inf by defaulting to 0% if the denominator is 0.\n  ',
        provider: 'spark_provider',
        created: '2024-04-24T17:26:06.469707206Z',
        status: 'READY',
        table: '',
        'training-sets': null,
        features: null,
        labels: null,
        lastUpdated: '1970-01-01T00:00:00Z',
        schedule: '',
        tags: [],
        properties: {},
        'source-type': 'Dataframe Transformation',
        error: '',
        specifications: {},
        inputs: [
          {
            Name: 'transactions',
            Variant: 'clean',
          },
        ],
      },
      '2024-04-24t18-04-49': {
        name: 'perc_balance',
        variant: '2024-04-24t18-04-49',
        definition:
          '@spark.df_transformation(inputs=[transactions])\ndef perc_balance(df):\n  """ The transaction amount divided by the customer account balance.\n  It handles NaNs and Inf by defaulting to 0% if the denominator is 0.\n  """\n  from pyspark.sql.functions import col, when\n  return df.withColumn("BalancePercent",\n                       when(col("CustAccountBalance") != 0, col("TransactionAmount") / col("CustAccountBalance")).\n                       otherwise(30.0))\n',
        owner: 'simba@featureform.com',
        description:
          ' The transaction amount divided by the customer account balance.\n  It handles NaNs and Inf by defaulting to 0% if the denominator is 0.\n  ',
        provider: 'spark_provider',
        created: '2024-04-25T16:26:06.283176209Z',
        status: 'READY',
        table: '',
        'training-sets': null,
        features: null,
        labels: null,
        lastUpdated: '1970-01-01T00:00:00Z',
        schedule: '',
        tags: [],
        properties: {},
        'source-type': 'Dataframe Transformation',
        error: '',
        specifications: {},
        inputs: [
          {
            Name: 'transactions',
            Variant: 'clean',
          },
        ],
      },
      'default-50': {
        name: 'perc_balance',
        variant: 'default-50',
        definition:
          '@spark.df_transformation(inputs=[transactions], variant="default-50")\ndef perc_balance(df):\n  """ The transaction amount divided by the customer account balance.\n  It handles NaNs and Inf by defaulting to 0% if the denominator is 0.\n  """\n  from pyspark.sql.functions import col, when\n  return df.withColumn("BalancePercent",\n                       when(col("CustAccountBalance") != 0, col("TransactionAmount") / col("CustAccountBalance")).\n                       otherwise(50.0))\n',
        owner: 'simba@featureform.com',
        description:
          ' The transaction amount divided by the customer account balance.\n  It handles NaNs and Inf by defaulting to 0% if the denominator is 0.\n  ',
        provider: 'spark_provider',
        created: '2024-04-24T18:04:13.195683356Z',
        status: 'READY',
        table: '',
        'training-sets': null,
        features: null,
        labels: null,
        lastUpdated: '1970-01-01T00:00:00Z',
        schedule: '',
        tags: [],
        properties: {},
        'source-type': 'Dataframe Transformation',
        error: '',
        specifications: {},
        inputs: [
          {
            Name: 'transactions',
            Variant: 'clean',
          },
        ],
      },
    },
  },
  {
    'all-variants': ['raw', '2024-04-23t13-45-35'],
    type: 'Source',
    'default-variant': '2024-04-23t13-45-35',
    name: 'transactions-raw',
    variants: {
      '2024-04-23t13-45-35': {
        name: 'transactions-raw',
        variant: '2024-04-23t13-45-35',
        definition: 's3://featureform-internal-sandbox/transactions.csv',
        owner: 'simba@featureform.com',
        description: '',
        provider: 'spark_provider',
        created: '2024-04-23T13:47:48.14354182Z',
        status: 'READY',
        table: '',
        'training-sets': null,
        features: null,
        labels: null,
        lastUpdated: '1970-01-01T00:00:00Z',
        schedule: '',
        tags: [],
        properties: {},
        'source-type': 'Primary Table',
        error: '',
        specifications: {},
        inputs: [],
      },
      raw: {
        name: 'transactions-raw',
        variant: 'raw',
        definition: 's3://featureform-internal-sandbox/transactions.csv',
        owner: 'simba@featureform.com',
        description: '',
        provider: 'spark_provider',
        created: '2024-04-23T13:42:16.443787571Z',
        status: 'READY',
        table: '',
        'training-sets': null,
        features: null,
        labels: null,
        lastUpdated: '1970-01-01T00:00:00Z',
        schedule: '',
        tags: [],
        properties: {},
        'source-type': 'Primary Table',
        error: '',
        specifications: {},
        inputs: [],
      },
    },
  },
  {
    'all-variants': ['2024-04-23t13-49-46'],
    type: 'Source',
    'default-variant': '2024-04-23t13-49-46',
    name: 'window_aggs',
    variants: {
      '2024-04-23t13-49-46': {
        name: 'window_aggs',
        variant: '2024-04-23t13-49-46',
        definition:
          '@spark.df_transformation(inputs=[transactions])\ndef window_aggs(df):\n  """ Adds two columns: mean and count.\n  mean is the average transaction amount of the last 30 transactions at the time of the transaction in the row.\n  count is the number of transactions if that number is less than 30.\n  """\n  from pyspark.sql import functions as F\n  from pyspark.sql.window import Window\n  df = df.withColumn("Timestamp", F.to_timestamp(df["Timestamp"]))\n\n  # Sort by \'Timestamp\'\n  df = df.orderBy("Timestamp")\n\n  # Define a Window spec\n  window_spec = Window.partitionBy("CustomerID").orderBy("Timestamp").rowsBetween(Window.currentRow - 29, Window.currentRow)\n\n  # Compute rolling mean and count\n  result = df.withColumn("mean", F.avg("TransactionAmount").over(window_spec))\n  return result.withColumn("count", F.count("TransactionAmount").over(window_spec))\n',
        owner: 'simba@featureform.com',
        description:
          ' Adds two columns: mean and count.\n  mean is the average transaction amount of the last 30 transactions at the time of the transaction in the row.\n  count is the number of transactions if that number is less than 30.\n  ',
        provider: 'spark_provider',
        created: '2024-04-23T13:49:47.769402751Z',
        status: 'READY',
        table: '',
        'training-sets': null,
        features: null,
        labels: null,
        lastUpdated: '1970-01-01T00:00:00Z',
        schedule: '',
        tags: [],
        properties: {},
        'source-type': 'Dataframe Transformation',
        error: '',
        specifications: {},
        inputs: [
          {
            Name: 'transactions',
            Variant: 'clean',
          },
        ],
      },
    },
  },
];