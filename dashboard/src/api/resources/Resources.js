import Resource from './Resource.js';
const SearchClient = require('./Search.js');
// Set to true run locally
const local = false;

export const testData = [
  {
    name: 'User sample preferences',
    'default-variant': 'first-variant',
    type: 'Feature',
    'all-variants': ['first-variant', 'normalized variant'],
    variants: {
      'first-variant': {
        'variant-name': 'first-variant',
        dimensions: 3,
        created: '2020-08-09-0290499',
        owner: 'Simba Khadder',
        visibility: 'private',
        revision: '2020-08-10-39402409',
        tags: ['model2vec', 'compressed'],
        description: 'Vector generated based on user preferences',
      },
      'normalized variant': {
        'variant-name': 'normalized variant',
        dimensions: 3,
        created: '2020-08-09-0290499',
        owner: 'Simba Khadder',
        visibility: 'private',
        revision: '2020-08-10-39402409',
        tags: ['model2vec', 'compressed'],
        description: 'Vector generated based on user preferences, normalized',
      },
    },
  },
];

export const testListData = {
  Source: [
    {
      'all-variants': ['default'],
      type: 'Source',
      'default-variant': 'default',
      name: 'my_transformation',
      variants: {
        default: {
          created: '2022-12-15T00:41:10.247138911Z',
          description: 'A dummy transformation',
          name: 'my_transformation',
          'source-type': 'Transformation',
          owner: 'default_user',
          provider: 'k8s',
          variant: 'default',
          labels: null,
          features: null,
          'training-sets': null,
          status: 'READY',
          error: '',
          definition: '',
        },
      },
    },
  ],
};
export const testDetailsData = {
  sources: {
    my_transformation: {
      'all-variants': ['default', 'empty_specs'],
      type: 'Source',
      'default-variant': 'default',
      name: 'my_transformation',
      variants: {
        default: {
          created: '2022-12-15T00:41:10.247138911Z',
          description: 'A dummy transformation',
          name: 'my_transformation',
          'source-type': 'Transformation',
          owner: 'default_user',
          provider: 'k8s',
          variant: 'default',
          labels: {},
          features: {},
          'training-sets': {},
          status: 'READY',
          error: '',
          definition: '',
          specifications: {
            'Docker Image': 'featureformcom/k8s_runner:latest',
          },
        },
        empty_specs: {
          created: '2022-12-15T00:41:10.247138911Z',
          description: 'A dummy transformation',
          name: 'my_transformation',
          'source-type': 'Transformation',
          owner: 'default_user',
          provider: 'k8s',
          variant: 'default',
          labels: {},
          features: {},
          'training-sets': {},
          status: 'READY',
          error: '',
          definition: '',
          specifications: {
            'Docker Image': '',
          },
        },
      },
    },
  },
};

export const providerLogos = Object.freeze({
  // local mode
  LOCALMODE: 'static/localmode.png',

  // Offline Stores
  BIGQUERY: 'static/google_bigquery.svg',
  POSTGRES: 'static/Postgresql_elephant.svg',
  REDSHIFT: 'static/amazon_redshift.svg',
  SNOWFLAKE: 'static/Snowflake_Logo.svg',
  SPARK: 'static/Apache_Spark_logo.svg',
  KUBERNETES: 'static/kubernetes_logo.svg',

  // Filestores
  AZURE: 'static/azure_storage_accounts.svg',
  GCS: 'static/google_cloud_storage.svg',
  S3: 'static/amazon_s3.svg',
  HDFS: 'static/apache_hadoop.svg',

  // Online Stores
  REDIS: 'static/Redis_Logo.svg',
  CASSANDRA: 'static/apache_cassandra.svg',
  DYNAMODB: 'static/amazon_dynamoDB.svg',
  FIRESTORE: 'static/google_firestore.svg',
  MONGODB: 'static/mongoDB.svg',
});

var hostname = 'localhost';
var port = 3000;

if (typeof window !== 'undefined') {
  hostname = window.location.hostname;
  port = window.location.port;
}

var API_URL = '//' + hostname + ':' + port;
const SEARCH_URL = {
  port: '7700',
  host: hostname,
  apiKey: '',
};

if (process.env.REACT_APP_API_URL) {
  API_URL = process.env.REACT_APP_API_URL.trim();
}

//if you want to override the api url (in any environment local to your machine, set this in ".env.local")
//.env.local is not tracked in source
if (process.env.NEXT_PUBLIC_REACT_APP_API_URL) {
  API_URL = process.env.NEXT_PUBLIC_REACT_APP_API_URL.trim();
}

export var PROMETHEUS_URL = API_URL + '/prometheus';

if (typeof process.env.REACT_APP_PROMETHEUS_URL != 'undefined') {
  PROMETHEUS_URL = process.env.REACT_APP_PROMETHEUS_URL.trim();
}

export default class ResourcesAPI {
  static searchClient = new SearchClient(
    SEARCH_URL.port,
    SEARCH_URL.host,
    SEARCH_URL.apiKey
  );
  checkStatus() {
    return fetch(API_URL, {
      headers: {
        'Content-Type': 'application/json',
      },
    })
      .then((res) => {
        res.json().then((json_data) => ({ data: json_data }));
      })
      .catch((error) => {
        console.error(error);
      });
  }

  fetchResources(type) {
    var fetchAddress;
    let resourceType = Resource[type];
    if (local) {
      return { data: testListData[type] };
    } else {
      fetchAddress = `${API_URL + '/data'}${resourceType.urlPath}`;
    }
    if (process.env.REACT_APP_EMPTY_RESOURCE_VIEW === 'true') {
      fetchAddress = '/data/lists/wine-data-empty.json';
    }
    return fetch(fetchAddress, {
      headers: {
        'Content-Type': 'application/json',
      },
    })
      .then((res) =>
        res.json().then((json_data) => {
          if (local) {
            return { data: testData[type] };
          } else {
            return { data: json_data };
          }
        })
      )
      .catch((error) => {
        console.error(error);
      });
  }

  fetchEntity(type, title) {
    var fetchAddress;
    if (local) {
      return { data: testDetailsData[type][title] };
    } else {
      fetchAddress = `${API_URL + '/data'}/${type}/${title}`;
    }

    return fetch(fetchAddress, {
      headers: {
        'Content-Type': 'application/json',
      },
    })
      .then((res) =>
        res.json().then((json_data) => {
          return { data: json_data };
        })
      )
      .catch((error) => {
        console.error(error);
      });
  }

  fetchVersionMap() {
    const fetchAddress = `${API_URL + '/data/version'}`;

    return fetch(fetchAddress, {
      headers: {
        'Content-Type': 'application/json',
      },
    })
      .then((res) =>
        res.json().then((json_data) => {
          return { data: json_data };
        })
      )
      .catch((error) => {
        console.error(error);
        return { data: 'Default' };
      });
  }

  fetchSearch(query) {
    return query === null
      ? {}
      : fetch(`${API_URL}/data/search?q=${query}`)
          .then((res) => res.json())
          .catch((error) => {
            console.error(error);
          });
  }

  fetchSourceModalData(name, variant = 'default') {
    return name === null
      ? {}
      : fetch(`${API_URL}/data/sourcedata?name=${name}&variant=${variant}`)
          .then((res) => res.json())
          .catch((error) => {
            console.error(error);
          });
  }

  fetchFeatureFileStats(name, variant = 'default') {
    return name === null
      ? {}
      : fetch(`${API_URL}/data/filestatdata?name=${name}&variant=${variant}`)
          .then((res) => res.json())
          .catch((error) => {
            console.error(error);
          });
  }

  fetchVariantSearchStub() {
    const fetchAddress = '/data/lists/search_results_example.json';

    return fetch(fetchAddress, {
      headers: {
        'Content-Type': 'application/json',
      },
    })
      .then((res) => res.json().then((json_data) => ({ data: json_data })))
      .catch((error) => {
        console.error(error);
      });
  }
}
