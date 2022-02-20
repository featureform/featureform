export const resourceTypes = Object.freeze({
  FEATURE: "Feature",
  FEATURE_SET: "Feature Set",
  LABEL: "Label",
  ENTITY: "Entity",
  MODEL: "Model",
  TRANSFORMATION: "Transformation",
  TRAINING_DATASET: "Training Dataset",
  PROVIDER: "Provider",
  USER: "User",
  PRIMARY_DATA: "Primary Data",
});

export const resourceIcons = Object.freeze({
  Feature: "description",
  Entity: "fingerprint",
  Label: "label",
  "Feature Set": "account_tree",
  Model: "model_training",
  Transformation: "workspaces",
  "Training Dataset": "storage",
  Provider: "device_hub",
  User: "person",
  "Primary Data": "source",
});

export const resourcePaths = Object.freeze({
  Feature: "/features",
  Entity: "/entities",
  Label: "/labels",
  "Feature Set": "/feature-sets",
  Model: "/models",
  Transformation: "/transformations",
  "Training Dataset": "/training-datasets",
  Provider: "/providers",
  User: "/users",
  "Primary Data": "/primary-data",
});
export const testData = [
  {
    name: "User sample preferences",
    "default-variant": "first-variant",
    "all-versions": ["first-variant", "normalized variant"],
    versions: {
      "first-variant": {
        "version-name": "first-variant",
        dimensions: 3,
        created: "2020-08-09-0290499",
        owner: "Simba Khadder",
        visibility: "private",
        revision: "2020-08-10-39402409",
        tags: ["model2vec", "compressed"],
        description: "Vector generated based on user preferences",
      },
      "normalized variant": {
        "version-name": "normalized variant",
        dimensions: 3,
        created: "2020-08-09-0290499",
        owner: "Simba Khadder",
        visibility: "private",
        revision: "2020-08-10-39402409",
        tags: ["model2vec", "compressed"],
        description: "Vector generated based on user preferences, normalized",
      },
    },
  },
];

export const providerLogos = Object.freeze({
  Redis: "/Redis_Logo.svg",
  BigQuery: "/google_bigquery-ar21.svg",
  "Apache Spark": "/Apache_Spark_logo.svg",
});

const API_URL = "http://localhost:8181";
const local = false;

export default class ResourcesAPI {
  checkStatus() {
    return fetch(API_URL, {
      headers: {
        "Content-Type": "application/json",
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
    if (local) {
      fetchAddress = `/data/lists/wine-data.json`;
    } else {
      fetchAddress = `${API_URL}${resourcePaths[type]}`;
    }
    if (process.env.REACT_APP_EMPTY_RESOURCE_VIEW == "true") {
      fetchAddress = "/data/lists/wine-data-empty.json";
    }
    return fetch(fetchAddress, {
      headers: {
        "Content-Type": "application/json",
      },
    })
      .then((res) =>
        res.json().then((json_data) => {
          if (local) {
            return { data: json_data[type] };
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
      fetchAddress = "/data/" + type + "/" + title + ".json";
    } else {
      fetchAddress = `${API_URL}/${type}/${title}`;
    }

    return fetch(fetchAddress, {
      headers: {
        "Content-Type": "application/json",
      },
    })
      .then((res) => res.json().then((json_data) => ({ data: json_data })))
      .catch((error) => {
        console.error(error);
      });
  }

  fetchSearch(query) {
    const fetchAddress = "/data/lists/wine-data.json";

    return fetch(fetchAddress, {
      headers: {
        "Content-Type": "application/json",
      },
    })
      .then((res) => res.json().then((json_data) => ({ data: json_data })))
      .catch((error) => {
        console.error(error);
      });
  }

  fetchVersionSearchStub(query) {
    const fetchAddress = "/data/lists/search_results_example.json";

    return fetch(fetchAddress, {
      headers: {
        "Content-Type": "application/json",
      },
    })
      .then((res) => res.json().then((json_data) => ({ data: json_data })))
      .catch((error) => {
        console.error(error);
      });
  }
}
