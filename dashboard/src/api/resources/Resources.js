import Resource from "api/resources/Resource.js";
import TypesenseClient from "./Search.js";

export const testData = [
  {
    name: "User sample preferences",
    "default-variant": "first-variant",
    type: "Feature",
    "all-variants": ["first-variant", "normalized variant"],
    variants: {
      "first-variant": {
        "variant-name": "first-variant",
        dimensions: 3,
        created: "2020-08-09-0290499",
        owner: "Simba Khadder",
        visibility: "private",
        revision: "2020-08-10-39402409",
        tags: ["model2vec", "compressed"],
        description: "Vector generated based on user preferences",
      },
      "normalized variant": {
        "variant-name": "normalized variant",
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
  REDIS: "/Redis_Logo.svg",
  BIGQUERY: "/google_bigquery-ar21.svg",
  "APACHE SPARK": "/Apache_Spark_logo.svg",
  POSTGRESQL: "Postgresql_elephant.svg",
  SNOWFLAKE: "Snowflake_Logo.svg",
});

var API_URL = "//"+ window.location.hostname
//var API_URL = "http://a57f7235b9e0e49cf97d9ba661188650-73543dde19a3fca9.elb.us-east-1.amazonaws.com/data"
if (typeof process.env.REACT_APP_API_URL != "undefined") {
  API_URL = process.env.REACT_APP_API_URL.trim();
}
export var PROMETHEUS_URL = "//"+ window.location.hostname+"/prometheus";

if (typeof process.env.REACT_APP_PROMETHEUS_URL != "undefined") {
  PROMETHEUS_URL = process.env.REACT_APP_PROMETHEUS_URL.trim();
}
var TYPESENSE_PORT = "443";
if (typeof process.env.REACT_APP_TYPESENSE_PORT != "undefined") {
  TYPESENSE_PORT = process.env.REACT_APP_TYPESENSE_PORT.trim();
}
//var TYPESENSE_URL = "localhost";
var TYPESENSE_URL = window.location.hostname
if (typeof process.env.REACT_APP_TYPESENSE_URL != "undefined") {
  TYPESENSE_URL = process.env.REACT_APP_TYPESENSE_URL.trim();
}
var TYPESENSE_API_KEY = "xyz";
if (typeof process.env.REACT_APP_TYPESENSE_API_KEY != "undefined") {
  TYPESENSE_API_KEY = process.env.REACT_APP_TYPESENSE_API_KEY.trim();
}

const local = false;

export default class ResourcesAPI {
  static typeSenseClient = new TypesenseClient(
    API_URL + "/search/",
  );
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
    let resourceType = Resource[type];
    if (local) {
      fetchAddress = `/data/lists/wine-data.json`;
    } else {
      fetchAddress = `${API_URL + "/data"}${resourceType.urlPath}`;
    }
    if (process.env.REACT_APP_EMPTY_RESOURCE_VIEW === "true") {
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
      fetchAddress = `${API_URL + "/data"}/${type}/${title}`;
    }

    return fetch(fetchAddress, {
      headers: {
        "Content-Type": "application/json",
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

  fetchSearch(query) {
    let typeSenseResults = this.constructor.typeSenseClient.search(query);
    return typeSenseResults.then((results) => {
      return results.results();
    });
  }

  fetchVariantSearchStub(query) {
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