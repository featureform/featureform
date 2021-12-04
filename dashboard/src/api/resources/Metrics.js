const METRICS_API_URL = "http://localhost:9090/api/v1";

export default class MetricsAPI {
  checkStatus() {
    return fetch(`${METRICS_API_URL}/label/__name__/values`, {
      headers: {
        "Content-Type": "application/json",
      },
    })
      .then((response) => {
        response.json();
      })
      .catch((error) => {
        console.error(error);
      });
  }
  fetchInstances() {
    var fetchAddress = `${METRICS_API_URL}/label/__name__/values`;

    return fetch(fetchAddress, {
      headers: {
        "Content-Type": "application/json",
      },
    })
      .then((res) => res.json())
      .catch((error) => {
        console.error(error);
      });
  }

  fetchMetrics(instance) {
    var fetchAddress = `${METRICS_API_URL}/metadata`;
    return fetch(fetchAddress, {
      headers: {
        "Content-Type": "application/json",
      },
    })
      .then((res) => res.json())
      .catch((error) => {
        console.error(error);
      });
  }
}
