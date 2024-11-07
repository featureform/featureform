// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

const METRICS_API_URL = 'http://localhost:9090/api/v1';

export default class MetricsAPI {
  async checkStatus() {
    return fetch(`${METRICS_API_URL}/label/__name__/values`, {
      headers: {
        'Content-Type': 'application/json',
      },
    })
      .then((response) => {
        response.json();
      })
      .catch((error) => {
        console.error(error);
      });
  }
  async fetchInstances() {
    var fetchAddress = `${METRICS_API_URL}/label/__name__/values`;

    return fetch(fetchAddress, {
      headers: {
        'Content-Type': 'application/json',
      },
    })
      .then((res) => res.json())
      .catch((error) => {
        console.error(error);
      });
  }

  async fetchMetrics() {
    var fetchAddress = `${METRICS_API_URL}/metadata`;
    try {
      const res = await fetch(fetchAddress, {
        headers: {
          'Content-Type': 'application/json',
        },
      });
      return await res.json();
    } catch (error) {
      console.error(error);
    }
  }
}
