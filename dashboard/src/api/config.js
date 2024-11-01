// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

let hostname = 'localhost';
let port = 3000;
if (typeof window !== 'undefined') {
  hostname = window.location.hostname;
  port = window.location.port;
}
let apiUrl = '//' + hostname + ':' + port;
if (process.env.REACT_APP_API_URL) {
  apiUrl = process.env.REACT_APP_API_URL.trim();
}

//if you want to override the api url (in any environment local to your machine, set this in ".env.local")
//.env.local is not tracked in source
if (process.env.NEXT_PUBLIC_REACT_APP_API_URL) {
  apiUrl = process.env.NEXT_PUBLIC_REACT_APP_API_URL.trim();
}

export const API_URL = apiUrl;
