// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package helpers

import (
	"fmt"
	"net/url"
	"strings"
)

var ResourceTypeToResourceURLMap = map[string]string{
	"FEATURE_VARIANT":      "features",
	"SOURCE_VARIANT":       "sources",
	"LABEL_VARIANT":        "labels",
	"TRAINING_SET_VARIANT": "training-sets",
	"PROVIDER":             "providers",
	// TODO add others here
}

func BuildDashboardUrl(host, resourceType, resourceName, resourceVariant string) (string, error) {
	scheme := "https"
	if strings.Contains(host, "localhost") {
		scheme = "http"
	}

	// TODO fix url to include variant (if applicable)
	resourceUrl, ok := ResourceTypeToResourceURLMap[resourceType]
	if !ok {
		return "", fmt.Errorf("resource type %s not found in map", resourceType)
	}
	dashboardUrl := &url.URL{
		Scheme: scheme,
		Host:   host,
		Path:   fmt.Sprintf("/%s/%s", resourceUrl, resourceName),
	}

	//include variant if applicable
	if resourceVariant != "" {
		query := url.Values{}
		query.Set("variant", resourceVariant)
		dashboardUrl.RawQuery = query.Encode()
	}

	return dashboardUrl.String(), nil
}
