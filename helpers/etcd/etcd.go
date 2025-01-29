// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2025 FeatureForm Inc.
//

package etcd

import (
	"fmt"
	"net/url"
	"time"

	"github.com/featureform/logging/redacted"
)

type Config struct {
	Host        string
	Port        string
	Username    string
	Password    string
	DialTimeout time.Duration
}

func (cfg Config) Redacted() map[string]any {
	return map[string]any{
		"Host":        cfg.Host,
		"Port":        cfg.Port,
		"Username":    cfg.Username,
		"Password":    redacted.String,
		"DialTimeout": cfg.DialTimeout.String(),
	}
}

func (c Config) URL() string {
	u := &url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%s", c.Host, c.Port),
	}
	return u.String()
}
