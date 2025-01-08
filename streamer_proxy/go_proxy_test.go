// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package main

import (
	"encoding/json"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/featureform/logging"
	"github.com/stretchr/testify/assert"
)

func TestValidTicket(t *testing.T) {

	proxyFlightServer := &GoProxyServer{
		streamerAddress: "test-address:8080",
		logger:          logging.NewLogger("iceberg-proxy-test"),
	}

	var proxyBytes = []byte(`{"location": "booking_glue_db.test_csv", 
		"client.region": "someRegion",
		"client.access-key-id":"someKey",
		"client.secret-access-key":"someSecret",
		 "limit": 5}`)

	ticket := flight.Ticket{
		Ticket: proxyBytes,
	}

	hyrdatedTicket, err := proxyFlightServer.hydrateTicket(&ticket)

	assert.NoError(t, err, "hydrateTicket returned an error")
	assert.NotEmpty(t, hyrdatedTicket.Ticket)
}

func TestInvalidTicket(t *testing.T) {

	proxyFlightServer := &GoProxyServer{
		streamerAddress: "test-address:8080",
		logger:          logging.NewLogger("iceberg-proxy-test"),
	}

	tests := []struct {
		name        string
		ticketMap   any
		expectedMsg string
	}{
		{
			name: "malformed location key",
			ticketMap: map[string]any{
				"locationasdf":             "namespace.table",
				"client.access-key-id":     "someKey",
				"client.secret-access-key": "someAccess",
				"client.region":            "someRegion",
				"limit":                    5,
			},
			expectedMsg: "missing 'location' in ticket data",
		},
		{
			name: "no location value",
			ticketMap: map[string]any{
				"location":                 "",
				"client.access-key-id":     "someKey",
				"client.secret-access-key": "someAccess",
				"client.region":            "someRegion",
				"limit":                    5,
			},
			expectedMsg: "missing 'location' in ticket data",
		},
		{
			name: "malformed location value",
			ticketMap: map[string]any{
				"location":                 ".",
				"client.access-key-id":     "someKey",
				"client.secret-access-key": "someAccess",
				"client.region":            "someRegion",
				"limit":                    5,
			},
			expectedMsg: "invalid location format, expected 'namespace.table' but got: .",
		},
		{
			name: "missing location namespace",
			ticketMap: map[string]any{
				"location":                 ".table",
				"client.access-key-id":     "someKey",
				"client.secret-access-key": "someAccess",
				"client.region":            "someRegion",
				"limit":                    5,
			},
			expectedMsg: "invalid location format, expected 'namespace.table' but got: .table",
		},
		{
			name: "missing location namespace",
			ticketMap: map[string]any{
				"location":                 "namespace.",
				"client.access-key-id":     "someKey",
				"client.secret-access-key": "someAccess",
				"client.region":            "someRegion",
				"limit":                    5,
			},
			expectedMsg: "invalid location format, expected 'namespace.table' but got: namespace.",
		},
		{
			name: "missing region",
			ticketMap: map[string]any{
				"location":                 "namespace.table",
				"client.access-key-id":     "someKey",
				"client.secret-access-key": "someAccess",
				"limit":                    5,
			},
			expectedMsg: "missing 'client.region' in ticket data",
		},
		{
			name: "missing access key id",
			ticketMap: map[string]any{
				"location":                 "namespace.table",
				"client.secret-access-key": "someAccess",
				"client.region":            "someRegion",
				"limit":                    5,
			},
			expectedMsg: "missing 'client.access-key-id' in ticket data",
		},
		{
			name: "missing secret access key",
			ticketMap: map[string]any{
				"location":             "namespace.table",
				"client.access-key-id": "someKey",
				"client.region":        "someRegion",
				"limit":                5,
			},
			expectedMsg: "missing 'client.secret-access-key' in ticket data",
		},
		{
			name:        "malformed json",
			ticketMap:   []byte(`{i'm not valid!}`),
			expectedMsg: "failed to parse ticket JSON: json: cannot unmarshal string into Go value of type main.TicketData",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ticketBytes, err := json.Marshal(tt.ticketMap)
			if err != nil {
				assert.FailNow(t, "Failed to marshal the ticketMap", "error", ticketBytes)
			}

			ticket := flight.Ticket{
				Ticket: ticketBytes,
			}

			hyrdatedTicket, err := proxyFlightServer.hydrateTicket(&ticket)
			assert.EqualError(t, err, tt.expectedMsg)
			assert.Nil(t, hyrdatedTicket, "the ticket should be 'nil'")
		})
	}

}
