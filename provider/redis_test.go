//go:build online
// +build online

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package provider

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/redis/rueidis"
)

func Test_redisOnlineTable_Get(t *testing.T) {
	miniRedis := mockRedis()
	miniRedis.Addr()
	redisClient, err := rueidis.NewClient(
		rueidis.ClientOption{
			InitAddress:  []string{miniRedis.Addr()},
			Password:     "",
			SelectDB:     0,
			DisableCache: true,
		},
	)
	if err != nil {
		t.Fatalf("Failed to create redis client: %v", err)
	}
	type fields struct {
		client    rueidis.Client
		key       redisTableKey
		valueType ValueType
	}
	type args struct {
		entity string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		given   interface{}
		want    interface{}
		wantErr bool
	}{
		{"String Success", fields{redisClient, redisTableKey{}, String}, args{"entity1"}, "somestring", "somestring", false},
		{"Int Success", fields{redisClient, redisTableKey{}, Int}, args{"entity2"}, 1, 1, false},
		{"Int32 Success", fields{redisClient, redisTableKey{}, Int32}, args{"entity3"}, 1, int32(1), false},
		{"Int64 Success", fields{redisClient, redisTableKey{}, Int64}, args{"entity4"}, 1, int64(1), false},
		{"Float32 Success", fields{redisClient, redisTableKey{}, Float32}, args{"entity5"}, 1, float32(1), false},
		{"Float64 Success", fields{redisClient, redisTableKey{}, Float64}, args{"entity6"}, 1, float64(1), false},
		{"Bool Success", fields{redisClient, redisTableKey{}, Bool}, args{"entity7"}, true, true, false},
		{"Timestamp Success", fields{redisClient, redisTableKey{}, Timestamp}, args{"entity8"}, time.UnixMilli(0), time.UnixMilli(0).Local(), false},
		{
			"Vector32 Success",
			fields{
				redisClient,
				redisTableKey{},
				VectorType{
					ScalarType: Float32,
				},
			},
			args{"entity9"},
			[]float32{0.08067775, 0.0012904393, 0.14408082, -0.028135499, 0.076197624},
			[]float32{0.08067775, 0.0012904393, 0.14408082, -0.028135499, 0.076197624},
			false,
		},
		// These will allow any previously created tables with incorrect valueTypes to be called as a string
		// if the valueType is not recognized
		{"String Default", fields{redisClient, redisTableKey{}, ScalarType("Invalid")}, args{"entity9"}, "somestring", "somestring", false},
		{"Int Default", fields{redisClient, redisTableKey{}, ScalarType("Invalid")}, args{"entity10"}, 1, fmt.Sprintf("%d", 1), false},
		{"Int32 Default", fields{redisClient, redisTableKey{}, ScalarType("Invalid")}, args{"entity11"}, 1, fmt.Sprintf("%d", 1), false},
		{"Int64 Default", fields{redisClient, redisTableKey{}, ScalarType("Invalid")}, args{"entity12"}, 1, fmt.Sprintf("%d", 1), false},
		{"Float32 Default", fields{redisClient, redisTableKey{}, ScalarType("Invalid")}, args{"entity13"}, 1, fmt.Sprintf("%d", 1), false},
		{"Float64 Default", fields{redisClient, redisTableKey{}, ScalarType("Invalid")}, args{"entity14"}, 1, fmt.Sprintf("%d", 1), false},
		{"Bool Default", fields{redisClient, redisTableKey{}, ScalarType("Invalid")}, args{"entity15"}, true, fmt.Sprintf("%d", 1), false},
		{"Timestamp Default", fields{redisClient, redisTableKey{}, ScalarType("Invalid")}, args{"entity16"}, time.UnixMilli(0), time.UnixMilli(0).Format(time.RFC3339), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table := redisOnlineTable{
				client:    tt.fields.client,
				key:       tt.fields.key,
				valueType: tt.fields.valueType,
			}
			err := table.Set(tt.args.entity, tt.given)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			got, err := table.Get(tt.args.entity)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get() got = %v (%T), want %v (%T)", got, got, tt.want, tt.want)
			}
		})
	}
}

// func TestFTCreateCommands(t *testing.T) {
// 	// TODO: use miniredis to mock redis; this currently only work by running
// 	// > kubectl port-forward redisearch-<ID> 6379:6379
// 	redisOnlineStore, err := NewRedisOnlineStore(&pc.RedisConfig{
// 		Addr:   "localhost:6379",
// 		Prefix: "Featureform_table__",
// 	})
// 	if err != nil {
// 		t.Fatalf("failed to create redis online store: %v", err)
// 	}

// 	cmd := redisOnlineStore.createIndexCmd(redisTableKey("test_key"), "feature", "variant", VectorType{ScalarType: Float32, Dimension: 384})
// }
