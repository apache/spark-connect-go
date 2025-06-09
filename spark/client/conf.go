// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"context"

	proto "github.com/apache/spark-connect-go/v40/internal/generated"
	"github.com/apache/spark-connect-go/v40/spark/client/base"
)

// Public interface RuntimeConfig
type RuntimeConfig interface {
	GetAll(ctx context.Context) (map[string]string, error)
	Set(ctx context.Context, key string, value string) error
	Get(ctx context.Context, key string) (string, error)
	Unset(ctx context.Context, key string) error
	IsModifiable(ctx context.Context, key string) (bool, error)
	GetWithDefault(ctx context.Context, key string, default_value string) (string, error)
}

// private type with private member client
type runtimeConfig struct {
	client *base.SparkConnectClient
}

// GetAll returns all configured keys in a map of strings
func (r runtimeConfig) GetAll(ctx context.Context) (map[string]string, error) {
	req := &proto.ConfigRequest_GetAll{}
	operation := &proto.ConfigRequest_Operation_GetAll{GetAll: req}
	op := &proto.ConfigRequest_Operation{OpType: operation}
	resp, err := (*r.client).Config(ctx, op)
	if err != nil {
		return nil, err
	}
	m := make(map[string]string, 0)
	for _, k := range resp.GetPairs() {
		if k.Value != nil {
			m[k.Key] = *k.Value
		}
	}
	return m, nil
}

// Set takes a key and a value and sets it in the config
func (r runtimeConfig) Set(ctx context.Context, key string, value string) error {
	reqArr := []*proto.KeyValue{{Key: key, Value: &value}}
	req := &proto.ConfigRequest_Set{
		Pairs: reqArr,
	}
	op := &proto.ConfigRequest_Operation{OpType: &proto.ConfigRequest_Operation_Set{Set: req}}
	_, err := (*r.client).Config(ctx, op)
	if err != nil {
		return err
	}
	return nil
}

func (r runtimeConfig) Get(ctx context.Context, key string) (string, error) {
	req := &proto.ConfigRequest_Get{Keys: []string{key}}
	operation := &proto.ConfigRequest_Operation_Get{Get: req}
	op := &proto.ConfigRequest_Operation{OpType: operation}
	resp, err := (*r.client).Config(ctx, op)
	if err != nil {
		return "", err
	}
	return *resp.GetPairs()[0].Value, nil
}

func (r runtimeConfig) Unset(ctx context.Context, key string) error {
	req := &proto.ConfigRequest_Unset{Keys: []string{key}}
	operation := &proto.ConfigRequest_Operation_Unset{Unset: req}
	op := &proto.ConfigRequest_Operation{OpType: operation}
	_, err := (*r.client).Config(ctx, op)
	if err != nil {
		return err
	}
	return nil
}

func (r runtimeConfig) IsModifiable(ctx context.Context, key string) (bool, error) {
	req := &proto.ConfigRequest_IsModifiable{Keys: []string{key}}
	operation := &proto.ConfigRequest_Operation_IsModifiable{IsModifiable: req}
	op := &proto.ConfigRequest_Operation{OpType: operation}
	resp, err := (*r.client).Config(ctx, op)
	if err != nil {
		return false, err
	}
	re := *resp.GetPairs()[0].Value
	if re == "true" {
		return true, nil
	} else {
		return false, nil
	}
}

func (r runtimeConfig) GetWithDefault(ctx context.Context, key string, default_value string) (string, error) {
	p := make([]*proto.KeyValue, 0)
	p = append(p, &proto.KeyValue{Key: key, Value: &default_value})
	req := &proto.ConfigRequest_GetWithDefault{Pairs: p}
	operation := &proto.ConfigRequest_Operation_GetWithDefault{GetWithDefault: req}
	op := &proto.ConfigRequest_Operation{OpType: operation}
	resp, err := (*r.client).Config(ctx, op)
	if err != nil {
		return "", err
	}

	return *resp.GetPairs()[0].Value, nil
}

// Constructor for runtimeConfig used by SparkSession
func NewRuntimeConfig(client *base.SparkConnectClient) *runtimeConfig {
	return &runtimeConfig{client: client}
}
