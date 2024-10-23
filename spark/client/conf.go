package client

import (
	"context"

	proto "github.com/apache/spark-connect-go/v35/internal/generated"
	"github.com/apache/spark-connect-go/v35/spark/client/base"
)

// Public interface RuntimeConfig
type RuntimeConfig interface {
	GetAll(ctx context.Context) (map[string]string, error)
	Set(ctx context.Context, key string, value string) error
	Get(ctx context.Context, key string) (map[string]string, error)
	Unset(ctx context.Context, key string) error
	IsModifiable(ctx context.Context, key string) (map[string]string, error)
	GetWithDefault(ctx context.Context, key string, default_value string) (map[string]string, error)
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
func (r runtimeConfig) Get(ctx context.Context, key string) (map[string]string, error) {
	req := &proto.ConfigRequest_Get{Keys: []string{key}}
	operation := &proto.ConfigRequest_Operation_Get{Get: req}
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

func (r runtimeConfig) IsModifiable(ctx context.Context, key string) (map[string]string, error) {
	req := &proto.ConfigRequest_IsModifiable{Keys: []string{key}}
	operation := &proto.ConfigRequest_Operation_IsModifiable{IsModifiable: req}
	op := &proto.ConfigRequest_Operation{OpType: operation}
	resp, err := (*r.client).Config(ctx, op)
	if err != nil {
		return nil, err
	}
	m := make(map[string]string)
	for _, k := range resp.GetPairs() {
		if k.Value != nil {
			m[k.Key] = *k.Value
		}
	}
	return m, nil
}

func (r runtimeConfig) GetWithDefault(ctx context.Context, key string, default_value string) (map[string]string, error) {
	p := make([]*proto.KeyValue, 0)
	p = append(p, &proto.KeyValue{Key: key, Value: &default_value})
	req := &proto.ConfigRequest_GetWithDefault{Pairs: p}
	operation := &proto.ConfigRequest_Operation_GetWithDefault{GetWithDefault: req}
	op := &proto.ConfigRequest_Operation{OpType: operation}
	resp, err := (*r.client).Config(ctx, op)
	if err != nil {
		return nil, err
	}
	m := make(map[string]string, 0)
	for _, k := range resp.GetPairs() {
		m[k.Key] = *k.Value
	}
	return m, nil
}

// Constructor for runtimeConfig used by SparkSession
func NewRuntimeConfig(client *base.SparkConnectClient) *runtimeConfig {
	return &runtimeConfig{client: client}
}
