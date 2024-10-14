package client

import (
	"context"

	proto "github.com/apache/spark-connect-go/v35/internal/generated"
	"github.com/apache/spark-connect-go/v35/spark/client/base"
)

// Public interface RuntimeConfig
type RuntimeConfig interface {
	GetAll(ctx context.Context) (*map[string]string, error)
	Set(ctx context.Context, key string, value string) error
	Get(ctx context.Context, keys []string) (*map[string]string, error)
	Unset(ctx context.Context, keys []string) error
	IsModifiable(ctx context.Context, keys []string) (*map[string]string, error)
}

// private type with private member client
type runtimeConfig struct {
	client *base.SparkConnectClient
}

// GetAll returns all configured keys in a map of strings
func (r runtimeConfig) GetAll(ctx context.Context) (*map[string]string, error) {
	req := &proto.ConfigRequest_GetAll{}
	operation := &proto.ConfigRequest_Operation_GetAll{GetAll: req}
	op := &proto.ConfigRequest_Operation{OpType: operation}
	request := &proto.ConfigRequest{Operation: op}
	resp, err := (*r.client).Config(ctx, request)
	if err != nil {
		return nil, err
	}
	m := make(map[string]string, 0)
	for _, k := range resp.GetPairs() {
		m[k.Key] = *k.Value
	}
	return &m, nil
}

// Set takes a key and a value and sets it in the config
func (r runtimeConfig) Set(ctx context.Context, key string, value string) error {
	reqArr := []*proto.KeyValue{{Key: key, Value: &value}}
	req := &proto.ConfigRequest_Set{
		Pairs: reqArr,
	}
	op := &proto.ConfigRequest_Operation{OpType: &proto.ConfigRequest_Operation_Set{Set: req}}
	request := &proto.ConfigRequest{Operation: op}
	_, err := (*r.client).Config(ctx, request)
	if err != nil {
		return err
	}
	return nil
}

// Get takes an array of keys and returns a pointer to map of strings
func (r runtimeConfig) Get(ctx context.Context, keys []string) (*map[string]string, error) {
	req := &proto.ConfigRequest_Get{Keys: keys}
	operation := &proto.ConfigRequest_Operation_Get{Get: req}
	op := &proto.ConfigRequest_Operation{OpType: operation}
	request := &proto.ConfigRequest{Operation: op}
	resp, err := (*r.client).Config(ctx, request)
	if err != nil {
		return nil, err
	}
	m := make(map[string]string, 0)
	for _, k := range resp.GetPairs() {
		m[k.Key] = *k.Value
	}

	return &m, nil
}

// Unset take an array of keys and unsets the keys in config
func (r runtimeConfig) Unset(ctx context.Context, keys []string) error {
	req := &proto.ConfigRequest_Unset{Keys: keys}
	operation := &proto.ConfigRequest_Operation_Unset{Unset: req}
	op := &proto.ConfigRequest_Operation{OpType: operation}
	request := &proto.ConfigRequest{Operation: op}
	_, err := (*r.client).Config(ctx, request)
	if err != nil {
		return err
	}
	return nil
}

// IsModifiable take an array of keys and returns status per key in a map
func (r runtimeConfig) IsModifiable(ctx context.Context, keys []string) (*map[string]string, error) {
	req := &proto.ConfigRequest_IsModifiable{Keys: keys}
	operation := &proto.ConfigRequest_Operation_IsModifiable{IsModifiable: req}
	op := &proto.ConfigRequest_Operation{OpType: operation}
	request := &proto.ConfigRequest{Operation: op}
	resp, err := (*r.client).Config(ctx, request)
	if err != nil {
		return nil, err
	}
	m := make(map[string]string, 0)
	for _, k := range resp.GetPairs() {
		m[k.Key] = *k.Value
	}

	return &m, nil
}

// Constructor for runtimeConfig used by SparkSession
func NewRuntimeConfig(client *base.SparkConnectClient) *runtimeConfig {
	return &runtimeConfig{client: client}
}
