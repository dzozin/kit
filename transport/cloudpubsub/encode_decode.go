package cloudpubsub

import (
	"bytes"
	"context"
	"encoding/json"
)

// EncodeMessageFunc encodes a message being published
type EncodeMessageFunc func(context.Context, interface{}) ([]byte, error)

// DecodeMessageFunc decodes a message coming from a subscription
type DecodeMessageFunc func(context.Context, []byte) (interface{}, error)

// EncodeJSONMessage is an EncodeMessageFunc that serializes the message as a JSON object
func EncodeJSONMessage(_ context.Context, msg interface{}) ([]byte, error) {
	var b bytes.Buffer
	if err := json.NewEncoder(&b).Encode(msg); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}
