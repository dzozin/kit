package cloudpubsub

import (
	"context"

	"cloud.google.com/go/pubsub"
	"github.com/go-kit/kit/endpoint"
)

// Publisher publishes messages to a Google Cloud pubsub topic
type Publisher struct {
	client *pubsub.Client
	topic  *pubsub.Topic

	enc   EncodeMessageFunc
	after []MessageFunc
}

// PublisherOption sets an optional parameter for clients
type PublisherOption func(*Publisher)

// PublisherAfter functions are executed on the pubsub message object after the
// message is encoded.
func PublisherAfter(after ...MessageFunc) PublisherOption {
	return func(s *Publisher) { s.after = append(s.after, after...) }
}

// NewPublisher creates a publisher that will publish to the given topic
func NewPublisher(client *pubsub.Client, enc EncodeMessageFunc, topic string, options ...PublisherOption) *Publisher {
	p := &Publisher{
		client: client,
		topic:  client.Topic(topic),

		enc:   enc,
		after: []MessageFunc{},
	}

	for _, option := range options {
		option(p)
	}

	return p
}

// Endpoint returns a usable endpoint for publishing messages on the publisher transport
func (p *Publisher) Endpoint() endpoint.Endpoint {
	return func(ctx context.Context, msg interface{}) (response interface{}, err error) {
		payload, err := p.enc(ctx, msg)
		if err != nil {
			return nil, err
		}

		pubsubMsg := &pubsub.Message{Data: payload}

		for _, f := range p.after {
			ctx = f(ctx, pubsubMsg)
		}

		_, err = p.topic.Publish(ctx, pubsubMsg).Get(ctx)

		return nil, err
	}
}

// Stop sends all remaining published messages and stop goroutines created for handling
// publishing. Returns once all outstanding messages have been sent or have
// failed to be sent.
func (p *Publisher) Stop() {
	p.topic.Stop()
}
