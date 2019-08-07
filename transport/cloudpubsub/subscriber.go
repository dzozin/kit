package cloudpubsub

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"cloud.google.com/go/pubsub"
	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/transport"
)

// Subscriber receives messages from a Google Cloud pubsub subscription
type Subscriber struct {
	client       *pubsub.Client
	subscription *pubsub.Subscription

	endpoint        endpoint.Endpoint
	dec             DecodeMessageFunc
	errorHandler    transport.ErrorHandler
	cancelReception context.CancelFunc
	before          []MessageFunc
}

// SubscriberOption sets an optional parameter for clients
type SubscriberOption func(*Subscriber)

// ImmediateAck immediately acknowledge pubsub messages.
// Used with SubscriberBefore option allows to acknoledge before decoding and passing it to the endpoint.
func ImmediateAck(ctx context.Context, msg *pubsub.Message) context.Context {
	msg.Ack()
	return ctx
}

// SubscriberBefore functions are executed on the pubsub message object before the
// message is decoded.
func SubscriberBefore(before ...MessageFunc) SubscriberOption {
	return func(s *Subscriber) { s.before = append(s.before, before...) }
}

// SubscriberErrorHandler is used to handle non-terminal errors. By default, non-terminal errors
// are ignored. This is intended as a diagnostic measure.
func SubscriberErrorHandler(errorHandler transport.ErrorHandler) SubscriberOption {
	return func(s *Subscriber) { s.errorHandler = errorHandler }
}

// NewSubscriber create a subscription of the endpoint to the given event
func NewSubscriber(client *pubsub.Client, e endpoint.Endpoint, dec DecodeMessageFunc, subscription string, options ...SubscriberOption) *Subscriber {
	s := &Subscriber{
		client:       client,
		subscription: client.Subscription(subscription),

		endpoint:        e,
		dec:             dec,
		errorHandler:    transport.NewLogErrorHandler(log.NewNopLogger()),
		cancelReception: func() {},
		before:          []MessageFunc{},
	}

	for _, option := range options {
		option(s)
	}

	return s
}

// Subscribe begins listening for messages and passing them to the endpoint
func (s *Subscriber) Subscribe() error {
	var ctx context.Context
	ctx, s.cancelReception = context.WithCancel(context.Background())
	if err := s.subscription.Receive(ctx, s.rcv); err != nil {
		if grpc.Code(err) == codes.Canceled {
			return nil
		}
		return err
	}
	return nil
}

func (s *Subscriber) rcv(ctx context.Context, msg *pubsub.Message) {
	for _, f := range s.before {
		ctx = f(ctx, msg)
	}

	payload, err := s.dec(ctx, msg.Data)
	if err != nil {
		s.errorHandler.Handle(ctx, err)
		return
	}

	_, err = s.endpoint(ctx, payload)

	if err != nil {
		s.errorHandler.Handle(ctx, err)
	}

	msg.Ack()
}

// Unsubscribe gracefully stops the subscriber from receiving messages
func (s *Subscriber) Unsubscribe() {
	s.cancelReception()
}
