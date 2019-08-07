package cloudpubsub

import (
	"context"

	"cloud.google.com/go/pubsub"
)

// MessageFunc may take information from a pubsub Message and put it into a
// Message context. In Subscribers, MessageFuncs are executed prior to invoking the
// endpoint. In Publishers, MessageFuncs are executed after creating the Message
// but prior to submitting to pubsub.
type MessageFunc func(context.Context, *pubsub.Message) context.Context
