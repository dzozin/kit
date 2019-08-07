package cloudpubsub_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	stdpubsub "cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	"github.com/go-kit/kit/transport/cloudpubsub"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

const testTimeout = time.Second * 1
const testTopic = "test-topic"

func newClient(t *testing.T) (*pstest.Server, *stdpubsub.Client) {
	pubsubServer := pstest.NewServer()

	conn, err := grpc.Dial(pubsubServer.Addr, grpc.WithInsecure())
	if err != nil {
		t.Fatalf("failed to dial grpc pubsub server: %s", err)
	}

	// Use the connection when creating a pubsub client.
	client, err := stdpubsub.NewClient(context.Background(), "project", option.WithGRPCConn(conn))

	if err != nil {
		t.Fatalf("failed to connect client to pubsub server: %s", err)
	}

	topic, err := client.CreateTopic(context.Background(), testTopic)

	if err != nil {
		panic("failed to create test topic on pubsub server")
	}
	_, err = client.CreateSubscription(context.Background(), testTopic, stdpubsub.SubscriptionConfig{
		Topic: topic,
	})
	if err != nil {
		panic("failed to create test subscription on pubsub server")
	}

	return pubsubServer, client
}

type TestMsg struct {
	Body string `json:"str"`
}

func TestPublisher(t *testing.T) {
	var (
		testdata = "testdata"
		encode   = func(_ context.Context, msg interface{}) ([]byte, error) { return []byte(msg.(string)), nil }
		decode   = func(_ context.Context, data []byte) (interface{}, error) {
			return TestMsg{string(data)}, nil
		}
	)

	_, pc := newClient(t)
	defer pc.Close()

	pub := cloudpubsub.NewPublisher(pc, encode, testTopic)
	defer pub.Stop()

	_, err := pub.Endpoint()(context.Background(), testdata)
	if err != nil {
		t.Fatal(err)
	}

	wg := new(sync.WaitGroup)
	wg.Add(1)
	sub := cloudpubsub.NewSubscriber(pc, func(ctx context.Context, msg interface{}) (interface{}, error) {
		message, ok := msg.(TestMsg)
		if !ok {
			t.Fatal("message should be TestMsg")
		}
		if want, have := testdata, message.Body; want != have {
			t.Errorf("want %q, have %q", want, have)
		}
		wg.Done()
		return nil, nil
	}, decode, testTopic)

	go func() {
		if err := sub.Subscribe(); err != nil {
			t.Fatal(err)
		}
	}()
	defer sub.Unsubscribe()

	// Wait message or timeout
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		// Test succeded
	case <-time.After(testTimeout):
		t.Fatalf("Message TestMsg not received after %d seconds", testTimeout/time.Second)
	}
}

func TestEncodeJSONMessage(t *testing.T) {
	var (
		encode = cloudpubsub.EncodeJSONMessage
		decode = func(_ context.Context, msg []byte) (interface{}, error) {
			return strings.TrimSuffix(string(msg), "\n"), nil
		}
	)
	var receivedData string

	_, pc := newClient(t)
	defer pc.Close()

	var wg = &sync.WaitGroup{}

	sub := cloudpubsub.NewSubscriber(pc, func(ctx context.Context, request interface{}) (response interface{}, err error) {
		receivedData = request.(string)
		wg.Done()
		return nil, nil
	}, decode, testTopic)

	go func() {
		if err := sub.Subscribe(); err != nil {
			t.Fatal(err)
		}
	}()
	defer sub.Unsubscribe()

	pub := cloudpubsub.NewPublisher(pc, encode, testTopic)
	defer pub.Stop()

	for _, test := range []struct {
		value interface{}
		body  string
	}{
		{nil, "null"},
		{12, "12"},
		{1.2, "1.2"},
		{true, "true"},
		{"test", "\"test\""},
		{struct {
			Foo string `json:"foo"`
		}{"foo"}, "{\"foo\":\"foo\"}"},
	} {
		wg.Add(1)
		if _, err := pub.Endpoint()(context.Background(), test.value); err != nil {
			t.Fatal(err)
			continue
		}

		wg.Wait()

		if receivedData != test.body {
			t.Errorf("%v: actual %#v, expected %#v", test.value, receivedData, test.body)
		}
	}
}
