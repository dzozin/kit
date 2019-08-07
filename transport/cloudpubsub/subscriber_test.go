package cloudpubsub_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/kit/transport"
	"github.com/go-kit/kit/transport/cloudpubsub"
)

var ErrTestError = errors.New("Test error")

type testErrorHandler struct {
	handler transport.ErrorHandlerFunc
}

func (t *testErrorHandler) Handle(ctx context.Context, err error) {
	t.handler(ctx, err)
}

func TestSubscriberErrorEndpoint(t *testing.T) {
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
	errHandler := &testErrorHandler{
		handler: func(ctx context.Context, err error) {
			if ErrTestError != err {
				t.Errorf("want %q, have %q", ErrTestError, err)
			}
			wg.Done()
		},
	}

	sub := cloudpubsub.NewSubscriber(pc, errorResponder, decode, testTopic, cloudpubsub.SubscriberErrorHandler(errHandler))
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
		t.Fatalf("error message not received after %d seconds", testTimeout/time.Second)
	}
}

func errorResponder(ctx context.Context, msg interface{}) (interface{}, error) {
	return nil, ErrTestError
}
