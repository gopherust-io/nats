package nats

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"

	"github.com/gopherust-io/nats/internal/bytesconv"
)

// OrderedConsumerConfig configures a client-managed ordered consumer.
//
// goalign:ignore
type OrderedConsumerConfig struct {
	FilterSubjects    []string
	FilterSubject     string
	OptStartTime      *time.Time
	NamePrefix        string
	InactiveThreshold time.Duration
	OptStartSeq       uint64
	DeliverPolicy     DeliverPolicy
	ReplayPolicy      ReplayPolicy
	HeadersOnly       bool
	HasDeliverPolicy  bool
	HasReplayPolicy   bool
}

// OrderedSubscription is a running ordered consume loop.
type OrderedSubscription interface {
	Stop()
	Stopped() <-chan struct{}
}

type orderedSubscription struct {
	stopOnce sync.Once
	stopped  chan struct{}
	cc       jetstream.ConsumeContext
}

func (o *orderedSubscription) Stop() {
	o.stopOnce.Do(func() {
		if o.cc != nil {
			o.cc.Stop()
		}
		select {
		case <-o.stopped:
		default:
			close(o.stopped)
		}
	})
}

func (o *orderedSubscription) Stopped() <-chan struct{} {
	return o.stopped
}

// OrderedSubscribe binds an ordered JetStream consumer and delivers messages
// one-at-a-time with sequence preservation (new jetstream API).
// Handlers should not Ack/Nak — ordered consumers use AckNone and the client
// manages cursor reset on gaps.
func (c *consumer) OrderedSubscribe(ctx context.Context, stream string, cfg OrderedConsumerConfig, handler MsgHandler) (OrderedSubscription, error) {
	if err := ValidateStreamName(stream); err != nil {
		return nil, fmt.Errorf("ordered subscribe stream=%q: %w", stream, err)
	}
	if c.jsNew == nil {
		return nil, fmt.Errorf("ordered subscribe stream=%q: %w", stream, ErrJetStreamV2Required)
	}
	if handler == nil {
		return nil, fmt.Errorf("ordered subscribe stream=%q: handler is nil", stream)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	ocfg := jetstream.OrderedConsumerConfig{
		FilterSubjects:    append([]string(nil), cfg.FilterSubjects...),
		InactiveThreshold: cfg.InactiveThreshold,
		HeadersOnly:       cfg.HeadersOnly,
		NamePrefix:        cfg.NamePrefix,
		OptStartSeq:       cfg.OptStartSeq,
		OptStartTime:      cfg.OptStartTime,
	}
	if !bytesconv.IsEmpty(cfg.FilterSubject) && len(cfg.FilterSubjects) == 0 {
		ocfg.FilterSubjects = []string{cfg.FilterSubject}
	}
	if cfg.HasDeliverPolicy {
		ocfg.DeliverPolicy = jetstream.DeliverPolicy(cfg.DeliverPolicy)
	}
	if cfg.HasReplayPolicy {
		ocfg.ReplayPolicy = jetstream.ReplayPolicy(cfg.ReplayPolicy)
	}

	cons, err := c.jsNew.OrderedConsumer(ctx, stream, ocfg)
	if err != nil {
		return nil, fmt.Errorf("ordered consumer stream=%q: %w", stream, err)
	}

	sub := &orderedSubscription{stopped: make(chan struct{})}
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		nmsg := jetstreamMsgToNATS(msg)
		if c.cfg.PayloadDecompression {
			if derr := maybeDecompressMsg(nmsg); derr != nil {
				zerolog.Ctx(ctx).Error().Err(derr).Str("subject", nmsg.Subject).Msg("ordered decompress")

				return
			}
		}
		if herr := invokeMsgHandler(ctx, nmsg, handler); herr != nil {
			zerolog.Ctx(ctx).Error().Err(herr).Str("subject", nmsg.Subject).Msg("ordered handler")
			c.notifyMessageHandled(0)

			return
		}
		c.notifyProcessSuccess()
	})
	if err != nil {
		return nil, fmt.Errorf("ordered consume stream=%q: %w", stream, err)
	}
	sub.cc = cc

	go func() {
		select {
		case <-ctx.Done():
			sub.Stop()
		case <-sub.stopped:
		}
	}()

	return sub, nil
}

// OrderedSubscribe is exposed on Client for the ordered consumer façade.
func (c *client) OrderedSubscribe(ctx context.Context, stream string, cfg OrderedConsumerConfig, handler MsgHandler) (OrderedSubscription, error) {
	return c.consumer.OrderedSubscribe(ctx, stream, cfg, handler)
}
