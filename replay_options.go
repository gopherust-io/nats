package nats

import "time"

// ReplayOpt configures ResetConsumer / CreateReplayConsumer.
type ReplayOpt func(*ReplayConfig)

// WithReplayDurable sets the side-car durable name for CreateReplayConsumer.
// Ignored by ResetConsumer (which uses the durable argument).
func WithReplayDurable(name string) ReplayOpt {
	return func(c *ReplayConfig) { c.Durable = name }
}

// WithFilterSubject sets a single filter subject for the replay consumer.
func WithFilterSubject(subject string) ReplayOpt {
	return func(c *ReplayConfig) {
		c.FilterSubject = subject
		c.FilterSubjects = nil
	}
}

// WithFilterSubjects sets multi-filter subjects for the replay consumer.
func WithFilterSubjects(subjects ...string) ReplayOpt {
	return func(c *ReplayConfig) {
		c.FilterSubjects = append([]string(nil), subjects...)
		c.FilterSubject = empty
	}
}

// WithDeliverPolicy sets an explicit deliver policy (including DeliverAll).
func WithDeliverPolicy(policy DeliverPolicy) ReplayOpt {
	return func(c *ReplayConfig) {
		c.DeliverPolicy = policy
		c.deliverSet = true
	}
}

// WithReplayPolicy sets Instant vs Original timing replay (including ReplayInstant).
func WithReplayPolicy(policy ReplayPolicy) ReplayOpt {
	return func(c *ReplayConfig) {
		c.ReplayPolicy = policy
		c.replaySet = true
	}
}

// WithStartSeq sets OptStartSeq (does not change DeliverPolicy by itself).
func WithStartSeq(seq uint64) ReplayOpt {
	return func(c *ReplayConfig) {
		c.OptStartSeq = seq
		c.optStartSeqSet = true
	}
}

// WithStartTime sets OptStartTime (does not change DeliverPolicy by itself).
func WithStartTime(t time.Time) ReplayOpt {
	return func(c *ReplayConfig) {
		ts := t
		c.OptStartTime = &ts
		c.optStartTimeSet = true
	}
}

// FromSeq seeks by stream sequence (DeliverByStartSequence + OptStartSeq).
func FromSeq(seq uint64) ReplayOpt {
	return func(c *ReplayConfig) {
		c.DeliverPolicy = DeliverByStartSequence
		c.deliverSet = true
		c.OptStartSeq = seq
		c.optStartSeqSet = true
		c.OptStartTime = nil
		c.optStartTimeSet = true
	}
}

// FromTime seeks by start timestamp (DeliverByStartTime + OptStartTime).
func FromTime(t time.Time) ReplayOpt {
	return func(c *ReplayConfig) {
		c.DeliverPolicy = DeliverByStartTime
		c.deliverSet = true
		ts := t
		c.OptStartTime = &ts
		c.optStartTimeSet = true
		c.OptStartSeq = 0
		c.optStartSeqSet = true
	}
}

// FromBeginning replays from the first available message (DeliverAll).
func FromBeginning() ReplayOpt {
	return func(c *ReplayConfig) {
		c.DeliverPolicy = DeliverAll
		c.deliverSet = true
		c.OptStartSeq = 0
		c.optStartSeqSet = true
		c.OptStartTime = nil
		c.optStartTimeSet = true
	}
}

// FromNew only delivers messages published after the consumer is (re)created.
func FromNew() ReplayOpt {
	return func(c *ReplayConfig) {
		c.DeliverPolicy = DeliverNew
		c.deliverSet = true
		c.OptStartSeq = 0
		c.optStartSeqSet = true
		c.OptStartTime = nil
		c.optStartTimeSet = true
	}
}

func applyReplayOpts(opts []ReplayOpt) ReplayConfig {
	var cfg ReplayConfig
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	return cfg
}
