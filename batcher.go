package batcher

import (
	"errors"
	"time"
)

type Batcher struct {
	batchSize    int
	delay        time.Duration
	batchCount   int
	currentBatch int
	maxLen       int
}

func New(itemsLen, batchSize int, delay time.Duration) *Batcher {
	return &Batcher{
		batchSize:  batchSize,
		delay:      delay,
		batchCount: (itemsLen + batchSize - 1) / batchSize,
		maxLen:     itemsLen,
	}
}

func (b *Batcher) Next() error {
	defer func() {
		b.currentBatch++
	}()

	if b.currentBatch >= b.batchCount {
		return errors.New("end of batch")
	}

	if b.currentBatch > 0 {
		time.Sleep(b.delay)
	}

	return nil
}

func (b *Batcher) StartKey() int {
	if b.currentBatch == 0 {
		return 0
	}

	return (b.currentBatch - 1) * b.batchSize
}

func (b *Batcher) EndKey() int {
	if b.currentBatch == b.batchCount {
		return b.maxLen
	}

	return b.StartKey() + b.batchSize
}
