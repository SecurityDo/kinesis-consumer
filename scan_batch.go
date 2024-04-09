package consumer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/awslabs/kinesis-aggregation/go/v2/deaggregator"
)

type BatchRecord struct {
	Records            []types.Record
	ShardID            string
	MillisBehindLatest *int64
}

// add an additional type for scanning multiple records at once
type ScanBatchFunc func(*BatchRecord) error

func (c *Consumer) ScanBatch(ctx context.Context, fn ScanBatchFunc) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		errc   = make(chan error, 1)
		shardc = make(chan types.Shard, 1)
	)

	go func() {
		c.group.Start(ctx, shardc)
		<-ctx.Done()
		close(shardc)
	}()

	wg := new(sync.WaitGroup)
	// process each of the shards
	for shard := range shardc {
		wg.Add(1)
		go func(shardID string) {
			defer wg.Done()
			if err := c.ScanShardBatch(ctx, shardID, fn); err != nil {
				select {
				case errc <- fmt.Errorf("shard %s error: %w", shardID, err):
					// first error to occur
					cancel()
				default:
					// error has already occurred
				}
			}
		}(aws.ToString(shard.ShardId))
	}

	go func() {
		wg.Wait()
		close(errc)
	}()

	return <-errc
}

func (c *Consumer) ScanShardBatch(ctx context.Context, shardID string, fn ScanBatchFunc) error {
	// get last seq number from checkpoint
	lastSeqNum, err := c.group.GetCheckpoint(c.streamName, shardID)
	if err != nil {
		return fmt.Errorf("get checkpoint error: %w", err)
	}

	// get shard iterator
	shardIterator, err := c.getShardIterator(ctx, c.streamName, shardID, lastSeqNum)
	if err != nil {
		return fmt.Errorf("get shard iterator error: %w", err)
	}

	c.logger.Log("[CONSUMER] start scan:", shardID, lastSeqNum)
	defer func() {
		c.logger.Log("[CONSUMER] stop scan:", shardID)
	}()

	scanTicker := time.NewTicker(c.scanInterval)
	defer scanTicker.Stop()

	for {
		resp, err := c.client.GetRecords(ctx, &kinesis.GetRecordsInput{
			Limit:         aws.Int32(int32(c.maxRecords)),
			ShardIterator: shardIterator,
		})

		// attempt to recover from GetRecords error
		if err != nil {
			c.logger.Log("[CONSUMER] get records error:", err.Error())

			if !isRetriableError(err) {
				return fmt.Errorf("get records error: %v", err.Error())
			}

			shardIterator, err = c.getShardIterator(ctx, c.streamName, shardID, lastSeqNum)
			if err != nil {
				return fmt.Errorf("get shard iterator error: %w", err)
			}
		} else {
			// loop over records, call callback func
			var records []types.Record

			// deaggregate records
			if c.isAggregated {
				//records, err = deaggregateRecords(resp.Records)
				records, err = deaggregator.DeaggregateRecords(resp.Records)
				if err != nil {
					return err
				}
			} else {
				records = resp.Records
			}
			for _, record := range records {
				c.counter.Add("records", 1)
				lastSeqNum = *record.SequenceNumber
			}

			err := fn(&BatchRecord{records, shardID, resp.MillisBehindLatest})
			if err != nil && err != ErrSkipCheckpoint {
				return err
			}

			if err != ErrSkipCheckpoint {
				if err := c.group.SetCheckpoint(c.streamName, shardID, lastSeqNum); err != nil {
					return err
				}
			}

			//c.counter.Add("records", 1)
			//lastSeqNum = *r.SequenceNumber

			/*
				for _, r := range records {
					select {
					case <-ctx.Done():
						return nil
					default:
						err := fn(&Record{r, shardID, resp.MillisBehindLatest})
						if err != nil && err != ErrSkipCheckpoint {
							return err
						}

						if err != ErrSkipCheckpoint {
							if err := c.group.SetCheckpoint(c.streamName, shardID, *r.SequenceNumber); err != nil {
								return err
							}
						}

						c.counter.Add("records", 1)
						lastSeqNum = *r.SequenceNumber
					}
				}*/

			if isShardClosed(resp.NextShardIterator, shardIterator) {
				c.logger.Log("[CONSUMER] shard closed:", shardID)

				if c.shardClosedHandler != nil {
					err := c.shardClosedHandler(c.streamName, shardID)

					if err != nil {
						return fmt.Errorf("shard closed handler error: %w", err)
					}
				}
				return nil
			}

			shardIterator = resp.NextShardIterator
		}

		// Wait for next scan
		select {
		case <-ctx.Done():
			return nil
		case <-scanTicker.C:
			continue
		}
	}
}
