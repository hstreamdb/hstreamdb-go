# hstreamdb-go
Go Client for HStreamDB

**Note that the current release is not suitable for production use - APIs are not yet stable and the package has not been thoroughly tested in real-life use.**

## Content

- [Installation](#installation)
- [Example Usage](#example-usage)
  - [Connect to HServer](#connect-to-hserver)
  - [Work with Streams](#work-with-streams)
  - [Write to Stream](#write-to-stream)
  - [Work with Subscriptions](#work-with-subscriptions)
  - [Consume from Subscription](#consume-from-subscription)
  - [Work with ShardReader](#Work_with_ShardReader)

## Installation

**Go 1.19** or later is required.

Add the package to your project dependencies (go.mod).

```shell
go get github.com/hstreamdb/hstreamdb-go
```

## Example Usage
### Connect to HServer

```go
import (
    "log"
    "github.com/hstreamdb/hstreamdb-go/hstream"
)

func main() {
	serverUrl := "hstream://localhost:6580,localhost:6581,localhost:6582"
	client, err := hstream.NewHStreamClient(serverUrl)
	if err != nil {
		log.Fatalf("Creating client error: %s", err)
	}
	// do sth.
	client.Close()
}
```

### Work with Streams

```go
import (
    "log"
    "github.com/hstreamdb/hstreamdb-go/hstream"
)

func main() {
	// -------------- connect to server first --------------------
	// Create a new stream with 1 replica, 5 shards, set the data retention to 1800s.
	err := client.CreateStream("testStream",
             hstream.WithReplicationFactor(1),
             hstream.WithShardCount(5),
             hstream.EnableBacklog(1800))
	if err != nil {
		log.Fatalf("Creating stream error: %s", err)
	}

	// List all streams
	streams, err := client.ListStreams()
	if err != nil {
		log.Fatalf("Listing streams error: %s", err)
	}
	for _, stream := range streams {
		log.Printf("Stream: %+v\n", stream)
	}
	
	// Delete stream
	err := client.DeleteStream("testStream", 
		hstream.EnableForceDelete, 
		hstream.EnableIgnoreNoneExist)
	if err != nil {
		log.Fatalf("Delete stream error: %s", err)
	}
}
```

### Write to Stream

#### Write RawRecord

```go
import (
    "log"
    "strconv"
    "github.com/hstreamdb/hstreamdb-go/hstream"
    "github.com/hstreamdb/hstreamdb-go/hstream/Record"
)

func main() {
	//------------- connect to server and create related stream first --------------------
	producer, err := client.NewProducer("testStream")
	if err != nil {
		log.Fatalf("Create producer error: %s", err)
	}
	defer producer.Stop()

	res := make([]hstream.AppendResult, 0, 100)
	for i := 0; i < 100; i++ {
		rawRecord, err := Record.NewHStreamRawRecord("key-1", []byte("test-value"+strconv.Itoa(i)))
		if err != nil {
			log.Fatalf("Creating rawRecord error: %s", err)
		}
		r := producer.Append(rawRecord)
		res = append(res, r)
	}

	for _, r := range res {
		resp, err := r.Ready()
		if err != nil {
			log.Printf("Append error: %s", err)
		} else {
			log.Printf("Append response: %s", resp)
		}
	}
}
```

#### Write HRecord

```go
import (
    "log"
    "strconv"
    "github.com/hstreamdb/hstreamdb-go/hstream"
    "github.com/hstreamdb/hstreamdb-go/hstream/Record"
)

func main() {
	//------------- connect to server and create related stream first --------------------
	producer, err := client.NewProducer("testStream")
	if err != nil {
		log.Fatalf("Create producer error: %s", err)
	}
	defer producer.Stop()

	payload := map[string]interface{}{
		"key1": "value1",
		"key2": 123,
		"key3": struct {
			name string
			age  int
		}{
			name: "John",
			age:  30,
		},
	}

	hRecord, err := Record.NewHStreamHRecord("testStream", payload)
	if err != nil {
		log.Fatalf("Creating hRecord error: %s", err)
	}
	value := producer.Append(hRecord)
	if resp, err := value.Ready(); err != nil {
		log.Printf("Append error: %s", err)
	} else {
		log.Printf("Append response: %s", resp)
	}
}
```

#### Batch Writter

```go
import (
    "fmt"
    "log"
    "sync"
    "github.com/hstreamdb/hstreamdb-go/hstream"
    "github.com/hstreamdb/hstreamdb-go/hstream/Record"
)

func main() {
	//------------- connect to server and create related stream first --------------------
	producer, err := client.NewBatchProducer("testStream",
		// optional: set batch size and max batch bytes trigger
		hstream.WithBatch(10, 1000),
		// optional: set timeout trigger
		hstream.TimeOut(-1), 
		// optional: set client compression
		hstream.WithCompression(compression.Zstd), 
		// optional: set flow control
		hstream.WithFlowControl(80 * 1024 * 1024)) 
	defer producer.Stop()

	keys := []string{"test-key1", "test-key2", "test-key3"}
	rids := sync.Map{}
	wg := sync.WaitGroup{}
	wg.Add(3)
	for _, key := range keys {
		go func(key string) {
			result := make([]hstream.AppendResult, 0, 100)
			for i := 0; i < 100; i++ {
				rawRecord, _ := Record.NewHStreamRawRecord("key-1", []byte(fmt.Sprintf("test-value-%s-%d", key, i)))
				r := producer.Append(rawRecord)
				result = append(result, r)
			}
			rids.Store(key, result)
			wg.Done()
		}(key)
	}

	wg.Wait()
	rids.Range(func(key, value interface{}) bool {
		k := key.(string)
		res := value.([]hstream.AppendResult)
		for idx, r := range res {
			resp, err := r.Ready()
			if err != nil {
				log.Printf("write error: %s\n", err.Error())
			}
			log.Printf("[key: %s]: record[%d]=%s\n", k, idx, resp.String())
		}
		return true
	})
}
```

### Work with Subscriptions

```go
import (
    "log"
    "github.com/hstreamdb/hstreamdb-go/hstream"
)

func main() {
	// -------------- connect to server and create related stream first --------------------
	// Create a new subscription
	streamName := "testStream"
	subId := "SubscriptionId"
	err := client.CreateSubscription(subId, streamName, 
		hstream.WithAckTimeout(60), 
		hstream.WithOffset(hstream.LATEST))

	// List all subscriptions
	subs, err := client.ListSubscriptions()
	if err != nil {
		log.Fatalf("Listing subscriptions error: %s", err)
	}
	for _, sub := range subs {
		log.Printf("Subscription: %+v", sub)
	}
}
```

### Consume from Subscription

```go
import (
    "log"
    "github.com/hstreamdb/hstreamdb-go/hstream"
    "github.com/hstreamdb/hstreamdb-go/hstream/Record"
)

func main() {
	// ------- connect to server and create related stream and subscription first -------
	streamName := "testStream"
	subId := "SubscriptionId"
	consumer := client.NewConsumer("consumer-1", subId)
	defer consumer.Stop()

	dataCh := consumer.StartFetch()
	fetchRes := make([]Record.RecordId, 0, 100)
	for res := range dataCh {
		if res.Err != nil {
			log.Printf("Fetch error: %s\n", res.Err)
			continue
		}

		for _, record := range res.Result {
			rid := record.GetRecordId()
			log.Printf("receive recordId: %s\n", rid.String())
			fetchRes = append(fetchRes, rid)
			record.Ack()
		}
		if len(fetchRes) == 100 {
			break
		}
	}
}
```

### Work with ShardReader

```go
import (
    "log"
    "github.com/hstreamdb/hstreamdb-go/hstream"
    "github.com/hstreamdb/hstreamdb-go/hstream/Record"
)

func main() {
    // ------- connect to server and create related stream first -------
    shards, err := client.ListShards(streamName)
    if err != nil {
        log.Fatalf("List Shards error: %s", err)
    }
    
    readerId := "reader"
    reader, err := client.NewShardReader(streamName, readerId, shards[0].ShardId, 
		hstream.WithShardOffset(hstream.EarliestShardOffset), 
		hstream.WithReaderTimeout(100),hstream.WithMaxRecords(10))
    if err != nil {
        log.Fatalf("Create shard reader error: %s", err)
    }
    defer client.DeleteShardReader(shards[0].ShardId, readerId)
    defer reader.Close()
    
    // ------- make sure that data has been written to the target shard ------
    readRecords := make([]Record.ReceivedRecord, 0, totalRecords)
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    for {
        res, err := reader.Read(ctx)
        s.NoError(err)
        readRecords = append(readRecords, res...)
        if len(readRecords) >= totalRecords {
            break
        }
    }
}
```

