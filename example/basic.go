package example

import (
	"client/client"
	"client/hstream"
	"client/util"
	"context"
	"fmt"
	"time"
)

func main() {
	cli := hstream.NewHStreamClient([]string{"localhost:8080"})
	stream := hstream.NewStream(cli)
	// batchAppendStream := hstream.NewStream(cli, hstream.EnableBatch(100))
	// timeTriggerStream := hstream.NewStream(cli, hstream.Timeout(1 * time.Second))
	// stream := hstream.NewStream(cli, hstream.Timeout(1 * time.Second), hstream.EnableBatch(100))

	ctx := context.Background()
	if err := stream.Create(ctx, "example", 3); err != nil {
		panic(err)
	}

	for i := 0; i < 10; i++ {
		s := util.RandomString(1024)
		stream.Append(ctx, "example", client.DEFAULTKEY, client.RAWRECORD, []byte(s))
	}

	sub := hstream.NewSubscription(cli)
	if err := sub.Create(ctx, "example", "test", 3*time.Second); err != nil {
		panic(err)
	}

	handler := func(item interface{}) {
		fmt.Println(item)
	}

	if err := sub.Fetch(ctx, "example", "test", handler); err != nil {
		panic(err)
	}
}
