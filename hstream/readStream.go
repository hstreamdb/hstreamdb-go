package hstream

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/hstreamdb/hstreamdb-go/hstream/Record"
	"github.com/hstreamdb/hstreamdb-go/internal/hstreamrpc"
	hstreampb "github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstreamdb/hstream/server"
	"github.com/hstreamdb/hstreamdb-go/util"
)

type StreamReadResult struct {
	Records []Record.ReceivedRecord
	Err     error
}

func (c *HStreamClient) ReadStream(streamName string, from StreamOffset, until StreamOffset, totalBatches uint64) (chan StreamReadResult, error) {
	randomReaderId := fmt.Sprintf("read_stream_reader_%d", time.Now().UnixNano())
	address, err := c.lookUpShardReader(randomReaderId)
	if err != nil {
		return nil, err
	}

	readStreamReq := &hstreampb.ReadStreamRequest{
		ReaderId:       randomReaderId,
		StreamName:     streamName,
		From:           from.toStreamOffset(),
		Until:          until.toStreamOffset(),
		MaxReadBatches: totalBatches,
	}

	req := &hstreamrpc.Request{
		Type: hstreamrpc.ReadStream,
		Req:  readStreamReq,
	}

	res, err := c.SendRequest(context.Background(), address, req)
	if err != nil {
		return nil, err
	}

	stream := res.Resp.(hstreampb.HStreamApi_ReadStreamClient)
	resultChan := make(chan StreamReadResult, 100)

	go func() {
		defer func() {
			close(resultChan)
		}()

		decompressors := sync.Map{}
		for {
			res, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					util.Logger().Info(fmt.Sprintf("read stream %s done", streamName))
					return
				}
				resultChan <- StreamReadResult{Err: err}
				continue
			}

			records := res.GetReceivedRecords()
			readRes := make([]Record.ReceivedRecord, 0, len(records))
			for _, record := range records {
				hstreamReocrds, err := decodeReceivedRecord(record, &decompressors)
				if err != nil {
					resultChan <- StreamReadResult{Err: err}
					continue
				}

				recordIds := record.GetRecordIds()
				// util.Logger().Info(fmt.Sprintf("read %d records from stream: %s", len(recordIds), streamName))
				for i := 0; i < len(recordIds); i++ {
					receivedRecord, err := ReceivedRecordFromPb(hstreamReocrds[i], recordIds[i])
					if err != nil {
						resultChan <- StreamReadResult{Err: err}
						break
					}
					readRes = append(readRes, receivedRecord)
				}
			}
			resultChan <- StreamReadResult{Records: readRes}
		}
	}()

	return resultChan, nil
}
