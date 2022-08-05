package hstream

import (
	"math/big"

	"github.com/google/btree"
	"github.com/hstreamdb/hstreamdb-go/internal/hstreamrpc"
	hstreampb "github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstreamdb/hstream/server"
)

const DEFAULT_SHARDMAP_DEGREE = 32

type Shard struct {
	ShardId      uint64
	StreamName   string
	StartHashKey string
	EndHashKey   string
}

// FIXME: maybe need to use object pool here
func (s *Shard) Less(other btree.Item) bool {
	//a1, _ := new(big.Int).SetString(s.StartHashKey, 0)
	//util.Logger().Debug("Less func", zap.String("startHashKey", s.StartHashKey), zap.Any("a1", a1))
	//b1, _ := new(big.Int).SetString(other.(*Shard).StartHashKey, 0)

	a1 := new(big.Int)
	a1.SetBytes([]byte(s.StartHashKey))
	b1 := new(big.Int)
	b1.SetBytes([]byte(other.(*Shard).StartHashKey))
	return a1.Cmp(b1) < 0
}

// ListShards will send a ListShardsRPC to HStreamDB server and wait for response.
func (c *HStreamClient) ListShards(streamName string) ([]Shard, error) {
	address, err := c.randomServer()
	if err != nil {
		return nil, err
	}

	req := &hstreamrpc.Request{
		Type: hstreamrpc.ListShards,
		Req:  &hstreampb.ListShardsRequest{StreamName: streamName},
	}

	var resp *hstreamrpc.Response
	if resp, err = c.sendRequest(address, req); err != nil {
		return nil, err
	}
	shards := resp.Resp.(*hstreampb.ListShardsResponse).GetShards()
	res := make([]Shard, 0, len(shards))
	for _, shard := range shards {
		res = append(res, ShardFromPb(shard))
	}
	return res, nil
}

type ShardMap struct {
	mp *btree.BTreeG[*Shard]
}

func NewShardMap(degree int) *ShardMap {
	return &ShardMap{
		mp: btree.NewG(degree, func(a, b *Shard) bool { return a.Less(b) }),
	}
}
func newSearchItem(key string) *Shard {
	return &Shard{StartHashKey: key}
}

func (m *ShardMap) FindLessOrEqual(key string) *Shard {
	var s *Shard
	m.mp.DescendLessOrEqual(newSearchItem(key), func(item *Shard) bool {
		s = item
		return false
	})
	return s
}

func (m *ShardMap) ReplaceOrInsert(shard *Shard) {
	m.mp.ReplaceOrInsert(shard)
}

func (m *ShardMap) Ascend() []Shard {
	shards := []Shard{}
	m.mp.Ascend(func(shard *Shard) bool {
		shards = append(shards, *shard)
		return true
	})
	return shards
}

func (m *ShardMap) Delete(shard *Shard) {
	m.mp.Delete(shard)
}

func (m *ShardMap) Clear() {
	m.mp.Clear(false)
}
