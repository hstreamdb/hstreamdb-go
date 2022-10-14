package compression

import (
	"bytes"
	"github.com/hstreamdb/hstreamdb-go/util"
	"github.com/klauspost/compress/gzip"
	"go.uber.org/zap"
)

type GzipCompressor struct {
	encoder *gzip.Writer
}

func NewGzipCompressor() Compressor {
	zw := gzip.NewWriter(nil)
	return &GzipCompressor{
		encoder: zw,
	}
}

func (g *GzipCompressor) GetAlgorithm() CompressionType {
	return Gzip
}

func (g *GzipCompressor) Compress(dst, src []byte) []byte {
	g.encoder.Reset(bytes.NewBuffer(dst))
	if _, err := g.encoder.Write(src); err != nil {
		util.Logger().Error("gzip compress error", zap.String("error", err.Error()))
		return nil
	}
	g.encoder.Flush()
	return dst
}

func (g *GzipCompressor) Close() {
	g.encoder.Close() //TODO implement me
}

type GzipDeCompressor struct {
	decoder *gzip.Reader
}

func NewGzipDeCompressor() Decompressor {
	//var writeBuf, readBuf bytes.Buffer
	zr, _ := gzip.NewReader(nil)
	return &GzipDeCompressor{
		decoder: zr,
	}
}

func (g *GzipDeCompressor) GetAlgorithm() CompressionType {
	return Gzip
}

func (g *GzipDeCompressor) Decompress(dst, src []byte) ([]byte, error) {
	g.decoder.Reset(bytes.NewBuffer(dst))
	if _, err := g.decoder.Read(src); err != nil {
		return nil, err
	}
	return dst, nil
}

func (g *GzipDeCompressor) Close() {
	g.decoder.Close()
}
