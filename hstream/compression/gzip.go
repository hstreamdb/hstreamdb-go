package compression

import (
	"bytes"
	"io"

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
	buffer := bytes.NewBuffer(dst)
	g.encoder.Reset(buffer)
	if _, err := g.encoder.Write(src); err != nil {
		util.Logger().Error("gzip compress error", zap.String("error", err.Error()))
		return nil
	}
	g.encoder.Flush()
	return buffer.Bytes()
}

func (g *GzipCompressor) Close() {
	g.encoder.Close()
}

type GzipDeCompressor struct {
	decoder *gzip.Reader
}

func NewGzipDeCompressor() Decompressor {
	return &GzipDeCompressor{
		decoder: new(gzip.Reader),
	}
}

func (g *GzipDeCompressor) GetAlgorithm() CompressionType {
	return Gzip
}

func (g *GzipDeCompressor) Decompress(dst, src []byte) ([]byte, error) {
	if err := g.decoder.Reset(bytes.NewBuffer(src)); err != nil {
		return nil, err
	}
	buffer := bytes.NewBuffer(dst)
	io.Copy(buffer, g.decoder)
	return buffer.Bytes(), nil
}

func (g *GzipDeCompressor) Close() {
	g.decoder.Close()
}
