package compression

import "github.com/klauspost/compress/zstd"

type ZstdCompressor struct {
	encoder *zstd.Encoder
}

func NewZstdCompressor() Compressor {
	encoder, _ := zstd.NewWriter(nil)
	return &ZstdCompressor{
		encoder: encoder,
	}
}

func (z *ZstdCompressor) GetAlgorithm() CompressionType {
	return Zstd
}

func (z *ZstdCompressor) Compress(dst, src []byte) []byte {
	return z.encoder.EncodeAll(src, dst)
}

func (z *ZstdCompressor) Close() {
	z.encoder.Close()
}

type ZstdDeCompressor struct {
	decoder *zstd.Decoder
}

func NewZstdDeCompressor() Decompressor {
	decoder, _ := zstd.NewReader(nil)
	return &ZstdDeCompressor{
		decoder: decoder,
	}
}

func (z *ZstdDeCompressor) GetAlgorithm() CompressionType {
	return Zstd
}

func (z *ZstdDeCompressor) Decompress(dst, src []byte) ([]byte, error) {
	return z.decoder.DecodeAll(src, dst)
}

func (z *ZstdDeCompressor) Close() {
	z.decoder.Close()
}
