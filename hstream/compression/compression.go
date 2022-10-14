package compression

type Compressor interface {
	GetAlgorithm() CompressionType
	Compress(dst, src []byte) []byte
	Close()
}

type Decompressor interface {
	GetAlgorithm() CompressionType
	Decompress(dst, src []byte) ([]byte, error)
	Close()
}

type CompressionType int

const (
	None CompressionType = iota
	Gzip
	Zstd
)
