package compression

type Compressor interface {
	GetAlgorithm() CompressionType
	// Compress a []byte, the param is a []byte with the uncompressed content.
	// The reader/writer indexes will not be modified. The return is a []byte
	// with the compressed content.
	Compress(dst, src []byte) []byte

	Close()
}

type Decompressor interface {
	GetAlgorithm() CompressionType

	// Decompress a []byte. The buffer needs to have been compressed with the matching Encoder.
	// The src is compressed content. If dst is passed, the decompressed data will be written there
	// The return were the result will be passed, if err is nil, the buffer was decompressed, no nil otherwise.
	Decompress(dst, src []byte) ([]byte, error)
	Close()
}

type CompressionType int

const (
	None CompressionType = iota
	Gzip
	Zstd
)
