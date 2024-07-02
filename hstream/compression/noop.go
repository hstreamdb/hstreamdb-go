package compression

import "bytes"

type NoneCompressor struct{}

func NewNoneCompressor() Compressor {
	return &NoneCompressor{}
}

func (n *NoneCompressor) GetAlgorithm() CompressionType {
	return None
}

func (n *NoneCompressor) Compress(dst, src []byte) []byte {
	if dst == nil {
		dst = make([]byte, len(src))
	}

	b := bytes.NewBuffer(dst[:0])
	b.Write(src)
	return dst[:len(src)]
}

func (n *NoneCompressor) Close() {
}

type NoneDeCompressor struct{}

func NewNoneDeCompressor() Decompressor {
	return &NoneDeCompressor{}
}

func (n *NoneDeCompressor) GetAlgorithm() CompressionType {
	return None
}

func (n *NoneDeCompressor) Decompress(dst, src []byte) ([]byte, error) {
	if dst == nil {
		dst = make([]byte, len(src))
	}

	b := bytes.NewBuffer(dst[:0])
	b.Write(src)
	return dst[:len(src)], nil
}

func (n *NoneDeCompressor) Close() {
}
