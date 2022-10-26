package compression

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func FuzzNoneCompression(f *testing.F) {
	testcases := []byte("abcdefghijklmnopqrstuvwxyz")

	f.Add(testcases)
	f.Fuzz(func(t *testing.T, input []byte) {
		compressor := NewNoneCompressor()
		decompressor := NewNoneDeCompressor()
		encode := compressor.Compress(nil, input)
		decode, err := decompressor.Decompress(nil, encode)
		require.NoError(t, err)
		require.Equal(t, input, decode)
	})
}

func FuzzZstdCompression(f *testing.F) {
	testcases := []byte("abcdefghijklmnopqrstuvwxyz")

	f.Add(testcases)
	f.Fuzz(func(t *testing.T, input []byte) {
		compressor := NewZstdCompressor()
		decompressor := NewZstdDeCompressor()
		res := []byte{}
		encode := compressor.Compress(res, input)
		decode, err := decompressor.Decompress(res, encode)
		require.NoError(t, err)
		require.Equal(t, input, decode)
	})
}

func FuzzGzipCompression(f *testing.F) {
	testcases := []byte("abcdefghijklmnopqrstuvwxyz")

	f.Add(testcases)
	f.Fuzz(func(t *testing.T, input []byte) {
		compressor := NewGzipCompressor()
		decompressor := NewGzipDeCompressor()
		res := []byte{}
		encode := compressor.Compress(res, input)
		decode, err := decompressor.Decompress(res, encode)
		require.NoError(t, err)
		require.Equal(t, input, decode)
	})
}
