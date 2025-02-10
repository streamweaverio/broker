package archiver

import (
	"bytes"
	"fmt"
	"io"

	"github.com/xitongsys/parquet-go/source"
)

// BufferFile is an implementation of the ParquetFile interface that writes to an in-memorry buffer
type BufferFile struct {
	Buffer *bytes.Buffer
	Offset int64
}

func NewBufferFile(buffer *bytes.Buffer, offset int64) *BufferFile {
	return &BufferFile{
		Buffer: buffer,
		Offset: offset,
	}
}

func (f *BufferFile) Write(p []byte) (int, error) {
	n, err := f.Buffer.Write(p)
	f.Offset += int64(n)
	return n, err
}

func (f *BufferFile) Read(p []byte) (int, error) {
	n, err := f.Buffer.Read(p)
	f.Offset += int64(n)
	return n, err
}

func (f *BufferFile) Open(name string) (source.ParquetFile, error) {
	return &BufferFile{
		Buffer: bytes.NewBuffer(f.Buffer.Bytes()),
		Offset: 0,
	}, nil
}

func (f *BufferFile) Create(name string) (source.ParquetFile, error) {
	return &BufferFile{
		Buffer: new(bytes.Buffer),
		Offset: 0,
	}, nil
}

func (f *BufferFile) Close() error {
	return nil
}

func (f *BufferFile) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = f.Offset + offset
	case io.SeekEnd:
		abs = int64(f.Buffer.Len()) + offset
	default:
		return 0, fmt.Errorf("invalid whence value: %d", whence)
	}

	if abs < 0 {
		return 0, fmt.Errorf(("negative position: %d"), abs)
	}

	f.Buffer = bytes.NewBuffer(f.Buffer.Bytes()[abs:])
	f.Offset = abs
	return abs, nil
}
