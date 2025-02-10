package archiver

import (
	"bytes"

	"github.com/xitongsys/parquet-go/source"
)

// BufferFile is an implementation of the ParquetFile interface that writes to an in-memory buffer
type BufferFile struct {
	Buffer     *bytes.Buffer
	ReadBuffer *bytes.Reader
	Offset     int64
}

func NewBufferFile(buffer *bytes.Buffer, offset int64) *BufferFile {
	return &BufferFile{
		Buffer:     buffer,
		ReadBuffer: bytes.NewReader(buffer.Bytes()),
		Offset:     offset,
	}
}

func (f *BufferFile) Write(p []byte) (int, error) {
	n, err := f.Buffer.Write(p)
	if err == nil {
		f.ReadBuffer = bytes.NewReader(f.Buffer.Bytes())
	}
	f.Offset += int64(n)
	return n, err
}

func (f *BufferFile) Read(p []byte) (int, error) {
	n, err := f.ReadBuffer.Read(p)
	f.Offset += int64(n)
	return n, err
}

func (f *BufferFile) Open(name string) (source.ParquetFile, error) {
	return &BufferFile{
		Buffer:     bytes.NewBuffer(f.Buffer.Bytes()),
		ReadBuffer: bytes.NewReader(f.Buffer.Bytes()),
		Offset:     0,
	}, nil
}

func (f *BufferFile) Create(name string) (source.ParquetFile, error) {
	buf := new(bytes.Buffer)
	return &BufferFile{
		Buffer:     buf,
		ReadBuffer: bytes.NewReader(buf.Bytes()),
		Offset:     0,
	}, nil
}

func (f *BufferFile) Close() error {
	return nil
}

func (f *BufferFile) Seek(offset int64, whence int) (int64, error) {
	abs, err := f.ReadBuffer.Seek(offset, whence)
	if err != nil {
		return 0, err
	}
	f.Offset = abs
	return abs, nil
}
