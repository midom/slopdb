package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
)

const (
	kBlockSize  = 32768
	kHeaderSize = 4 + 2 + 1
	kFullType   = 1
	kFirstType  = 2
	kMiddleType = 3
	kLastType   = 4
)

type walFile interface {
	io.WriteCloser
	Sync() error
}

type WalWriter struct {
	stats  *walStatistics
	file   walFile
	writer *bufio.Writer
	block  []byte
	offset int

	directory       string
	directorySynced bool
}

type walStatistics struct{ records, payloadBytes, syncs, writeErrors, syncErrors atomic.Int64 }

func NewWalWriter(path string) (*WalWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &WalWriter{
		file:      f,
		writer:    bufio.NewWriterSize(f, kBlockSize*2),
		block:     make([]byte, kBlockSize),
		offset:    0,
		directory: filepath.Dir(path),
	}, nil
}

func (w *WalWriter) AddRecord(data []byte) (err error) {
	length := len(data)
	defer func() {
		if w.stats == nil {
			return
		}
		if err != nil {
			w.stats.writeErrors.Add(1)
		} else {
			w.stats.records.Add(1)
			w.stats.payloadBytes.Add(int64(length))
		}
	}()
	begin := true
	for begin || len(data) > 0 {
		left := kBlockSize - w.offset
		if left < kHeaderSize {
			if _, err := w.writer.Write(make([]byte, left)); err != nil {
				return err
			}
			w.offset = 0
			left = kBlockSize
		}
		n := left - kHeaderSize
		if len(data) < n {
			n = len(data)
		}
		end := n == len(data)
		typ := byte(kMiddleType)
		if begin && end {
			typ = kFullType
		} else if begin {
			typ = kFirstType
		} else if end {
			typ = kLastType
		}
		crc := crc32.Update(crc32.ChecksumIEEE(data[:n]), crc32.IEEETable, []byte{typ})
		header := make([]byte, kHeaderSize)
		binary.LittleEndian.PutUint32(header, crc)
		binary.LittleEndian.PutUint16(header[4:], uint16(n))
		header[6] = typ
		if _, err := w.writer.Write(header); err != nil {
			return err
		}
		if _, err := w.writer.Write(data[:n]); err != nil {
			return err
		}
		w.offset += kHeaderSize + n
		if w.offset == kBlockSize {
			w.offset = 0
		}
		data = data[n:]
		begin = false
	}
	// Flush to the OS before acknowledging a write, so process crashes can replay it.
	return w.writer.Flush()
}

func (w *WalWriter) Sync() (err error) {
	defer func() {
		if w.stats == nil {
			return
		}
		if err != nil {
			w.stats.syncErrors.Add(1)
		} else {
			w.stats.syncs.Add(1)
		}
	}()
	if err := w.writer.Flush(); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	if !w.directorySynced {
		// A durable WAL must survive loss of its newly created directory entry,
		// too. Later syncs of this generation need only synchronize the file.
		dir, err := os.Open(w.directory)
		if err != nil {
			return err
		}
		err = dir.Sync()
		if e := dir.Close(); err == nil {
			err = e
		}
		if err != nil {
			return err
		}
		w.directorySynced = true
	}
	return nil
}

func (w *WalWriter) Close() error {
	err := w.writer.Flush()
	closeErr := w.file.Close()
	if err != nil {
		return err
	}
	return closeErr
}

type WalReader struct {
	file   *os.File
	reader *bufio.Reader
	offset int
}

func NewWalReader(path string) (*WalReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &WalReader{file: f, reader: bufio.NewReaderSize(f, kBlockSize*2)}, nil
}

func (r *WalReader) ReadRecord() ([]byte, error) {
	var result []byte
	fragmented := false
	for {
		left := kBlockSize - r.offset
		// Older writers did not pad block trailers. Only skip an actual zero trailer.
		if left < kHeaderSize {
			trailer, err := r.reader.Peek(left)
			if err == nil {
				zero := true
				for _, v := range trailer {
					if v != 0 {
						zero = false
					}
				}
				if zero {
					r.reader.Discard(left)
					r.offset = 0
				}
			}
		}
		header := make([]byte, kHeaderSize)
		_, err := io.ReadFull(r.reader, header)
		if err != nil {
			if err == io.EOF && !fragmented {
				return nil, io.EOF
			}
			return nil, fmt.Errorf("truncated WAL record: %w", io.ErrUnexpectedEOF)
		}
		n := int(binary.LittleEndian.Uint16(header[4:]))
		typ := header[6]
		fragment := make([]byte, n)
		if _, err := io.ReadFull(r.reader, fragment); err != nil {
			return nil, fmt.Errorf("truncated WAL record: %w", io.ErrUnexpectedEOF)
		}
		r.offset = (r.offset + kHeaderSize + n) % kBlockSize
		crc := crc32.Update(crc32.ChecksumIEEE(fragment), crc32.IEEETable, []byte{typ})
		if crc != binary.LittleEndian.Uint32(header) {
			return nil, fmt.Errorf("WAL CRC mismatch")
		}
		switch typ {
		case kFullType:
			if fragmented {
				return nil, fmt.Errorf("unexpected full record")
			}
			return fragment, nil
		case kFirstType:
			if fragmented {
				return nil, fmt.Errorf("unexpected first record")
			}
			result = fragment
			fragmented = true
		case kMiddleType:
			if !fragmented {
				return nil, fmt.Errorf("unexpected middle record")
			}
			result = append(result, fragment...)
		case kLastType:
			if !fragmented {
				return nil, fmt.Errorf("unexpected last record")
			}
			return append(result, fragment...), nil
		default:
			return nil, fmt.Errorf("unknown WAL record type: %d", typ)
		}
	}
}

func (r *WalReader) Close() error {
	return r.file.Close()
}
