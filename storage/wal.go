package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

const (
	kBlockSize  = 32768
	kHeaderSize = 4 + 2 + 1
	kFullType   = 1
	kFirstType  = 2
	kMiddleType = 3
	kLastType   = 4
)

type WalWriter struct {
	file   *os.File
	writer *bufio.Writer
	block  []byte
	offset int
}

func NewWalWriter(path string) (*WalWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &WalWriter{
		file:   f,
		writer: bufio.NewWriterSize(f, kBlockSize*2),
		block:  make([]byte, kBlockSize),
		offset: 0,
	}, nil
}

func (w *WalWriter) AddRecord(data []byte) error {
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

func (w *WalWriter) Sync() error {
	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
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
