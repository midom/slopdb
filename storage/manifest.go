package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
)

const (
	manifestMagic   uint32 = 0x4D414E49 // "MANI"
	manifestVersion uint32 = 1
)

type manifestRecord struct {
	level  int
	number uint64
	size   uint64
	fMin   InternalKey
	fMax   InternalKey
}

func encodeManifest(levels [maxLevels][]*FileMetaData) []byte {
	buf := make([]byte, 0, 4096)
	header := make([]byte, 12)
	binary.LittleEndian.PutUint32(header[0:4], manifestMagic)
	binary.LittleEndian.PutUint32(header[4:8], manifestVersion)
	binary.LittleEndian.PutUint32(header[8:12], uint32(maxLevels))
	buf = append(buf, header...)

	for level := 0; level < maxLevels; level++ {
		count := uint32(len(levels[level]))
		countBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(countBuf, count)
		buf = append(buf, countBuf...)

		for _, f := range levels[level] {
			numBuf := make([]byte, 8)
			sizeBuf := make([]byte, 8)
			binary.LittleEndian.PutUint64(numBuf, f.Number)
			binary.LittleEndian.PutUint64(sizeBuf, f.FileSize)
			buf = append(buf, numBuf...)
			buf = append(buf, sizeBuf...)

			eMin := EncodeInternalKey(f.Smallest)
			eMax := EncodeInternalKey(f.Largest)
			buf = append(buf, encodeBytesBuf(eMin)...)
			buf = append(buf, encodeBytesBuf(eMax)...)
		}
	}
	return buf
}

func decodeManifest(data []byte) ([maxLevels][]*FileMetaData, error) {
	var levels [maxLevels][]*FileMetaData
	if len(data) < 12 {
		return levels, fmt.Errorf("manifest too short")
	}

	magic := binary.LittleEndian.Uint32(data[0:4])
	version := binary.LittleEndian.Uint32(data[4:8])
	nl := binary.LittleEndian.Uint32(data[8:12])
	if magic != manifestMagic || version != manifestVersion || nl != maxLevels {
		return levels, fmt.Errorf("bad manifest magic")
	}

	maxLvl := int(nl)
	if maxLvl > maxLevels {
		maxLvl = maxLevels
	}

	offset := 12
	for level := 0; level < maxLvl; level++ {
		if offset+4 > len(data) {
			return levels, fmt.Errorf("truncated manifest")
		}
		count := binary.LittleEndian.Uint32(data[offset:])
		offset += 4

		for i := uint32(0); i < count; i++ {
			if offset+16 > len(data) {
				return levels, fmt.Errorf("truncated manifest")
			}

			number := binary.LittleEndian.Uint64(data[offset:])
			size := binary.LittleEndian.Uint64(data[offset+8:])
			offset += 16

			eMin, nMin := decodeBytes(data[offset:])
			if nMin <= 0 || offset+nMin > len(data) {
				return levels, fmt.Errorf("invalid manifest key")
			}
			offset += nMin

			eMax, nMax := decodeBytes(data[offset:])
			if nMax <= 0 || offset+nMax > len(data) {
				return levels, fmt.Errorf("invalid manifest key")
			}
			offset += nMax

			f := &FileMetaData{
				Number:   number,
				FileSize: size,
				Smallest: ParseInternalKey(eMin),
				Largest:  ParseInternalKey(eMax),
			}
			levels[level] = append(levels[level], f)
		}
	}
	return levels, nil
}

func writeManifestFile(dir string, levels [maxLevels][]*FileMetaData, fileNum uint64) error {
	tmpPath := filepath.Join(dir, fmt.Sprintf("MANIFEST-%d.tmp", fileNum))
	manifestPath := filepath.Join(dir, "MANIFEST")

	data := encodeManifest(levels)

	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmpPath, manifestPath); err != nil {
		os.Remove(tmpPath)
		return err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func readManifestFile(dir string) ([maxLevels][]*FileMetaData, uint64, error) {
	var levels [maxLevels][]*FileMetaData

	manifestPath := filepath.Join(dir, "MANIFEST")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		if os.IsNotExist(err) {
			return levels, 0, nil
		}
		return levels, 0, err
	}

	decoded, err := decodeManifest(data)
	if err != nil {
		return levels, 0, err
	}

	var maxNum uint64
	for _, fl := range decoded {
		for _, f := range fl {
			if f.Number >= maxNum {
				maxNum = f.Number + 1
			}
		}
	}

	return decoded, maxNum, nil
}

func encodeBytesBuf(data []byte) []byte {
	lenBuf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(lenBuf, uint64(len(data)))
	result := make([]byte, n+len(data))
	copy(result, lenBuf[:n])
	copy(result[n:], data)
	return result
}
