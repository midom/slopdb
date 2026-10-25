package mysqlserver

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
)

type gtidInterval struct{ start, end uint64 }

func parseGTIDSet(data []byte, source string) ([]gtidInterval, error) {
	if len(data) < 8 {
		return nil, errMalformed
	}
	count := binary.LittleEndian.Uint64(data)
	data = data[8:]
	if count > uint64(len(data)/24) {
		return nil, errMalformed
	}
	sid, err := hex.DecodeString(source)
	if err != nil {
		return nil, err
	}
	var intervals []gtidInterval
	for n := uint64(0); n < count; n++ {
		if len(data) < 24 {
			return nil, errMalformed
		}
		match := bytes.Equal(data[:16], sid)
		size := binary.LittleEndian.Uint64(data[16:])
		data = data[24:]
		if size > uint64(len(data)/16) {
			return nil, errMalformed
		}
		previous := uint64(0)
		for i := uint64(0); i < size; i++ {
			start, end := binary.LittleEndian.Uint64(data), binary.LittleEndian.Uint64(data[8:])
			data = data[16:]
			if start == 0 || end <= start || start < previous {
				return nil, errMalformed
			}
			previous = end
			if match {
				intervals = append(intervals, gtidInterval{start, end})
			}
		}
	}
	if len(data) != 0 {
		return nil, errMalformed
	}
	sort.Slice(intervals, func(i, j int) bool { return intervals[i].start < intervals[j].start })
	for i := 1; i < len(intervals); i++ {
		if intervals[i].start < intervals[i-1].end {
			return nil, errMalformed
		}
	}
	return intervals, nil
}
func (c *client) dumpBinlogGTID(packet []byte) error {
	if len(packet) < 22 {
		return c.writeError(errMalformed)
	}
	flags := binary.LittleEndian.Uint16(packet)
	if flags&^uint16(7) != 0 {
		return c.writeError(binlogFailure(fmt.Errorf("unsupported GTID dump flags")))
	}
	serverID := binary.LittleEndian.Uint32(packet[2:])
	nameLength := uint64(binary.LittleEndian.Uint32(packet[6:]))
	packet = packet[10:]
	if nameLength > uint64(len(packet)) || uint64(len(packet))-nameLength < 12 {
		return c.writeError(errMalformed)
	}
	name := string(packet[:nameLength])
	packet = packet[nameLength:]
	position := binary.LittleEndian.Uint64(packet)
	size := uint64(binary.LittleEndian.Uint32(packet[8:]))
	packet = packet[12:]
	if size != uint64(len(packet)) || position > math.MaxUint32 {
		return c.writeError(errMalformed)
	}
	snapshot, err := c.server.binlogSnapshot(c.ctx)
	if err != nil {
		return c.writeError(binlogFailure(err))
	}
	skip, err := parseGTIDSet(packet, snapshot.source)
	if err != nil {
		return c.writeError(err)
	}
	latest := uint64(0)
	if len(snapshot.entries) > 0 {
		latest = snapshot.entries[len(snapshot.entries)-1].sequence
	}
	for _, interval := range skip {
		if interval.end-1 > latest {
			return c.writeError(binlogFailure(fmt.Errorf("replica has source GTIDs beyond durable source history")))
		}
	}
	c.binlogSkip = skip
	defer func() { c.binlogSkip = nil }()
	request := binary.LittleEndian.AppendUint32(nil, uint32(position))
	request = binary.LittleEndian.AppendUint16(request, flags&1)
	request = binary.LittleEndian.AppendUint32(request, serverID)
	request = append(request, name...)
	return c.dumpBinlog(request)
}
func (c *client) skipBinlogSequence(sequence uint64) bool {
	i := sort.Search(len(c.binlogSkip), func(i int) bool { return c.binlogSkip[i].end > sequence })
	return i < len(c.binlogSkip) && sequence >= c.binlogSkip[i].start
}
func (c *client) registerReplica(packet []byte) error {
	if len(packet) < 4 {
		return c.writeError(errMalformed)
	}
	serverID := binary.LittleEndian.Uint32(packet)
	packet = packet[4:]
	for i := 0; i < 3; i++ {
		if len(packet) == 0 {
			return c.writeError(errMalformed)
		}
		size := int(packet[0])
		packet = packet[1:]
		if size > len(packet) {
			return c.writeError(errMalformed)
		}
		packet = packet[size:]
	}
	if len(packet) != 10 || serverID == 0 {
		return c.writeError(errMalformed)
	}
	c.replicaID = serverID
	return c.ok(nil)
}
