package mysqlserver

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"slopdb/db"
	"slopdb/sql"
)

type binlogEntry struct {
	sequence         uint64
	file, start, end uint32
}
type binlogFile struct {
	number, size uint32
	rotated      bool
	previous     uint64
}
type binlogSnapshot struct {
	source   string
	serverID uint32
	entries  []binlogEntry
	files    []binlogFile
}
type binlogState struct {
	mu           sync.Mutex
	reader       *db.ChangeReader
	source       string
	serverID     uint32
	entries      []binlogEntry
	files        []binlogFile
	maxFileBytes uint32
	err          error
}

func (s *Server) binlogSnapshot(ctx context.Context) (*binlogSnapshot, error) {
	s.binlogMu.Lock()
	if s.binlog == nil {
		metadata, err := s.database.EnableChangefeed(ctx)
		if err != nil {
			s.binlogMu.Unlock()
			return nil, err
		}
		reader, err := s.database.NewChangeReader(ctx, metadata.StartSequence)
		if err != nil {
			s.binlogMu.Unlock()
			return nil, err
		}
		sid, _ := hex.DecodeString(metadata.SourceID)
		serverID := crc32.ChecksumIEEE(sid)
		if serverID == 0 {
			serverID = 1
		}
		s.binlog = &binlogState{reader: reader, source: metadata.SourceID, serverID: serverID, files: []binlogFile{{number: 1, size: 4 + binlogFormatEvent().size() + binlogPreviousEvent(metadata.SourceID, 0).size()}}, maxFileBytes: math.MaxUint32 - 1024}
	}
	state := s.binlog
	s.binlogMu.Unlock()
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.err != nil {
		return nil, state.err
	}
	target, err := s.database.ChangefeedPosition(ctx)
	if err != nil {
		return nil, err
	}
	for state.reader.Position() < target {
		limit := int(min(uint64(128), target-state.reader.Position()))
		transactions, err := state.reader.Read(ctx, limit)
		if err != nil {
			return nil, err
		}
		for _, transaction := range transactions {
			events, err := binlogTransaction(state.source, transaction)
			if err != nil {
				state.err = err
				return nil, err
			}
			size := uint64(0)
			for _, event := range events {
				size += uint64(event.size())
			}
			file := &state.files[len(state.files)-1]
			rotateSize := uint64(binlogRotateEvent(file.number+1, 0).size())
			newHeader := 4 + binlogFormatEvent().size() + binlogPreviousEvent(state.source, transaction.Sequence-1).size()
			if size+uint64(newHeader)+rotateSize > uint64(state.maxFileBytes) {
				state.err = fmt.Errorf("CDC transaction %d exceeds MySQL binary log file limit", transaction.Sequence)
				return nil, state.err
			}
			if uint64(file.size)+size+rotateSize > uint64(state.maxFileBytes) {
				file.size += uint32(rotateSize)
				file.rotated = true
				state.files = append(state.files, binlogFile{number: file.number + 1, size: newHeader, previous: transaction.Sequence - 1})
				file = &state.files[len(state.files)-1]
			}
			state.entries = append(state.entries, binlogEntry{sequence: transaction.Sequence, file: file.number, start: file.size, end: file.size + uint32(size)})
			file.size += uint32(size)
		}
		if len(transactions) == 0 {
			break
		}
	}
	// Entries are append-only: published elements never change, so readers can
	// retain the immutable prefix without copying the complete retained history.
	return &binlogSnapshot{source: state.source, serverID: state.serverID, entries: state.entries, files: append([]binlogFile(nil), state.files...)}, nil
}
func (c *client) binlogControl(query string) (*db.ExecResult, bool, error) {
	tokens := sqlTokens(query, c.parseOptions())
	if len(tokens) == 0 {
		return nil, false, nil
	}
	upper := strings.ToUpper(tokens[0].Value)
	if upper == "SELECT" && len(tokens) == 3 && tokens[1].Value == "@" {
		name := strings.ToLower(tokens[2].Value)
		switch name {
		case "source_binlog_checksum", "master_binlog_checksum":
			value := "CRC32"
			if c.replicationChecksumNone {
				value = "NONE"
			}
			return scalar("@"+name, sql.NewTextValue(value)), true, nil
		case "source_heartbeat_period", "master_heartbeat_period":
			return scalar("@"+name, sql.NewUnsignedValue(uint64(c.replicationHeartbeat))), true, nil
		case "replica_uuid", "slave_uuid":
			return scalar("@"+name, sql.NewTextValue(c.replicaUUID)), true, nil
		}
	}
	if upper == "SET" {
		// These replication-session user variables configure this stream only. The
		// checksum remains CRC32, as declared by the format description event.
		if len(tokens) >= 5 && tokens[1].Value == "@" {
			name := strings.ToLower(tokens[2].Value)
			if name == "master_binlog_checksum" || name == "source_binlog_checksum" {
				value := strings.ToUpper(tokens[len(tokens)-1].Value)
				if strings.EqualFold(value, "binlog_checksum") && strings.Contains(query, "@@") {
					value = "CRC32"
				}
				if value != "CRC32" && value != "NONE" && value != "NULL" {
					return nil, true, fmt.Errorf("unsupported replication checksum %q", value)
				}
				// Presence of this variable signals checksum awareness. "NONE"
				// is used by mysqlbinlog before it reads the actual FDE algorithm;
				// it does not turn off checksums on source files.
				c.replicationChecksumNone = value == "NONE" || value == "NULL"
				return nil, true, nil
			}
			if name == "master_heartbeat_period" || name == "source_heartbeat_period" {
				n, err := strconv.ParseUint(tokens[len(tokens)-1].Value, 10, 63)
				if err != nil {
					return nil, true, fmt.Errorf("invalid heartbeat period")
				}
				c.replicationHeartbeat = time.Duration(n)
				return nil, true, nil
			}
			if name == "slave_uuid" || name == "replica_uuid" {
				c.replicaUUID = tokens[len(tokens)-1].Value
				return nil, true, nil
			}
		}
		return nil, false, nil
	}
	if upper != "SHOW" || len(tokens) < 2 {
		return nil, false, nil
	}
	second := strings.ToUpper(tokens[1].Value)
	status := (second == "MASTER" && len(tokens) == 3 && strings.EqualFold(tokens[2].Value, "STATUS")) || (second == "BINARY" && len(tokens) == 4 && strings.EqualFold(tokens[2].Value, "LOG") && strings.EqualFold(tokens[3].Value, "STATUS"))
	logs := (second == "BINARY" || second == "MASTER") && len(tokens) == 3 && strings.EqualFold(tokens[2].Value, "LOGS")
	if !status && !logs {
		return nil, false, nil
	}
	snapshot, err := c.server.binlogSnapshot(c.ctx)
	if err != nil {
		return nil, true, err
	}
	result := &db.ExecResult{}
	if logs {
		result.Columns = []sql.ColumnDef{{Name: "Log_name", ColType: sql.ColText}, {Name: "File_size", ColType: sql.ColUnsigned}, {Name: "Encrypted", ColType: sql.ColText}}
		for _, file := range snapshot.files {
			result.Rows = append(result.Rows, map[string]sql.Value{"Log_name": sql.NewTextValue(binlogFilename(file.number)), "File_size": sql.NewUnsignedValue(uint64(file.size)), "Encrypted": sql.NewTextValue("No")})
		}
	} else {
		file := snapshot.files[len(snapshot.files)-1]
		result.Columns = []sql.ColumnDef{{Name: "File", ColType: sql.ColText}, {Name: "Position", ColType: sql.ColUnsigned}, {Name: "Binlog_Do_DB", ColType: sql.ColText}, {Name: "Binlog_Ignore_DB", ColType: sql.ColText}, {Name: "Executed_Gtid_Set", ColType: sql.ColText}}
		gtid := ""
		if len(snapshot.entries) > 0 {
			source := snapshot.source
			gtid = source[:8] + "-" + source[8:12] + "-" + source[12:16] + "-" + source[16:20] + "-" + source[20:] + ":1-" + fmt.Sprint(snapshot.entries[len(snapshot.entries)-1].sequence)
		}
		result.Rows = []map[string]sql.Value{{"File": sql.NewTextValue(binlogFilename(file.number)), "Position": sql.NewUnsignedValue(uint64(file.size)), "Binlog_Do_DB": sql.NewTextValue(""), "Binlog_Ignore_DB": sql.NewTextValue(""), "Executed_Gtid_Set": sql.NewTextValue(gtid)}}
	}
	return result, true, nil
}
func binlogFailure(err error) error { return &protocolError{1236, "HY000", err.Error()} }
func (c *client) dumpBinlog(packet []byte) error {
	if len(packet) < 10 {
		return c.writeError(errMalformed)
	}
	position := binary.LittleEndian.Uint32(packet)
	flags := binary.LittleEndian.Uint16(packet[4:])
	name := string(packet[10:])
	if flags&^uint16(1) != 0 {
		return c.writeError(binlogFailure(fmt.Errorf("unsupported binlog dump flags")))
	}
	if position < 4 {
		position = 4
	}
	snapshot, err := c.server.binlogSnapshot(c.ctx)
	if err != nil {
		return c.writeError(binlogFailure(err))
	}
	file := uint32(1)
	if name != "" {
		file, err = parseBinlogFilename(name)
		if err != nil {
			return c.writeError(binlogFailure(err))
		}
	}
	if file > uint32(len(snapshot.files)) {
		return c.writeError(binlogFailure(fmt.Errorf("binary log file does not exist")))
	}
	if position > snapshot.files[file-1].size {
		return c.writeError(binlogFailure(fmt.Errorf("binary log position exceeds file length")))
	}
	// A replication connection starts with an artificial rotate and format event,
	// including when resuming in the middle of a file.
	if err = c.sendInitialBinlogRotate(file, position, snapshot.serverID); err != nil {
		return err
	}
	formatEnd := 4 + binlogFormatEvent().size()
	if position != 4 {
		if err = c.sendBinlogEvent(binlogFormatEvent(), snapshot.serverID, 0); err != nil {
			return err
		}
	}
	notifier, notifyErr := c.server.database.NewChangeReader(c.ctx, 0)
	if notifyErr != nil {
		return c.writeError(binlogFailure(notifyErr))
	}
	var streamReader *db.ChangeReader
	var changed <-chan struct{}
	refresh := true
	for {
		// Capture notification before refreshing; a publication between Read and
		// waiting must not leave a live replica asleep until another transaction.
		if refresh {
			changed = notifier.Changes()
			snapshot, err = c.server.binlogSnapshot(c.ctx)
			if err != nil {
				return c.writeError(binlogFailure(err))
			}
			refresh = false
		}
		if position == 4 {
			if err = c.sendBinlogEvent(binlogFormatEvent(), snapshot.serverID, formatEnd); err != nil {
				return err
			}
			position = formatEnd
		}
		if position == formatEnd {
			previous := binlogPreviousEvent(snapshot.source, snapshot.files[file-1].previous)
			if err = c.sendBinlogEvent(previous, snapshot.serverID, position+previous.size()); err != nil {
				return err
			}
			position += previous.size()
		}
		entries := snapshot.entries
		index := sort.Search(len(entries), func(i int) bool {
			return entries[i].file > file || (entries[i].file == file && entries[i].end > position)
		})
		if index < len(entries) && entries[index].file == file {
			entry := entries[index]
			if position < entry.start {
				return c.writeError(binlogFailure(fmt.Errorf("binary log position is not an event boundary")))
			}
			if position == entry.start && c.skipBinlogSequence(entry.sequence) {
				position = entry.end
				continue
			}
			if streamReader == nil || streamReader.Position() != entry.sequence-1 {
				streamReader, err = c.server.database.NewChangeReader(c.ctx, entry.sequence-1)
				if err != nil {
					return c.writeError(binlogFailure(err))
				}
			}
			transactions, err := streamReader.Read(c.ctx, 1)
			if err != nil {
				return c.writeError(binlogFailure(err))
			}
			if len(transactions) != 1 || transactions[0].Sequence != entry.sequence {
				return c.writeError(binlogFailure(fmt.Errorf("binary log transaction unavailable")))
			}
			events, err := binlogTransaction(snapshot.source, transactions[0])
			if err != nil {
				return c.writeError(binlogFailure(err))
			}
			eventPosition := entry.start
			valid := position == entry.start
			for _, event := range events {
				if eventPosition == position {
					valid = true
				}
				eventPosition += event.size()
			}
			if !valid {
				return c.writeError(binlogFailure(fmt.Errorf("binary log position is not an event boundary")))
			}
			eventPosition = entry.start
			for _, event := range events {
				next := eventPosition + event.size()
				if eventPosition == position {
					valid = true
				}
				if eventPosition >= position {
					if err = c.sendBinlogEvent(event, snapshot.serverID, next); err != nil {
						return err
					}
				}
				eventPosition = next
			}
			position = entry.end
			continue
		}
		current := snapshot.files[file-1]
		if current.rotated {
			rotate := binlogRotateEvent(file+1, 0)
			if position != current.size-rotate.size() {
				return c.writeError(binlogFailure(fmt.Errorf("binary log position is not an event boundary")))
			}
			if err = c.sendBinlogEvent(rotate, snapshot.serverID, current.size); err != nil {
				return err
			}
			file++
			position = 4
			continue
		}
		if position != current.size {
			return c.writeError(binlogFailure(fmt.Errorf("binary log position is not an event boundary")))
		}
		if flags&1 != 0 {
			return c.eof()
		}
		var heartbeat <-chan time.Time
		var timer *time.Timer
		if c.replicationHeartbeat > 0 {
			timer = time.NewTimer(c.replicationHeartbeat)
			heartbeat = timer.C
		}
		select {
		case <-changed:
		case <-heartbeat:
			if err = c.sendBinlogEvent(binlogEvent{kind: 27, data: []byte(binlogFilename(file))}, snapshot.serverID, position); err != nil {
				if timer != nil {
					timer.Stop()
				}
				return err
			}
		case <-c.ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return c.ctx.Err()
		}
		if timer != nil {
			timer.Stop()
		}
		refresh = true
	}
}
func (c *client) sendBinlogEvent(event binlogEvent, serverID, position uint32) error {
	payload := event.encode(serverID, position)
	return c.writer.write(append([]byte{0}, payload...))
}

func (c *client) sendInitialBinlogRotate(file, position, serverID uint32) error {
	event := binlogRotateEvent(file, 0)
	binary.LittleEndian.PutUint64(event.data, uint64(position))
	payload := event.encode(serverID, 0)
	binary.LittleEndian.PutUint16(payload[17:], 0x20) // LOG_EVENT_ARTIFICIAL_F
	if c.replicationChecksumNone {
		payload = payload[:len(payload)-4]
		binary.LittleEndian.PutUint32(payload[9:], uint32(len(payload)))
	} else {
		binary.LittleEndian.PutUint32(payload[len(payload)-4:], crc32.ChecksumIEEE(payload[:len(payload)-4]))
	}
	return c.writer.write(append([]byte{0}, payload...))
}
