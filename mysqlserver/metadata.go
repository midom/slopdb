package mysqlserver

import (
	"slopdb/db"
	"slopdb/sql"
)

const (
	resultsetMetadataNone byte = iota
	resultsetMetadataFull
)

// Result metadata is optional only after both peers negotiate bit 25. The
// session defaults to FULL; ordinary drivers keep their historical wire shape.
func (c *client) sendResultMetadata() bool {
	return c.capabilities&clientOptionalMetadata == 0 || c.variables.resultsetMetadata != resultsetMetadataNone
}

func (c *client) resultMetadataFlag() byte {
	if c.sendResultMetadata() {
		return resultsetMetadataFull
	}
	return resultsetMetadataNone
}

func (c *client) writeResultHeader(columns []sql.ColumnDef) error {
	return c.writeResultHeaderStatus(columns, c.status())
}

// Streaming rows retain their session gate. Their caller supplies the status
// captured before opening rows, rather than reentering Session.InTransaction.
func (c *client) writeResultHeaderStatus(columns []sql.ColumnDef, status uint16) error {
	// MySQL's implementation emits count THEN flag, despite the opposite order
	// in some protocol reference tables: Protocol_classic::start_result_metadata.
	// https://github.com/mysql/mysql-server/blob/8.0/sql/protocol_classic.cc
	packet := appendLen(nil, uint64(len(columns)))
	if c.capabilities&clientOptionalMetadata != 0 {
		packet = append(packet, c.resultMetadataFlag())
	}
	if err := c.writer.write(packet); err != nil {
		return err
	}
	if c.sendResultMetadata() {
		for _, column := range columns {
			if err := c.writeColumnDefinition(column, c.selected); err != nil {
				return err
			}
		}
	}
	return c.resultMetadataEnd(status)
}

func (c *client) resultMetadataEnd(status uint16) error {
	if c.capabilities&clientDeprecateEOF != 0 {
		return nil
	}
	// MySQL THD::send_result_metadata calls end_result_metadata even for NONE.
	// Consequently a legacy metadata EOF remains, without any field packets.
	return c.writer.write(append16(append16([]byte{0xfe}, c.warningCount()), status))
}

func (c *client) resultTerminator(result *db.ExecResult) error {
	return c.resultTerminatorStatus(result, c.status())
}

func (c *client) resultTerminatorStatus(result *db.ExecResult, status uint16) error {
	if c.capabilities&clientDeprecateEOF == 0 {
		return c.writer.write(append16(append16([]byte{0xfe}, c.warningCount()), status))
	}
	// EOF replacement is an OK packet with header FE, zero affected rows and
	// zero insert ID. Preserve statement status, warnings and session tracking.
	position := ""
	if result != nil {
		position = result.CommitPosition
	}
	if position != "" && c.capabilities&clientSessionTrack != 0 {
		status |= 1 << 14
	}
	packet := append16([]byte{0xfe, 0, 0}, status)
	packet = append16(packet, c.warningCount())
	info := ""
	if position != "" {
		info = "slopdb_commit_position=" + position
	}
	if c.capabilities&clientSessionTrack != 0 {
		packet = appendString(packet, []byte(info))
		if position != "" {
			variables := appendString(nil, []byte("slopdb_commit_position"))
			variables = appendString(variables, []byte(position))
			state := appendString([]byte{0}, variables)
			packet = appendString(packet, state)
		}
	} else {
		packet = append(packet, info...)
	}
	return c.writer.write(packet)
}
