package mysqlserver

import (
	"crypto/rand"
	"crypto/sha1"
	"crypto/subtle"
	"crypto/tls"
	"fmt"
	"time"
)

const (
	clientLongPassword     uint32 = 1
	clientLongFlag         uint32 = 1 << 2
	clientConnectDB        uint32 = 1 << 3
	clientProtocol41       uint32 = 1 << 9
	clientSSL              uint32 = 1 << 11
	clientTransactions     uint32 = 1 << 13
	clientSecureConnection uint32 = 1 << 15
	clientPluginAuth       uint32 = 1 << 19
	clientConnectAttrs     uint32 = 1 << 20
	clientPluginAuthLenenc uint32 = 1 << 21
	clientSessionTrack     uint32 = 1 << 23
	capabilities                  = clientLongPassword | clientLongFlag | clientConnectDB | clientProtocol41 | clientTransactions | clientSecureConnection | clientPluginAuth | clientConnectAttrs | clientPluginAuthLenenc | clientSessionTrack
	authPlugin                    = "mysql_native_password"
	serverVersion                 = "5.7.0-SlopDB"
)

func (c *client) handshake() error {
	if err := c.conn.SetDeadline(time.Now().Add(c.server.config.HandshakeTimeout)); err != nil {
		return err
	}
	defer c.conn.SetDeadline(time.Time{})
	var salt [20]byte
	if _, err := rand.Read(salt[:]); err != nil {
		return err
	}
	// Native clients treat scramble bytes as a C string.
	for i, b := range salt {
		if b == 0 {
			salt[i] = 1
		}
	}
	serverCapabilities := uint32(capabilities)
	if c.server.config.TLSConfig != nil {
		serverCapabilities |= clientSSL
	}
	p := append([]byte{10}, serverVersion...)
	p = append(p, 0)
	p = append32(p, c.id)
	p = append(p, salt[:8]...)
	p = append(p, 0)
	p = append16(p, uint16(serverCapabilities&0xffff))
	p = append(p, 45)
	p = append16(p, 2)
	p = append16(p, uint16(serverCapabilities>>16))
	p = append(p, 21)
	p = append(p, make([]byte, 10)...)
	p = append(p, salt[8:]...)
	p = append(p, 0)
	p = append(p, authPlugin...)
	p = append(p, 0)
	if err := c.writer.write(p); err != nil {
		return err
	}
	response, next, err := readPacket(c.conn, c.writer.sequence, c.server.config.MaxPacketBytes)
	if err != nil {
		return err
	}
	c.writer.sequence = next
	d := decoder{data: response}
	flags := d.u32()
	d.u32()
	charset := d.u8()
	d.take(23)
	if d.err != nil {
		return c.fail(1043, "08S01", "malformed handshake")
	}
	if flags&clientProtocol41 == 0 {
		return c.fail(1251, "08004", "MySQL 4.1 authentication required")
	}
	if flags&clientSSL != 0 {
		if c.server.config.TLSConfig == nil {
			return c.fail(1251, "08004", "TLS is not configured")
		}
		if len(response) != 32 {
			return c.fail(1043, "08S01", "malformed SSLRequest")
		}
		secure := tls.Server(c.conn, c.server.config.TLSConfig)
		if err := secure.HandshakeContext(c.ctx); err != nil {
			return err
		}
		c.conn, c.writer.conn = secure, secure
		// TLS records do not consume MySQL packet sequence numbers. Continue
		// from SSLRequest (normally 1) with the encrypted response (normally 2).
		response, next, err = readPacket(c.conn, next, c.server.config.MaxPacketBytes)
		if err != nil {
			return err
		}
		c.writer.sequence = next
		d = decoder{data: response}
		flags = d.u32()
		d.u32()
		charset = d.u8()
		d.take(23)
		if d.err != nil || flags&clientProtocol41 == 0 || flags&clientSSL == 0 {
			return c.fail(1043, "08S01", "malformed encrypted handshake")
		}
	} else if c.server.config.RequireTLS {
		return c.fail(3159, "HY000", "TLS is required for this MySQL server")
	}
	// The engine stores UTF-8 text. Accept UTF-8 and binary client encodings.
	switch charset {
	case 33, 45, 46, 63, 83, 192, 224, 255:
	default:
		return c.fail(1115, "42000", "unsupported client character set; use utf8mb4")
	}
	username := d.nul()
	var auth []byte
	switch {
	case flags&clientPluginAuthLenenc != 0:
		auth = d.str()
	case flags&clientSecureConnection != 0:
		n := d.u8()
		auth = d.take(int(n))
	default:
		auth = []byte(d.nul())
	}
	database := ""
	if flags&clientConnectDB != 0 {
		database = d.nul()
	}
	plugin := authPlugin
	if flags&clientPluginAuth != 0 {
		plugin = d.nul()
	}
	if flags&clientConnectAttrs != 0 {
		d.str()
	}
	if d.err != nil || len(d.data) != 0 {
		return c.fail(1043, "08S01", "malformed handshake")
	}
	if plugin != authPlugin {
		p := append([]byte{0xfe}, authPlugin...)
		p = append(p, 0)
		p = append(p, salt[:]...)
		p = append(p, 0)
		if err := c.writer.write(p); err != nil {
			return err
		}
		auth, next, err = readPacket(c.conn, c.writer.sequence, c.server.config.MaxPacketBytes)
		if err != nil {
			return err
		}
		c.writer.sequence = next
	}
	if username != c.server.config.Username || !checkPassword(c.server.config.Password, salt[:], auth) {
		return c.fail(1045, "28000", "access denied")
	}
	if database != "" && database != c.server.config.Database {
		return c.fail(1049, "42000", fmt.Sprintf("unknown database %q", database))
	}
	c.selected = database
	c.capabilities = flags & serverCapabilities
	session, err := c.server.database.NewSession()
	if err != nil {
		return c.fail(1040, "08004", err.Error())
	}
	c.session = session
	return c.ok(nil)
}
func checkPassword(password string, salt, response []byte) bool {
	if password == "" {
		return len(response) == 0
	}
	if len(response) != sha1.Size {
		return false
	}
	first := sha1.Sum([]byte(password))
	second := sha1.Sum(first[:])
	h := sha1.New()
	_, _ = h.Write(salt)
	_, _ = h.Write(second[:])
	scramble := h.Sum(nil)
	for i := range scramble {
		scramble[i] ^= first[i]
	}
	return subtle.ConstantTimeCompare(scramble, response) == 1
}

// fail sends a handshake error and also terminates the connection.
func (c *client) fail(code uint16, state, message string) error {
	if err := c.writeError(&protocolError{code, state, message}); err != nil {
		return err
	}
	return fmt.Errorf("MySQL handshake: %s", message)
}
