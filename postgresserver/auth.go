package postgresserver

import (
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

const (
	protocolVersion uint32 = 196608
	sslRequest      uint32 = 80877103
	gssRequest      uint32 = 80877104
	cancelRequest   uint32 = 80877102
)

var errCancelConnection = errors.New("cancel request completed")

func (c *client) startup() error {
	if err := c.conn.SetDeadline(time.Now().Add(c.server.config.HandshakeTimeout)); err != nil {
		return err
	}
	defer c.conn.SetDeadline(time.Time{})
	secure, seenSSL, seenGSS := false, false, false
	var parameters map[string]string
	for {
		body, err := readBody(c.conn, c.server.config.MaxPacketBytes)
		if err != nil {
			return err
		}
		d := decoder{data: body}
		code := d.u32()
		if d.err != nil {
			return c.fatal("08P01", "malformed startup packet")
		}
		switch code {
		case sslRequest:
			if seenSSL || secure || len(body) != 4 {
				return c.fatal("08P01", "malformed SSLRequest")
			}
			seenSSL = true
			if c.server.config.TLSConfig == nil {
				if err := writeAll(c.conn, []byte{'N'}); err != nil {
					return err
				}
				continue
			}
			if err := writeAll(c.conn, []byte{'S'}); err != nil {
				return err
			}
			connection := tls.Server(c.conn, c.server.config.TLSConfig)
			if err := connection.HandshakeContext(c.ctx); err != nil {
				return err
			}
			c.conn = connection
			secure = true
			continue
		case gssRequest:
			if seenGSS || secure || seenSSL || len(body) != 4 {
				return c.fatal("08P01", "malformed GSSENCRequest")
			}
			seenGSS = true
			if err := writeAll(c.conn, []byte{'N'}); err != nil {
				return err
			}
			continue
		case cancelRequest:
			if len(body) != 12 {
				return errMalformed
			}
			if c.server.config.RequireTLS && !secure {
				return c.fatal("28000", "TLS is required for cancellation requests")
			}
			id, key := d.u32(), d.u32()
			c.server.cancelRequest(id, key)
			return errCancelConnection
		case protocolVersion:
			if c.server.config.RequireTLS && !secure {
				return c.fatal("28000", "TLS is required for this PostgreSQL server")
			}
			parameters = make(map[string]string)
			for {
				key := d.nul()
				if d.err != nil {
					return c.fatal("08P01", "malformed startup parameters")
				}
				if key == "" {
					break
				}
				value := d.nul()
				if _, exists := parameters[key]; exists {
					return c.fatal("08P01", "duplicate startup parameter")
				}
				parameters[key] = value
			}
			if d.end() != nil {
				return c.fatal("08P01", "malformed startup parameters")
			}
		default:
			return c.fatal("0A000", "only PostgreSQL protocol 3.0 is supported")
		}
		break
	}
	if parameters["user"] != c.server.config.Username {
		return c.fatal("28P01", "authentication failed")
	}
	database := parameters["database"]
	if database == "" {
		database = parameters["user"]
	}
	if database != c.server.config.Database {
		return c.fatal("3D000", "unknown database")
	}
	for key, value := range parameters {
		switch key {
		case "user", "database":
		case "application_name":
			c.applicationName = value
		case "datestyle", "DateStyle":
			if value != "ISO, MDY" && value != "ISO,MDY" {
				return c.fatal("22023", "only ISO, MDY date style is supported")
			}
		case "extra_float_digits":
			if value != "2" && value != "3" {
				return c.fatal("22023", "unsupported float output precision")
			}
		case "TimeZone", "timezone":
			if value != "UTC" {
				return c.fatal("22023", "only UTC timezone is supported")
			}
		case "client_encoding":
			if !strings.EqualFold(value, "UTF8") && !strings.EqualFold(value, "UTF-8") {
				return c.fatal("22023", "only UTF8 client encoding is supported")
			}
		default:
			return c.fatal("0A000", "unsupported startup parameter: "+key)
		}
	}
	if c.server.scram != nil {
		if err := c.send(&pgproto3.AuthenticationSASL{AuthMechanisms: []string{"SCRAM-SHA-256"}}); err != nil {
			return err
		}
		kind, body, err := c.authMessage()
		if err != nil {
			return err
		}
		if kind != 'p' {
			return c.fatal("08P01", "expected SASL initial response")
		}
		d := decoder{data: body}
		mechanism := d.nul()
		n := int32(d.u32())
		initial := d.take(int(n))
		if mechanism != "SCRAM-SHA-256" || d.end() != nil {
			return c.fatal("08P01", "invalid SASL initial response")
		}
		conversation := c.server.scram.NewConversation()
		challenge, err := conversation.Step(string(initial))
		if err != nil || conversation.AuthzID() != "" {
			return c.fatal("28P01", "authentication failed")
		}
		if err := c.send(&pgproto3.AuthenticationSASLContinue{Data: []byte(challenge)}); err != nil {
			return err
		}
		kind, body, err = c.authMessage()
		if err != nil {
			return err
		}
		if kind != 'p' {
			return c.fatal("08P01", "expected SASL response")
		}
		final, err := conversation.Step(string(body))
		if err != nil || !conversation.Valid() {
			return c.fatal("28P01", "authentication failed")
		}
		if err := c.send(&pgproto3.AuthenticationSASLFinal{Data: []byte(final)}); err != nil {
			return err
		}
	}
	var secret [4]byte
	if _, err := rand.Read(secret[:]); err != nil {
		return err
	}
	c.secret = binary.BigEndian.Uint32(secret[:])
	return nil
}
func (c *client) authMessage() (byte, []byte, error) {
	var kind [1]byte
	if _, err := io.ReadFull(c.conn, kind[:]); err != nil {
		return 0, nil, err
	}
	body, err := readBody(c.conn, c.server.config.MaxPacketBytes)
	return kind[0], body, err
}
func (c *client) welcome() error {
	if err := c.send(&pgproto3.AuthenticationOk{}); err != nil {
		return err
	}
	for _, p := range [][2]string{{"server_version", "15.0"}, {"server_encoding", "UTF8"}, {"client_encoding", "UTF8"}, {"DateStyle", "ISO, MDY"}, {"TimeZone", "UTC"}, {"integer_datetimes", "on"}, {"standard_conforming_strings", "on"}, {"application_name", c.applicationName}, {"session_authorization", c.server.config.Username}} {
		if err := c.send(&pgproto3.ParameterStatus{Name: p[0], Value: p[1]}); err != nil {
			return err
		}
	}
	if err := c.send(&pgproto3.BackendKeyData{ProcessID: c.id, SecretKey: c.secret}); err != nil {
		return err
	}
	return c.ready()
}
func (c *client) fatal(code, message string) error {
	if err := c.send(&pgproto3.ErrorResponse{Severity: "FATAL", SeverityUnlocalized: "FATAL", Code: code, Message: message}); err != nil {
		return err
	}
	return errors.New(message)
}
