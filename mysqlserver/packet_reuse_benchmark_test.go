package mysqlserver

import (
	"net"
	"testing"
)

type packetByteCounter struct {
	net.Conn
	bytes uint64
}

func (c *packetByteCounter) Write(p []byte) (int, error) {
	c.bytes += uint64(len(p))
	return len(p), nil
}

// Isolate framing allocations from transport and payload allocation. The
// functional packet tests separately verify contents, sequences, continuation
// frames, compression, partial writes and response-buffer ownership.
func BenchmarkPacketWriterReuse(b *testing.B) {
	for _, batched := range []bool{false, true} {
		name := "immediate"
		if batched {
			name = "batched"
		}
		b.Run(name, func(b *testing.B) {
			conn := &packetByteCounter{}
			writer := packetWriter{conn: conn}
			var payload [240]byte
			if batched {
				writer.beginBatch()
			}
			if err := writer.write(payload[:]); err != nil {
				b.Fatal(err)
			}
			if err := writer.flush(); err != nil {
				b.Fatal(err)
			}
			conn.bytes = 0
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := writer.write(payload[:]); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if err := writer.flush(); err != nil {
				b.Fatal(err)
			}
			if want := uint64(b.N) * uint64(len(payload)+4); conn.bytes != want {
				b.Fatalf("sent %d bytes, want %d", conn.bytes, want)
			}
		})
	}
}
