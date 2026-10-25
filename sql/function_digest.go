package sql

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
)

// Digest computation is shared; dialect descriptors own accepted input types
// and whether the output is raw bytes or hexadecimal text.
func functionDigest(name string, data []byte) []byte {
	switch name {
	case "MD5":
		v := md5.Sum(data)
		return v[:]
	case "SHA", "SHA1":
		v := sha1.Sum(data)
		return v[:]
	case "SHA224":
		v := sha256.Sum224(data)
		return v[:]
	case "SHA256":
		v := sha256.Sum256(data)
		return v[:]
	case "SHA384":
		v := sha512.Sum384(data)
		return v[:]
	case "SHA512":
		v := sha512.Sum512(data)
		return v[:]
	}
	return nil
}
