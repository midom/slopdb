package replication

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func randomID() string {
	var b [16]byte
	if _, e := rand.Read(b[:]); e != nil {
		panic(e)
	}
	return hex.EncodeToString(b[:])
}
func syncDir(path string) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	defer f.Close()
	return f.Sync()
}
func atomicFile(path string, data []byte) error {
	f, e := os.CreateTemp(filepath.Dir(path), ".publish-")
	if e != nil {
		return e
	}
	name := f.Name()
	defer os.Remove(name)
	if e = f.Chmod(0600); e == nil {
		_, e = f.Write(data)
	}
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e == nil {
		e = os.Rename(name, path)
	}
	if e == nil {
		e = syncDir(filepath.Dir(path))
	}
	return e
}
func generation(root string) (string, error) {
	b, e := os.ReadFile(filepath.Join(root, "CURRENT"))
	if e != nil {
		return "", e
	}
	name := strings.TrimSpace(string(b))
	if !strings.HasPrefix(name, "data-") || filepath.Base(name) != name {
		return "", fmt.Errorf("invalid data generation")
	}
	return filepath.Join(root, name), nil
}
func publishGeneration(root, dir string) error {
	return atomicFile(filepath.Join(root, "CURRENT"), []byte(filepath.Base(dir)+"\n"))
}
