//go:build !linux && !darwin && !freebsd

package segment

import "os"

// openMmap on platforms without mmap support falls back to a plain
// ReadFile. The Closer is a no-op.
func openMmap(path string) ([]byte, func() error, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	return data, func() error { return nil }, nil
}
