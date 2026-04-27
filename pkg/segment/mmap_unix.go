//go:build linux || darwin || freebsd

package segment

import (
	"fmt"
	"os"
	"syscall"
)

// openMmap maps the file at path read-only and returns the mapped bytes
// alongside a closer that unmaps the region and closes the file.
//
// The returned bytes must not be used after the closer runs. Reading from
// an unmapped region triggers SIGBUS. be care
func openMmap(path string) ([]byte, func() error, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("segment: open %q: %w", path, err)
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, nil, fmt.Errorf("segment: stat %q: %w", path, err)
	}
	size := fi.Size()
	if size == 0 {
		_ = f.Close()
		return nil, nil, fmt.Errorf("segment: %q is empty", path)
	}
	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		_ = f.Close()
		return nil, nil, fmt.Errorf("segment: mmap %q: %w", path, err)
	}
	closer := func() error {
		errUnmap := syscall.Munmap(data)
		errClose := f.Close()
		if errUnmap != nil {
			return errUnmap
		}
		return errClose
	}
	return data, closer, nil
}
