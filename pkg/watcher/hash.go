package watcher

import (
	"encoding/binary"
	"hash/fnv"
	"io"
	"os"

	"github.com/sdsc-ordes/quitsh/pkg/errors"
	"github.com/sdsc-ordes/quitsh/pkg/log"
)

type (
	// ScanID identifies one scan generation of the repository.
	ScanID int64

	// Digest is the content digest over an input set.
	Digest uint64

	// HashMode selects how file stamps are computed.
	HashMode string

	// Stamp identifies the state of a single file.
	Stamp struct {
		ModTimeNs int64  `json:"modTimeNs"`
		Size      int64  `json:"size"`
		Sum       uint64 `json:"sum"` // Only set in `HashModeChecksum`.
	}

	// Hasher computes a [Stamp] for a file.
	// `prev` is the stamp from the previous scan (if `prevOk`), which
	// implementations may reuse to avoid reading file content.
	Hasher interface {
		Stamp(
			absPath string,
			modTimeNs int64,
			size int64,
			prev *Stamp) (Stamp, error)
	}
)

const (
	// HashModeMTimeSize stamps files by modification time and size (fast, default).
	HashModeMTimeSize HashMode = "mtime-size"

	// HashModeChecksum stamps files by a checksum over their content.
	HashModeChecksum HashMode = "checksum"
)

// NewHasher returns the hasher for the given mode.
func NewHasher(mode HashMode) (Hasher, error) {
	switch mode {
	case HashModeMTimeSize:
		return mtimeSizeHasher{}, nil
	case HashModeChecksum:
		return checksumHasher{}, nil
	default:
		return nil, errors.New(
			"unknown hash mode '%v' (use '%v' or '%v')",
			mode, HashModeMTimeSize, HashModeChecksum)
	}
}

type mtimeSizeHasher struct{}

func (mtimeSizeHasher) Stamp(
	absPath string, modTimeNs int64, size int64, prev *Stamp,
) (Stamp, error) {
	if prev != nil && (prev.ModTimeNs != modTimeNs || prev.Size != size) {
		log.Debugf("File mod time or size changed '%v'", absPath)
	}

	return Stamp{ModTimeNs: modTimeNs, Size: size}, nil
}

type checksumHasher struct{}

func (checksumHasher) Stamp(
	absPath string, modTimeNs int64, size int64, prev *Stamp,
) (Stamp, error) {
	// Content cannot have changed if neither mtime nor size moved.
	if prev != nil && prev.ModTimeNs == modTimeNs && prev.Size == size {
		return *prev, nil
	} else {
		log.Debugf("File mod time or size changed '%v'", absPath)
	}

	f, err := os.Open(absPath)
	if err != nil {
		return Stamp{}, errors.AddContext(err, "could not open '%v' for hashing", absPath)
	}
	defer f.Close()

	h := fnv.New64a()
	if _, err = io.Copy(h, f); err != nil {
		return Stamp{}, errors.AddContext(err, "could not hash '%v'", absPath)
	}

	return Stamp{ModTimeNs: modTimeNs, Size: size, Sum: h.Sum64()}, nil
}

// DigestOf computes the digest over `sortedPaths` and their stamps.
// The paths must be sorted for the digest to be stable.
func DigestOf(sortedPaths []string, stamps map[string]Stamp) Digest {
	h := fnv.New64a()

	var buf [8]byte

	write := func(v uint64) {
		binary.LittleEndian.PutUint64(buf[:], v)
		_, _ = h.Write(buf[:])
	}

	for _, p := range sortedPaths {
		s := stamps[p]
		_, _ = h.Write([]byte(p))
		if s.Sum != 0 {
			write(s.Sum)
		} else {
			write(uint64(s.ModTimeNs)) //nolint:gosec // wrap-around is fine for hashing.
			write(uint64(s.Size))      //nolint:gosec // wrap-around is fine for hashing.
		}
	}

	return Digest(h.Sum64())
}
