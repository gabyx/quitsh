package server

import (
	"context"
	"net"
	"os"
	"path"
	"strings"
	"time"

	"github.com/sdsc-ordes/quitsh/pkg/errors"
	"github.com/sdsc-ordes/quitsh/pkg/log"
)

const (
	socketDirPerms  = 0o750
	socketFilePerms = 0o600
	probeTimeout    = 300 * time.Millisecond
)

// splitAddress splits a gRPC target into network and address.
func splitAddress(address string) (network string, addr string, err error) {
	switch {
	case strings.HasPrefix(address, "unix://"):
		return "unix", strings.TrimPrefix(address, "unix://"), nil
	case strings.HasPrefix(address, "tcp://"):
		return "tcp", strings.TrimPrefix(address, "tcp://"), nil
	default:
		return "", "", errors.New(
			"watcher address '%v' must start with 'unix://' or 'tcp://'", address)
	}
}

// Listen creates the listener for `address`, cleaning up a stale unix socket.
// It fails when another server is already listening.
func Listen(address string) (net.Listener, error) {
	return ListenContext(context.Background(), address)
}

// ListenContext is [Listen] bound to `ctx`.
func ListenContext(ctx context.Context, address string) (net.Listener, error) {
	var config net.ListenConfig

	network, addr, err := splitAddress(address)
	if err != nil {
		return nil, err
	}

	if network == "tcp" {
		l, e := config.Listen(ctx, network, addr)

		return l, errors.AddContext(e, "could not listen on '%v'", address)
	}

	if e := os.MkdirAll(path.Dir(addr), socketDirPerms); e != nil {
		return nil, errors.AddContext(e, "could not create socket dir for '%v'", addr)
	}

	if _, e := os.Stat(addr); e == nil {
		if isAlive(ctx, network, addr) {
			return nil, errors.New(
				"a watcher server is already running on '%v'", address)
		}

		log.Debug("Removing stale socket.", "socket", addr)

		if re := os.Remove(addr); re != nil {
			return nil, errors.AddContext(re, "could not remove stale socket '%v'", addr)
		}
	}

	l, err := config.Listen(ctx, network, addr)
	if err != nil {
		return nil, errors.AddContext(err, "could not listen on '%v'", address)
	}

	if e := os.Chmod(addr, socketFilePerms); e != nil {
		_ = l.Close()

		return nil, errors.AddContext(e, "could not set permissions on '%v'", addr)
	}

	return l, nil
}

// isAlive reports whether something is listening on the unix socket `addr`.
// A refused connection means the socket file is stale.
func isAlive(ctx context.Context, network string, addr string) bool {
	dialer := net.Dialer{Timeout: probeTimeout}

	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()

	conn, err := dialer.DialContext(ctx, network, addr)
	if err != nil {
		return false
	}

	log.WarnE(conn.Close(), "Could not close probe connection.")

	return true
}
