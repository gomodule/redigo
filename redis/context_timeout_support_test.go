package redis_test

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func readBudgetCommand(r *bufio.Reader) ([]string, error) {
	header, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(header, "*") {
		return nil, fmt.Errorf("expected RESP array, got %q", header)
	}
	n, err := strconv.Atoi(strings.TrimSpace(header[1:]))
	if err != nil {
		return nil, err
	}
	args := make([]string, n)
	for i := range args {
		header, err = r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		size, err := strconv.Atoi(strings.TrimSpace(header[1:]))
		if err != nil {
			return nil, err
		}
		buf := make([]byte, size+2)
		if _, err = io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		args[i] = string(buf[:size])
	}
	return args, nil
}

func startBudgetServer(t *testing.T, handler func(net.Conn, []string)) (string, *budgetAtomicInt64) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var accepted budgetAtomicInt64
	var wg sync.WaitGroup
	var mu sync.Mutex
	connections := make(map[net.Conn]struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			mu.Lock()
			connections[conn] = struct{}{}
			mu.Unlock()
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
				r := bufio.NewReader(conn)
				for {
					args, err := readBudgetCommand(r)
					if err != nil {
						return
					}
					handler(conn, args)
				}
			}()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		mu.Lock()
		for conn := range connections {
			_ = conn.Close()
		}
		mu.Unlock()
		wg.Wait()
	})
	return listener.Addr().String(), &accepted
}

// Typed test atomics using operations available with the module's Go 1.17 minimum.
type budgetAtomicInt64 struct{ value int64 }

func (a *budgetAtomicInt64) Load() int64       { return atomic.LoadInt64(&a.value) }
func (a *budgetAtomicInt64) Add(n int64) int64 { return atomic.AddInt64(&a.value, n) }

type budgetAtomicBool struct{ value uint32 }

func (a *budgetAtomicBool) Load() bool { return atomic.LoadUint32(&a.value) != 0 }
func (a *budgetAtomicBool) Store(v bool) {
	var n uint32
	if v {
		n = 1
	}
	atomic.StoreUint32(&a.value, n)
}
