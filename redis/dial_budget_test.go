package redis_test

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	redigo "github.com/gomodule/redigo/redis"
)

func TestDialBudgetInitializationCancellation(t *testing.T) {
	for _, stage := range []string{"AUTH", "CLIENT", "SELECT"} {
		t.Run(stage, func(t *testing.T) {
			seen := make(chan struct{})
			var commands budgetAtomicInt64
			address, _ := startBudgetServer(t, func(c net.Conn, args []string) {
				commands.Add(1)
				if args[0] == stage {
					close(seen)
					return
				}
				_, _ = io.WriteString(c, "+OK\r\n")
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				c, err := redigo.DialContext(ctx, "tcp", address,
					redigo.DialPassword("test-only"), redigo.DialClientName("budget-test"), redigo.DialDatabase(2),
					redigo.DialReadTimeout(time.Second), redigo.DialWriteTimeout(time.Second))
				if c != nil {
					c.Close()
				}
				done <- err
			}()
			select {
			case <-seen:
			case <-time.After(time.Second):
				t.Fatal("initialization stage not reached")
			}
			before := commands.Load()
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("initialization ignored cancellation")
			}
			if commands.Load() != before {
				t.Fatal("later initialization command sent after cancellation")
			}
		})
	}
}

func TestDialBudgetSharedByInitializationCommands(t *testing.T) {
	address, _ := startBudgetServer(t, func(c net.Conn, _ []string) {
		time.Sleep(35 * time.Millisecond)
		_, _ = io.WriteString(c, "+OK\r\n")
	})
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	c, err := redigo.DialContext(ctx, "tcp", address,
		redigo.DialPassword("test-only"), redigo.DialClientName("budget-test"), redigo.DialDatabase(2),
		redigo.DialReadTimeout(time.Second), redigo.DialWriteTimeout(time.Second))
	if c != nil {
		c.Close()
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("initialization renewed total budget: %v", err)
	}
}

func TestDialBudgetCanceledBeforeAndAfterTCP(t *testing.T) {
	for _, before := range []bool{true, false} {
		t.Run(map[bool]string{true: "before", false: "after"}[before], func(t *testing.T) {
			client, server := net.Pipe()
			defer client.Close()
			defer server.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if before {
				cancel()
			}
			calls := 0
			c, err := redigo.DialContext(ctx, "tcp", "localhost:6379",
				redigo.DialContextFunc(func(context.Context, string, string) (net.Conn, error) {
					calls++
					cancel()
					return client, nil
				}), redigo.DialPassword("test-only"))
			if c != nil {
				c.Close()
			}
			if !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if before && calls != 0 {
				t.Fatal("dialed a canceled request")
			}
			if !before {
				server.SetReadDeadline(time.Now().Add(time.Second))
				var b [1]byte
				if _, err := server.Read(b[:]); !errors.Is(err, io.EOF) {
					t.Fatalf("socket not closed or AUTH sent: %v", err)
				}
			}
		})
	}
}

func TestDialBudgetTLSCancellation(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		c, err := redigo.DialContext(ctx, "tcp", "localhost:6379", redigo.DialUseTLS(true),
			redigo.DialContextFunc(func(context.Context, string, string) (net.Conn, error) { return client, nil }))
		if c != nil {
			c.Close()
		}
		done <- err
	}()
	server.SetReadDeadline(time.Now().Add(time.Second))
	var b [1]byte
	if _, err := server.Read(b[:]); err != nil {
		t.Fatal("ClientHello not started", err)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("TLS ignored cancellation")
	}
}

func TestDialBudgetStopsWatchingBeforeHandoff(t *testing.T) {
	address, _ := startBudgetServer(t, func(c net.Conn, _ []string) { _, _ = io.WriteString(c, "+OK\r\n") })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := redigo.DialContext(ctx, "tcp", address, redigo.DialPassword("test-only"), redigo.DialReadTimeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	cancel()
	if _, err := c.Do("PING"); err != nil {
		t.Fatal("completed initialization canceled later operation", err)
	}
}
