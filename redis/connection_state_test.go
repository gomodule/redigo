package redis

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type lookupTest struct {
	state       connectionState
	expected    connectionState
	commandName string
	args        []interface{}
	info        map[string]*commandAction
}

func buildLookupTests(tests map[string]lookupTest, root, infos map[string]*commandAction, args ...string) {
	for cmd, ci := range infos {
		if cmd != strings.ToLower(cmd) {
			// Skip non lower case commands as we only need to test one case per action.
			continue
		}

		cmdArgs := append(args, cmd)
		if ci.Next != nil {
			buildLookupTests(tests, root, ci.Next, cmdArgs...)
			continue
		}

		t := lookupTest{
			commandName: cmdArgs[0],
			args:        make([]interface{}, len(cmdArgs)-1),
			info:        root,
		}
		for i, arg := range cmdArgs[1:] {
			t.args[i] = arg
		}
		var state connectionState
		ci.Action(&state)
		if state != 0 {
			// Action is a Set.
			t.expected = state
		} else {
			// Action is a Clear.
			state = ^connectionState(0)
			ci.Action(&state)
			t.state = ^state
		}

		tests[strings.Join(cmdArgs, "-")] = t
	}
}

func TestConnectionStateUpdate(t *testing.T) {
	tests := make(map[string]lookupTest)
	buildLookupTests(tests, activeConnActions, activeConnActions)
	buildLookupTests(tests, connActions, connActions)

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			tc.state.update(tc.info, tc.commandName, tc.args...)
			require.Equal(t, tc.expected, tc.state)
		})
	}
}

func benchmarkStateUpdate(b *testing.B, names ...string) {
	var state connectionState
	for i := 0; i < b.N; i++ {
		for _, cmd := range names {
			state.update(activeConnActions, cmd)
		}
	}
}

func BenchmarkConnectionStateUpdateCorrectCase(b *testing.B) {
	benchmarkStateUpdate(b, "watch", "WATCH", "monitor", "MONITOR")
}

func BenchmarkConnectionStateUpdateMixedCase(b *testing.B) {
	benchmarkStateUpdate(b, "wAtch", "WeTCH", "monItor", "MONiTOR")
}

func BenchmarkConnectionStateUpdateNoMatch(b *testing.B) {
	benchmarkStateUpdate(b, "GET", "SET", "HMGET", "HMSET")
}

func TestClientReply(t *testing.T) {
	p := &Pool{
		MaxIdle:   1,
		MaxActive: 2,
		Dial: func() (Conn, error) {
			return DialDefaultServer()
		},
	}
	defer p.Close()

	closeCheck := func(t *testing.T, c Conn) {
		t.Helper()

		done := make(chan error)
		go func() {
			done <- c.Close()
		}()

		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for connection close")
		}
	}

	var defaultState connectionState
	tests := map[string]func(t *testing.T){
		"off": func(t *testing.T) {
			c := p.Get()
			defer closeCheck(t, c)

			err := c.Send("CLIENT", "REPLY", "OFF")
			require.NoError(t, err)
			require.Equal(t, stateClientReplyOff, c.(*activeConn).state)

			err = c.Send("ECHO", "first")
			require.NoError(t, err)
			require.Equal(t, stateClientReplyOff, c.(*activeConn).state)

			_, err = c.Do("CLIENT", "REPLY", "ON")
			require.NoError(t, err)
			require.Equal(t, defaultState, c.(*activeConn).state)

			reply, err := String(c.Do("ECHO", "second"))
			require.NoError(t, err)
			require.Equal(t, "second", reply)
		},
		"off-skip": func(t *testing.T) {
			c := p.Get()
			defer closeCheck(t, c)

			err := c.Send("CLIENT", "REPLY", "OFF")
			require.NoError(t, err)
			require.Equal(t, stateClientReplyOff, c.(*activeConn).state)

			err = c.Send("ECHO", "first")
			require.NoError(t, err)
			require.Equal(t, stateClientReplyOff, c.(*activeConn).state)

			// Skip should be ignored as we are already in a off state.
			err = c.Send("CLIENT", "REPLY", "SKIP")
			require.NoError(t, err)
			require.Equal(t, stateClientReplyOff, c.(*activeConn).state)

			_, err = c.Do("CLIENT", "REPLY", "ON")
			require.NoError(t, err)
			require.Equal(t, defaultState, c.(*activeConn).state)

			reply, err := String(c.Do("ECHO", "second"))
			require.NoError(t, err)
			require.Equal(t, "second", reply)
		},
		"skip": func(t *testing.T) {
			c := p.Get()
			defer closeCheck(t, c)

			err := c.Send("CLIENT", "REPLY", "SKIP")
			require.NoError(t, err)
			require.Equal(t, stateClientReplySkipNext, c.(*activeConn).state)

			err = c.Send("ECHO", "first")
			require.NoError(t, err)
			require.Equal(t, stateClientReplySkip, c.(*activeConn).state)

			err = c.Send("ECHO", "second")
			require.NoError(t, err)
			require.Equal(t, defaultState, c.(*activeConn).state)

			err = c.Flush()
			require.NoError(t, err)

			reply, err := String(c.Receive())
			require.NoError(t, err)
			require.Equal(t, "second", reply)

			reply, err = String(c.Do("ECHO", "third"))
			require.NoError(t, err)
			require.Equal(t, "third", reply)
		},
	}

	for name, tc := range tests {
		t.Run(name, tc)
	}
}
