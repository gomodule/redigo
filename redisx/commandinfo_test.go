package redisx

import "testing"

func TestLookupCommandInfo(t *testing.T) {
	for _, n := range []string{"watch", "WATCH", "wAtch"} {
		if lookupCommandInfo(n) == (commandInfo{}) {
			t.Errorf("LookupCommandInfo(%q) = CommandInfo{}, expected non-zero value", n)
		}
	}
}
