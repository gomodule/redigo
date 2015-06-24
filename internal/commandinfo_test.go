package internal

import "testing"

func TestLookupCommandInfo(t *testing.T) {

	for _, c := range []string{"watch", "WATCH", "wAtch"} {
		w := LookupCommandInfo(c)
		if w.Set != WatchState {
			t.Errorf("watch state not equal!")
		}
		if w.Clear != 0 {
			t.Errorf("watch state not equal!")
		}

	}

}

func BenchmarkLookupCommandInfoCorrectCase(b *testing.B) {

	cases := []string{
		"watch", "WATCH", "monitor", "MONITOR"}
	// run the Fib function b.N times
	for n := 0; n < b.N; n++ {
		for _, c := range cases {
			LookupCommandInfo(c)
		}
	}
}

func BenchmarkLookupCommandInfoMixedCase(b *testing.B) {

	cases := []string{
		"wAtch", "mOnitor"}
	// run the Fib function b.N times
	for n := 0; n < b.N; n++ {
		for _, c := range cases {
			LookupCommandInfo(c)
		}
	}
}
