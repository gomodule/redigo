// Copyright 2014 Gary Burd
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package redis

// connectionState represents a connection state.
type connectionState uint8

const (
	stateWatch connectionState = 1 << iota
	stateMulti
	stateSubscribe
	stateMonitor
	stateClientReplyOff
	stateClientReplySkipNext
	stateClientReplySkip
)

// commandAction represents an action to be taken when processing a command.
type commandAction struct {
	Action func(*connectionState)

	// Next specifies sub actions for given arguments.
	Next map[string]*commandAction
}

// update updates the connection state based on the command and arguments if needed.
func (cs *connectionState) update(info map[string]*commandAction, first string, args ...interface{}) {
	*cs &^= stateClientReplySkip
	if *cs&stateClientReplySkipNext != 0 {
		*cs |= stateClientReplySkip
		*cs &^= stateClientReplySkipNext
	}

	ci, ok := info[first]
	if !ok {
		// No match.
		return
	}

	if ci.Action != nil {
		// Match with no more args.
		ci.Action(cs)
		return
	}

	if len(args) == 0 {
		// No match due to lack of args.
		return
	}

	first, ok = args[0].(string)
	if !ok {
		// No match due type miss-match.
		return
	}

	// Check the next argument.
	cs.update(ci.Next, first, args[1:]...)
}
