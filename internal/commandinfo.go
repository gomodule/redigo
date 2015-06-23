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

package internal // import "github.com/garyburd/redigo/internal"

import (
	"strings"
)

const (
	WatchState = 1 << iota
	MultiState
	SubscribeState
	MonitorState
)

type CommandInfo struct {
	Set, Clear int
}

var (
	watch      = CommandInfo{Set: WatchState}
	unwatch    = CommandInfo{Clear: WatchState}
	multi      = CommandInfo{Set: MultiState}
	exec       = CommandInfo{Clear: WatchState | MultiState}
	discard    = CommandInfo{Clear: WatchState | MultiState}
	psubscribe = CommandInfo{Set: SubscribeState}
	subscribe  = CommandInfo{Set: SubscribeState}
	monitor    = CommandInfo{Set: MonitorState}
)

var commandInfos = map[string]CommandInfo{
	"WATCH":      watch,
	"UNWATCH":    unwatch,
	"MULTI":      multi,
	"EXEC":       exec,
	"DISCARD":    discard,
	"PSUBSCRIBE": psubscribe,
	"SUBSCRIBE":  subscribe,
	"MONITOR":    monitor,
}

func LookupCommandInfo(commandName string) CommandInfo {

	// optimize for correctly cased strings
	switch commandName {
	case "MULTI", "multi":
		return multi
	case "EXEC", "exec":
		return exec
	case "WATCH", "watch":
		return watch
	case "UNWATCH", "unwatch":
		return unwatch
	case "PSUBSCRIBE", "psubcribe":
		return psubscribe
	case "SUBSCRIBE", "subscribe":
		return subscribe
	case "MONITOR", "monitor":
		return monitor
	default:
		// if it's mixed case or unknown, fallback to our map which is
		// slower.
		return commandInfos[strings.ToUpper(commandName)]
	}
}
