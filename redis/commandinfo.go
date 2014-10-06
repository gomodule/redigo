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

import (
	"strings"
)

const (
	watchState = 1 << iota
	multiState
	subscribeState
	monitorState
)

type commandInfo struct {
	set, clear int
}

var commandInfos = map[string]commandInfo{
	"WATCH":      {set: watchState},
	"UNWATCH":    {clear: watchState},
	"MULTI":      {set: multiState},
	"EXEC":       {clear: watchState | multiState},
	"DISCARD":    {clear: watchState | multiState},
	"PSUBSCRIBE": {set: subscribeState},
	"SUBSCRIBE":  {set: subscribeState},
	"MONITOR":    {set: monitorState},
}

func lookupCommandInfo(commandName string) commandInfo {
	return commandInfos[strings.ToUpper(commandName)]
}
