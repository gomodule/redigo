// Copyright 2012 Gary Burd
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

// Returned when we are unable to connect to Redis for any reason.
type ConnectionError struct {
	message string
}

func (err *ConnectionError) Error() string {
	return err.message
}

// Returned when Redis returns an invalid reply.
type ProtocolError struct {
	message string
}

func (err *ProtocolError) Error() string {
	return err.message
}

// Returned when the pool is used in an invalid way (connections are exhausted,
// the pool is closed, etc.)
type ConnectionPoolError struct {
	message string
}

func (err *ConnectionPoolError) Error() string {
	return err.message
}

// Returned when there is an error scanning a multi-bulk reply.
type ScanStructError struct {
	message string
}

func (err *ScanStructError) Error() string {
	return err.message
}

// Returned when expecting a non-nil value but getting nil, anyways.
type NilError struct {
	message string
}

func (err *NilError) Error() string {
	return err.message
}
