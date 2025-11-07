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

import (
	"context"
	"errors"
	"time"
)

// Error represents an error returned in a command reply.
type Error string

func (err Error) Error() string { return string(err) }

// Conn represents a connection to a Redis server.
type Conn interface {
	// Close closes the connection.
	Close() error

	// Err returns a non-nil value when the connection is not usable.
	Err() error

	// Do sends a command to the server and returns the received reply.
	// This function will use the timeout which was set when the connection is created
	Do(commandName string, args ...interface{}) (reply interface{}, err error)

	// Send writes the command to the client's output buffer.
	Send(commandName string, args ...interface{}) error

	// Flush flushes the output buffer to the Redis server.
	Flush() error

	// Receive receives a single reply from the Redis server
	Receive() (reply interface{}, err error)
}

// Argument is the interface implemented by an object which wants to control how
// the object is converted to Redis bulk strings.
type Argument interface {
	// RedisArg returns a value to be encoded as a bulk string per the
	// conversions listed in the section 'Executing Commands'.
	// Implementations should typically return a []byte or string.
	RedisArg() interface{}
}

// Scanner is implemented by an object which wants to control its value is
// interpreted when read from Redis.
type Scanner interface {
	// RedisScan assigns a value from a Redis value. The argument src is one of
	// the reply types listed in the section `Executing Commands`.
	//
	// An error should be returned if the value cannot be stored without
	// loss of information.
	RedisScan(src interface{}) error
}

// ConnWithTimeout is an optional interface that allows the caller to override
// a connection's default read timeout. This interface is useful for executing
// the BLPOP, BRPOP, BRPOPLPUSH, XREAD and other commands that block at the
// server.
//
// A connection's default read timeout is set with the DialReadTimeout dial
// option. Applications should rely on the default timeout for commands that do
// not block at the server.
//
// All of the Conn implementations in this package satisfy the ConnWithTimeout
// interface.
//
// Use the DoWithTimeout and ReceiveWithTimeout helper functions to simplify
// use of this interface.
type ConnWithTimeout interface {
	Conn

	// DoWithTimeout sends a command to the server and returns the received reply.
	// The timeout overrides the readtimeout set when dialing the connection.
	DoWithTimeout(timeout time.Duration, commandName string, args ...interface{}) (reply interface{}, err error)

	// ReceiveWithTimeout receives a single reply from the Redis server.
	// The timeout overrides the readtimeout set when dialing the connection.
	ReceiveWithTimeout(timeout time.Duration) (reply interface{}, err error)
}

// ConnWithContext is an optional interface that allows the caller to control the command's life with context.
type ConnWithContext interface {
	Conn

	// DoContext sends a command to server and returns the received reply.
	// min(ctx,DialReadTimeout()) will be used as the deadline.
	// The connection will be closed if DialReadTimeout() timeout or ctx timeout or ctx canceled when this function is running.
	// DialReadTimeout() timeout return err can be checked by errors.Is(err, os.ErrDeadlineExceeded).
	// ctx timeout return err context.DeadlineExceeded.
	// ctx canceled return err context.Canceled.
	DoContext(ctx context.Context, commandName string, args ...interface{}) (reply interface{}, err error)

	// ReceiveContext receives a single reply from the Redis server.
	// min(ctx,DialReadTimeout()) will be used as the deadline.
	// The connection will be closed if DialReadTimeout() timeout or ctx timeout or ctx canceled when this function is running.
	// DialReadTimeout() timeout return err can be checked by errors.Is(err, os.ErrDeadlineExceeded).
	// ctx timeout return err context.DeadlineExceeded.
	// ctx canceled return err context.Canceled.
	ReceiveContext(ctx context.Context) (reply interface{}, err error)
}

var errTimeoutNotSupported = errors.New("redis: connection does not support ConnWithTimeout")
var errContextNotSupported = errors.New("redis: connection does not support ConnWithContext")

// DoContext sends a command to server and returns the received reply.
// min(ctx,DialReadTimeout()) will be used as the deadline.
// The connection will be closed if DialReadTimeout() timeout or ctx timeout or ctx canceled when this function is running.
// DialReadTimeout() timeout return err can be checked by errors.Is(err, os.ErrDeadlineExceeded).
// ctx timeout return err context.DeadlineExceeded.
// ctx canceled return err context.Canceled.
func DoContext(c Conn, ctx context.Context, cmd string, args ...interface{}) (interface{}, error) {
	cwt, ok := c.(ConnWithContext)
	if !ok {
		return nil, errContextNotSupported
	}
	return cwt.DoContext(ctx, cmd, args...)
}

// DoWithTimeout executes a Redis command with the specified read timeout. If
// the connection does not satisfy the ConnWithTimeout interface, then an error
// is returned.
func DoWithTimeout(c Conn, timeout time.Duration, cmd string, args ...interface{}) (interface{}, error) {
	cwt, ok := c.(ConnWithTimeout)
	if !ok {
		return nil, errTimeoutNotSupported
	}
	return cwt.DoWithTimeout(timeout, cmd, args...)
}

// ReceiveContext receives a single reply from the Redis server.
// min(ctx,DialReadTimeout()) will be used as the deadline.
// The connection will be closed if DialReadTimeout() timeout or ctx timeout or ctx canceled when this function is running.
// DialReadTimeout() timeout return err can be checked by strings.Contains(e.Error(), "io/timeout").
// ctx timeout return err context.DeadlineExceeded.
// ctx canceled return err context.Canceled.
func ReceiveContext(c Conn, ctx context.Context) (interface{}, error) {
	cwt, ok := c.(ConnWithContext)
	if !ok {
		return nil, errContextNotSupported
	}
	return cwt.ReceiveContext(ctx)
}

// ReceiveWithTimeout receives a reply with the specified read timeout. If the
// connection does not satisfy the ConnWithTimeout interface, then an error is
// returned.
func ReceiveWithTimeout(c Conn, timeout time.Duration) (interface{}, error) {
	cwt, ok := c.(ConnWithTimeout)
	if !ok {
		return nil, errTimeoutNotSupported
	}
	return cwt.ReceiveWithTimeout(timeout)
}

// SlowLog represents a redis SlowLog
type SlowLog struct {
	// ID is a unique progressive identifier for every slow log entry.
	ID int64

	// Time is the unix timestamp at which the logged command was processed.
	Time time.Time

	// ExecutationTime is the amount of time needed for the command execution.
	ExecutionTime time.Duration

	// Args is the command name and arguments
	Args []string

	// ClientAddr is the client IP address (4.0 only).
	ClientAddr string

	// ClientName is the name set via the CLIENT SETNAME command (4.0 only).
	ClientName string
}

// Latency represents a redis LATENCY LATEST.
type Latency struct {
	// Name of the latest latency spike event.
	Name string

	// Time of the latest latency spike for the event.
	Time time.Time

	// Latest is the latest recorded latency for the named event.
	Latest time.Duration

	// Max is the maximum latency for the named event.
	Max time.Duration
}

// LatencyHistory represents a redis LATENCY HISTORY.
type LatencyHistory struct {
	// Time is the unix timestamp at which the event was processed.
	Time time.Time

	// ExecutationTime is the amount of time needed for the command execution.
	ExecutionTime time.Duration
}

// ClientFlags is redis-server client flags, copy from redis/src/server.h (redis 8.2)
type ClientFlags uint64

const (
	ClientSlave                 ClientFlags = 1 << iota /* This client is a replica */
	ClientMaster                                        /* This client is a master */
	ClientMonitor                                       /* This client is a slave monitor, see MONITOR */
	ClientMulti                                         /* This client is in a MULTI context */
	ClientBlocked                                       /* The client is waiting in a blocking operation */
	ClientDirtyCAS                                      /* Watched keys modified. EXEC will fail. */
	ClientCloseAfterReply                               /* Close after writing entire reply. */
	ClientUnBlocked                                     /* This client was unblocked and is stored in server.unblocked_clients */
	ClientScript                                        /* This is a non connected client used by Lua */
	ClientAsking                                        /* Client issued the ASKING command */
	ClientCloseASAP                                     /* Close this client ASAP */
	ClientUnixSocket                                    /* Client connected via Unix domain socket */
	ClientDirtyExec                                     /* EXEC will fail for errors while queueing */
	ClientMasterForceReply                              /* Queue replies even if is master */
	ClientForceAOF                                      /* Force AOF propagation of current cmd. */
	ClientForceRepl                                     /* Force replication of current cmd. */
	ClientPrePSync                                      /* Instance don't understand PSYNC. */
	ClientReadOnly                                      /* Cluster client is in read-only state. */
	ClientPubSub                                        /* Client is in Pub/Sub mode. */
	ClientPreventAOFProp                                /* Don't propagate to AOF. */
	ClientPreventReplProp                               /* Don't propagate to slaves. */
	ClientPreventProp           ClientFlags = ClientPreventAOFProp | ClientPreventReplProp
	ClientPendingWrite                      /* Client has output to send but a write handler is yet not installed. */
	ClientReplyOff                          /* Don't send replies to client. */
	ClientReplySkipNext                     /* Set CLIENT_REPLY_SKIP for next cmd */
	ClientReplySkip                         /* Don't send just this reply. */
	ClientLuaDebug                          /* Run EVAL in debug mode. */
	ClientLuaDebugSync                      /* EVAL debugging without fork() */
	ClientModule                            /* Non connected client used by some module. */
	ClientProtected                         /* Client should not be freed for now. */
	ClientExecutingCommand                  /* Indicates that the client is currently in the process of handling a command. usually this will be marked only during call() however, blocked clients might have this flag kept until they will try to reprocess the command. */
	ClientPendingCommand                    /* Indicates the client has a fully parsed command ready for execution. */
	ClientTracking                          /* Client enabled keys tracking in order to perform client side caching. */
	ClientTrackingBrokenRedir               /* Target client is invalid. */
	ClientTrackingBCAST                     /* Tracking in BCAST mode. */
	ClientTrackingOptIn                     /* Tracking in opt-in mode. */
	ClientTrackingOptOut                    /* Tracking in opt-out mode. */
	ClientTrackingCaching                   /* CACHING yes/no was given, depending on optin/optout mode. */
	ClientTrackingNoLoop                    /* Don't send invalidation messages about writes performed by myself.*/
	ClientInToTable                         /* This client is in the timeout table. */
	ClientProtocolError                     /* Protocol error chatting with it. */
	ClientCloseAfterCommand                 /* Close after executing commands and writing entire reply. */
	ClientDenyBlocking                      /* Indicate that the client should not be blocked. currently, turned on inside MULTI, Lua, RM_Call, and AOF client */
	ClientReplRDBOnly                       /* This client is a replica that only wants RDB without replication buffer. */
	ClientNoEvict                           /* This client is protected against client memory eviction. */
	ClientAllowOOM                          /* Client used by RM_Call is allowed to fully execute scripts even when in OOM */
	ClientNoTouch                           /* This client will not touch LFU/LRU stats. */
	ClientPushing                           /* This client is pushing notifications. */
	ClientModuleAuthHasResult               /* Indicates a client in the middle of module based auth had been authenticated from the Module. */
	ClientModulePreventAOFProp              /* Module client do not want to propagate to AOF */
	ClientModulePreventReplProp             /* Module client do not want to propagate to replica */
	ClientReExecutingCommand                /* The client is re-executing the command. */
	ClientReplRDBChannel                    /* Client which is used for RDB delivery as part of RDB channel replication */
	ClientInternal                          /* Internal client connection */
)

// Client represents a redis-server client.
type Client struct {
	ID                 int64         // redis version 2.8.12, a unique 64-bit client ID
	Addr               string        // address/port of the client
	LAddr              string        // address/port of local address client connected to (bind address)
	FD                 int64         // file descriptor corresponding to the socket
	Name               string        // the name set by the client with CLIENT SETNAME
	Age                time.Duration // total duration of the connection in seconds
	Idle               time.Duration // idle time of the connection in seconds
	Flags              ClientFlags   // client flags (see below)
	DB                 int           // current database ID
	Sub                int           // number of channel subscriptions
	PSub               int           // number of pattern matching subscriptions
	SSub               int           // number of shard channel subscriptions. Added in Redis 7.0.3
	Multi              int           // number of commands in a MULTI/EXEC context
	Watch              int           // number of keys this client is currently watching. Added in Redis 7.4
	QueryBuf           int           // qbuf, query buffer length (0 means no query pending)
	QueryBufFree       int           // qbuf-free, free space of the query buffer (0 means the buffer is full)
	ArgvMem            int           // incomplete arguments for the next command (already extracted from query buffer)
	MultiMem           int           // memory is used up by buffered multi commands. Added in Redis 7.0
	BufferSize         int           // rbs, current size of the client's read buffer in bytes. Added in Redis 7.0
	BufferPeak         int           // rbp, peak size of the client's read buffer since the client connected. Added in Redis 7.0
	OutputBufferLength int           // obl, output buffer length
	OutputListLength   int           // oll, output list length (replies are queued in this list when the buffer is full)
	OutputMemory       int           // omem, output buffer memory usage
	TotalMemory        int           // tot-mem, total memory consumed by this client in its various buffers
	Events             string        // file descriptor events (see below)
	LastCmd            string        // cmd, last command played
	User               string        // the authenticated username of the client
	Redir              int64         // client id of current client tracking redirection
	Resp               int           // client RESP protocol version. Added in Redis 7.0
	LibName            string        // client library name. Added in Redis 7.2
	LibVer             string        // client library version. Added in Redis 7.2
	IoThread           int           // io-thread, id of I/O thread assigned to the client. Added in Redis 8.0
	TotalNetIn         int           // tot-net-in, total network input bytes read from this client.
	TotalNetOut        int           // tot-net-out, total network output bytes sent to this client.
	TotalCmds          int           // tot-cmds, total count of commands this client executed.
}
