package redis

import (
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"
)

type SentinelClient struct {
	SentinelAddrs  []string
	net            string
	Conn           Conn
	ElectSentinel  func([]string) int
	connectTimeout time.Duration
	readTimeout    time.Duration
	writeTimeout   time.Duration
	remainingAddrs []string
}

// Returns a SentinelClient with an active connection to at least
// one currently active master.
//
// ElectSentinel expects a function that dictates the behavior for dialing
// (see ElectSentinel, below). If nil, the sentinel will use a simple
// fallback scheme, trying each address until one works.
//
// As per the redis-sentinel client guidelines, a timeout is mandatory
// while connecting to sentinels, and should not be set to 0.
//
// Note that in a worst-case scenario, the timeout for performing an
// operation with a SentinelClient wrapper method may take (# sentinels) *
// timeout to test and connect to the various configured addresses.
func NewSentinelClient(net string, addrs []string, electSentinel func([]string) int,
	connectTimeout, readTimeout, writeTimeout time.Duration) (*SentinelClient, error) {
	if electSentinel == nil {
		electSentinel = fallbackElectSentinel
	}
	sc := &SentinelClient{
		net:            net,
		SentinelAddrs:  addrs,
		ElectSentinel:  electSentinel,
		connectTimeout: connectTimeout,
		readTimeout:    readTimeout,
		writeTimeout:   writeTimeout,
		remainingAddrs: make([]string, 0),
	}

	err := sc.Dial()

	return sc, err
}

// The default ElectSentinel implementation, just elects the first sentinel
// in the list every time.
func fallbackElectSentinel(addrs []string) int {
	return 0
}

// For convenience, a purely random ElectSentinel implementation.
func RandomElectSentinel(addrs []string) int {
	return int(rand.Int31n(int32(len(addrs))))
}

// NoSentinelsLeft is returned when all sentinels in the list are exhausted
// (or none configured), and contains the last error returned by Dial (which
// may be nil)
type NoSentinelsLeft struct {
	lastError error
}

func (ns NoSentinelsLeft) Error() string {
	if ns.lastError != nil {
		return fmt.Sprintf("redigo: no configured sentinels successfully connected; last error: %s", ns.lastError.Error())
	} else {
		return fmt.Sprintf("redigo: no configured sentinels successfully connected.")
	}
}

// Dial connects to the sentinel, NOT the members of a monitored instance
// group. You can lookup the configuration of any named instance group after
// dial or use one of the convenience methods to obtain a connection to the
// redis instances.
func (sc *SentinelClient) Dial() error {
	if sc.SentinelAddrs == nil {
		return errors.New("No configured sentinel addresses")
	}

	err, leftovers := sc.dial(sc.SentinelAddrs)
	sc.remainingAddrs = leftovers
	return err
}

func (sc *SentinelClient) dial(addrs []string) (error, []string) {
	var lastErr error

	for subSentList := addrs; len(subSentList) > 0; {
		i := sc.ElectSentinel(subSentList)
		addr := subSentList[i]
		subSentList = append(subSentList[:i], subSentList[i+1:]...)
		conn, err := DialTimeout(sc.net, addr, sc.connectTimeout, sc.readTimeout,
			sc.writeTimeout)
		if err == nil {
			sc.Conn = conn
			return nil, subSentList
		}
		lastErr = err
	}

	return NoSentinelsLeft{lastError: lastErr}, []string{}
}

// Do is the entry point to the recursive do(), and performs the requested
// command on the currently connected sentinel, trying all other configured
// sentinels until the list is exhausted.
func (sc *SentinelClient) Do(cmd string, args ...interface{}) (interface{}, error) {
	return sc.do(sc.SentinelAddrs, cmd, args...)
}

// A wrapped version of the underlying conn.Do that attempts to restablish
// the sentinel connection on failure, and tries until all sentinels have
// been tried. Note that the most likely scenario is that dial() will exhaust
// sentinels until a working one is found.
func (sc *SentinelClient) do(addrs []string, cmd string, args ...interface{}) (interface{}, error) {
	res, err := sc.Conn.Do(cmd, args...)
	if err != nil && res == nil { // indicates connection error of some sort
		sc.Conn.Close()
		err, leftovers := sc.dial(addrs)
		if err != nil {
			return nil, err
		} else {
			return sc.do(leftovers, cmd, args...)
		}
	} else {
		return res, err
	}
}

// QueryConfForMaster looks up the configuration for a named monitored instance
// set and returns the master's configuration.
func (sc *SentinelClient) QueryConfForMaster(name string) (string, error) {
	res, err := Strings(sc.Do("SENTINEL", "get-master-addr-by-name", name))
	masterAddr := strings.Join(res, ":")
	return masterAddr, err
}

// QueryConfForSlaves looks up the configuration for a named monitored
// instance set and returns all the slave configuration. Note that the return is
// a []map[string]string, and will most likely need to be interpreted by
// SlaveAddr to be usable.
func (sc *SentinelClient) QueryConfForSlaves(name string) ([]map[string]string, error) {
	res, err := Values(sc.Do("SENTINEL", "slaves", name))
	if err != nil {
		return nil, err
	}
	slaves := make([]map[string]string, 0)
	for _, a := range res {
		sm, err := StringMap(a, err)
		if err != nil {
			return slaves, err
		}
		slaves = append(slaves, sm)
	}
	return slaves, err
}

// A convenience function which formats only the relevant parts of a single
// slave map[string]string into an address ip:port pair.
func SlaveAddr(slaveMap map[string]string) string {
	return fmt.Sprintf("%s:%s", slaveMap["ip"], slaveMap["port"])
}

// A convenience function that turns the comma-separated list of string flags
// from a slave's map[string]string into a map[string]bool for easy testing.
func SlaveReadFlags(slaveMap map[string]string) map[string]bool {
	keys := strings.Split(slaveMap["flags"], ",")
	res := make(map[string]bool)
	for _, k := range keys {
		res[k] = true
	}
	return res
}

// DialMaster returns a connection to the master of the named monitored
// instance set.
//
// Assumes the same network will be used to contact the master as the one used
// for contacting the sentinels.
//
// DialMaster returns immediately on failure.
func (sc *SentinelClient) DialMaster(name string) (Conn, error) {
	masterAddr, err := sc.QueryConfForMaster(name)

	if err != nil {
		return nil, err
	}

	return Dial(sc.net, masterAddr)
}

var NoSlavesRemaining error = errors.New("No connected slaves with active master-link available")

// DialSlave returns a connection to a slave. This routine mandates that the
// slave have an active link to the master, and not be currently flagged as
// disconnected. Then a slave is randomly selected from the list.
//
// On failure, this method tries again through all slaves that meet the
// aforementioned criteria until success or all available options are
// exhausted, at which point NoSlavesRemaining is returned.
//
// To change this behavior, implement a dialer using QueryConfForSlaves.
func (sc *SentinelClient) DialSlave(name string) (Conn, error) {
	slaves, err := sc.QueryConfForSlaves(name)
	if err != nil {
		return nil, err
	}
	for len(slaves) > 0 {
		index := rand.Int31n(int32(len(slaves)))
		flags := SlaveReadFlags(slaves[index])
		if slaves[index]["master-link-status"] == "ok" && !(flags["disconnected"] || flags["sdown"]) {
			conn, err := Dial(sc.net, SlaveAddr(slaves[index]))
			if err == nil {
				return conn, err
			}
		}
		slaves = append(slaves[:index], slaves[index+1:]...)
	}
	return nil, NoSlavesRemaining
}

// Exposes the Close of the underlying Conn
func (sc *SentinelClient) Close() error {
	return sc.Conn.Close()
}

// Exposes the Err of the underlying Conn
func (sc *SentinelClient) Err() error {
	return sc.Conn.Err()
}

// Recursive Send, like Do, performs a Send() on the underlying Conn, trying
// all configured sentinels if the current connection fails.
func (sc *SentinelClient) Send(commandName string, args ...interface{}) error {
	return sc.send(sc.SentinelAddrs, commandName, args...)
}

func (sc *SentinelClient) send(addrs []string, commandName string, args ...interface{}) error {
	err := sc.Conn.Send(commandName, args...)
	if err != nil {
		sc.Conn.Close()
		err, leftovers := sc.dial(addrs)
		if err != nil {
			return err
		} else {
			return sc.send(leftovers, commandName, args...)
		}
	}
	return nil
}

// Flushes the underlying connection.
//
// Users should be aware that the underlying connection can change through
// subsequent calls to Send or Do, or through intermediate calls to Dial,
// if fallback behavior is required due to a broken connection.
func (sc *SentinelClient) Flush() error {
	return sc.Conn.Flush()
}

// Receives a single reply from the underlying redis connection.
//
// If the underlying connection is broken, the method does not attempt to
// reestablish the connection to another sentinel, as the desired response
// is no longer accessible.
//
// Most likely, use of Do() is more appropriate than Send + Receive, since
// Do will guarantee that the sending and receiving steps occur on the
// same connection.
func (sc *SentinelClient) Receive() (reply interface{}, err error) {
	return sc.Conn.Receive()
}

// GetRole is a convenience function supplied to query an instance (master or
// slave) for its role. It attempts to use the ROLE command introduced in
// redis 2.8.12. Failing this, the INFO replication command is used instead.
//
// If it is known that the cluster is older than 2.8.12, GetReplicationRole
// should be used instead, as this bypasses an extraneous ROLE command.
//
// It is recommended by the redis client guidelines to test the role of any
// newly established connection before use. Additionally, if sentinels in use
// are older than 2.8.12, they will not force clients to reconnect on role
// change; use of long-lived connections in an environment like this (for
// example, when using the pool) should involve periodic role testing to
// protect against unexpected changes. See the SentinelAwarePool example for
// details.
func GetRole(c Conn) (string, error) {
	res, err := c.Do("ROLE")

	rres, ok := res.([]interface{})
	if err == nil && ok {
		return String(rres[0], nil)
	}
	return GetReplicationRole(c)
}

// Queries the role of a connected redis instance by checking the output of the
// INFO replication section. GetRole should be used if the redis instance
// is sufficiently new to support it.
func GetReplicationRole(c Conn) (string, error) {
	res, err := String(c.Do("INFO", "replication"))
	if err == nil {
		sres := strings.Split(res, "\r\n")
		for _, s := range sres {
			si := strings.Split(s, ":")
			if si[0] == "role" {
				return si[1], nil
			}
		}
	}
	return "", err
}

// TestRole wraps GetRole in a test to verify if the role matches an expected
// role string. If there was any error in querying the supplied connection,
// the function returns false.
func TestRole(c Conn, expectedRole string) bool {
	role, err := GetRole(c)
	if err != nil || role != expectedRole {
		return false
	}
	return true
}
