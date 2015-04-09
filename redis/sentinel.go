package redis

import (
  "errors"
  "fmt"
  "math/rand"
  "time"
  "strings"
)

type SentinelClient struct {
  SentinelAddrs []string
  net string
  Conn Conn
  ElectSentinel func([]string) (int)
  connectTimeout time.Duration
  readTimeout time.Duration
  writeTimeout time.Duration
  remainingAddrs []string
}

// Returns a SentinelClient with an active connection to at least 
// one currently active master.
// ElectSentinel expects a function that dictates the behavior for dialing
// (see ElectSentinel, below). If nil, the sentinel will use a simple
// fallback scheme, trying each address until one works.
// As per the redis-sentinel client guidelines, a timeout is mandatory
// while connecting to sentinels.
// Note that in a worst-case scenario, the timeout for performing an
// operation with a SentinelClient wrapper method may take (# sentinels) *
// timeout to test and connect to the various configured addresses.
func NewSentinelClient(net string, addrs []string, electSentinel func([]string) int,
  connectTimeout, readTimeout, writeTimeout time.Duration) (*SentinelClient, error) {
  if electSentinel == nil {
    electSentinel = fallbackElectSentinel
  }
  sc := &SentinelClient{
    net : net,
    SentinelAddrs : addrs,
    ElectSentinel : electSentinel,
    connectTimeout : connectTimeout,
    readTimeout : readTimeout,
    writeTimeout : writeTimeout,
    remainingAddrs : make([]string, 0),
  }

  err := sc.Dial()

  return sc, err
}

// The default ElectSentinel implementation, just elects the first sentinel
// in the list every time.
func fallbackElectSentinel(addrs []string) int {
  return 0
}

// For convenience, supply a purely random sentinel elector
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
  return fmt.Sprintf("redigo: no configured sentinels successfully connected; last error: %s", ns.lastError.Error())
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
    conn, err := DialTimeout(sc.net, addr, sc.connectTimeout, sc.readTimeout, sc.writeTimeout)
    if err == nil {
      sc.Conn = conn
      return nil, subSentList
    }
  }

  return NoSentinelsLeft{lastError : lastErr}, []string{}
}

// Do is the entry point to the recursive do(), and performs the requested 
// command on the currently connected sentinel, trying all other configured
// sentinels until the list is exhausted. 
func (sc *SentinelClient) Do(cmd string, args ...interface{}) (interface{}, error) {
  return sc.do(sc.SentinelAddrs, cmd, args...)
}

// A wrapped version of the underlying conn.Do that attempts to restablish
// the sentinel connection on failure, and tries until all sentinels have
// been tried.
func (sc *SentinelClient) do(addrs []string, cmd string, args ...interface{}) (interface{}, error) {
  res, err := sc.Conn.Do(cmd, args...)
  if err != nil && res == nil { // indicates connection error of some sort
    sc.Conn.Close()
    err, leftovers = sc.dial(addrs)
    if err != nil {
      return nil, err
    } else {
      return sc.do(leftovers, cmd, args...)
    }
  } else {
    return res, err
  }
}

// QueryConfForMaster looks up the configuration for a named monitored instance set
// and returns the master's configuration.
func (sc *SentinelClient) QueryConfForMaster(name string) (string, error) {
  res, err := Strings(sc.Do("SENTINEL", "get-master-addr-by-name", name))
  masterAddr := strings.Join(res, ":")
  return masterAddr, err
}

// QueryConfForSlaves looks up the configuration for a named monitored instance set
// and returns all the slaves.
func (sc *SentinelClient) QueryConfForSlaves(name string) ([]map[string]string, error) {
  res, err := Values(sc.Do("SENTINEL", "slaves", name))
  if err != nil {
    return nil, err
  }
  slaves := make([]map[string]string, 0)
  for _,a := range(res) {
    sm, err := StringMap(a, err)
    if err != nil {
      return slaves, err
    }
    slaves = append(slaves, sm)
  }
  return slaves, err
}

// A convenience function which formats only the relevant parts of a single slave
// map[string]string into an address ip:port pair.
func SlaveAddr(slaveMap map[string]string) string {
  return fmt.Sprintf("%s:%s", slaveMap["ip"], slaveMap["port"])
}

// A convenience function that turns the comma-separated list of string flags
// into a map[string]bool for easy testing.
func SlaveReadFlags(slaveMap map[string]string) map[string]bool {
  keys := strings.Split(slaveMap["flags"], ",")
  res := make(map[string]bool)
  for _,k := range(keys) {
    res[k] = true
  }
  return res
}

// DialMaster returns a connection to the master of the named monitored 
// instance set. 
// Assumes the same network will be used to contact the master as the one used 
// for contacting the sentinels.
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
// disconnected.
// Then a slave is randomly selected from the list.
// On failure, this method tries again through all slaves that meet the 
// aforementioned criteria until success or all available options are 
// exhausted, at which point NoSlavesRemaining is returned.
// To change this behavior, implement a dialer using QueryConfForSlaves.
func (sc *SentinelClient) DialSlave(name string) (Conn, error) {
  slaves, err := sc.QueryConfForSlaves(name)
  if err != nil {
    return nil, err
  }
  for len(slaves) > 0 {
    index := rand.Int31n(int32(len(slaves)))
    flags := SlaveReadFlags(slaves[index])
    if slaves[index]["master-link-status"] == "ok" && !(flags["disconnected"] || flags["sdown"]){
      conn, err := Dial(sc.net, SlaveAddr(slaves[index]))
      if err == nil {
        return conn, err
      }
    }
    slaves = append(slaves[:index], slaves[index+1:]...)
  }
  return nil, NoSlavesRemaining
}

// GetRole is a convenience function supplied to query an instance (master or 
// slave) for its role. It attempts to use the ROLE command introduced in 
// redis 2.8.12.
// Failing this, the INFO replication command is used instead.
// If it is known that the cluster is older than 2.8.12, GetReplicationRole 
// should be used instead, as this bypasses an extraneous ROLE command.
// It is recommended by the redis client guidelines to test the role of any
// newly established connection before use. Additionally, if sentinels in use 
// are older than 2.8.12, they will not force clients to reconnect on role 
// change; use of long-lived connections in an environment like this (for 
// example, when using the pool) should involve periodic role testing to 
// protect against unexpected changes.
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
    for _,s := range(sres) {
      si := strings.Split(s,":")
      if si[0] == "role" {
        return si[1], nil
      }
    }
  }
  return "", err
}

// TestRole wraps GetRole in a test to verify if the role matches an expected 
// role string. If there was any error in querying the supplied connection, o
// the function returns false.
func TestRole(c Conn, expectedRole string) bool {
  role, err := GetRole(c)
  if err != nil || role != expectedRole {
    return false
  }
  return true
}

