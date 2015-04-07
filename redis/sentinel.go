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

// Dial connects to the sentinel, NOT the members of a monitored instance group.
// You can lookup the configuration of any named instance group after dial or use
// one of the convenience methods to obtain a connection to the redis instances.
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
  var leftovers []string
  if err := sc.Conn.Err(); err != nil {
    err, leftovers = sc.dial(addrs)
    if err != nil {
      return nil, err
    }
  }
  res, err := sc.Conn.Do(cmd, args...)
  if err != nil && res == nil { // indicates connection error of some sort
    sc.Conn.Close()
    return sc.do(leftovers, cmd, args...)
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

type SlaveDef struct {
  Name  string
  IP    string
  Port  uint16
  Runid string
  Flags string
  PendingCommands int
  LastPingSent  int
  LastOkPingReply int
  LastPingReply int
  DownAfterMilliseconds int
  InfoRefresh int
  RoleReported string
  RoleReportedTime int
  MasterLinkDownTime int
  MasterLinkStatus string
  MasterHost string
  MasterPort uint16
  SlavePriority int
  SlaveReplOffset int
}

// QueryConfForSlaves looks up the configuration for a named monitored instance set
// and returns all the slaves.
func (sc *SentinelClient) QueryConfForSlaves(name string) ([]SlaveDef, error) {
  res, err := sc.Do("SENTINEL", "slaves", name)
  if err != nil {
    return nil, err
  }
  var slaves []SlaveDef
  err = ScanSlice(res.([]interface{}), slaves)
  return slaves, err
}

// DialMaster returns a connection to the master of the named monitored instance set
// Assumes the same network will be used to contact the master as the one used for
// contacting the sentinels.
func (sc *SentinelClient) DialMaster(name string) (Conn, error) {
  masterAddr, err := sc.QueryConfForMaster(name)

  if err != nil {
    return nil, err
  }

  return Dial(sc.net, masterAddr)
}

// DialSlave returns a connection to any slave of the named monitored instance set.
//func (sc *SentinelClient) DialSlave(name string) (Conn, error) {
//}
