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
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func SetNowFunc(f func() time.Time) {
	nowFunc = f
}

var (
	ErrNegativeInt = errNegativeInt

	serverPath     = flag.String("redis-server", "redis-server", "Path to redis server binary")
	serverHost     = flag.String("redis-host", "127.0.0.1", "local host")
	serverBasePort = flag.Int("redis-port", 16379, "Beginning of port range for test servers")
	serverLogName  = flag.String("redis-log", "", "Write Redis server logs to `filename`")
	serverLog      = ioutil.Discard
	//cluster
	//in windows, you may set redis-trib like this: "ruby yourpath/redis-trib.rb", 'yourpath' should not including spaces.
	serverTrip       = flag.String("redis-trib", "redis-trib.rb", "Path to redis trib binary")
	serverClusterNum = flag.Int("cluster-num", 3, "Number of cluster node, at least 3")
	clusterEnable    = flag.Bool("cluster", false, "Enable cluster for test")

	defaultServerMu       sync.Mutex
	defaultServer         *Server
	defaultClusterServers []*Server
	defaultServerErr      error
)

type Server struct {
	name        string
	port        int
	cmd         *exec.Cmd
	clusterDone chan struct{} //for create cluster
	done        chan struct{}
}

func NewServer(name string, port int, args ...string) (*Server, error) {
	args = append(args, "--port", strconv.Itoa(port))
	s := &Server{
		name: name,
		port: port,
		cmd:  exec.Command(*serverPath, args...),
		done: make(chan struct{}),
	}

	r, err := s.cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}

	err = s.cmd.Start()
	if err != nil {
		return nil, err
	}

	ready := make(chan error, 1)
	go s.watch(r, ready)

	select {
	case err = <-ready:
	case <-time.After(time.Second * 10):
		err = errors.New("timeout waiting for server to start")
	}

	if err != nil {
		s.Stop()
		return nil, err
	}

	return s, nil
}

func (s *Server) watch(r io.Reader, ready chan error) {
	fmt.Fprintf(serverLog, "%d START %s \n", s.cmd.Process.Pid, s.name)
	var listening bool
	var text string
	scn := bufio.NewScanner(r)
	for scn.Scan() {
		text = scn.Text()
		fmt.Fprintf(serverLog, "%s\n", text)
		if !listening {
			if strings.Contains(text, "The server is now ready to accept connections on port") {
				listening = true
				ready <- nil
			}
		}
		if s.clusterDone != nil {
			if strings.Contains(text, "Cluster state changed: ok") {
				s.clusterDone <- struct{}{}
			}
		}
	}
	if !listening {
		ready <- fmt.Errorf("server exited: %s", text)
	}
	s.cmd.Wait()
	fmt.Fprintf(serverLog, "%d STOP %s \n", s.cmd.Process.Pid, s.name)
	close(s.done)
}

func (s *Server) Stop() {
	if err := s.cmd.Process.Signal(os.Interrupt); err != nil {
		//os.Interrupt not supported by windows
		s.cmd.Process.Signal(os.Kill)
	}
	<-s.done

	//try to remove redis autocreate file
	os.Remove(fmt.Sprintf("nodes-%v.conf", s.port))
}

// stopDefaultServer stops the server created by DialDefaultServer.
func stopDefaultServer() {
	defaultServerMu.Lock()
	defer defaultServerMu.Unlock()
	if defaultServer != nil {
		defaultServer.Stop()
		defaultServer = nil
	}
	for _, server := range defaultClusterServers {
		server.Stop()
	}
	defaultClusterServers = nil
}

// startDefaultServer starts the default server if not already running.
func startDefaultServer() error {
	defaultServerMu.Lock()
	defer defaultServerMu.Unlock()
	if defaultServer != nil || defaultServerErr != nil {
		return defaultServerErr
	}
	startPort := *serverBasePort
	defaultServer, defaultServerErr = NewServer(
		"default",
		startPort,
		"--save", "",
		"--appendonly", "no")

	if defaultServerErr == nil &&
		IsClusterEnable() {
		defaultServerErr = createCluster(startPort + 1)
	}
	return defaultServerErr
}

// create redis cluster by startDefaultServer
func createCluster(startPort int) error {
	//new cluster server
	for i := 0; i < *serverClusterNum; i++ {
		cs, err := NewServer(
			fmt.Sprintf("default_cluster%v", i),
			startPort+i,
			"--appendonly", "no",
			"--cluster-enabled", "yes",
			"--cluster-node-timeout", "5000",
			"--cluster-config-file", fmt.Sprintf("nodes-%v.conf", startPort+i))
		if err != nil {
			for _, server := range defaultClusterServers {
				server.Stop()
			}
			defaultClusterServers = nil
			return err
		}
		defaultClusterServers = append(defaultClusterServers, cs)
	}

	//create the cluster
	if len(defaultClusterServers) > 0 {
		var nodes []string
		for _, server := range defaultClusterServers {
			server.clusterDone = make(chan struct{}, 1)
			nodes = append(nodes, fmt.Sprintf("%s:%d", *serverHost, server.port))
		}
		tripCmd := strings.Split(*serverTrip, " ")
		var args []string
		if len(tripCmd) > 1 {
			args = append(args, tripCmd[1:]...)
		}
		args = append(args, "create")
		args = append(args, nodes...)
		cmd := exec.Command(tripCmd[0], args...)
		w, err := cmd.StdinPipe()
		if err != nil {
			return err
		}
		_, err = io.WriteString(w, "yes\n")
		if err != nil {
			return err
		}
		output, err := cmd.CombinedOutput()
		if err != nil || !strings.Contains(string(output), "[OK] All 16384 slots covered") {
			return fmt.Errorf("create cluster failed:%s, err:%v", output, err)
		}

		//wait for done
		for _, server := range defaultClusterServers {
			select {
			case <-server.clusterDone:
			case <-time.After(time.Second * 20):
				return fmt.Errorf("timeout waiting for cluster state changed: ok")
			}
			server.clusterDone = nil
		}
	}
	return nil
}

func dailServer(server *Server) (Conn, error) {
	c, err := Dial("tcp",
		fmt.Sprintf("%s:%d", *serverHost, server.port),
		DialReadTimeout(1*time.Second),
		DialWriteTimeout(1*time.Second))
	if err != nil {
		return nil, fmt.Errorf("dail redis server failed:%v", err)
	}
	return c, nil
}

// DefaultServerAddr return no cluster server to test cluster command
func DefaultServerAddr() string {
	return fmt.Sprintf("%v:%v", *serverHost, *serverBasePort)
}

// DialDefaultServer starts the test server if not already started and dials a
// connection to the server.
func DialDefaultServer() (Conn, error) {
	if err := startDefaultServer(); err != nil {
		return nil, err
	}
	c, err := dailServer(defaultServer)
	if err != nil {
		return nil, err
	}
	c.Do("FLUSHDB")
	return c, nil
}

//Fetch the server info in redis cluster for cluster test
func FetchClusterNodes() ([]string, error) {
	if err := startDefaultServer(); err != nil {
		return nil, err
	}
	var addrs []string
	for i := 0; i < *serverClusterNum; i++ {
		addrs = append(addrs, fmt.Sprintf("%s:%d", *serverHost, defaultClusterServers[i].port))
	}
	return addrs, nil
}

//Migrate slot for test ASK/MOVED command.
func MigrateSlot(slot uint16, moved bool) error {
	if len(defaultClusterServers) <= 0 {
		return fmt.Errorf("cluster server is empty.")
	}

	server := defaultClusterServers[len(defaultClusterServers)-1]
	c, err := dailServer(server)
	if err != nil {
		return err
	}
	reply, err := String(c.Do("CLUSTER", "NODES"))
	if err != nil {
		return fmt.Errorf("migrate slot failed:%v", err)
	}
	var masterNodes = []struct {
		id    string
		port  int
		start uint16
		end   uint16
	}{}
	for _, node := range strings.Split(reply, "\n") {
		fields := strings.Split(node, " ")
		if len(fields) < 3 || !strings.Contains(fields[2], "master") {
			continue
		}
		addr := fields[1]
		port, _ := strconv.Atoi(addr[strings.LastIndex(addr, ":")+1:])
		if len(fields) > 8 {
			slots := strings.Split(fields[8], "-")
			if len(slots) == 2 {
				if start, err := strconv.Atoi(slots[0]); err == nil {
					if end, err := strconv.Atoi(slots[1]); err == nil {
						masterNodes = append(masterNodes, struct {
							id    string
							port  int
							start uint16
							end   uint16
						}{fields[0], port, uint16(start), uint16(end)})
					}
				}
			}
		}
	}
	if len(masterNodes) < 2 {
		return fmt.Errorf("cluster nodes invaild response.")
	}

	findServer := func(port int) *Server {
		for _, server := range defaultClusterServers {
			if server.port == port {
				return server
			}
		}
		return nil
	}

	migratingErr := "cluster setslot migrating: %v"
	for i, node := range masterNodes {
		if slot >= node.start && slot <= node.end {
			i++
			if i >= len(masterNodes) {
				i = 0
			}
			sourceServer := findServer(node.port)
			destServer := findServer(masterNodes[i].port)
			if sourceServer == nil || destServer == nil {
				return fmt.Errorf(migratingErr, "can not found server.")
			}
			sourceConn, err := dailServer(sourceServer)
			if err != nil {
				return fmt.Errorf(migratingErr, err)
			}
			destConn, err := dailServer(destServer)
			if err != nil {
				return fmt.Errorf(migratingErr, err)
			}

			if _, err := destConn.Do("CLUSTER", "SETSLOT", slot, "IMPORTING", node.id); err != nil {
				return fmt.Errorf(migratingErr, err)
			}

			if _, err := sourceConn.Do("CLUSTER", "SETSLOT", slot, "MIGRATING", masterNodes[i].id); err != nil {
				return fmt.Errorf(migratingErr, err)
			}

			if moved {
				if _, err := destConn.Do("CLUSTER", "SETSLOT", slot, "NODE", masterNodes[i].id); err != nil {
					return fmt.Errorf(migratingErr, err)
				}
			}
		}
	}
	return nil
}

// calc key in slot
func HashSlot(key string) uint16 {
	return hashSlot(key)
}

// Is enable to test cluster
func IsClusterEnable() bool {
	return *clusterEnable
}

func TestMain(m *testing.M) {
	os.Exit(func() int {
		flag.Parse()

		var f *os.File
		if *serverLogName != "" {
			var err error
			f, err = os.OpenFile(*serverLogName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error opening redis-log: %v\n", err)
				return 1
			}
			defer f.Close()
			serverLog = f
		}

		defer stopDefaultServer()

		return m.Run()
	}())
}
