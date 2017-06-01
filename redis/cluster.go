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
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/garyburd/redigo/internal"
)

const (
	ClusterSlots = 16384
)

var (
	ClosedErr = fmt.Errorf("cluster is closed.")
)

type Cluster struct {
	//each node's pool config
	MaxIdle     int
	MaxActive   int
	IdleTimeout time.Duration
	Wait        bool

	//connection config
	network     string
	dailOptions []DialOption

	//nodes
	slots [ClusterSlots]*clusterNode
	nodes map[string]*clusterNode

	closed bool
	mu     sync.RWMutex
}

func NewCluster(network string, startNodes []string, options ...DialOption) (*Cluster, error) {
	c := &Cluster{
		network:     network,
		dailOptions: options,
		nodes:       make(map[string]*clusterNode),
	}
	isConnect := false
	var err error
	for _, addr := range startNodes {
		var node *clusterNode
		node, err = c.addNode(addr)
		if err != nil {
			return nil, err
		}
		if _, err = c.update(node); err == nil {
			isConnect = true
			break
		}
	}
	if !isConnect {
		return nil, err
	}
	return c, nil
}

func (c *Cluster) Do(cmd string, args ...interface{}) (interface{}, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("key is cannot found.")
	}
	key := fmt.Sprintf("%v", args[0])
	node := c.calcNode(key)
	if node == nil {
		return nil, fmt.Errorf("cannot found node with key slot:%v.", key)
	}

	//direct call Do, if conn is bad, try other node to Do.
	conn := node.pool.Get()
	if _, ok := conn.(errorConnection); ok {
		for _, tryNode := range c.nodes {
			conn = tryNode.pool.Get()
			if _, ok := conn.(errorConnection); !ok {
				node = tryNode
				break
			}
		}
	}
	defer conn.Close()
	reply, err := conn.Do(cmd, args...)
	if err == nil {
		return reply, err
	}

	//try check cluster error
	invaildReposeErr := "invaild ask error format: %v"
	errMsg := err.Error()
	if len(errMsg) > 3 && strings.HasPrefix(errMsg, "ASK ") {
		replyErr, ok := reply.(Error)
		if !ok {
			return nil, fmt.Errorf(invaildReposeErr, reply)
		}
		fields := strings.Split(replyErr.Error(), " ")
		if len(fields) != 3 {
			return nil, fmt.Errorf("invaild ask error format: %v", replyErr.Error())
		}

		//try get a node
		node, err := c.addNode(fields[2])
		if err != nil {
			return nil, err
		}

		//to asking but no update slots, and try cmd again
		//but must use the same conn
		return node.askingDo(cmd, args...)
	} else if len(errMsg) > 5 && strings.HasPrefix(errMsg, "MOVED ") {
		replyErr, ok := reply.(Error)
		if !ok {
			return nil, fmt.Errorf(invaildReposeErr, reply)
		}
		fields := strings.Split(replyErr.Error(), " ")
		if len(fields) != 3 {
			return nil, fmt.Errorf("invaild moved error format: %v", replyErr.Error())
		}

		//cluster has changed, to update
		if reply, err := c.update(node); err != nil {
			return reply, err
		}

		//try cmd again
		targetNode, err := c.addNode(fields[2])
		if err != nil {
			return nil, err
		}
		return targetNode.do(cmd, args...)
	}

	return reply, err
}

func (c *Cluster) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, node := range c.nodes {
		node.close()
	}
	c.nodes = nil
	c.closed = true
}

func (c *Cluster) update(node *clusterNode) (interface{}, error) {
	reply, err := Values(node.do("CLUSTER", "SLOTS"))
	if err != nil {
		return reply, fmt.Errorf("cluster update slots failed:%v", err.Error())
	}

	errFormat := "cluster update invalid response: %v"
	//solt entry info
	// start slot
	// end slot
	// nodes
	// -- master addr info
	//     host
	//     port
	//     id(new version)
	// -- slave  --todo samething
	//     host
	//     port
	//     id(new version)

	updateSlots := make(map[string][]uint16)
	for _, entry := range reply {
		entryInfo, err := Values(entry, nil)
		if err != nil || len(entryInfo) < 3 {
			return reply, fmt.Errorf(errFormat, err)
		}
		// start slot
		start, err := Int(entryInfo[0], nil)
		if err != nil {
			return reply, fmt.Errorf(errFormat, err)
		}
		// end slot
		end, err := Int(entryInfo[1], nil)
		if err != nil {
			return reply, fmt.Errorf(errFormat, err)
		}
		// addr info
		addrInfo, err := Values(entryInfo[2], nil)
		if err != nil || len(addrInfo) < 2 {
			return reply, fmt.Errorf(errFormat, err)
		}
		//    host
		host, err := String(addrInfo[0], nil)
		if err != nil {
			return reply, fmt.Errorf(errFormat, err)
		}
		//    port
		port, err := Int(addrInfo[1], nil)
		if err != nil {
			return reply, fmt.Errorf(errFormat, err)
		}

		addr := fmt.Sprintf("%v:%v", host, port)
		slot := updateSlots[addr]
		slot = append(slot, uint16(start))
		slot = append(slot, uint16(end))
		updateSlots[addr] = slot
	}

	err = func() error {
		c.mu.Lock()
		defer c.mu.Unlock()
		for addr, slot := range updateSlots {
			node, ok := c.nodes[addr]
			if !ok {
				node, err = c.createNode(addr)
				if err != nil {
					return err
				}
				c.nodes[addr] = node
			}

			for i := 0; i < len(slot)-1; i += 2 {
				for j := slot[i]; j <= slot[i+1]; j++ {
					c.slots[j] = node
				}
			}
		}

		for addr, node := range c.nodes {
			if _, ok := updateSlots[addr]; !ok {
				node.close()
				delete(c.nodes, addr)
			}
		}
		return nil
	}()

	return nil, err
}

func (c *Cluster) createNode(addr string) (*clusterNode, error) {
	if c.closed {
		return nil, ClosedErr
	}
	node := &clusterNode{
		pool: &Pool{
			MaxIdle:     c.MaxIdle,
			MaxActive:   c.MaxActive,
			IdleTimeout: c.IdleTimeout,
			Wait:        c.Wait,
			Dial: func() (Conn, error) {
				redisConn, err := Dial(c.network, addr, c.dailOptions...)
				return redisConn, err
			},
			TestOnBorrow: func(c Conn, t time.Time) error {
				_, err := c.Do("PING")
				return err
			},
		},
	}
	return node, nil
}

func (c *Cluster) addNode(addr string) (*clusterNode, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	node, ok := c.nodes[addr]
	if !ok {
		var err error
		node, err = c.createNode(addr)
		if err != nil {
			return nil, err
		}
		c.nodes[addr] = node
	}
	return node, nil
}

func (c *Cluster) calcNode(key string) *clusterNode {
	slot := hashSlot(key)
	var node *clusterNode
	func() {
		c.mu.RLock()
		defer c.mu.RUnlock()
		node = c.slots[slot]
	}()
	return node
}

func hashSlot(key string) uint16 {
	//keys hash tags
	s := strings.IndexRune(key, '{')
	if s >= 0 && s+1 < len(key) {
		e := strings.IndexRune(key[s+1:], '}')
		if e > 0 {
			key = key[s+1 : e+s+1]
		}
	}
	return internal.Crc16(key) % ClusterSlots
}

type clusterNode struct {
	pool   *Pool
	closed bool
	once   sync.Once
}

func (cn *clusterNode) do(cmd string, args ...interface{}) (interface{}, error) {
	if cn.closed {
		return nil, ClosedErr
	}

	conn := cn.pool.Get()
	defer conn.Close()
	return conn.Do(cmd, args...)
}

func (cn *clusterNode) askingDo(cmd string, args ...interface{}) (interface{}, error) {
	if cn.closed {
		return nil, ClosedErr
	}

	conn := cn.pool.Get()
	defer conn.Close()
	if ok, err := String(conn.Do("ASKING")); err != nil || ok != okReply {
		return nil, fmt.Errorf("asking faied:%v", err)
	}
	return conn.Do(cmd, args...)
}

func (cn clusterNode) close() {
	cn.once.Do(func() {
		cn.closed = true
		cn.pool.Close()
	})
}
