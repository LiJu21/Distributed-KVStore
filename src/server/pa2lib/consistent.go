package pa2lib

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"net"
	"sort"
	"strconv"
	"sync"
)

type Consistent struct {
	circle   map[uint32]NodeVal // hash ring
	// nodeList map[NodeVal]bool
	sync.RWMutex
}

func newConsistent() *Consistent {
	return &Consistent{
		circle: make(map[uint32]NodeVal),
		// nodeList: make(map[NodeVal]bool),
	}
}

// map the initial node list to hash ring
func (c *Consistent) generateHashRing(nodeList map[string]*NodeVal) {
	for _, node := range nodeList {
			ipAdr := node.ipAdr
			port := node.port
			hashKey := hashKey(ipAdr, port)
			c.circle[hashKey] = *node
	}
}

// find the corresponding node according to a certain key from kvstore
func (c *Consistent) getNode(key []byte) NodeVal{
	//fmt.Println("start getting node")
	c.Lock()
	defer c.Unlock()
	hashKey := hashKeyfromKey(key)
	//fmt.Println("Hash key:", hashKey)
	keys, _ := getSortedNodeList(c.circle)
	for _, k := range keys {

		// move clockwise from the position of hashkey to find the correct node
		if hashKey <= k {
			return c.circle[k]
		}
	}
	//fmt.Println("length of nodelist", len(keys))

	// if the hashkey is larger than any other node keys, it will return the first node
	return c.circle[keys[0]]
}

func (c *Consistent) getNextNode(node NodeVal) NodeVal{
	c.Lock()
	defer c.Unlock()
	hashringLength := len(c.circle)
	_, nodes := getSortedNodeList(c.circle)
	for i, _ := range nodes {
		if nodes[i].ipAdr == node.ipAdr && nodes[i].port == node.port{
			return nodes[(i+1) % hashringLength]
		}
	}

	//if node does not exist in the hashring, return node itself
	fmt.Println("cannot find the next node of unknown: ", node.port)
	return node
}

func (c *Consistent) getLastNode(node NodeVal) NodeVal{
	c.Lock()
	defer c.Unlock()
	hashringLength := len(c.circle)
	_, nodes := getSortedNodeList(c.circle)
	for i, _ := range nodes {
		if nodes[i].ipAdr == node.ipAdr && nodes[i].port == node.port{
			return nodes[(i-1+hashringLength) % hashringLength]
		}
	}

	//if node does not exist in the hashring, return node itself
	fmt.Println("cannot find the next node of unknown: ", node.port)
	return node
}

// add new node
func (c *Consistent) addNodetoHashring(msgId []byte){
	c.Lock()
	defer c.Unlock()
	ip := net.IPv4(msgId[0],msgId[1],msgId[2],msgId[3]).String()
	port := binary.LittleEndian.Uint16(msgId[4:6])
	addr := ip + ":" + strconv.Itoa(int(port))
	node := nodeList[addr]
	hashKey := hashKey(node.ipAdr, node.port)
	c.circle[hashKey] = *node
}

// delete an existing node
func (c *Consistent) removeNodefromHashring(ip string, port string){
	c.Lock()
	defer c.Unlock()
	hashKey := hashKey(ip, port)
	delete(c.circle, hashKey)
}

// sort the node list according to keys
func getSortedNodeList(circle map[uint32]NodeVal) ([]uint32, []NodeVal) {
	//fmt.Println(len(circle))
	keys := []uint32{}
	for k := range circle {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {return keys[i] < keys[j]})
	
	nodes := make([]NodeVal, len(circle))
	for _, k := range keys {
		nodes = append(nodes, circle[k])
	}
	return keys, nodes
}

func hashKeyfromKey(key []byte) uint32 {
	return crc32.ChecksumIEEE(key)
}

func hashKey(ipAdr string, port string) uint32 { // we could change to Hash32 later
	return crc32.ChecksumIEEE([]byte(ipAdr + ":" + port))
}

func checkNode(key []byte) (NodeVal, bool) {
	//log.Println("circle length", len(consistent.circle))
	node := consistent.getNode(key)
	//log.Println(node)
	nodeIP := node.ipAdr
	//log.Println(localIP, "?==", nodeIP)
	nodePort := node.port
	//log.Println(localPort, "?==", nodePort)
	if localIP == nodeIP && localPort == nodePort {
		//log.Println("current node is correct")
		return node, true
	} else {
		return node, false
	}
}
