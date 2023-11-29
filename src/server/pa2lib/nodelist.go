package pa2lib

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"pa2/pb/protobuf"
	pb "pa2/pb/protobuf"
	"strconv"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
)

// NodeVal Data type used to store a node
type NodeVal struct {
	ipAdr      string
	port       string
	isOn       bool
	membership string
	time       uint64
}

var nodeList = map[string]*NodeVal{}
var startNodeList = map[string]NodeVal{}
var localPort = os.Args[1]

func initNodeList(serverListFile string ) {
	file, err := os.OpenFile(serverListFile, os.O_RDONLY, 0666)
	if err != nil {
		log.Println("Open file error!", err)
		return
	}
	defer file.Close()

	buf := bufio.NewReader(file)
	for {
		line, err := buf.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				log.Println(line, " file read ok!")
				break
			} else {
				log.Println(line, " read file error!", err)
				return
			}
		}

		line = strings.TrimSpace(line)
		//log.Println(line)
		s := strings.Split(line, ":")
		log.Println("Initialize node: " + s[0] + ":" + s[1])
		IP := s[0]
		port := s[1]
		node := NodeVal{ipAdr: IP, port: port, isOn: true, membership: "0", time: 0}
		nodeList[IP + ":" + port] = &node
		startNodeList[IP + ":" + port] = node
	}
}

func GetMemberShipList() (map[string][]byte, int32, uint32) {
	log.Println("send membership list")
	var returnMap = map[string][]byte{}
	for addr, node := range nodeList {
		nodeValPro := pb.NodeVal{IpAdr: node.ipAdr,
											Port: node.port,
											IsOn: node.isOn,
											Membership: node.membership,
											Time: node.time}
		returnMap[addr], _ = proto.Marshal(&nodeValPro)
	}
	mutex.Lock()
	defer mutex.Unlock()
	return returnMap, 0, KEY_DNE_ERR
}

func requestNodeList(ipAdr string, port string) {
	log.Println("request ndoe list form", ipAdr, port)
	// Resolve the address to an address of UDP end point
	raddr, err := net.ResolveUDPAddr("udp", ipAdr+":"+port)
	//log.Printf("%v:%v\n", raddr.IP, raddr.Port)
	if err != nil {
		log.Println("Fail to resolve address", err)
	}

	// Connect laddr with raddr
	conn, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		//log.Printf("Dial fialed: %v:%v\n", raddr.IP, raddr.Port)
		log.Println(err)
	}

	// Defer closing the underlying file discriptor associated with the socket
	defer conn.Close()

	// Use UUIDs as messageID and convert String to byte array
	uuid := uuid.NewString()
	msgID := []byte(strings.ReplaceAll(uuid, "-", ""))

	// Get a request message
	reqMessage, err := getGossipRequestMessage(msgID)
	// Send request and get respond
	newNodeList, err := sendReqAndGetRes(reqMessage, msgID, conn)

	if err == nil {
		// the gossip succeed, now merge node lists
		log.Println("get node list from", ipAdr, port)
		err = mergeNodeLists(newNodeList)
		log.Printf("Gossip succeed, now merge two node lists.\n")
	} else {
		// if the gossip fail, it mean the target node is dead, so update the list
		log.Println(nodeList[ipAdr + ":" + port].isOn)
		err = turnOffNodeFromList(ipAdr, port)
		currentPort, _ := strconv.Atoi(os.Args[1])
		log.Printf("%v, Gossip failed, now remove %v:%v from current node list.\n", currentPort, ipAdr, port)
		//log.Println(nodeList[ipAdr + ":" + port].isOn)
	}
	if err != nil {
		log.Println(err)
	}
}

func doGossip() {
	currentPort, _ := strconv.Atoi(os.Args[1])
	log.Printf("Port # %v, Start gossiping", currentPort)
	// Randomly generate a list of listeners
	var numListeners int
	var activeNodeList = map[string]NodeVal{}
	for key, node := range nodeList {
		if node.isOn == true && (node.ipAdr != localIP || node.port != localPort) {
			activeNodeList[key] = *node
		}
	}

	if len(activeNodeList) > 4 {
		if len(activeNodeList)/5 < 4 {
			numListeners = 4
		} else {
			numListeners = len(activeNodeList) / 5
		}
	} else {
		numListeners = len(activeNodeList)
	}
	log.Printf("The number of gossip targets is: %v\n", numListeners)
	log.Printf("The length of active node list is: %v\n", len(activeNodeList))
	//log.Println( "node list:", nodeList)


	var list = rand.Perm(len(activeNodeList))

	var listenerList = map[string]NodeVal{}
	i := 0
	for key, val := range activeNodeList {
		if i >= numListeners { break }
		for _, j := range list {
			if i == j {
				listenerList[key] = val
			}
			i++
		}
	}

	// request nodeList from listeners
	for _, listener := range listenerList {
		if listener.ipAdr == localIP && listener.port == localPort {
			continue
		}
		requestNodeList(listener.ipAdr, listener.port)
	}
}

func getGossipRequestMessage(messageID []byte) ([]byte, error) {
	reqPayload := getGossipRequestPayload()
	checksum := crc32.ChecksumIEEE(append(messageID, reqPayload...))
	request := &protobuf.Msg{
		MessageID: messageID,
		Payload:   reqPayload,
		CheckSum:  uint64(checksum),
	}
	reqMessage, err := proto.Marshal(request)
	if err != nil {
		log.Println(err)
	}
	return reqMessage, err
}

func getGossipRequestPayload() []byte {
	requestPayload := &protobuf.KVRequest{
		Command: GET_MEMBERSHIP_LIST,
	}
	reqPayload, err := proto.Marshal(requestPayload)
	if err != nil {
		log.Println(err)
	}
	return reqPayload
}

func getResponseMessage(reply []byte) ([]byte, []byte, uint64) {
	replyMsg := &protobuf.Msg{}
	err := proto.Unmarshal(reply, replyMsg)
	if err != nil {
		log.Println(err)
	}
	return replyMsg.GetMessageID(), replyMsg.GetPayload(), replyMsg.GetCheckSum()
}

func nodeListParseFromByteArray(nodeListPayload map[string][]byte) map[string]NodeVal {
	var newNodeList = map[string]NodeVal{}
	for addr, nodeByte := range nodeListPayload {
		var nodePb = pb.NodeVal{}
		err := proto.Unmarshal(nodeByte, &nodePb)
		if err != nil {
			log.Println("nodeListParseFromByteArray error")
		}
		newNodeList[addr] = NodeVal{ipAdr: nodePb.IpAdr, port: nodePb.Port, isOn: nodePb.IsOn, membership: nodePb.Membership, time: nodePb.Time}
	}
	return newNodeList
}

func sendReqAndGetRes(reqMessage []byte, msgID []byte, conn *net.UDPConn) (map[string]NodeVal, error) {
	// Initialize timeout and times of retry
	timeout := 100 // 100ms
	attempts := 0

	for attempts < 1 {
		err := conn.SetReadDeadline(time.Now().Add(time.Millisecond * time.Duration(timeout)))
		if err != nil {
			log.Println(err)
		}

		// Send request message to the server
		buf := reqMessage
		_, err = conn.Write(buf)
		if err != nil {
			log.Println(err)
		}

		// Receive response from the server
		buf = make([]byte, 65535) // clean the buffer
		len, _, err := conn.ReadFrom(buf)
		replyMsg := buf[0:len]
		if err != nil {
			// fmt.Println(err)
			log.Println("retry: sendReqAndGetRes readFrom error!")
			timeout = timeout * 2
			attempts++
		} else {
			// check whether the checksum is corrrect or not
			messageID, msgPayload, checksum := getResponseMessage(replyMsg)
			expectedChecksum := uint64(crc32.ChecksumIEEE(append(messageID, msgPayload...)))
			var resPayload = &protobuf.KVResponse{}
			if checksum == expectedChecksum &&
				hex.EncodeToString(messageID) == hex.EncodeToString(msgID) {
				err := proto.Unmarshal(msgPayload, resPayload)
				if err != nil {
					log.Println(err)
				}
				return nodeListParseFromByteArray(resPayload.NodeList), nil
				//return secretCode, nil
			} else {
				log.Println("retry: received wrong message")
				// fmt.Println("Corrupt Data")
				timeout = timeout * 2
				attempts++
			}
		}
	}
	err := errors.New("All the retries fail")
	log.Println(err)
	return nil, err
}

// TODO: consider the situation where removed nodes can be readded
func mergeNodeLists(newNodeList map[string]NodeVal) error {
	log.Println("Start merging two node lists.")
	for addr, _ := range nodeList {
		if  _, v := newNodeList[addr]; v{
		} else {
			return errors.New("!")
		}
		if nodeList[addr].isOn != newNodeList[addr].isOn {
			if nodeList[addr].time < newNodeList[addr].time {
				if nodeList[addr].isOn == true && newNodeList[addr].isOn == false {
					ip := nodeList[addr].ipAdr
					port := nodeList[addr].port
					foundDeadNode(ip, port)
				}
				*nodeList[addr] = newNodeList[addr]
				log.Println("change node list:", addr)
			}
		}
	}
	return nil
}

func nodeExists(nodeList []NodeVal, node NodeVal) bool {
	for i := 0; i < len(nodeList); i++ {
		if nodeList[i].ipAdr == node.ipAdr && nodeList[i].port == node.port {
			return true
		}
	}
	return false
}

// Remove a target node from the nodeList
// it also returns the new nodeList
func turnOffNodeFromList(ipAdr string, port string) error {
	addr := ipAdr + ":" + port
	if val, ok := nodeList[addr]; ok {
		val.isOn = false
		val.time = uint64(time.Now().UnixNano())
	} else {
		log.Println("Error: can't turn off " + addr + ":" + port + "from node list, because it doesn't exist!")
		return errors.New("turnOffNodeFromList error")
	}
	return nil
}

func turnOnNodeFromList(msgId []byte) error {
	ip := net.IPv4(msgId[0],msgId[1],msgId[2],msgId[3]).String()
	port := binary.LittleEndian.Uint16(msgId[4:6])
	addr := ip + ":" + strconv.Itoa(int(port))
	sentTime := binary.LittleEndian.Uint64(msgId[8:])
	if val, ok := nodeList[addr]; ok {
		val.isOn = true
		val.time = sentTime
	} else {
		log.Println("Error: can't turn on " + addr + "from node list, because it doesn't exist!")
		return errors.New("turnOnNodeFromList error")
	}
	return nil
}


func getNodeList() map[string]*NodeVal {
	return nodeList
}

func checkAlive(ipAdr string, port string) bool {
	log.Printf("Check is alive: %v:%v\n", ipAdr, port)
	raddr, err := net.ResolveUDPAddr("udp", ipAdr+":"+port)
	//log.Printf("%v:%v\n", raddr.IP, raddr.Port)
	if err != nil {
		log.Println("Fail to resolve address", err)
		return false
	}

	// Connect laddr with raddr
	conn, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		//log.Printf("Dial fialed: %v:%v\n", raddr.IP, raddr.Port)
		log.Println(err)
		return false
	}

	// Defer closing the underlying file discriptor associated with the socket
	defer conn.Close()

	// Use UUIDs as messageID and convert String to byte array
	uuid := uuid.NewString()
	msgID := []byte(strings.ReplaceAll(uuid, "-", ""))

	// Send isAlive message
	reqMessage, err := getIsAliveRequestMessage(msgID)
	// Send request and get respond
	isAlive, err := getIsAliveRes(reqMessage, msgID, conn)

	if err != nil {
		log.Printf("%v:%v is dead.", ipAdr, port)
		log.Println(err)
		return false
	}
	return isAlive
}

func getIsAliveRequestMessage(messageID []byte) ([]byte, error) {
	requestPayload := &protobuf.KVRequest{
		Command: IS_ALIVE,
	}
	reqPayload, err := proto.Marshal(requestPayload)
	if err != nil {
		log.Println(err)
	}
	checksum := crc32.ChecksumIEEE(append(messageID, reqPayload...))
	request := &protobuf.Msg{
		MessageID: messageID,
		Payload:   reqPayload,
		CheckSum:  uint64(checksum),
	}
	reqMessage, err := proto.Marshal(request)
	if err != nil {
		log.Println(err)
	}
	return reqMessage, err
}

func getIsAliveRes(reqMessage []byte, msgID []byte, conn *net.UDPConn) (bool, error) {
	// Initialize timeout and times of retry
	timeout := 100 // 100ms
	attempts := 0

	for attempts < 1 {
		err := conn.SetReadDeadline(time.Now().Add(time.Millisecond * time.Duration(timeout)))
		if err != nil {
			log.Println(err)
		}

		// Send request message to the server
		buf := reqMessage
		_, err = conn.Write(buf)
		if err != nil {
			log.Println(err)
		}

		// Receive response from the server
		buf = make([]byte, 65535) // clean the buffer
		len, _, err := conn.ReadFrom(buf)
		replyMsg := buf[0:len]
		if err != nil {
			log.Println("retry: getIsAliveRes readFrom error!")
			timeout = timeout * 2
			attempts++
		} else {
			// check whether the checksum is corrrect or not
			messageID, msgPayload, checksum := getResponseMessage(replyMsg)
			expectedChecksum := uint64(crc32.ChecksumIEEE(append(messageID, msgPayload...)))
			var resPayload = &protobuf.KVResponse{}
			if checksum == expectedChecksum &&
				hex.EncodeToString(messageID) == hex.EncodeToString(msgID) {
				err := proto.Unmarshal(msgPayload, resPayload)
				if err != nil {
					log.Println(err)
				}
				return true, nil
			} else {
				log.Println("retry: received wrong message")
				// fmt.Println("Corrupt Data")
				timeout = timeout * 2
				attempts++
			}
		}
	}
	err := errors.New("Tried to check isAlive, but all the retries failed")
	log.Println(err)
	return false, err
}

func receiveHello(addr *net.UDPAddr, mesId []byte) {
	log.Println("Receive hello from: "+addr.IP.String()+":", addr.Port)
	//modify nodelist
	turnOnNodeFromList(mesId)

	//update hashring
	consistent.addNodetoHashring(mesId)

	//replicate
	port := strconv.Itoa(addr.Port)
	welcomeNewNode(*nodeList[addr.IP.String()+":"+port])
}

func foundDeadNode(ip string, port string) {

	//modify nodelist
	// turnOffNodeFromList(ip, port)

	//replicate
	addr := ip + ":" + port
	nodeDieReplicate(*nodeList[addr])

	//update hashring
	consistent.removeNodefromHashring(ip, port)
}
