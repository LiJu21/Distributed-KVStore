package pa2lib

import (
	"net"
	"strconv"
)

func getRelationWith(node NodeVal) int {
	curNode := *nodeList[localIP+":"+localPort]

	curNodeSon := consistent.getNextNode(curNode)
	if curNodeSon == node {
		return 1
	}

	curNodeGrandSon := consistent.getNextNode(curNodeSon)
	if curNodeGrandSon == node {
		return 2
	}

	curNodeFather := consistent.getLastNode(curNode)
	if curNodeFather == node {
		return -1
	}

	curNodeGrandFather := consistent.getLastNode(curNodeFather)
	if curNodeGrandFather == node {
		return -2
	}

	return 0
}

//this function should be called after a node receives hello
//this function should be called after hashring is recalculated!
func welcomeNewNode(node NodeVal){
	port, _ := strconv.Atoi(node.port)

	nodeAddr := net.UDPAddr{
		Port: port,
		IP:   net.ParseIP(node.ipAdr),
	}

	switch getRelationWith(node) {
		case -2:
			onGrandFatherResurrect()
		case -1:
			onFatherResurrect(node)
		case 1:
			onSonResurrect(&nodeAddr)
		case 2:
			onGrandSonResurrect(&nodeAddr)
	}
}

func onSonResurrect(addr *net.UDPAddr){
	sendNodeDieReplicateRequest(I_AM_YOUR_FATHER, KVStore, addr)
}

func onGrandSonResurrect(addr *net.UDPAddr){
	sendNodeDieReplicateRequest(I_AM_YOUR_GRANDFATHER, KVStore, addr)
}

func onGrandFatherResurrect(){
	//do nothing
}

func onFatherResurrect(father NodeVal){
	var KVPairs []StoreVal
	for i := 0 ; i < len(KVStore); {
		KVPair := KVStore[i]
		_, isMineKV := checkNode(KVPair.key)
		//out of range
		//should belong to father
		if !isMineKV{
			KVPairs = append(KVPairs, KVPair)
			KVStore = append(KVStore[:i], KVStore[i+1:]...)
		}
	}

	port, _ := strconv.Atoi(father.port)

	fatherAddr := net.UDPAddr{
		Port: port,
		IP:   net.ParseIP(father.ipAdr),
	}

	sendNodeDieReplicateRequest(I_AM_YOUR_SON, KVPairs, &fatherAddr)
}


