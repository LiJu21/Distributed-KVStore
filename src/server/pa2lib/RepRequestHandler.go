package pa2lib

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"log"
	"net"
	pb "pa2/pb/protobuf"
)

const REP_REQUEST_PORT = 10043

func unmarshalRepRequest(payload []byte)(uint32, []*pb.RepRequest_KVPair){
	repPayload := pb.RepRequest{}
	err := proto.Unmarshal(payload, &repPayload)
	if err != nil{
		log.Println("Error unmarshal replicate request")
	}

	return repPayload.Command, repPayload.Kvs
}

func handleRepRequest(command uint32, kvs []*pb.RepRequest_KVPair){
	switch command {
	case GRANDSON_DIED:
		//addr, _ := net.ResolveUDPAddr("udp",string(reqPay.Addr))
		//sendNodeDieReplicateRequest(FATHER_DIED, KVStore, addr)
		return
	case SON_DIED:
		//addr, _ := net.ResolveUDPAddr("udp",string(reqPay.Addr))
		//sendNodeDieReplicateRequest(GRANDFATHER_DIED_1, KVStore, addr)
		return

	case FATHER_DIED:
		onFatherDie(kvs)
		break

	case GRANDFATHER_DIED_1://copy kvs to repKVStore[1]
		ReplicateFromGrandFather(kvs)
		break

	case GRANDFATHER_DIED_2://copy kvs to repKVStore[0]
		ReplicateFromFather(kvs)
		break

	case I_AM_YOUR_FATHER:
		ReplicateFromFather(kvs)
		break

	case I_AM_YOUR_GRANDFATHER:
		ReplicateFromGrandFather(kvs)
		break

	case I_AM_YOUR_SON:
		ReplicateFromSon(kvs)
		break
	}
}

func RepRequestHandler(){
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", REP_REQUEST_PORT))
	if err != nil {
		log.Println("Error resolving replicate server address:", err)
		return
	}

	conn, err = net.ListenUDP("udp", addr)
	if err != nil {
		log.Println("Error listening to port for replicate request:", err)
		return
	}
	defer conn.Close()
	log.Println("Now listening to port for replicate request")

	rcvBuffer := make([]byte, 11000)
	for{
		numBytes, _, err := conn.ReadFromUDP(rcvBuffer)
		if err == nil{
			_, repReqPayload, _ := unmarshalMsg(rcvBuffer[:numBytes])
			if repReqPayload == nil{
				continue
			}

			command, kvs := unmarshalRepRequest(repReqPayload)
			go handleRepRequest(command, kvs)
		}
	}
}
