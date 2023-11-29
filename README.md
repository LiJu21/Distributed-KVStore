#  Project Description

- Constructed a distributed key-value store in Golang based on consistent hashing, leveraging the gossip protocol for node state management and data replication to ensure fault tolerance
- Deployed the system on 40 nodes in Google Cloud Platform for testing.

## Design

### Bootstrap
1. A node program takes two arguments: a list containing all the other serve, and a port number it should be listening on. The program can be run by command:
```
go run ./src/server/dht-server.go /path/to/peers.txt <port number>
```
2. After a node starts, it will call ```initNodeList()```, where it reads lines from peers.txt and create a list with all the other nodes, called ```nodeList```.
3. A ```nodeList``` is an array of ```NodeVal```, which represents a single node other than it self and is defined as follow:
```go
type NodeVal struct {
	ipAdr      string
	port       string
	membership string
}
```
4. Now we can say this node has full information of other nodes at the beginning, though some of them may fail to start.


### Gossip
1. Each node has a node list, which contains all the other node that are alive to the best of its knowledge.
2. New command code added: GET_MEMBERSHIP_LIST  = 0x22, a server receives this command will return its own node list.
3. The node list is maintained by gossipping.
4. The way a node does gossip is that it randomly chooses several nodes as targets, and sends command 0x22 to them sequentially. If it failed to receive response from a target, it would retry several. If all retries failed, the target would be regarded as dead, and this node would delete the target from its node list.
4. If a node got the response, which is a node list, from its target, it would merge two node lists.
5. The frequency of gossipping is set to a constant.
6. Each node has an initialized node list.

### Route
1. After a client sending a request to a certain node, first the node will check whether the command is `GET`, `PUT` or `REMOVE`.
2. If not, the node will handle the request as usual. Otherwise, the current node will check the hash ring to make sure whether the key should be stored or has already been stored inside it. If not,  it will find the correct node and send client address (ip + port) to that node. 
3. When the correct node receive the request, it will handle the request according and directly send back to the client.
