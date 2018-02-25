/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

using std::hash;

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *address) {
    this->memberNode = memberNode;
    this->par = par;
    this->emulNet = emulNet;
    this->log = log;
    ht = new HashTable();
    this->memberNode->addr = *address;
    this->ring = vector<Node>();
    this->transactionMap = unordered_map<int, Message>();
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
    delete ht;
    delete memberNode;
}

bool ringsAreEqual(vector<Node> &fst, vector<Node> &snd) {
    if (fst.size() != snd.size()) return false;

    for (int i = 0; i < fst.size(); ++i) {
        if (!(*(fst[i].getAddress()) == *(snd[i].getAddress()))) return false;
    }

    return true;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
    /*
     * Implement this. Parts of it are already implemented
     */
    vector<Node> curMemList;
    bool change;

    /*
     *  Step 1. Get the current membership list from Membership Protocol / MP1
     */
    curMemList = getMembershipList();

    /*
     * Step 2: Construct the ring
     */
    // Sort the list based on the hashCode
    sort(curMemList.begin(), curMemList.end());
    change = !ringsAreEqual(this->ring, curMemList);

    /*
     * Step 3: Run the stabilization protocol IF REQUIRED
     */
    // Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
    if (change) {
        stabilizationProtocol();
        this->ring = curMemList;
    }
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
    unsigned int i;
    vector<Node> curMemList;
    for (i = 0; i < this->memberNode->memberList.size(); i++) {
        Address addressOfThisMember;
        int id = this->memberNode->memberList.at(i).getid();
        short port = this->memberNode->memberList.at(i).getport();
        memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
        memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
        curMemList.emplace_back(Node(addressOfThisMember));
    }
    return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
    std::hash<string> hashFunc;
    size_t ret = hashFunc(key);
    return ret % RING_SIZE;
}


ReplicaType getReplicaType(int index) {
    if (index == 0) {
        return PRIMARY;
    }

    if (index == 1) {
        return SECONDARY;
    }

    return TERTIARY;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
    auto vector = this->findNodes(key);
    for (int i = 0; i < vector.size(); ++i) {
        ReplicaType replicaType = getReplicaType(i);

        Message message(g_transID++, this->memberNode->addr, CREATE, key, value, replicaType);
        int transId = g_transID;
        this->emulNet->ENsend(&this->memberNode->addr, vector[i].getAddress(), message.toString());
        this->transactionMap.insert(std::pair<int, Message>(std::move(transId), std::move(message)));
    }
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key) {
    auto vector = this->findNodes(key);
    Message message(g_transID++, this->memberNode->addr, READ, key);
    this->emulNet->ENsend(&this->memberNode->addr, vector[0].getAddress(), message.toString());
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value) {
    auto vector = this->findNodes(key);
    for (int i = 0; i < vector.size(); ++i) {
        ReplicaType replicaType = getReplicaType(i);

        Message message(g_transID++, this->memberNode->addr, UPDATE, key, value, replicaType);
        this->emulNet->ENsend(&this->memberNode->addr, vector[i].getAddress(), message.toString());
    }
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key) {
    auto vector = this->findNodes(key);
    for (auto &i : vector) {
        Message message(g_transID++, this->memberNode->addr, DELETE, key);
        this->emulNet->ENsend(&this->memberNode->addr, i.getAddress(), message.toString());
    }
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
    // Insert key, value, replicaType into the hash table
    return ht->create(std::move(key), Entry(std::move(value), this->par->getcurrtime(), replica).convertToString());
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
    // Read key from local hash table and return value
    return ht->read(key);
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
    // Update key in local hash table and return true or false
    return ht->update(std::move(key), Entry(std::move(value), par->getcurrtime(), replica).convertToString());
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
    // Delete the key from the local hash table
    return ht->deleteKey(std::move(key));
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
    /*
     * Implement this. Parts of it are already implemented
     */
    char *data;
    int size;

    /*
     * Declare your local variables here
     */

    // dequeue all messages and handle them
    while (!memberNode->mp2q.empty()) {
        /*
         * Pop a message from the queue
         */
        data = (char *) memberNode->mp2q.front().elt;
        size = memberNode->mp2q.front().size;
        memberNode->mp2q.pop();

        string messageStr(data, data + size);

        /*
         * Handle the message types here
         */
        Message message(std::move(messageStr));
        if (message.type == CREATE) {

            bool succeed = createKeyValue(message.key, message.value, message.replica);
            if (succeed) {
                log->logCreateSuccess(&memberNode->addr, false, message.transID, message.key, message.value);
            } else {
                log->logCreateFail(&memberNode->addr, false, message.transID, message.key, message.value);
            }
            Message response = Message(message.transID, memberNode->addr, REPLY, succeed);
            emulNet->ENsend(&memberNode->addr, &message.fromAddr, response.toString());
        } else if (message.type == UPDATE) {
            bool succeed = updateKeyValue(message.key, message.value, message.replica);
            if (succeed) {
                log->logUpdateSuccess(&memberNode->addr, false, message.transID, message.key, message.value);
            } else {
                log->logUpdateFail(&memberNode->addr, false, message.transID, message.key, message.value);
            }
            Message response = Message(message.transID, memberNode->addr, REPLY, succeed);
            emulNet->ENsend(&memberNode->addr, &message.fromAddr, response.toString());
        } else if (message.type == DELETE) {
            bool succeed = deletekey(message.key);
            if (succeed) {
                log->logDeleteSuccess(&memberNode->addr, false, message.transID, message.key);
            } else {
                log->logDeleteFail(&memberNode->addr, false, message.transID, message.key);
            }
            Message response = Message(message.transID, memberNode->addr, REPLY, succeed);
            emulNet->ENsend(&memberNode->addr, &message.fromAddr, response.toString());
        } else if (message.type == READ) {
            string value = readKey(message.key);
            if (!value.empty()) {
                log->logReadSuccess(&memberNode->addr, false, message.transID, message.key, value);

            } else {
                log->logReadFail(&memberNode->addr, false, message.transID, message.key);
            }
            Message response = Message(message.transID, memberNode->addr, READREPLY, value);
            emulNet->ENsend(&memberNode->addr, &message.fromAddr, response.toString());
        } else if (message.type == READ) {
            Message &saved_message = (this->transactionMap.find(message.transID))->second;
            if (message.success) {
                if (saved_message.type == CREATE) {
                    log->logCreateSuccess(&memberNode->addr, true, message.transID, saved_message.key,
                                          saved_message.value);
                } else if (saved_message.type == DELETE) {
                    log->logDeleteSuccess(&memberNode->addr, true, message.transID, saved_message.key);
                } else if (saved_message.type == UPDATE) {
                    log->logUpdateSuccess(&memberNode->addr, true, message.transID, saved_message.key,
                                          saved_message.value);
                }
            } else {
                if (saved_message.type == CREATE) {
                    log->logCreateFail(&memberNode->addr, true, message.transID, saved_message.key,
                                       saved_message.value);
                } else if (saved_message.type == DELETE) {
                    log->logDeleteFail(&memberNode->addr, true, message.transID, saved_message.key);
                } else if (saved_message.type == UPDATE) {
                    log->logUpdateFail(&memberNode->addr, true, message.transID, saved_message.key,
                                       saved_message.value);
                }
            }
            this->transactionMap.erase(message.transID);
        } else if (message.type == READREPLY) {
            Message &saved_message =(this->transactionMap.find(message.transID))->second;
            if (message.success) {
                log->logReadSuccess(&memberNode->addr, true, message.transID, saved_message.key, message.value);
            } else {
                log->logReadFail(&memberNode->addr, true, message.transID, saved_message.key);
            }
        }
    }

    /*
     * This function should also ensure all READ and UPDATE operation
     * get QUORUM replies
     */
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
    size_t pos = hashFunction(key);
    vector<Node> addr_vec;
    if (ring.size() >= 3) {
        // if pos <= min || pos > max, the leader is the min
        if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size() - 1).getHashCode()) {
            addr_vec.emplace_back(ring.at(0));
            addr_vec.emplace_back(ring.at(1));
            addr_vec.emplace_back(ring.at(2));
        } else {
            // go through the ring until pos <= node
            for (int i = 1; i < ring.size(); i++) {
                Node addr = ring.at(i);
                if (pos <= addr.getHashCode()) {
                    addr_vec.emplace_back(addr);
                    addr_vec.emplace_back(ring.at((i + 1) % ring.size()));
                    addr_vec.emplace_back(ring.at((i + 2) % ring.size()));
                    break;
                }
            }
        }
    }
    return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if (memberNode->bFailed) {
        return false;
    } else {
        return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
    Queue q;
    return q.enqueue((queue<q_elt> *) env, (void *) buff, size);
}

/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
    /*
     * Implement this
     */
}
