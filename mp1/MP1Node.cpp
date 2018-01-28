/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
    for (int i = 0; i < 6; i++) {
        NULLADDR[i] = 0;
    }
    this->memberNode = member;
    this->emulNet = emul;
    this->log = log;
    this->par = params;
    this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if (memberNode->bFailed) {
        return false;
    } else {
        return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
    Queue q;
    return q.enqueue((queue<q_elt> *) env, (void *) buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if (initThisNode(&joinaddr) == -1) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if (!introduceSelfToGroup(&joinaddr)) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
    /*
     * This function is partially implemented and may require changes
     */
    int id = *(int *) (&memberNode->addr.addr);
    int port = *(short *) (&memberNode->addr.addr[4]);

    memberNode->bFailed = false;
    memberNode->inited = true;
    memberNode->inGroup = false;
    // node is up!
    memberNode->nnb = 0;
    memberNode->heartbeat = 0;
    memberNode->pingCounter = TFAIL;
    memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    memberNode->memberList.emplace_back(id, port, memberNode->heartbeat, par->getcurrtime());
    memberNode->myPos = memberNode->memberList.begin();

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
    MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if (0 == memcmp((char *) &(memberNode->addr.addr), (char *) &(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    } else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *) (msg + 1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *) (msg + 1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));
#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *) msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode() {
    /*
     * Your code goes here
     */
    return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
        return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if (!memberNode->inGroup) {
        return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while (!memberNode->mp1q.empty()) {
        ptr = memberNode->mp1q.front().elt;
        size = memberNode->mp1q.front().size;
        memberNode->mp1q.pop();
        recvCallBack((void *) memberNode, (char *) ptr, size);
    }
    return;
}

int alreadyContains(int id, const vector<MemberListEntry> &memberList) {
    for (size_t i = 0; i < memberList.size(); ++i) {
        if (memberList[i].id == id) {
            return i;
        }
    }

    return -1;
}

void serialize_member_list(const vector<MemberListEntry> &memberList, char *buff) {
    size_t size = memberList.size();
    memcpy(buff, &size, sizeof(size_t));
    int ptr = sizeof(size_t);
    for (int i = 0; i < size; ++i) {
        memcpy(buff + ptr, &memberList[i], sizeof(MemberListEntry));
        ptr += sizeof(MemberListEntry);
    }
}

unique_ptr<vector<MemberListEntry>> deserialize_member_list(char *buff, Log *log, int msg_size, Address *addr) {
    size_t size = 0;
    memcpy(&size, buff, sizeof(size_t));
    int ptr = sizeof(size_t);
    auto member_list = unique_ptr<vector<MemberListEntry>>(new vector<MemberListEntry>());
    for (size_t i = 0; i < size; ++i) {
        MemberListEntry memberListEntry = MemberListEntry();
        memcpy(&memberListEntry, buff + ptr, sizeof(MemberListEntry));
        member_list->push_back(memberListEntry);
        ptr += sizeof(MemberListEntry);
    }

    return member_list;
}

Address extractAddress(const MemberListEntry &entry) {
    Address address = Address();
    address.init();
    memcpy(&address.addr, &entry.id, sizeof(int));
    memcpy(&address.addr[4], &entry.port, sizeof(short));

    return address;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size) {
    auto *hdr = (MessageHdr *) data;
    if (hdr->msgType == JOINREQ) {
        auto member = unique_ptr<Member>(new Member);
        member->addr = Address();
        member->addr.init();

        memcpy(&member->addr.addr, data + sizeof(MessageHdr), sizeof(member->addr.addr));
        memcpy(&member->heartbeat, data + size - sizeof(long), sizeof(long));

        int id = *(int *) (&member->addr.addr);
        int port = *(short *) (&member->addr.addr[4]);
        int entry_index = alreadyContains(id, memberNode->memberList);
        if (entry_index == -1) {
            memberNode->memberList.emplace_back(id, port, member->heartbeat, par->getcurrtime());
            log->logNodeAdd(&memberNode->addr, &member->addr);

            size_t message_size =
                    sizeof(MessageHdr) + sizeof(size_t) + memberNode->memberList.size() * sizeof(MemberListEntry);
            auto msg = (MessageHdr *) malloc(message_size * sizeof(char));
            msg->msgType = JOINREP;
            serialize_member_list(memberNode->memberList, (char *) (msg + 1));
            emulNet->ENsend(&memberNode->addr, &member->addr, (char *) msg, message_size);
        }

    } else if (hdr->msgType == JOINREP) {
        auto member_list = deserialize_member_list((char *) (hdr + 1), log, size, &memberNode->addr);

        for (auto entry: *member_list) {

            if (alreadyContains(entry.id, memberNode->memberList) == -1) {
                memberNode->memberList.push_back(MemberListEntry(entry));
                auto address = extractAddress(entry);
                log->logNodeAdd(&memberNode->addr, &address);
            }
        }
    } else if (hdr->msgType == HEARTBEAT) {
        auto member_list = deserialize_member_list((char *) (hdr + 1), log, size, &memberNode->addr);
        for (auto entry: *member_list) {
            log->LOG(&memberNode->addr,
                     ("entry " + to_string(entry.getheartbeat()) + " " + to_string(entry.id)).c_str());

            int entry_index = alreadyContains(entry.id, memberNode->memberList);
            if (entry_index == -1) {
                memberNode->memberList.push_back(entry);
                auto address = extractAddress(entry);
                log->logNodeAdd(&memberNode->addr, &address);
            } else {
                log->LOG(&memberNode->addr,
                         ("here " + to_string(memberNode->memberList[entry_index].getheartbeat()) + " " +
                          to_string(entry.getheartbeat())).c_str());
                if (memberNode->memberList[entry_index].getheartbeat() < entry.getheartbeat()) {
                    memberNode->memberList[entry_index].setheartbeat(entry.getheartbeat());
                    memberNode->memberList[entry_index].settimestamp(par->getcurrtime());
                }
            }
        }
    }

    return true;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
//    log->LOG(&memberNode->addr, ("nodeLoopOps: " + to_string(memberNode->pingCounter) + " " +
//                                 to_string(memberNode->timeOutCounter)).c_str());
    if (par->getcurrtime() - memberNode->pingCounter < memberNode->timeOutCounter) {
        return;
    }

    auto it = memberNode->memberList.begin();
    while (it != memberNode->memberList.end()) {
        if (it == memberNode->myPos) {
            ++it;
            continue;
        }

        MemberListEntry &entry = *it;
        int diff = par->getcurrtime() - entry.heartbeat;
        if (diff > TREMOVE) {
            auto address = extractAddress(entry);
            log->logNodeRemove(&memberNode->addr, &address);
            it = memberNode->memberList.erase(it);
        } else {
            ++it;
        }
    }

    memberNode->timeOutCounter = par->getcurrtime();
    memberNode->heartbeat++;
    memberNode->myPos->setheartbeat(memberNode->heartbeat);
    memberNode->myPos->settimestamp(par->getcurrtime());
    log->LOG(&memberNode->addr, ("heartbeat: " + to_string(memberNode->heartbeat)).c_str());
    for (auto it = memberNode->memberList.begin(); it != memberNode->memberList.end(); ++it) {
        int id = *(int *) (&memberNode->addr.addr);
        if (it->id == id) {
            it->setheartbeat(memberNode->heartbeat);
            it->settimestamp(par->getcurrtime());
        }
    }

    for (auto &entry: memberNode->memberList) {
        if (entry.heartbeat != 0) {
            log->LOG(&memberNode->addr, ("entry heartbeat: " + to_string(entry.heartbeat)).c_str());
        }
    }
    //TODO: replace with gossip strategy
    for (auto &entry: memberNode->memberList) {
        if (&entry == memberNode->myPos.base() || par->getcurrtime() - entry.getheartbeat() > TFAIL) {
            continue;
        }
        auto address = extractAddress(entry);
        size_t message_size =
                sizeof(MessageHdr) + sizeof(size_t) + memberNode->memberList.size() * (sizeof(MemberListEntry));

        auto msg = (MessageHdr *) malloc(message_size * sizeof(char));
        msg->msgType = HEARTBEAT;
        serialize_member_list(memberNode->memberList, (char *) (msg + 1));
        emulNet->ENsend(&memberNode->addr, &address, (char *) msg, message_size);
    }
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
    return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *) (&joinaddr.addr) = 1;
    *(short *) (&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
    memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr) {
    printf("%d.%d.%d.%d:%d \n", addr->addr[0], addr->addr[1], addr->addr[2],
           addr->addr[3], *(short *) &addr->addr[4]);
}
