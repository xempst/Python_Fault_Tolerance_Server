import socket
import json
import threading
from time import sleep
import select
import sys
import os
import pickle
import queue

from blockchain import Blockchain
from block import Block
import util

# global variables
IP = "127.0.0.1"
fname = ""
PID = 0
server_on = True
leader = -1
Lock = threading.Lock()
port_dict = dict()
sendSock_dict = dict()
seq_lock = threading.Lock()
seq_num = 0
promised = 0
promised_list = list()
promise_lock = threading.Lock()
rejected = 0
reject_lock = threading.Lock()
promise_cond = threading.Condition()
accepted = 0
accept_lock = threading.Lock()
declined = 0
decline_lock = threading.Lock()
accept_cond = threading.Condition()

depth = 0
electing = False


# data structures
bc = Blockchain()
q = queue.Queue()
data_dict = dict()

q_cond = threading.Condition()

# total time for leader electing is 6 sec
def leaderElection():
    global seq_num, promised, rejected, accepted, declined, leader, electing
    with Lock:
        electing = True

    # broadcast prepare msg to all server
    print("broadcasting prepare msg")
    with seq_lock:
        seq_num += 1
        msg = f"prepare {seq_num} {PID} {depth}"
    with promise_lock:
        promised = 0
    with reject_lock:
        rejected = 0
    sleep(2)
    for i, (server_id, sock) in enumerate(sendSock_dict.items()):
        if server_id < 5 and port_dict[server_id][1]:
            threading.Thread(target=sendPrepare, args=(sock, msg)).start()

    while promised < 2 and rejected < 3 and server_on:
        with promise_cond:
            promise_cond.wait()

    # received majority; becomes leader
    if promised >= 2:
        print(f"{PID}: Look at me. I am the leader now")
        with seq_lock:
            seq_num += 1
            msg = f"leader {seq_num} {PID}"
        with Lock:
            leader = PID
        sleep(2)
        # broadcast the elected leader to all servers and clients
        for i, (server_id, sock) in enumerate(sendSock_dict.items()):
            if port_dict[server_id][1]:
                sock.sendall(bytes(msg, "utf-8"))
                print(f"{PID}: sending leader election to {server_id}")
        with Lock:
            electing = False
        return True
    else:
        with Lock:
            electing = False
        return False


def sendAccept(sock, msg):
    global seq_num, accepted, declined, depth
    sock.sendall(bytes(msg, "utf-8"))
    ready = select.select([sock], [], [], 3)
    if ready[0]:
        resp = sock.recv(1024).decode('utf-8').split(" ")
        if resp[0] == "accepted":
            with seq_lock:
                seq_num = max(seq_num, int(resp[3])) + 1
            print(f"{PID}: received accepted from {resp[2]}")
            with accept_lock:
                accepted += 1
            with accept_cond:
                accept_cond.notify()
        elif resp[0] == "failed":
            with Lock:
                depth = int(msg[1])
    else:
        with decline_lock:
            declined += 1
        with accept_cond:
            accept_cond.notify()


def sendPrepare(sock, msg):
    global seq_num, promised, rejected
    sock.sendall(bytes(msg, "utf-8"))
    ready = select.select([sock], [], [], 3)
    if ready[0]:
        resp = sock.recv(1024).decode('utf-8').split(" ")
        if resp[0] == "promise":
            with seq_lock:
                seq_num = max(seq_num, int(resp[3])) + 1
            print(f"{PID}: received promise from {resp[2]}")
            with promise_lock:
                promised += 1
            with promise_cond:
                promise_cond.notify()
    else:
        with reject_lock:
            rejected += 1
        with promise_cond:
            promise_cond.notify()


def serverAction(stream, server_id):
    global seq_num, leader, depth, data_dict, bc
    while port_dict[server_id][1] and server_on:
        # 1 second timeout on stream
        ready = select.select([stream], [], [], 1)
        # check if there is anything in the stream
        if ready[0]:
            resp = stream.recv(1024)
            if resp:
                msg = resp.decode('utf-8')
                print(f'{PID}: from server {server_id}: {msg}')

                msg = msg.split(" ")

                if msg[0] == "prepare":
                    if int(msg[3]) >= depth:
                        # the leader has the same blockchain; promise the leader
                        with seq_lock:
                            seq_num = max(seq_num, int(msg[1])) + 1
                        sleep(2)
                        if port_dict[int(msg[2])][1]:
                            print(f"{PID}: send promise to {msg[2]}")
                            stream.sendall(bytes(f"promise {depth} {PID} {seq_num}", 'utf-8'))
                    else:
                        with seq_lock:
                            seq_num = max(seq_num, int(msg[1])) + 1
                            msg = f"failed {depth} {PID} {seq_num}"
                        if port_dict[int(msg[2])][1]:
                            sleep(2)
                            stream.sendall(bytes(msg, 'utf-8'))
                        print(f"{PID}: rejects {msg[2]}'s proposal because {msg[2]}'s blockchain is outdated")
                elif msg[0] == "accept":
                    if int(msg[3]) >= depth + 1 and leader == int(msg[2]):
                        with seq_lock:
                            seq_num = max(seq_num, int(msg[1])) + 1
                        with Lock:
                            depth = int(msg[3])
                        b = Block(int(msg[3]), msg[4] + " " + msg[5] + " " + msg[6], msg[7], msg[8], msg[9])
                        with Lock:
                            bc.addBlock(b)
                        print(f"{PID}: send accepted to {msg[2]}")
                        sleep(2)
                        if port_dict[int(msg[2])][1]:
                            stream.sendall(bytes(f"accepted {bc.lastBlock().index} {PID} {seq_num}", "utf-8"))
                elif msg[0] == "leader":
                    with seq_lock:
                        seq_num = max(seq_num, int(msg[1])) + 1
                    with Lock:
                        leader = int(msg[2])
                    print(f"{PID}: {msg[2]} is now the leader")
                elif msg[0] == "decision":
                    if int(msg[3]) >= depth and leader == int(msg[2]):
                        with Lock:
                            bc.lastBlock().decision = "decided"
                            bc.lastBlock().writeBlock(fname)
                            data_dict[msg[5]] = msg[6]
                        print(f"{PID}: adding key: {msg[5]} value: {msg[6]} to dict")
                elif msg[0] == "disconnect":
                    with seq_lock:
                        seq_num = max(seq_num, int(msg[1])) + 1
                    with Lock:
                        port_dict[int(msg[2])] = (port_dict[int(msg[2])][0], False)
                    print(f"{PID}: disconnected from server {msg[2]}")
                elif msg[0] == "reconnect":
                    # create an endpoint on their receive server
                    address = (IP, port_dict[int(msg[2])][0])
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.connect(address)
                    sendSock_dict[int(msg[2])] = sock
                    sleep(2)
                    sock.sendall(bytes("server " + str(PID), "utf-8"))
                    
                    with seq_lock:
                        seq_num = max(seq_num, int(msg[1])) + 1
                    if int(msg[4]) >= depth:
                        with Lock:
                            port_dict[int(msg[2])] = (port_dict[int(msg[2])][0], True)
                            depth = int(msg[4])
                            leader = int(msg[3])
                        if recoverBlockchain():
                            print(f"{PID}: successfully recovered blockchain")
                            with Lock:
                                data_dict = util.updateDict(bc)
                            print(f"{PID}: updated dict() with blockchain")
                        else:
                            print(f"{PID}: failed to recover blockchain")
                    stream.sendall(bytes(f"leader {leader} {seq_num} {depth}", "utf-8"))
                    print(f"{PID}: connection to server {msg[2]} fixed")
                # operation forwarded to you
                # if not leader, respond with leader id
                # else queue operation
                elif msg[0] == "forwarded":
                    if leader == PID:
                        q.put((stream, f"{msg[1]} {msg[2]} {msg[3]}"))
                        with q_cond:
                            q_cond.notify()
                    else:
                        stream.sendall(bytes(f"leader {leader}", "utf-8"))
                # a server has restarted thus require you to connect back it
                # send leader and depth and seq_num
                elif msg[0] == "rejoin":
                    # create an endpoint on their receive server
                    address = (IP, port_dict[int(msg[1])][0])
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.connect(address)
                    sendSock_dict[int(msg[1])] = sock
                    sleep(2)
                    sock.sendall(bytes("server " + str(PID), "utf-8"))
                    port_dict[int(msg[1])] = (port_dict[int(msg[1])][0], True)
                    
                    # send leader, depth, and seq_num info
                    stream.sendall(bytes(f"info {leader} {depth} {seq_num}", "utf-8"))
                # requesting blockchain
                # dump blockchain with pickle and send it back
                elif msg[0] == "request":
                    print(f"{PID}: sending blockchain to {msg[1]}")
                    resp = pickle.dumps(bc)
                    sleep(2)
                    stream.sendall(resp)
            else: # empty resp; connection error
                with Lock:
                    port_dict[server_id] = (port_dict[server_id][0], False)

    stream.shutdown(socket.SHUT_RDWR)
    stream.close()
    with Lock:
        port_dict[server_id] = (port_dict[server_id][0], False)
    print(f'{PID}: Disconnected from {server_id}')


def clientAction(stream):
    global leader
    while stream and server_on:
        ready = select.select([stream], [], [], 1)
        if ready[0]:
            resp = stream.recv(1024)
            if resp:
                msg = resp.decode('utf-8')
                print(f'{PID}: msg from client: {msg}')
                
                if msg.startswith("leader"):
                    msg = msg.split(" ")
                    # enqueue
                    q.put((stream, f"{msg[1]} {msg[2]} {msg[3]}"))
                    if leaderElection():
                        with q_cond:
                            q_cond.notify()
                    else:
                        # leader election failed
                        print(f"{PID}: failed to become leader")
                elif msg.startswith("get"):
                    q.put((stream, msg))
                    if q.qsize() == 1:
                        with q_cond:
                            q_cond.notify()
                else:
                    # server just started; there is no leader
                    if leader == -1:
                        # enqueue
                        q.put((stream, msg))
                        if not electing:
                            if leaderElection():
                                with q_cond:
                                    q_cond.notify()
                            else:
                                # leader election failed
                                print(f"{PID}: failed to become leader")
                                stream.sendall(bytes(f"failed {PID}", "utf-8"))
                                while not q.empty:
                                    q.get()
                    # I am the leader
                    elif leader == PID:
                        # enqueue
                        q.put((stream, msg))
                        with q_cond:
                            q_cond.notify()
                    # I am not the leader
                    else:
                        if port_dict[leader][1]:
                            # forward to the leader
                            print(f"{PID}: I am not the leader. Forwarding to leader")
                            sleep(2)
                            sock = sendSock_dict[leader]
                            sock.sendall(bytes(f"forwarded {msg}", "utf-8"))
                            ready = select.select([sock], [], [], 9)
                            if ready[0]:
                                resp = sock.recv(1024)
                                if resp:
                                    msg = resp.decode("utf-8")
                                    if msg.startswith("ack"):
                                        sleep(2)
                                        stream.sendall(resp)
                                    else:
                                        msg = msg.split(" ")
                                        with Lock:
                                            leader = int(msg[1])
                        else:
                            # enqueue
                            q.put((stream, msg))
                            # try to be leader
                            if leaderElection():
                                with q_cond:
                                    q_cond.notify()
                            else:
                                # leader election failed
                                print(f"{PID}: failed to become leader")
                                stream.sendall(bytes(f"failed {PID}", "utf-8"))
                                while not q.empty:
                                    q.get()
    stream.close()
    print(f'{PID}: Connection from client closed.')


def connected(stream):
    # check if it is a client or server
    resp = stream.recv(1024)
    msg = resp.decode('utf-8').split(" ")

    # if it is server stream, perform server action
    if (msg[0] == "server"):                # first msg format: "server PID"
        with Lock:
            port_dict[int(msg[1])] = (port_dict[int(msg[1])][0], True)
        print(f'{PID}: Connected to {msg[1]}')
        serverAction(stream, int(msg[1]))
    # if it is a client stream, perform client action
    elif (msg[0] == "client"):
        print(f'{PID}: Connected to a client')
        clientAction(stream)
    # if a client want the server to connect back
    elif (msg[0] == "connect"):
        address = (IP, port_dict[int(msg[1])][0])
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(address)
        sendSock_dict[int(msg[1])] = sock
        port_dict[int(msg[1])] = (port_dict[int(msg[1])][0], True)
        print(f"{PID}: Connected to client {msg[1]}'s receive server.")

# listen for server connection
def listenerSocket():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((IP, port_dict[PID][0]))
    sock.listen(7)          # max 7: 4 servers and 3 clients
    listener_threads = list()

    while server_on:
        stream, addr = sock.accept()
        thread = threading.Thread(target=connected, args=[stream])
        listener_threads.append(thread)
        thread.start()

    for thread in enumerate(listener_threads):
        thread[1].join()

    sock.shutdown(socket.SHUT_RDWR)
    sock.close()

# listen for user input from command line
def userInput():
    global seq_num, leader, depth, data_dict, bc
    x = input()
    while x != "exit" and x != "failProcess":
        # connects to all server except for itself
        if x == "connect":
            for i in range(5):
                if i != PID:
                    address = (IP, port_dict[i][0])
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.connect(address)
                    sendSock_dict[i] = sock
                    sock.sendall(bytes("server " + str(PID), "utf-8"))
        elif x.startswith("failLink"):
            msg = x.split(" ")
            if int(msg[1]) == PID:
                if port_dict[int(msg[2])][1]:
                    sock = sendSock_dict[int(msg[2])]
                    sleep(2)
                    with seq_lock:
                        seq_num += 1
                        sock.sendall(bytes(f"disconnect {seq_num} {PID}", "utf-8"))
                    with Lock:
                        port_dict[int(msg[2])] = (port_dict[int(msg[2])][0], False)
                print(f"{PID}: terminated connection with server {msg[2]}")
            else:
                print(f"{PID}: wrong format; failLink src dst")
        elif x.startswith("fixLink"):
            msg = x.split(" ")
            if int(msg[1]) == PID:
                if not port_dict[int(msg[2])][1]:
                    address = (IP, port_dict[int(msg[2])][0])
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.connect(address)
                    sendSock_dict[int(msg[2])] = sock
                    sock.sendall(bytes("server " + str(PID), "utf-8"))
                    port_dict[int(msg[2])] = (port_dict[int(msg[2])][0], True)
                    
                    sleep(2)
                    with seq_lock:
                        seq_num += 1
                        msg1 = f"reconnect {seq_num} {PID} {leader} {depth}"
                    sock.sendall(bytes(msg1, "utf-8"))
                    ready = select.select([sock], [], [], 3)
                    if ready[0]:
                        resp = sock.recv(1024).decode("utf-8")
                        print(f"{PID}: fixlink resp1 = {resp}")
                        resp = resp.split(" ")
                        if int(resp[1]) != leader:
                            with Lock:
                                leader = int(resp[1])
                        with seq_lock:
                            seq_num = max(int(resp[2]), seq_num)
                        if int(resp[3]) > depth:
                            if recoverBlockchain():
                                print(f"{PID}: successfully recovered blockchain")
                                with Lock:
                                    data_dict = util.updateDict(bc)
                                print(f"{PID}: updated dict() with blockchain")
                            else:
                                print(f"{PID}: failed to recover blockchain")
                            with Lock:
                                depth = int(resp[3])
                        with Lock:
                            port_dict[int(msg[2])] = (port_dict[int(msg[2])][0], True)
                print(f"{PID}: connection with server {msg[2]} restored")
            else:
                print(f"{PID}: wrong format; fixLink src dst")
        # try to connect to all server
        # get info about the leader
        # request blockchain from leader
        # replace current blockchain with new blockchain
        elif x == "recoverProcess":
            # connect to all possible server
            for i in range(5):
                if i != PID:
                    address = (IP, port_dict[i][0])
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    result = sock.connect_ex(address)
                    if result == 0:
                        sendSock_dict[i] = sock
                        sock.sendall(bytes("server " + str(PID), "utf-8"))
                        port_dict[i] = (port_dict[i][0], True)
                        print(f"{PID}: connected to server {i}")
            
            # ask other server to connect back
            print(f"{PID}: requesting server info")
            sleep(2)
            for i, (server_id, sock) in enumerate(sendSock_dict.items()):
                if port_dict[server_id][1]:
                    sendSock_dict[server_id].sendall(bytes(f"rejoin {PID}", "utf-8"))
                    resp = sendSock_dict[server_id].recv(1024).decode("utf-8").split(" ")
                    if resp[0] == "info":
                        if int(resp[2]) >= depth:
                            with Lock:
                                depth = int(resp[2])
                                leader = int(resp[1])
                            with seq_lock:
                                seq_num = int(resp[3])
            
            # connect to all possible client's receive server
            for i in range(5,8):
                address = (IP, port_dict[i][0])
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                result = sock.connect_ex(address)
                if result == 0:
                    sendSock_dict[i] = sock
                    port_dict[i] = (port_dict[i][0], True)
                    print(f"{PID}: Connected to client {i}'s receive server.")
                    
            # recover blockchain
            if leader != PID:
                if recoverBlockchain():
                    print(f"{PID}: successfully recovered blockchain")
                else:
                    print(f"{PID}: failed to recover blockchain")
            # the leader has not been updated
            else:
                print(f"{PID}: recovering blockchain from file")
                with Lock:
                    bc = util.recoverBlockchain(fname)
            print(f"{PID}: updating dict() from blockchain")
            with Lock:
                data_dict = util.updateDict(bc)
        elif x == "printKVStore":
            print(data_dict)
        elif x == "printBlockchain":
            bc.printBlockchain()
        elif x == "printQueue":
            print(q.queue)
        x = input()

    # stop server from listening
    global server_on
    server_on = False

    for i, (server_id, sock) in enumerate(sendSock_dict.items()):
        print(f"closing server_id: {server_id}")
        sock.shutdown(socket.SHUT_RDWR)
        sock.close()
        with Lock:
            port_dict[server_id] = (port_dict[server_id][0], False)

    socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((IP, port_dict[PID][0]))
    with q_cond:
        q_cond.notify()
    with promise_cond:
        promise_cond.notify()
    with accept_cond:
        accept_cond.notify()

def recoverBlockchain():
    global bc
    if port_dict[leader][1]:
        print(f"{PID}: requesting blockchain from leader {leader}")
        sock = sendSock_dict[leader]
        sleep(2)
        sock.sendall(bytes(f"request {PID}", "utf-8"))
        
        ready = select.select([sock], [], [], 3)
        if ready[0]:
            resp = sock.recv(2048)
            if resp:
                with Lock:
                    bc = pickle.loads(resp)
                bc.rewriteBlockchain(fname)
                return True
            return False
        else:
            return False



def queueAction():
    global seq_num, accepted, declined, depth
    while server_on:
        # block thread until when it needs to be sent to other server as accept msg
        while q.empty() and server_on:
            with q_cond:
                q_cond.wait()
        
        # there is operation in queue
        if server_on:
            # msg[0] = put, msg[1] = key, msg[2] = value
            # msg[0] = get, msg[1] = key
            msg = q.queue[0][1].split(" ")
            print(msg)
            if msg[0] == "put":
                b = Block(bc.lastBlock().index + 1, f"{msg[0]} {msg[1]} {msg[2]}", bc.lastBlock().hash, "tentative")
                b.findNonce()

                with seq_lock:
                    seq_num += 1
                    msg = f"accept {seq_num} {PID} {b.index} {b.operation} {b.prev_hash} {b.decision} {b.nonce}"
                with accept_lock:
                    accepted = 0
                with decline_lock:
                    declined = 0
                # broadcast accept msg to all conencted servers
                sleep(2)
                for i, (server_id, sock) in enumerate(sendSock_dict.items()):
                    if port_dict[server_id][1]:
                        threading.Thread(target=sendAccept, args=(sock, msg)).start()
                # wait for majority to accept
                while accepted < 2 and declined < 3 and server_on:
                    with accept_cond:
                        accept_cond.wait()

                # received accepted from majority
                # add block to chain
                # broadcast decision
                # dequeue and send ack to client
                # add key value to dict
                if accepted >= 2:
                    # add block to chain
                    b.decision = "decided"
                    with Lock:
                        bc.addBlock(b)
                        depth += 1
                        bc.lastBlock().writeBlock(fname)
                    
                    with seq_lock:
                        seq_num += 1
                        msg = f"decision {seq_num} {PID} {b.index} {b.operation} {b.prev_hash} {b.decision} {b.nonce}"
                    
                    sleep(2)
                    # broadcast decision to every server connected
                    for i, (server_id, sock) in enumerate(sendSock_dict.items()):
                        if server_id < 5 and port_dict[server_id][1]:
                            sock.sendall(bytes(msg, "utf-8"))
                    
                    # dequeue and respond to client with an ack
                    op = q.get()
                    stream = op[0]
                    print(f"{PID}: responding ack to client")
                    sleep(2)
                    stream.sendall(bytes("ack", "utf-8"))
                    
                    # add key value to dict
                    data = bc.lastBlock().operation.split(" ")
                    print(f"{PID}: Adding key: {data[1]} value: {data[2]} to dict")
                    with Lock:
                        data_dict[data[1]] = data[2]
                # does not get majority of accepted
                # empty queue and do not respond to client
                # wait for client to timeout
                else:
                    while not q.empty:
                        q.get()
            elif msg[0] == "get": # op = get
                if msg[1] in data_dict:
                    resp = data_dict[msg[1]]
                else:
                    resp = "NO_KEY"
                op = q.get()
                stream = op[0]
                stream.sendall(bytes(resp, "utf-8"))
            # there are still op in queue
            # tell client to wait for another iteration
            if not q.empty():
                for i in q.queue:
                    i[0].sendall(bytes("inQueue", "utf-8"))


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f'Usage: python3 {sys.argv[0]} <pid> <hashFile>')
        sys.exit()

    PID = int(sys.argv[1])
    fname = sys.argv[2]

    with open('config.json') as f:
        data = json.load(f)
        for i, (k, v) in enumerate(data.items()):
            port_dict[int(k)] = (v, False)

    input_thread = threading.Thread(target=userInput, args=())
    input_thread.start()

    listener_thread = threading.Thread(target=listenerSocket, args=())
    listener_thread.start()

    queue_thread = threading.Thread(target=queueAction, args=())
    queue_thread.start()

    input_thread.join()
    listener_thread.join()
    queue_thread.join()

    os._exit(0)
