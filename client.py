import socket
import json
import threading
from time import sleep
import sys
import select
import os

IP = "127.0.0.1"
port_dict = dict()
server_on = True
PID = 0
PORT = 0
leader = 0
retry = 0
inQueue = 0
lock = threading.Lock()

def connectServer(address):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex(address)
    if result == 0:
        sock.sendall(bytes("client ", "utf-8"))
        sleep(1)
        return sock, True
    return sock, False

def connected(stream):
    global leader
    while server_on and stream:
        # 1 second timeout on stream
        ready = select.select([stream], [], [], 1)
        # check if there is anything in the stream
        if ready[0]:
            resp = stream.recv(1024)
            if resp:
                msg = resp.decode("utf-8").split(" ")
                if msg[0] == "leader":
                    with lock:
                        leader = int(msg[2])
                    print(f"{PID}: server {msg[2]} is now the leader")
                elif msg[0] == "server":
                    print(f"{PID}: connected to server {int(msg[1])}")

def listenerSocket():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((IP, port_dict[PID][0]))
    sock.listen(5)          # max 5: 5 servers
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

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f'Usage: python3 {sys.argv[0]} <pid>')
        sys.exit()
    PID = int(sys.argv[1])
    
    with open('config.json') as f:
        data = json.load(f)
        for i, (k,v) in enumerate(data.items()):
            port_dict[int(k)] = (v, 0, False)
    
    listener_thread = threading.Thread(target=listenerSocket, args=())
    listener_thread.start()
    
    for i in range(5):
        address = (IP, port_dict[i][0])
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(address)
        sock.sendall(bytes(f"connect {PID}", "utf-8"))
    
    print("Enter an operation:")
    x = input()
    while x != "exit":
        retry = 0
        if x.startswith("operation"):
            msg = x.split("(")
            msg1 = msg[1].split(", ")           # split into msg1[0] = op, msg1[1] = key, msg2[2] = value
            
            address = (IP, port_dict[leader][0])
            sock, success = connectServer(address)
            if success:
                sleep(2)
                if msg1[0] == "put":
                    sock.sendall(bytes(f"{msg1[0]} {msg1[1]} {msg1[2][:-1]}", "utf-8"))
                else:
                    sock.sendall(bytes(f"{msg1[0]} {msg1[1][:-1]}", "utf-8"))
                
                while True:
                    ready = select.select([sock], [], [], 15)
                    if ready[0]:
                        resp = sock.recv(1024)
                        if resp:
                            msg = resp.decode("utf-8")
                            if msg == "ack":
                                print(msg)
                                sock.close()
                                break
                            # reset timer if op is in queue
                            elif msg == "inQueue":
                                print("operation in queue; timer reset")
                            else:
                                print(msg)
                                sock.close()
                                break
                        else: # empty response
                            print(f"{PID}: connection error; switching leader")
                            print(f"{PID}: leader election failed; switching leader")
                            with lock:
                                leader = (leader + 1) % 5
                            sock.close()
                            retry = 1
                            break
                    else: # resp timeout
                        print(f"{PID}: connection timeouted; telling {leader} to become leader")
                        sock.sendall(bytes(f"leader {PID} {msg1[0]} {msg1[1]} {msg1[2][:-1]}", "utf-8"))
                        ready = select.select([sock], [], [], 13)
                        if ready[0]:
                            resp = sock.recv(1024)
                            if resp:
                                print(resp.decode("utf-8"))
                                sock.close()
                                break
                        else: # leader election failed
                            print(f"{PID}: leader election failed; switching leader")
                            with lock:
                                leader = (leader + 1) % 5
                            sock.close()
                            retry = 1
                            break
            else:
                print(f"{PID}: {leader} port is closed; switching leader")
                with lock:
                    leader = (leader + 1) % 5
                retry = 1
        elif x.startswith("leader"):  # manually changed the hinted leader for client
            msg = x.split(" ")
            print(f"{PID}: hinted leader changed to {msg[1]}")
            with lock:
                leader = int(msg[1])
        
        if not retry:
            print("Enter another operation:")
            x = input()
        
    # closing listenerSocket
    server_on = False
    socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((IP, port_dict[PID][0]))