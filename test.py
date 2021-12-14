from blockchain import Blockchain
from block import Block

bc = Blockchain()
b = Block(bc.lastBlock().index + 1, "put(55699, 123-222-0000)", bc.lastBlock().hash, "decided")
b.findNonce()
bc.addBlock(b, "output.txt")
b = Block(bc.lastBlock().index + 1, "put(599441, 091-239-2132)", bc.lastBlock().hash, "decided")
b.findNonce()
bc.addBlock(b, "output.txt")
b = Block(bc.lastBlock().index + 1, "get(599441)", bc.lastBlock().hash, "decided")
b.findNonce()
bc.addBlock(b, "output.txt")
#print(bc.lastBlock().prev_hash)

bc.printBlockchain()

# #send accept msg
        # with seq_lock:
        #     seq_num += 1
        #     msg = f"accept {seq_num} {PID} {depth}"
        # with accept_lock:
        #     accepted = 0
        # with decline_lock:
        #     declined = 0
        # for i in promised_list:
        #     if port_dict[i][2]:     #check if it is still connected
        #         threading.Thread(target=sendAccept, args=(sendSock_dict[i], msg)).start()
        
        # while accepted < 2 and declined < 3:
        #     with accept_cond:
        #         accept_cond.wait()
# if leader == -1:
#                         # the leader is not decided, so I will run for election
#                         print("Running for leader election")
#                         if (leaderElection()):
#                             sleep(2)
#                             stream.sendall(bytes("ack", "utf-8"))
#                         else:
#                             sleep(2)
#                             stream.sendall(bytes("failed to get leader", "utf-8"))


# msg[0] = op, msg[1] = key, msg[2] = value
        #msg = q.Queue[0][1].split(" ")