from blockchain import Blockchain
from block import Block

def recoverBlockchain(filePath):
    bc = Blockchain()
    with open(filePath, "r") as reader:
        line = reader.readline()
        while line != "":
            index = int(line)
            operation = reader.readline()[:-1]
            prev_hash = reader.readline()[:-1]
            nonce = int(reader.readline())
            decided = reader.readline()[:-1]
            b = Block(index, operation, prev_hash, decided, nonce)
            bc.addBlock(b)
            line = reader.readline()
    return bc

def updateDict(bc):
    data_dict = dict()
    for block in bc.chain[1:]:
        op = block.operation.split(" ")
        if op[0] == "put":
            data_dict[op[1]] = op[2]
    return data_dict