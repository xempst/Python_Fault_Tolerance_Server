from block import Block

class Blockchain:
    def __init__(self):
        self.chain = []
        self.create_first_block()
    
    def create_first_block(self):
        first_block = Block(0, "0th block", "decided", "0")
        first_block.findNonce()
        first_block.hash = first_block.hashFunction()
        self.chain.append(first_block)
    
    def lastBlock(self):
        return self.chain[-1]
    
    def addBlock(self, block):
        block.hash = block.hashFunction()
        self.chain.append(block)
    
    def printBlockchain(self):
        for block in self.chain[1:]:
            print(block.index)
            print(block.operation)
            print(block.prev_hash)
            print(block.nonce)
            print(block.decision)
    
    def rewriteBlockchain(self, filePath):
        f = open(filePath, "w")
        for block in self.chain[1:]:
            f.write(str(block.index) + "\n")
            f.write(block.operation + "\n")
            f.write(block.prev_hash + "\n")
            f.write(str(block.nonce) + "\n")
            f.write(block.decision + "\n")
        f.close()