from hashlib import sha256

class Block:
    def __init__(self, index, operation, prev_hash, decision, nonce=0):
        self.index = index
        self.operation = operation
        self.prev_hash = prev_hash
        self.decision = decision
        self.nonce = nonce
    
    def findNonce(self):
        self.nonce = 0
        computed_hash = self.hashFunction()
        while not (computed_hash.endswith('0') or computed_hash.endswith('1') or computed_hash.endswith('2')):
            self.nonce += 1
            computed_hash = self.hashFunction()
    
    def hashFunction(self):
        block_string = str(self.index) + self.operation + str(self.nonce) + self.prev_hash
        return sha256(block_string.encode()).hexdigest()
    
    def writeBlock(self, filePath):
        f = open(filePath, "a")
        f.write(str(self.index) + "\n")
        f.write(self.operation + "\n")
        f.write(self.prev_hash + "\n")
        f.write(str(self.nonce) + "\n")
        f.write(self.decision + "\n")
        f.close()