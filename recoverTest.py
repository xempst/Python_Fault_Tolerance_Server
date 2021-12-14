from util import recoverBlockchain, updateDict

bc = recoverBlockchain("test.txt")
bc.printBlockchain()

data_dict = dict()
data_dict = updateDict(bc)
print(data_dict)