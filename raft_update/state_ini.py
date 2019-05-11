import logging
import random
import pickle
from raft.datacenter import LogEntry
import json

CONFIG = json.load(open('config.json'))




# 前两个参数是self.term，self.index
log = [LogEntry(0, 0, {'config':'single', 'data':CONFIG['datacenters']})]
#当前term
current_term = 0
voted_for = None
dictobj = {'current_term': current_term, 'voted_for': voted_for, 'log': log}
filename = "./state1.pkl"
fileobj = open(filename, 'wb')
pickle.dump(dictobj, fileobj)
fileobj.close()
filename = "./state2.pkl"
fileobj = open(filename, 'wb')
pickle.dump(dictobj, fileobj)
fileobj.close()
filename = "./state3.pkl"
fileobj = open(filename, 'wb')
pickle.dump(dictobj, fileobj)
fileobj.close()
filename = "./state4.pkl"
fileobj = open(filename, 'wb')
pickle.dump(dictobj, fileobj)
fileobj.close()
filename = "./state5.pkl"
fileobj = open(filename, 'wb')
pickle.dump(dictobj, fileobj)
fileobj.close()