from enum import Enum
from collections import namedtuple
from collections import Counter
import zmq
import sys
import time
import json
import logging
import random
import operator

from ZMQ_Channel import ZMQ_Channel

class Role(Enum):
	Follower = 1
	Candidate = 2
	Leader = 3

Log = namedtuple('Log', ['term', 'record', 'remain', 'requestedBy'])

'''
Server Implementation
	id:          server's identifier
	host, port:  for now, only support localhost
	peers:       dictionary of pairs [id, port] for peer servers
	running:     server state
	lastUpdate:  For leaders, record the last time of sending
                 heartbeats;
                 Otherwise, record the last time of receiving
                 heartbeats or starting election
	nextTimeout: the next timeout to check
	newPeers:    Used in configuration change phrase

	waitingMessage: When server is candidate, the client's request will
				 be stored here until there is a new leader
'''

class Server(object):
	
	def __init__(self, id, conf):
		self._id = id
		self._host = "127.0.0.1"
		self._port = "0000"
		self._peers = {}
		self._clientPort = "0000"
		self.loadConfigure(conf, id)

		self._follower_timeout_base = 10
		self._election_timeout_base = 3
		self._heartbeat_timeout_base = 3

		self._running = False
		self._leader = None
		self._lastUpdate = 0
		self._nextTimeout = 0
		self._currentTerm = 0
		self._role = Role.Follower
		self._votedFor = -1
		self._lastApplied = 0
		self._commitIndex = 0
		self._nextIndex = [1] * len(conf)
		self._matchIndex = [0] * len(conf)
		self._log = list()
		self._log.append(Log(0, 100, 100, 0))
		self._newPeers = {}
		self._waitingMessage = None
		self._channel = ZMQ_Channel(self._host, self._port, self._peers, self._clientPort)
	
	def loadConfigure(self, conf, id):
		with open(conf, 'r') as f:
			for line in f:
				data = line.split()
				if int(data[0]) == id:
					self._port = int(data[1])
					self._clientPort = data[2]
				else:
					self._peers[int(data[0])] = int(data[1])
			
	def loadState(self):
		stateFile = ('/Users/jshao/Documents/Python'
						'/distributed-Sys/proj_2'
						'/server_state_%s' % self._port)
		try:
			with open(stateFile) as f:
				return pickle.loads(f.read())
		except IOError as e:
			if not e.errno == errno.ENOENT:
				raise

		return 0, -1, [], {} 

	def saveState(self):
		stateFile = ('/Users/jshao/Documents/Python'
						'/distributed-Sys/proj_2'
						'/server_state_%s' % self._port)
		with open(stateFile, 'w') as w:
			w.write(pickle.dumps(self._currentTerm, self._votedFor, self._log, self._peers, self._id), -1)
		
	def start(self):
		logging.info('Server Start')
		self._running = True
		self._lastUpdate = time.time()
		self._nextTimeout = self._follower_timeout_base * (1 + random.random())
		
		while self._running:
			msg = self._channel.recv(150)
			if msg is not None:
				logging.info('[ %s ] Got a message: ' % str(self._role))
				self.handleMessage(msg)

			self.houseKeeping()

	def houseKeeping(self):
		if self._waitingMessage is not None:
			logging.info("[ %s ] Process a waiting message" % self._role)
			self.handleMessage_cr(self._waitingMessage)
			self._waitingMessgae = None

		while self._commitIndex > self._lastApplied:
			logging.info("[ %s ] Applied a new log" % self._role)
			self._lastApplied += 1
			logging.info("New log: %s " % str(self._log[self._lastApplied]))
			if self._log[self._lastApplied].requestedBy == self._id:
				logging.info("Send reply to client")
				self._channel.send(self.crRPC_reply(True), self._clientPort)
			else:
				logging.info("Redirect reply to peers")
				self._channel.send(self.crRPC_reply(True), self._peers[self._log[self._lastApplied].requestedBy])

		now = time.time()
		
		if now - self._lastUpdate < self._nextTimeout:
			return

		if self._role == Role.Leader:
			logging.info('[ %s ] Sending heartbeat to all peers.' % str(self._role))
			self._lastUpdate = now
			self._nextTimeout = self._heartbeat_timeout_base

			for pid, pport in self._peers.items():
				self._channel.send(self.aeRPC(pid), pport)

		elif self._role == Role.Follower:
			self._leader = None
			logging.info(('[ Follower ]'
					' No response from the leader. Start election.'))
			self.callElection()

		elif self._role == Role.Candidate:
			logging.info(('[ Candidate ] '
				'Election timeout. Get %s Votes.'
				' Start election again.' % str(len(self._supporters))))
			self.callElection()

		
	def callElection(self):
		logging.info('Become Candidate. \nUpdate term from %d' % self._currentTerm) 
		self._role = Role.Candidate
		self._currentTerm += 1
		self._votedFor = self._id
		#self.saveState()
		self._supporters = set()
		self._supporters.add(self._id)
		self._lastUpdate = time.time()
		self._nextTimeout = self._election_timeout_base * (1 + random.random())
		
		for pid, pport in self._peers.items():
			self._channel.send(self.rvRPC(), pport)

	def handleMessage(self, msg):
		msgStart = msg.index(" ") + 1 
		rpc = json.loads(msg[msgStart:])
		if 'entries' not in rpc or len(rpc['entries']) > 0:
			logging.info(rpc)

		rpc_type = rpc['type']
		rpc_term = 0
		if 'term' in rpc:
			rpc_term = rpc['term']
		
		if rpc_term > self._currentTerm:
			self._leader = None
			self._currentTerm = rpc_term
			self._votedFor = -1
			self._role = Role.Follower

		handler_name = 'handleMessage_%s' % rpc_type
		if hasattr(self, handler_name):
			getattr(self, handler_name)(rpc)

	def handleMessage_ae(self, msg):
		if self._role is Role.Leader:
			logging.warning('hah')
			return

		prevIndex = int(msg['prevLogIndex'])
		ackIndex = prevIndex + len(msg['entries'])
		ackTerm = int(msg['prevLogTerm'])
		if msg['term'] < self._currentTerm:
			self._channel.send(self.aeRPC_reply(False, ackIndex), self._peers[msg['id']])
			return

		'''
		Candidate need to update logs
		'''
		if self._role is Role.Candidate:
			logging.info('[ %s ] Detect a leader. Become Follower' % self._role)
			self._role = Role.Follower
		
		self._leader = int(msg['id'])
		self._currentTerm = msg['term']
		self._lastUpdate = time.time()
		self._nextTimeout = self._follower_timeout_base * (1 + random.random())
		
		if int(msg['leaderCommit']) > self._commitIndex:
				logging.info("[ %s ] Update commit index" % self._role)
				self._commitIndex = min(int(msg['leaderCommit']), ackIndex)

		if len(msg['entries']) == 0:
			logging.debug('[ %s ] Received a heartbeat.' % self._role)
			return
		
		if self._log[prevIndex].term == ackTerm:
			'''
			Delete Conflict Entries
			'''	
			while len(self._log) > (prevIndex + 1):
				logging.info("[ %s ] Deleting conflict entry" % self._role)
				if self._log[-1].requestedBy == self._id:
					logging.info("Send reply to client")
					self._channel.send(self.crRPC_reply(False), self._clientPort)
				else:
					logging.info("Send reply to peers")
					self._channel.send(self.crRPC_reply(False), self._peers[self._log[-1].requestedBy])
				self._log = self._log[:-1]
			logging.info("Appending entries")
			for newLog in msg['entries']:
				self._log.append(Log(newLog[0], newLog[1], newLog[2], newLog[3]))
			self._channel.send(self.aeRPC_reply(True, ackIndex), self._peers[msg['id']])
		else:
			logging.info("Prev Log Index does not match")
			self._channel.send(self.aeRPC_reply(False, ackIndex), self._peers[msg['id']])

	def handleMessage_ae_reply(self, msg):
		if self._role == Role.Follower:
			self._leader = int(msg['id'])
			self._currentTerm = msg['term']
			self._lastUpdate = time.time()
			self._nextTimeout = self._follower_timeout_base * (1 + random.random())
			return

		'''
		TODO: Is this possible?
		'''
		if self._role == Role.Candidate:
			return
		
		sid = int(msg['id'])
		if msg['success']:
			logging.info("Get success append reply")
			self._matchIndex[sid - 1] = int(msg['index'])
			self._nextIndex[sid - 1] = self._matchIndex[sid - 1] + 1
			majorityIndex = self.getMajorityIndex()
			logging.info("Get majority index %s" % str(majorityIndex))
			if majorityIndex > self._commitIndex and self._log[majorityIndex].term == self._currentTerm:
				logging.info("Update commit Index")
				self._commitIndex = majorityIndex
		else:
			logging.info("Cannot append entries. Will retry.")
			self._nextIndex[sid - 1] -= 1

	def handleMessage_rv(self, msg):
		if self._role is Role.Candidate or msg['term'] < self._currentTerm:
			self._channel.send(self.rvRPC_reply(False), self._peers[msg['id']])
			return

		if self._votedFor == -1 or self._votedFor == msg['id']:
			self._votedFor = msg['id']
			#self.saveState()
			self._channel.send(self.rvRPC_reply(True), self._peers[msg['id']])
			self._lastUpdate = time.time()
			self._nextTimeout = self._follower_timeout_base * (1 + random.random())
			return
			
		self._channel.send(self.rvRPC_reply(False), self._peers[msg['id']])
		
	def handleMessage_rv_reply(self, msg):
		if self._role is Role.Candidate and msg['voteGranted'] == True:
			self._supporters.add(msg['id'])
			logging.info("Get Votes %d" % len(self._supporters))
			if len(self._supporters) > (len(self._peers) / 2):
				logging.info('Become a leader')
				self._matchIndex = [0] * (len(self._peers) + 1)
				self._nextIndex = [len(self._log)] * (len(self._peers) + 1)
				self._role = Role.Leader

	def handleMessage_cr(self, msg):
		'''
		Mark the client request with the server id that first
		receives this request
		'''
		if 'id' not in msg:
			msg['id'] = self._id

		if self._role == Role.Follower:
			if self._leader is None:
				logging.info("[ %s ] There is no leader. Store message" % self._role)
				self._waitingMessage = msg
			else:
				logging.info("[ %s ] Redirect request to leader" % self._role)
				self._channel.send(json.dumps(msg), self._peers[self._leader])
		elif self._role == Role.Candidate:
			logging.info("[ %s ] There is no leader. Store message" % self._role)
			self._waitingMessage = msg
		elif self._role == Role.Leader:
			logging.info("[ %s ] Get client Request" % self._role)
			requestNum = int(msg['num'])
			if requestNum > self._log[-1].remain:
				logging.info("Request too many")
				if msg['id'] == self._id:
					logging.info("Reply to client")
					self._channel.send(self.crRPC_reply(False), self._clientPort)
				else:
					logging.info("Redirect reply to peers")
					self._channel.send(self.crRPC_reply(False), self._peers[msg['id']])
			else:
				logging.info("Success. Sell %s tickets" % str(requestNum))
				newRemain = self._log[-1].remain - requestNum
				logging.info("Remain %s" % str(newRemain))
				self._log.append(Log(self._currentTerm, requestNum, newRemain, msg['id']))
				self._lastUpdate = time.time()
				self._nextTimeout = self._heartbeat_timeout_base * (1 + random.random())
				for pid, pport in self._peers.items():
					self._channel.send(self.aeRPC(pid), pport)

	def handleMessage_cr_reply(self, msg):
		logging.info("Got client request reply. Send to client")
		self._channel.send(json.dumps(msg), self._clientPort)

	def getMajorityIndex(self):
		cnt = Counter(self._matchIndex)
		sortedCnt = sorted(cnt.items(), key=operator.itemgetter(0), reverse=True)

		total = 1
		for k,v in sortedCnt:
			total += v
			if total > (len(self._peers) / 2):
				return k

	def aeRPC(self, serverId):
		prevLogInd = self._nextIndex[serverId - 1] - 1
		rpc = {
			'type': 'ae',
			'term': self._currentTerm,
			'id': self._id,
			'prevLogIndex': prevLogInd, 
			'prevLogTerm': self._log[prevLogInd].term,
			'entries': self._log[(prevLogInd + 1):],
			'leaderCommit': self._commitIndex
		}
		return json.dumps(rpc)

	def aeRPC_reply(self, success, index):
		rpc = {
			'type': 'ae_reply',
			'term': self._currentTerm,
			'id': self._id,
			'success': success,
			'index': index
		}
		return json.dumps(rpc)

	def rvRPC(self):
		l = len(self._log)
		rpc = {
			'type': 'rv',
			'term': self._currentTerm,
			'id': self._id,
			'lastLogIndex': l - 1,
			'lastLogTerm': self._log[l - 1].term
		}
		return json.dumps(rpc)

	def rvRPC_reply(self, granted):
		rpc = {
			'type': 'rv_reply',
			'term': self._currentTerm,
			'id': self._id,
			'voteGranted': granted
		}
		return json.dumps(rpc)

	def crRPC_reply(self, finished):
		rpc = {
			'type': 'cr_reply',
			'success': finished
		}
		return json.dumps(rpc)

def main(argv):
	logging.basicConfig(level=logging.DEBUG)
	s = Server(int(argv[1]), ('/Users/jshao/Documents/Python'
					'/distributed-Sys/proj_2'
					'/configure.cfg'))
	s.start()

if __name__=='__main__':
	main(sys.argv)
