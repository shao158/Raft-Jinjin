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
import errno

from ZMQ_Channel import ZMQ_Channel

class Role(Enum):
	Follower = 1
	Candidate = 2
	Leader = 3

'''
Log.type = 0, normal log entry
Log.type = 1, old & new
Log.type = 2, new
'''
Log = namedtuple('Log', ['type', 'term', 'record', 'remain', 'requestedBy', 'old', 'new'])

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
	
	def __init__(self, id, conf, clus):
		self._id = int(id)
		self._host = "127.0.0.1"
		self._port = 0
		self._clientPort = 0
		self._cluster = {}
		self.loadCluster(clus)

		self._conf = conf
		self._peers = {}
		self._included = False
		self._role = Role.Follower
		self.loadConfigure(conf)

		self._follower_timeout_base = 10
		self._election_timeout_base = 3
		self._heartbeat_timeout_base = 3

		self._running = False
		self._leader = None
		self._lastUpdate = 0
		self._nextTimeout = 0
		self._currentTerm = 0
		self._votedFor = -1
		self._lastApplied = 0
		self._commitIndex = 0
		self._nextIndex = None
		self._nextIndexNew = None
		self._matchIndex = None
		self._matchIndexNew = None
		self._log = list()
		self._log.append(Log(0, 0, 100, 100, 0, 'none', 'none'))
		self._peersNew = None
		self._includedNew = False
		self._waitingMessage = None
		if self.loadState():
			self._currentTerm, self._votedFor, self._peers, self._id, self._waitingMessage, self._lastApplied, self._commitIndex = self.loadState()
		
		'''
		change info from string to int
		'''
		tmp = {}
		for k,v in self._peers.items():
			tmp[int(k)] = int(v)

		self._peers = tmp
		
		self._currentTerm = int(self._currentTerm)
		self._votedFor = int(self._votedFor)
		self._id = int(self._id)
		self._lastApplied = int(self._lastApplied)
		self._commitIndex = int(self._commitIndex)

	def loadCluster(self, cluster):
		with open(cluster, 'r') as f:
			for line in f:
				data = line.split()
				if int(data[0]) == self._id:
					self._port = int(data[1])
					self._clientPort = int(data[2])
				else:
					self._cluster[int(data[0])] = int(data[1])
		self._channel = ZMQ_Channel(self._host, self._port, self._cluster, self._clientPort)

	def loadConfigure(self, conf):
		with open(conf, 'r') as f:
			self._conf = conf
			self._peers = {}
			self._included = False
			for line in f:
				sid = int(line)
				if sid == self._id:
					self._included = True
					logging.info("Join the configuration.")
				else:
					self._peers[sid] = int(self._cluster[sid])

		if self._role == Role.Leader:
			self._nextIndex = {}
			self._matchIndex = {}
			for pid, pport in self._peers.items():
				self._nextIndex[pid] = len(self._log)
				self._matchIndex[pid] = 0

		self._includedNew = False
		self._peersNew = None
		self._nextIndexNew = None
		self._matchIndexNew = None
		logging.info("Finishing loading configuration. ")

	def loadNewConfigure(self, conf):
		self._includedNew = False
		self._peersNew = {}
		if self._role == Role.Leader:
			self._nextIndexNew = {}
			self._matchIndexNew = {}

		with open(conf, 'r') as f:
			for line in f:
				sid = int(line)
				if sid == self._id:
					self._includedNew = True
				else:
					self._peersNew[sid] = int(self._cluster[sid])
					if self._role == Role.Leader:
						self._nextIndexNew[sid] = len(self._log)
						self._matchIndexNew[sid] = 0
		
		logging.info("old peers: " + str(self._peers))
		logging.info("new peers: " + str(self._peersNew))
		logging.info("Finishing loading new configuration. ")

	def loadLog(self):
		logFile = ('/Users/jshao/Documents/Python'
						'/distributed-Sys/proj_2'
						'/server_log_%s' % self._port)
		try:
			with open(logFile) as f:
				self._log = []
				for line in f:
					sl = line.split()
					self._log.append(Log(int(sl[0]), int(sl[1]), int(sl[2]), int(sl[3]), int(sl[4]), sl[5], sl[6]))
		except IOError as e:
			if not e.errno == errno.ENOENT:
				raise

	def saveLog(self):
		logFile = ('/Users/jshao/Documents/Python'
						'/distributed-Sys/proj_2'
						'/server_log_%s' % self._port)
		with open(logFile, 'w') as w:
			for elog in self._log:
				w.write(str(elog.type) + " " + str(elog.term) + " " + str(elog.record) + " " + str(elog.remain) + " " + str(elog.requestedBy) + " " + elog.old + " " + elog.new + "\n")
		
	def loadState(self):
		self.loadLog()
		stateFile = ('/Users/jshao/Documents/Python'
						'/distributed-Sys/proj_2'
						'/server_state_%s' % self._port)
		try:
			with open(stateFile) as f:
				return json.loads(f.read())
		except IOError as e:
			if not e.errno == errno.ENOENT:
				raise
		
		return None

	def saveState(self):
		self.saveLog()
		stateFile = ('/Users/jshao/Documents/Python'
						'/distributed-Sys/proj_2'
						'/server_state_%s' % self._port)
		with open(stateFile, 'w') as w:
			w.write(json.dumps([self._currentTerm, self._votedFor, self._peers, self._id, self._waitingMessage, self._lastApplied, self._commitIndex]))
		
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
		if not (self._included or self._includedNew or self._role == Role.Leader):
			self._lastUpdate = time.time()
			self._nextTimeout = self._follower_timeout_base * (1 + random.random())
			return
 
		if self._waitingMessage is not None:
			logging.info("[ %s ] Process a waiting message" % self._role)
			self.handleMessage_cr(self._waitingMessage)
			self._waitingMessgae = None
			self.saveState()

		while self._commitIndex > self._lastApplied:
			logging.info("[ %s ] Applied a new log" % self._role)
			self._lastApplied += 1
			logging.info("New log: %s " % str(self._log[self._lastApplied]))
			if self._log[self._lastApplied].type > 0 and self._role == Role.Leader:
				if self._log[self._lastApplied].type == 1:
					logging.info("Enter into the new state.")
					oldConfigureFile = self._log[self._lastApplied].old
					newConfigureFile = self._log[self._lastApplied].new
					self._log.append(Log(2, self._currentTerm, 0, self._log[-1].remain, 0, oldConfigureFile, newConfigureFile))
					for pid, pport in self._peers.items():
						self._channel.send(self.aeRPC(pid), pport)
					for pid, pport in self._peersNew.items():
						self._channel.send(self.aeRPC(pid), pport)

					self.loadConfigure(self._log[self._lastApplied].new)

				if self._log[self._lastApplied].type == 2 and not self._included:
					logging.info("Commit all and not in the configuration. Step Down. ")
					self._role = Role.Follower
					self._nextIndex = None
					self._matchIndex = None
					return

				continue	

			if self._log[self._lastApplied].requestedBy == self._id:
				logging.info("Send reply to client")
				self._channel.send(self.crRPC_reply(True), self._clientPort)
			'''
			else:
				logging.info("Redirect reply to peers")
				self._channel.send(self.crRPC_reply(True), self._peers[self._log[self._lastApplied].requestedBy])
			'''
			self.saveState()

		now = time.time()
		
		if now - self._lastUpdate < self._nextTimeout:
			return

		if self._role == Role.Leader:
			logging.info('[ %s ] Sending heartbeat to all peers.' % str(self._role))
			self._lastUpdate = now
			self._nextTimeout = self._heartbeat_timeout_base

			for pid, pport in self._peers.items():
				self._channel.send(self.aeRPC(pid), pport)
			if self._peersNew is not None:
				for pid, pport in self._peersNew.items():
					if pid not in self._peers:
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
		self.saveState()
		self._supporters = set()
		self._supporters.add(self._id)

		if self._peersNew is not None:
			self._supportersNew = set()
			if self._includedNew:
				self._supportersNew.add(self._id)
			for pid, pport in self._peersNew.items():
				if pid not in self._peers:
					self._channel.send(self.rvRPC(), pport)

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
		if (not rpc_type == 'ae') and (not self._included) and (not self._includedNew) and (not self._role == Role.Leader):
			return

		rpc_term = 0
		if 'term' in rpc:
			rpc_term = int(rpc['term'])
		
		if 'id' in rpc:
			if (int(rpc['id']) not in self._peers) and (self._peersNew is not None and int(rpc['id']) not in self._peersNew):
				return

		if rpc_term > self._currentTerm:
			self._leader = None
			self._currentTerm = rpc_term
			self._votedFor = -1
			self._role = Role.Follower
			self.saveState()

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
		if int(msg['term']) < self._currentTerm:
			self._channel.send(self.aeRPC_reply(False, ackIndex), self._peers[int(msg['id'])])
			return

		'''
		Candidate need to update logs
		'''
		if self._role is Role.Candidate:
			logging.info('[ %s ] Detect a leader. Become Follower' % self._role)
			self._role = Role.Follower
		
		self._leader = int(msg['id'])
		self._currentTerm = int(msg['term'])
		self._lastUpdate = time.time()
		self._nextTimeout = self._follower_timeout_base * (1 + random.random())
		self.saveState()

		if prevIndex < len(self._log) and self._log[prevIndex].term == ackTerm:
			'''
			Delete Conflict Entries
			'''
			while len(self._log) > (prevIndex + 1):
				logging.info("[ %s ] Deleting conflict entry" % self._role)
				if self._log[-1].type > 0:
					logging.info("[ %s ] Deleting configure change entry." % self._role)
				elif self._log[-1].requestedBy == self._id:
					logging.info("Send reply to client")
					self._channel.send(self.crRPC_reply(False), self._clientPort)
				else:
					logging.info("Send reply to peers")
					tmpId = self._log[-1].requestedBy
					if tmpId in self._peers:
						self._channel.send(self.crRPC_reply(False), self._peers[tmpId])
					if self._peersNew is not None:
						if tmpId in self._peersNew:
							self._channel.send(self.crRPC_reply(False), self._peersNew[tmpId])
				self._log = self._log[:-1]
			if int(msg['leaderCommit']) > self._commitIndex:
				logging.info("[ %s ] Update commit index" % self._role)
				self._commitIndex = min(int(msg['leaderCommit']), ackIndex)
				self.saveState()

			if len(msg['entries']) == 0:
				logging.debug('[ %s ] Received a heartbeat.' % self._role)
				return;

			logging.info("Appending entries")
			for newLog in msg['entries']:
				self._log.append(Log(int(newLog[0]), int(newLog[1]), int(newLog[2]), int(newLog[3]), int(newLog[4]), newLog[5], newLog[6]))
				if self._log[-1].type == 1:
					self.loadNewConfigure(self._log[-1].new)
					logging.info("Enter into (old + new) State. ")
				if self._log[-1].type == 2:
					logging.info("Enter into new State. ")
					self.loadConfigure(self._log[-1].new)

			if int(msg['id']) in self._peers:
				self._channel.send(self.aeRPC_reply(True, ackIndex), self._peers[int(msg['id'])])
			elif self._peersNew is not None and int(msg['id']) in self._peersNew:
				self._channel.send(self.aeRPC_reply(True, ackIndex), self._peersNew[int(msg['id'])])
			else:
				self._channel.send(self.aeRPC_reply(True, ackIndex), self._cluster[int(msg['id'])])
			self.saveState()
		else:
			logging.info("Prev Log Index does not match")
			if int(msg['id']) in self._peers:
				self._channel.send(self.aeRPC_reply(False, ackIndex), self._peers[int(msg['id'])])
			elif self._peersNew is not None and int(msg['id']) in self._peersNew:
				self._channel.send(self.aeRPC_reply(False, ackIndex), self._peersNew[int(msg['id'])])

	def handleMessage_ae_reply(self, msg):
		if self._role == Role.Follower:
			logging.warning("Follower Received a AE Reply! ")
			self._leader = int(msg['id'])
			self._currentTerm = int(msg['term'])
			self._lastUpdate = time.time()
			self._nextTimeout = self._follower_timeout_base * (1 + random.random())
			self.saveState()
			return

		'''
		TODO: Is this possible?
		'''
		if self._role == Role.Candidate:
			logging.warning("Candidate Received a AE Reply! ")
			return
		
		sid = int(msg['id'])
		if msg['success']:
			logging.info("Get success append reply")
			if sid in self._peers:
				self._matchIndex[sid] = int(msg['index'])
				self._nextIndex[sid] = self._matchIndex[sid] + 1
			if self._peersNew is not None and sid in self._peersNew:
				self._matchIndexNew[sid] = int(msg['index'])
				self._nextIndexNew[sid] = self._matchIndexNew[sid] + 1

			majorityIndex = self.getMajorityIndex()
			if majorityIndex > self._commitIndex and self._log[majorityIndex].term == self._currentTerm:
				logging.info("Update commit Index")
				self._commitIndex = majorityIndex
				self.saveState()
		else:
			logging.info("Cannot append entries. Will retry.")
			if sid in self._peers:
				self._nextIndex[sid] -= 1
			if self._peersNew is not None and sid in self._peersNew:
				self._nextIndexNew[sid] -= 1

	def handleMessage_rv(self, msg):
		if self._role is Role.Candidate or int(msg['term']) < self._currentTerm:
			if int(msg['id']) in self._peers:
				self._channel.send(self.rvRPC_reply(False), self._peers[int(msg['id'])])
			elif self._peersNew is not None and int(msg['id']) in self._peersNew:
				self._channel.send(self.rvRPC_reply(False), self._peersNew[int(msg['id'])])
			return

		if int(msg['lastLogTerm']) < self._log[-1].term or (int(msg['lastLogTerm']) == self._log[-1].term and int(msg['lastLogIndex']) < len(self._log) - 1):
			logging.info("Candiate log is less up-to-date. Don't Vote. ")
			if int(msg['id']) in self._peers:
				self._channel.send(self.rvRPC_reply(False), self._peers[int(msg['id'])])
			elif self._peersNew is not None and int(msg['id']) in self._peersNew:
				self._channel.send(self.rvRPC_reply(False), self._peersNew[int(msg['id'])])
			return;

		if self._votedFor == -1 or self._votedFor == int(msg['id']):
			self._votedFor = int(msg['id'])
			self.saveState()
			if int(msg['id']) in self._peers:
				self._channel.send(self.rvRPC_reply(True), self._peers[int(msg['id'])])
			elif self._peersNew is not None and int(msg['id']) in self._peersNew:
				self._channel.send(self.rvRPC_reply(True), self._peersNew[int(msg['id'])])
			self._lastUpdate = time.time()
			self._nextTimeout = self._follower_timeout_base * (1 + random.random())
			return
			
		if int(msg['id']) in self._peers:
			self._channel.send(self.rvRPC_reply(False), self._peers[int(msg['id'])])
		elif self._peersNew is not None and int(msg['id']) in self._peersNew:
			self._channel.send(self.rvRPC_reply(False), self._peersNew[int(msg['id'])])

	def handleMessage_rv_reply(self, msg):
		if self._role is Role.Candidate and msg['voteGranted'] == True:
			self._supporters.add(int(msg['id']))
			logging.info("Get Votes %d" % len(self._supporters))
			getMajorityNew = True
			if self._peersNew is not None:
				self._supportersNew.add(int(msg['id']))
				logging.info("Get Votes New %d" % len(self._supportersNew))
				thresholdVote = len(self._peersNew)
				if self._includedNew:
					thresholdVote += 1
				if len(self._supportersNew) <= (thresholdVote / 2):
					getMajorityNew = False

			if len(self._supporters) > ((len(self._peers) + 1) / 2) and getMajorityNew:
				logging.info('Become a leader')
				self._matchIndex = {}
				for pid, pport in self._peers.items():
					self._matchIndex[pid] = 0
				if self._peersNew is not None:
					self._matchInexNew = {}
					for pid, pport in self._peersNew.items():
						if pid not in self._peers:
							self._matchIndexNew[pid] = 0
				self._nextIndex = {}
				logLen = len(self._log)
				for pid, pport in self._peers.items():
					self._nextIndex[pid] = logLen
				if self._peersNew is not None:
					self._nextIndexNew = {}
					for pid, pport in self._peersNew.items():
						if pid not in self._peers:
							self._nextIndexNew[pid] = 0
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
				self.saveState()
			else:
				logging.info("[ %s ] Redirect request to leader" % self._role)
				self._channel.send(json.dumps(msg), self._peers[int(self._leader)])
		elif self._role == Role.Candidate:
			logging.info("[ %s ] There is no leader. Store message" % self._role)
			self._waitingMessage = msg
			self.saveState()
		elif self._role == Role.Leader:
			logging.info("[ %s ] Get client Request" % self._role)
			requestNum = int(msg['num'])
			if requestNum > self._log[-1].remain:
				logging.info("Request too many")
				if int(msg['id']) == self._id:
					logging.info("Reply to client")
					self._channel.send(self.crRPC_reply(False), self._clientPort)
				else:
					logging.info("Redirect reply to peers")
					self._channel.send(self.crRPC_reply(False), self._peers[int(msg['id'])])
			else:
				logging.info("Success. Sell %s tickets" % str(requestNum))
				newRemain = self._log[-1].remain - requestNum
				logging.info("Remain %s" % str(newRemain))
				self._log.append(Log(0, self._currentTerm, requestNum, newRemain, int(msg['id']), 'none', 'none'))
				self.saveState()
				self._lastUpdate = time.time()
				self._nextTimeout = self._heartbeat_timeout_base * (1 + random.random())
				for pid, pport in self._peers.items():
					self._channel.send(self.aeRPC(pid), pport)

	def handleMessage_cr_reply(self, msg):
		logging.info("Got client request reply. Send to client")
		self._channel.send(json.dumps(msg), self._clientPort)

	def handleMessage_configure(self, msg):
		logging.info("Got configure changes request. ")
		
		if self._role == Role.Leader:
			logging.info("Enter into (new + old) state.")
			self._log.append(Log(1, self._currentTerm, 0, self._log[-1].remain, 0, self._conf, msg['fname']))
			self.loadNewConfigure(msg['fname'])
			for pid, pport in self._peers.items():
				self._channel.send(self.aeRPC(pid), pport)
			for pid, pport in self._peersNew.items():
				if pid not in self._peers:
					self._channel.send(self.aeRPC(pid), pport)

		elif self._leader == None:
			logging.info("There is no leader. Abort configuration change.")
		else:
			logging.info("Redirect configure changes to the leader.")
			self._channel.send(json.dumps(msg), self._peers[self._leader])

	def getMajorityIndex(self):
		m1 = -1
		m2 = -1
		cnt = Counter(list(self._matchIndex.values()))
		sortedCnt = sorted(cnt.items(), key=operator.itemgetter(0), reverse=True)

		total = 1
		for k,v in sortedCnt:
			total += v
			if total > ((len(self._peers) + 1) / 2):
				m1 = k
				break

		if self._peersNew is not None:
			cnt = Counter(list(self._matchIndexNew.values()))
			sortedCnt = sorted(cnt.items(), key=operator.itemgetter(0), reverse=True)

			total = 0
			if self._includedNew:
				total = 1
			for k,v in sortedCnt:
				total += v
				if total > ((len(self._peersNew) + 1) / 2):
					m2 = k
					break
		else:
			logging.info("Returning " + str(m1))
			return m1

		logging.info("Comparing and Returning " + str(m1))
		return min(m1, m2)

	def aeRPC(self, serverId):
		prevLogInd = -1
		if serverId in self._peers:
			prevLogInd = self._nextIndex[serverId] - 1
		elif serverId in self._peersNew:
			prevLogInd = self._nextIndexNew[serverId] - 1
		else:
			logging.info("Error: Unrecognized Id: " + str(serverId))

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
	s = Server(int(argv[1]), argv[2], argv[3])
	s.start()

if __name__=='__main__':
	main(sys.argv)
