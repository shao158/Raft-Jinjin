from enum import Enum
from collections import namedtuple
import zmq
import sys
import time
import json
import logging
import random

from ZMQ_Channel import ZMQ_Channel

'''
Utility in Server
'''
class Role(Enum):
	Follower = 1
	Candidate = 2
	Leader = 3

Log = namedtuple('Log', ['term', 'record'])

class Server(object):
	
	def __init__(self, id, conf):
		self._id = id
		self._host = "127.0.0.1"
		self._port = "0000"
		self._peers = {}
		self.loadConfigure(conf, id)

		self._follower_timeout_base = 10
		self._election_timeout_base = 3
		self._heartbeat_timeout_base = 3

		self._running = False
		self._lastUpdate = 0
		self._nextExpire = 0
		self._currentTerm = 0
		self._role = Role.Follower
		self._votedFor = -1
		self._nextIndex = [0] * len(conf)
		self._log = list()
		self._log.append(Log(0, "+100"))
		self._newPeers = {}
		self._channel = ZMQ_Channel(self._host, self._port, self._peers)
	
	def loadConfigure(self, conf, id):
		with open(conf, 'r') as f:
			for line in f:
				data = line.split()
				if int(data[0]) == id:
					self._port = int(data[1])
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
		now = time.time()
		
		if now - self._lastUpdate < self._nextTimeout:
			return

		if self._role == Role.Leader:
			logging.info('[ Leader ] Sending heartbeat to all peers.')
			self._lastUpdate = now
			self._nextTimeout = self._heartbeat_timeout_base

			for pid, pport in self._peers.items():
				self._channel.send(self.aeRPC(), pport)

		elif self._role == Role.Follower:
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
		logging.info(rpc)

		rpc_type = rpc['type']
		rpc_term = rpc['term']
		
		if rpc_term > self._currentTerm:
			self._currentTerm = rpc_term
			self._votedFor = -1
			self._role = Role.Follower

		handler_name = 'handleMessage_%s' % rpc_type
		if hasattr(self, handler_name):
			getattr(self, handler_name)(rpc)

	def handleMessage_ae(self, msg):
		if self._role is Role.Leader:
			print('hah')
			return

		if msg['term'] < self._currentTerm:
			return

		'''
		Candidate need to update logs
		'''
		if self._role is Role.Candidate:
			logging.info('To Follower')
			self._role = Role.Follower
		
		self._leader = msg['id']
		self._currentTerm = msg['term']
		self._lastUpdate = time.time()
		self._nextTimeout = self._follower_timeout_base * (1 + random.random())
		
		if len(msg['entries']) == 0:
			return

	def handleMessage_ae_reply(self, msg):
		return

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
				self._role = Role.Leader

	def aeRPC(self):
		rpc = {
			'type': 'ae',
			'term': self._currentTerm,
			'id': self._id,
			'prevLogIndex': 0, 
			'prevLogTerm': 0,
			'entries': [],
			'leaderCommit': 0,
		}
		return json.dumps(rpc)

	def aeRPC_reply(self, success):
		rpc = {
			'type': 'ae_reply',
			'term': self._currentTerm,
			'id': self._id,
			'success': success
		}
		return json.dumps(rpc)

	def rvRPC(self):
		rpc = {
			'type': 'rv',
			'term': self._currentTerm,
			'id': self._id,
			'lastLogIndex': len(self._log) - 1,
			'lastLogTerm': self._log[0].term
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

def main(argv):
	logging.basicConfig(level=logging.DEBUG)
	s = Server(int(argv[1]), ('/Users/jshao/Documents/Python'
					'/distributed-Sys/proj_2'
					'/configure.cfg'))
	s.start()

if __name__=='__main__':
	main(sys.argv)
