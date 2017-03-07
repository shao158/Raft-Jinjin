import zmq
import logging

class ZMQ_Channel(object):
	def __init__(self, host, port, peers):
		ctx = zmq.Context()
		self._pub = ctx.socket(zmq.PUB)
		self._pub.bind("tcp://{addr}:{port}".format(addr=host, port=port))
		self._sub = ctx.socket(zmq.SUB)
		self._sub.setsockopt(zmq.SUBSCRIBE, str(port))
		for pid, pport in peers.items():
			self._sub.connect("tcp://127.0.0.1:{port}".format(port=pport))
	
	def send(self, msg, port):
		self._pub.send("{port} {msg}".format(port=port, msg=msg))

	def recv(self, timeout):
		
		self._sub.RCVTIMEO = timeout
	
		try:
			package = self._sub.recv()
			return package
		except zmq.error.Again:
			return None
