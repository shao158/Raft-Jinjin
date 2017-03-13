import zmq
import sys
import logging
import json

def main(argv):
	scriptName, cport, port = argv
	print("\n -----> Welcome to Raft Ticket Selling")
	print(" -----> Connect to Server in port: %s\n" % port)

	ctx = zmq.Context()
	pub = ctx.socket(zmq.PUB)
	pub.bind("tcp://127.0.0.1:%s" % cport)

	sub = ctx.socket(zmq.SUB)
	sub.setsockopt(zmq.SUBSCRIBE, str(cport))
	sub.connect("tcp://127.0.0.1:%s" % port)

	while True:
		c = raw_input("Do you want to buy tickets? (y/n) ")
		if c == "y":
			n = raw_input(("How many tickets you are going to buy?"
					"\nPlease enter a number:"))
			try:
				number = int(n)
				msg = rpc_client_request(number, cport)
				pub.send("{port} {msg}".format(port=port, msg=msg))
				reply = sub.recv()
				print(reply)

			except ValueError:
				print("Please input a valid number! ")
		elif c == "n":
			break
		else:
			print("Please input a valid option. ")
	print("Exit")
			
def rpc_client_request(number, cport):
	rpc = {
		'type': 'cr',
		'port': str(cport),
		'num': number
	}
	return json.dumps(rpc)

if __name__=="__main__":
	main(sys.argv)
