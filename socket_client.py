# An example script to connect to Google using socket 
# programming in Python 
import socket # for socket 
import sys 

try: 
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
	print ("Socket successfully created")
except socket.error as err: 
	print ("socket creation failed with error %s" %(err))

# default port for socket 
port = 12345

try: 
	host_ip = "172.24.80.1"
except socket.gaierror: 

	# this means could not resolve the host 
	print ("there was an error resolving the host")
	sys.exit() 

# connecting to the server 
s.connect((host_ip, port)) 
# receiving data from the server

message = sys.argv[1:]
s.sendall(" ".join(message).encode())

final_data = ""

while True:
	data = s.recv(2)
	final_data += data.decode()
	
	if not data:
		break

print("Received data:", final_data)

print (f"the socket has successfully connected to {host_ip} on port == {port}") 
