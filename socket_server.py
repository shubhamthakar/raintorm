import socket			 
import sys
from grepper import grep_from_python

s = socket.socket()		 
print ("Socket successfully created")
port = 12346

# Next bind to the port 
# we have not typed any ip in the ip field 
# instead we have inputted an empty string 
# this makes the server listen to requests 
# coming from other computers on the network 
s.bind(('', port))		 
print ("socket binded to %s" %(port)) 

s.listen(5)	 
print ("socket is listening")		 

while True: 

    # Establish connection with client. 
    c, addr = s.accept()	
    print ('Got connection from', addr )
    recvd_data = c.recv(1024) 
    print("Received from client:",recvd_data.decode())

    grep_data = grep_from_python(recvd_data.decode())
    c.send(grep_data.encode()) 
    c.close()
