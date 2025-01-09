import socket
import sys 
from hosts import hosts_dict
import time

# try: 
# 	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
# 	print ("Socket successfully created")
# except socket.error as err: 
# 	print ("socket creation failed with error %s" %(err))

port = 12345

#connect to all machines and get logs
#append logs and count
#print to terminal and dump in a log file
#Handle edge cases 1. host is down/throws exception
#other edge cases

def write_output(host_name, lines_matched, grep_output):
	print('Hostname:', host_name)
	print('Num of lines matched:', lines_matched)
	print(grep_output)

	with open('output.log', 'w') as log_file:
		print(f'Hostname: {host_name}', file=log_file)
		print(f'Num of lines matched: {lines_matched}', file=log_file)
		print(f'{grep_output}', file=log_file)
		print("This message will be written to the log file.", file=log_file)

grep_command = sys.argv[1:]
output_dict = {}
total_lines_matched = 0

start_time = time.time()
for _, host_name in hosts_dict.items():
	try:
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.connect((host_name, port))
		print(grep_command)
		s.sendall(",".join(grep_command).encode())
		grep_output = ""
		while True:
			data = s.recv(2)
			grep_output += data.decode()
			
			if not data:
				break
			
		if grep_output == "":
			lines_matched = 0
		else:
			lines_matched = len(grep_output.split('\n'))
		
		total_lines_matched += lines_matched
		write_output(host_name, lines_matched, grep_output)
		output_dict[host_name] = grep_output
	except Exception as e:
		write_output(host_name, 0, '')
		print('Error connecting to host', e) #Better error?
		output_dict[host_name] = ""
	finally:
		s.close()

end_time = time.time()
execution_time = end_time - start_time
print("Execution time:", execution_time)

print("total lines matched:", total_lines_matched)

