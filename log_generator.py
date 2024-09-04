import os
import random

# Create the log directory if it doesn't exist
log_dir = 'log'
os.makedirs(log_dir, exist_ok=True)

# List of possible HTTP request statuses
statuses = ['200 OK', '404 Not Found', '500 Internal Server Error', '302 Found']

# Generate 10 random log files
for i in range(100):
    # Generate a random file name
    file_name = f'log.txt'
    
    # Generate a random HTTP request status
    status = random.choice(statuses)
    
    # Generate the log message
    log_message = f'[{status}] Request from {random.choice(["127.0.0.1", "192.168.0.1"])}'
    
    # Write the log message to the file
    with open(os.path.join(log_dir, file_name), 'a') as file:
        file.write(log_message + '\n')
        
    # print(f'appended log to log file file: {file_name}')