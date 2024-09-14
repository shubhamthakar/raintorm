import os
import random

# Create the log directory if it doesn't exist
gen_log_dir = 'generated_logs'
os.makedirs(gen_log_dir, exist_ok=True)

# List of possible HTTP request statuses
known_logs = ['[200 OK] Request from 127.0.0.1']
statuses = ['200 OK', '404 Not Found', '500 Internal Server Error', '302 Found']
ips = ["192.168.0.2", "192.168.0.1"]

dict = {
    'rare_pattern' : known_logs + [f'[{random.choice(statuses)}] Request from {random.choice(ips)}' for _ in range(99)],
    'somewhat_freq_pattern' : known_logs*50 + [f'[{random.choice(statuses)}] Request from {random.choice(ips)}' for _ in range(50)],
    'freq_pattern' : known_logs*100,
    'random_pattern' : [f'[{random.choice(statuses)}] Request from {random.choice(ips)}' for _ in range(100)],
    'empty_pattern' : []
}


# Generate 10 random log files
for k, v in dict.items():
    with open(os.path.join(gen_log_dir, f'{k}.txt'), 'a') as file:
        if k != 'empty_pattern':
            random.shuffle(v)
            for i in range(len(v)):
                file.write(v[i] + '\n')
