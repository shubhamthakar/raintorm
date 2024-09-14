from socket_client_parallel import run_parallel_socket_calls
from hosts import hosts_dict
# run_parallel_socket_calls(hosts_dict, port, grep_command)
# output_dict[host_name] = grep_output
port = 12345

def test_your_function():
    grep_command = ['-r', '\[200 OK\] Request from 127.0.0.1', '/home/chaskar2/distributed-logger/tests/test_logs/test_rare_on_one_machine.txt']
    output_dict = run_parallel_socket_calls(hosts_dict, port, grep_command)

    expected_output ={
    'fa24-cs425-6901.cs.illinois.edu': '[200 OK] Request from 127.0.0.1\n',
    'fa24-cs425-6902.cs.illinois.edu': '',
    'fa24-cs425-6903.cs.illinois.edu': '',
    'fa24-cs425-6904.cs.illinois.edu': '',
    'fa24-cs425-6905.cs.illinois.edu': '',
    'fa24-cs425-6906.cs.illinois.edu': '',
    'fa24-cs425-6907.cs.illinois.edu': '',
    'fa24-cs425-6908.cs.illinois.edu': '',
    'fa24-cs425-6909.cs.illinois.edu': '',
    'fa24-cs425-6910.cs.illinois.edu': ''
}
    # Use assertions to compare actual and expected values
    assert output_dict == expected_output, f"Expected {expected_output}, got {output_dict}"