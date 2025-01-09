from socket_client_parallel import run_parallel_socket_calls
from hosts import hosts_dict
# run_parallel_socket_calls(hosts_dict, port, grep_command)
# output_dict[host_name] = grep_output
port = 12345

def test_rare_on_one_machine():
    grep_command = ['-r', r'\[200 OK\] Request from 127.0.0.1', '/home/chaskar2/distributed-logger/tests/test_logs/test_rare_on_one_machine.txt']
    output_dict = run_parallel_socket_calls(hosts_dict, port, grep_command)

    """
    host1: rare_pattern_on_one_machine
    host2: random
    host3: random
    host4: random
    host5: random
    host6: random
    host7: blank
    host8: blank
    host9: blank
    host10:  blank

    """

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

def test_somewhatfreq_on_5_machine():
    grep_command = ['-r', r'\[200 OK\] Request from 127.0.0.1', '/home/chaskar2/distributed-logger/tests/test_logs/test_somewhatfreq_on_5_machine.txt']
    output_dict = run_parallel_socket_calls(hosts_dict, port, grep_command)

    """
    host1: somewhatfreq_pattern_on_5_machine
    host2: somewhatfreq_pattern_on_5_machine
    host3: somewhatfreq_pattern_on_5_machine
    host4: somewhatfreq_pattern_on_5_machine
    host5: somewhatfreq_pattern_on_5_machine
    host6: random
    host7: random
    host8: blank
    host9: blank
    host10:  blank

    """

    expected_output ={
    'fa24-cs425-6901.cs.illinois.edu': '[200 OK] Request from 127.0.0.1\n'*50,
    'fa24-cs425-6902.cs.illinois.edu': '[200 OK] Request from 127.0.0.1\n'*50,
    'fa24-cs425-6903.cs.illinois.edu': '[200 OK] Request from 127.0.0.1\n'*50,
    'fa24-cs425-6904.cs.illinois.edu': '[200 OK] Request from 127.0.0.1\n'*50,
    'fa24-cs425-6905.cs.illinois.edu': '[200 OK] Request from 127.0.0.1\n'*50,
    'fa24-cs425-6906.cs.illinois.edu': '',
    'fa24-cs425-6907.cs.illinois.edu': '',
    'fa24-cs425-6908.cs.illinois.edu': '',
    'fa24-cs425-6909.cs.illinois.edu': '',
    'fa24-cs425-6910.cs.illinois.edu': ''
}
    # Use assertions to compare actual and expected values
    assert output_dict == expected_output, f"Expected {expected_output}, got {output_dict}"


def test_freq_on_all_machine():
    grep_command = ['-r', r'\[200 OK\] Request from 127.0.0.1', '/home/chaskar2/distributed-logger/tests/test_logs/test_freq_on_all_machine.txt']
    output_dict = run_parallel_socket_calls(hosts_dict, port, grep_command)

    """
    host1: test_freq_pattern_on_all_machine
    host2: test_freq_pattern_on_all_machine
    host3: test_freq_pattern_on_all_machine
    host4: test_freq_pattern_on_all_machine
    host5: test_freq_pattern_on_all_machine
    host6: test_freq_pattern_on_all_machine
    host7: test_freq_pattern_on_all_machine
    host8: test_freq_pattern_on_all_machine
    host9: test_freq_pattern_on_all_machine
    host10:  test_freq_pattern_on_all_machine

    """

    expected_output ={
    'fa24-cs425-6901.cs.illinois.edu': '[200 OK] Request from 127.0.0.1\n'*100,
    'fa24-cs425-6902.cs.illinois.edu': '[200 OK] Request from 127.0.0.1\n'*100,
    'fa24-cs425-6903.cs.illinois.edu': '[200 OK] Request from 127.0.0.1\n'*100,
    'fa24-cs425-6904.cs.illinois.edu': '[200 OK] Request from 127.0.0.1\n'*100,
    'fa24-cs425-6905.cs.illinois.edu': '[200 OK] Request from 127.0.0.1\n'*100,
    'fa24-cs425-6906.cs.illinois.edu': '[200 OK] Request from 127.0.0.1\n'*100,
    'fa24-cs425-6907.cs.illinois.edu': '[200 OK] Request from 127.0.0.1\n'*100,
    'fa24-cs425-6908.cs.illinois.edu': '[200 OK] Request from 127.0.0.1\n'*100,
    'fa24-cs425-6909.cs.illinois.edu': '[200 OK] Request from 127.0.0.1\n'*100,
    'fa24-cs425-6910.cs.illinois.edu': '[200 OK] Request from 127.0.0.1\n'*100
}
    # Use assertions to compare actual and expected values
    assert output_dict == expected_output, f"Expected {expected_output}, got {output_dict}"

#scp /home/chaskar2/distributed-logger/tests/generated_logs/somewhat_freq_pattern.txt sthakar3@fa24-cs425-6902.cs.illinois.edu:/home/chaskar2/distributed-logger/tests/test_logs/test_somewhatfreq_on_5_machine.txt
