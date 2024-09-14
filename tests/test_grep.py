# inputs host_name, port, grep_command
# outputs host_name, grep_output, lines_matched


# {test_name -> (grep_command, {host_name -> (grep_out, lines_matched)})}



def test_your_function():
    # Define inputs
    param1 = 10
    param2 = 5

    # Define expected outputs
    expected_output1 = 15  # param1 + param2
    expected_output2 = 5   # param1 - param2
    expected_output3 = 50  # param1 * param2

    # Call the function with the input parameters
    output1, output2, output3 = your_function(param1, param2)

    # Use assertions to compare actual and expected values
    assert output1 == expected_output1, f"Expected {expected_output1}, got {output1}"
    assert output2 == expected_output2, f"Expected {expected_output2}, got {output2}"
    assert output3 == expected_output3, f"Expected {expected_output3}, got {output3}"