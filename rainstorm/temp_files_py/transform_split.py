# This is op1

def transform(state_dict, input_tuple):
    """
    Splits the line into words and returns a list of tuples (word, 1).
    Args:
        state_dict (dict): The current state dictionary.
        input_tuple (tuple): The input tuple in the form (key, line).
    Returns:
        updated_state (dict): Updated state dictionary.
        processed (list): List of tuples in the form (word, 1).
    """
    inp_id_processed, state, output_rec = state_dict.values()

    # Extract key and line from input_tuple
    key, line = input_tuple

    # Generate list of (word, 1) tuples
    processed = [(word, 1) for word in line.split()]

    # Update the state dictionary
    inp_id_processed[input_tuple[0]] = 1
    output_rec += processed

    return state_dict, processed


if __name__ == "__main__":
    import argparse
    import ast
    import json

    parser = argparse.ArgumentParser(description="Run the Transform operation.")
    parser.add_argument("--state", type=str, required=True, help="Current state as a dictionary.")
    parser.add_argument("--input", type=str, required=True, help="Input tuple as a string.")

    args = parser.parse_args()
    state_dict = ast.literal_eval(args.state)
    input_tuple = ast.literal_eval(args.input)

    updated_state, processed = transform(state_dict, input_tuple)
    
    # Serialize and print the result as JSON
    result = {
        "updated_state": updated_state,
        "processed": processed
    }
    print(json.dumps(result))