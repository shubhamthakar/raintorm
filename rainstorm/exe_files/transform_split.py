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
    updated_state = {
        "inp_id_processed": inp_id_processed.union({input_tuple[0]}),
        "state": state,
        "output_rec": output_rec + processed
    }

    return updated_state, processed


if __name__ == "__main__":
    import argparse
    import ast

    parser = argparse.ArgumentParser(description="Run the Transform operation.")
    parser.add_argument("--state", type=str, required=True, help="Current state as a dictionary.")
    parser.add_argument("--input", type=str, required=True, help="Input tuple as a string.")

    args = parser.parse_args()
    state_dict = ast.literal_eval(args.state)
    input_tuple = ast.literal_eval(args.input)

    updated_state, processed = transform(state_dict, input_tuple)
    print("Updated State:", updated_state)
    print("Processed Tuples:", processed)
