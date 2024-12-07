import sys
import json

def process(batch):
    # Filter out key-value pairs where the key does not start with 'a'
    return [(k, v) for k, v in batch if k.startswith('a')]

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: op1.py '<json_encoded_batch>'")
        sys.exit(1)

    # Read input from command argument
    input_data = sys.argv[1]
    batch = json.loads(input_data)  # Input is a JSON array of tuples

    # Process batch
    output = process(batch)

    # Print output
    print(json.dumps(output))

