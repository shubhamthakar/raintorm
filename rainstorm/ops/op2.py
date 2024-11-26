import sys
import json
from collections import defaultdict

def process(batch):
    # Aggregate values by key
    result = defaultdict(int)
    for k, v in batch:
        result[k] += v
    return list(result.items())

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: op2.py '<json_encoded_batch>'")
        sys.exit(1)

    # Read input from command argument
    input_data = sys.argv[1]
    batch = json.loads(input_data)  # Input is a JSON array of tuples

    # Process batch
    output = process(batch)

    # Print output
    print(json.dumps(output))

