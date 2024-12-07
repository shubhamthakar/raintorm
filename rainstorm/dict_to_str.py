import json
import subprocess

# Dictionary to pass
data = {
    "op1": {
        "0": "fa24-cs425-6902.cs.illinois.edu:5000",
        "1": "fa24-cs425-6903.cs.illinois.edu:5000",
        "2": "fa24-cs425-6904.cs.illinois.edu:5000"
    },
    "source": {
        "0": "fa24-cs425-6909.cs.illinois.edu:5000",
        "1": "fa24-cs425-6902.cs.illinois.edu:5001"
    }
}

# Serialize and escape the dictionary as a JSON string
serialized_data = json.dumps(data,ensure_ascii=True)
escaped_data = serialized_data.replace('"', '\\"')  # Escape quotes for shell

print(escaped_data)