import os
import hashlib
import sys

def hash_string(content):
    md5_hash = hashlib.md5(content.encode('utf-8')).hexdigest()
    hash_int = int(md5_hash, 16)
    return hash_int % (2**10)

def handle_store():
    directory = "../filesystem"
    
    # Iterate over all files in the directory
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)

        # ignore log files
        if "--" in filename and filename.endswith(".log"):
            continue
        
        # Only process files (not directories)
        # Calculate the hash for the file content
        file_hash = hash_string(filename)
        
        # Print filename and its hash
        print(f"File: {filename}, Hash: {file_hash}")


if __name__ == "__main__":
    handle_store()
