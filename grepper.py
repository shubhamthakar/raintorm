import subprocess
import sys

def grep_from_python(args):
    try:

        result = subprocess.run(['grep', *args.split()], cwd='/home/chaskar2/distributed-logger', capture_output=True, text=True, check=True)
        print("Results: ",  result.stdout)
        return result.stdout
    except subprocess.CalledProcessError as e:
        if e.returncode == 1:
            print("No matches found.")
            return ""
        else:
            # Handle other possible errors (e.g., invalid options, file not found)
            print(f"Error: {e.stderr}")
            return ""
