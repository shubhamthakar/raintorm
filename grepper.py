import subprocess
import sys

def grep_from_python(args):
    try:

        mod_args = args
        print(mod_args)
        print(mod_args.split())
        result = subprocess.run(['grep', *mod_args.split()], capture_output=True, text=True, check=True)
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
