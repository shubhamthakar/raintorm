import subprocess
import sys

def grep_from_python(args):
    try:
        # Use subprocess to call grep with the pattern and file

        result = subprocess.run(['grep', *args.split()],
                                capture_output=True, text=True, check=True)
        # Print the output of grep
        print(type(result.stdout))
        return result.stdout
    except subprocess.CalledProcessError as e:
        # If grep finds no match, it will throw an error, handle it gracefully
        print(e.stderr)
        print(args.split())
        return 
