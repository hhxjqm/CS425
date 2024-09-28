import os

def clear_all_logs():
    # Iterate over all files in the current directory
    for filename in os.listdir('.'):
        # Check if the file ends with .log
        if filename.endswith('.log'):
            # Open the file in write mode to clear its content
            open(filename, 'w').close()
            print(f"Cleared log file: {filename}")

if __name__ == "__main__":
    clear_all_logs()
