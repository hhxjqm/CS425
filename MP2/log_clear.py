import os
import argparse

def clear_all_logs(directory):
    # Iterate over all files in the specified directory
    for filename in os.listdir(directory):
        # Check if the file ends with .log
        if filename.endswith('.log'):
            # Build the full path to the log file
            file_path = os.path.join(directory, filename)
            # Open the file in write mode to clear its content
            open(file_path, 'w').close()
            print(f"Cleared log file: {file_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clear all .log files in a specified directory.")
    parser.add_argument('directory', type=str, help='The path to the directory containing log files')
    
    args = parser.parse_args()
    clear_all_logs(args.directory)
