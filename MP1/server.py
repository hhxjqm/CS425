import socket
import subprocess
import re
import os
import argparse

def execute_grep_on_logs(query, log_directory):
    """
    Executes a grep search on all .log files in the current directory and returns the results.
    
    Args:
        query (str): The search pattern to use with grep.
        
    Returns:
        tuple: A string containing the results and an integer representing the total number of matches.
    """
    # Get a list of all .log files in the current directory
    # log_files = [f for f in os.listdir(log_directory) if f.endswith('.log')]
    log_files = [os.path.join(log_directory, f) for f in os.listdir(log_directory) if f.endswith('.log')]
    result = ""
    total_matches = 0

    # Process each log file
    for log_file in log_files:
        # Construct the grep command
        command = [query, log_file]
        # command = [query, log_file]
        try:
            # Execute the grep command and capture the output
            grep_result = subprocess.check_output(command, stderr=subprocess.STDOUT, universal_newlines=True)
            matches = grep_result.strip().split('\n')

            # Check if any matches were found
            if matches:
                # result += f"\nFile: {log_file}\n"
                for match in matches:
                    # Format each match and append to the result
                    result += f"Line {match.split(':')[0]}: {match.split(':', 1)[1]}\n"
                total_matches += len(matches)
            else:
                result += f"File: {log_file}\nNo matches found\n"
        except subprocess.CalledProcessError:
            # Handle the case where no matches are found or another error occurs
            result += f"File: {log_file}\nNo matches found\n"

    return result, total_matches

def handle_client(client_socket, log_directory):
    """
    Handles communication with a connected client. Receives queries, executes the search,
    and sends back the results.
    
    Args:
        client_socket (socket.socket): The socket object for the client connection.
    """
    try:
        while True:
            # Receive the query from the client
            query = client_socket.recv(1024).decode('utf-8')
            if not query or query.lower() == 'exit':
                print("Client requested disconnection or sent empty query.")
                break
            
            print(f"Received query: {query}")
            
            # Execute the search on the log files
            result, total_matches = execute_grep_on_logs(query, log_directory)

            # Send the results back to the client
            client_socket.send(result.encode('utf-8'))
            client_socket.send(b"EOF")  # End of file indicator
            client_socket.send(f"TOTAL_MATCHES:{total_matches}".encode('utf-8'))
            break
    finally:
        # Close the connection with the client
        client_socket.close()
        print("Connection closed with the client.")

def main():
    """
    Main function to start the server, listen for client connections, and handle them.
    """
    parser = argparse.ArgumentParser(description='Start a log grep server.')
    parser.add_argument('log_directory', type=str, help='Directory containing .log files')
    args = parser.parse_args()
    log_directory = args.log_directory

    # Create a socket object
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Bind the socket to the specified IP and port
    server.bind(('0.0.0.0', 9999))
    # Listen for incoming connections
    server.listen(5)
    print("Server listening on port 9999")
    
    while True:
        # Accept a new client connection
        client_socket, addr = server.accept()
        print(f"Accepted connection from {addr}")
        # Handle the client connection in a separate function
        handle_client(client_socket, log_directory)

if __name__ == "__main__":
    main()
