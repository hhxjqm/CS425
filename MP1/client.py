import socket
import threading
import argparse
import time
from dotenv import load_dotenv
import os

# Lock to manage concurrent access to shared resources
lock = threading.Lock()
total_matches = 0
server_matches = {}

def send_query_to_server(server_ip, server_port, query):
    """Sends a query to a server and processes the response."""
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.settimeout(5)
    try:
        # Connect to the server
        client.connect((server_ip, server_port))
        client.send(query.encode('utf-8'))

        # Receive and assemble the response
        response = ""
        while True:
            chunk = client.recv(32768).decode('utf-8')
            if "EOF" in chunk:
                response += chunk.replace("EOF", "")
                break
            response += chunk

        print(f"\nResults from {server_ip}:{server_port}:\n{response}")

        # Extract file name and total matches from the response
        # file_name = [line for line in response.split('\n') if line.startswith('File:')]
        # if file_name:
        #     name = file_name[0].split(':')[1].strip()
        
        # with open(f"{name}_response.txt", "w") as logfile:
        #     logfile.write(response)

        matches_part = [line for line in response.split('\n') if line.startswith('TOTAL_MATCHES:')]
        if matches_part:
            total_matches = int(matches_part[0].split(':')[1].strip())
            # server_matches[name] = total_matches
            server_matches[server_ip] = total_matches
            return total_matches

        return 0
    except Exception as e:
        print(f"Error connecting to {server_ip}:{server_port}: {e}")
        return 0
    finally:
        client.close()

def query_server(ip, port, query):
    """Wrapper function to handle querying a server and updating the global total_matches."""
    global total_matches
    matches = send_query_to_server(ip, port, query)
    with lock:
        total_matches += matches

def main():
    """Main function to handle user input, query servers, and display results."""
    # Load environment variables from .env file
    load_dotenv()
    servers = []
    index = 1

    # Retrieve server IPs and ports from environment variables
    while True:
        ip = os.getenv(f'SERVER_{index}_IP')
        port = os.getenv(f'SERVER_{index}_PORT')
        
        if ip is None or port is None:
            break
        
        servers.append((ip, int(port)))
        index += 1

    try:
        while True:
            # Get query input from user
            query = input("Enter search pattern (or type 'exit' to disconnect): ")
            if query.lower() == 'exit':
                print("Disconnecting from all servers...")
                break

            global total_matches
            total_matches = 0
            server_matches.clear()

            start_time = time.time()

            # Create and start a thread for each server
            threads = []
            for server_ip, server_port in servers:
                thread = threading.Thread(target=query_server, args=(server_ip, server_port, query))
                threads.append(thread)
                thread.start()

            # Wait for all threads to complete
            for thread in threads:
                thread.join()

            end_time = time.time()
            latency = (end_time - start_time) * 1000

            # Display the results
            for name, matches in server_matches.items():
                print(f"\nFile: {name}: {matches} matches")

            print(f"Total matches across all servers: {total_matches}")
            print(f"Total latency: {latency}ms\n")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
