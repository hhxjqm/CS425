import socket
import threading
import time
import random
import json
import re
import sys
from member_list import initialize_membership_list
from log_update import log_membership_change, initialize_log_file

FAIL_WAIT_TIME = 5
gossip_node_instance = None
PORT = 8888

class GossipNode:
    def __init__(self, membership_file="MP2/membership_list.json"):
        self.node_ip = self.get_ip()
        self.membership_file = membership_file

        # Ensure membership list initialized
        self.membership_list = initialize_membership_list(self.node_ip, self.membership_file)

        self.known_nodes = [node_ip for node_ip in self.membership_list if node_ip != self.node_ip]
        self.running = True
        # self.known_nodes = {node_id: info for node_id, info in parsed_data.items() if node_id != self.node_id}

        self.list_lock = threading.Lock()
        self.log_lock = threading.Lock()
        self.known_lock = threading.Lock()
        self.records_lock = threading.Lock()
        self.socket_lock = threading.Lock()

        # Generate the log file name based on node ID
        self.log_file = "MP2/mp2.log"
        initialize_log_file(self.node_ip ,self.log_file)

        # Start the UDP server to listen for messages (gossip, ping, ack)
        self.server_thread = threading.Thread(target=self.start_server)
        self.server_thread.start()

        # Periodically gossip membership list
        self.gossip_thread = threading.Thread(target=self.gossip)
        self.gossip_thread.start()

        # Periodically ping nodes to check for failures
        self.ping_thread = threading.Thread(target=self.ping)
        self.ping_thread.start()

        self.ping_records = []
        self.ping_check_thread = threading.Thread(target=self.check_ping_status)
        self.ping_check_thread.start()

    def start_server(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.server_socket.bind((self.node_ip, PORT))

        # print(f"Node {self.node_id} listening on {self.ip}:{self.port}")
        self.server_socket.settimeout(2)
        while self.running == True:
            try:
                message, (ip, port) = self.server_socket.recvfrom(1024)
                self.process_message(message, ip, port)
            except socket.timeout:
                continue
        self.server_socket.close()
        print("end server")

    def get_ip(self):
        hostname = socket.gethostname()
        local_ip = socket.gethostbyname(hostname)
        return local_ip
    
    def gossip(self):
        while self.running == True:
            time.sleep(3)
            with self.known_lock:
                if len(self.known_nodes) >= 3:
                    # Select 3 unique nodes to gossip with
                    target_nodes = random.sample(self.known_nodes, 3)
                else:
                    # If less than 3 nodes, sample as many as possible without repeating
                    target_nodes = self.known_nodes

            for target_node in target_nodes:
                self.send_gossip(target_node)

    def send_gossip(self, target_node):
        message = json.dumps({"type": "gossip", "membership_list": self.membership_list})
        with self.socket_lock:
            # self.membership_list = load_membership_list(self.membership_file)
            gossip_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            gossip_socket.sendto(message.encode(), (target_node, PORT))
            gossip_socket.close()
        # print(f"Gossip sent from {self.node_id} to {target_node}")

    def ping(self):
        while self.running == True:
            time.sleep(2)
            with self.known_lock:
                if self.known_nodes:
                    target_node = random.choice(self.known_nodes)
                    self.send_ping(target_node)

    def send_ping(self, target_node):
        seq = self.get_seq()
        ping_message = json.dumps({"type": "ping", "seq": seq})
        # Get unique number
        with self.socket_lock:
            ping_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            ping_socket.sendto(ping_message.encode(), (target_node, PORT))
            ping_socket.close()
        # print(f"Ping sent from {self.node_id} to {target_node}")

        wait_second = FAIL_WAIT_TIME
        with self.records_lock:
            record_to_check = {
                "target": target_node,
                "seq": seq,
                "time": wait_second
            }
            if record_to_check not in self.ping_records:
                self.ping_records.append(record_to_check)

    def check_ping_status(self):
        while self.running == True:
            time.sleep(1)
            with self.records_lock:
                for record in list(self.ping_records):
                    if record["time"] <= 0:
                        targetId = record["target"]
                        self.ping_records.remove(record)
                        # print(f"Node {record['target']} marked as failure.")
                        with self.list_lock:
                            self.membership_list[targetId]["status"] = "failure"
                            self.membership_list[targetId]["timestamp"] = time.time()
                        with self.log_lock:
                            log_membership_change(targetId, "failure", self.log_file)
                    else:
                        record["time"] -= 1
    

    def process_message(self, message, ip, port):
        message = json.loads(message.decode())
        if message["type"] == "gossip":
            self.process_gossip(message["membership_list"])
        elif message["type"] == "ping":
            self.process_ping(ip, port, message["seq"])
        elif message["type"] == "ack":
            self.process_ack(ip, message["seq"])
        else:
            raise ValueError("Wrong type in process_message()")  

    def process_ping(self, ip, port, seq):
        # print(f"Ping received by {self.node_id} from {source_id}")
        ack_message = json.dumps({"type": "ack", "seq": seq})
        with self.socket_lock:
            ack_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            ack_socket.sendto(ack_message.encode(), (ip, port))
            ack_socket.close()

        # Try ================================================================
        self.membership_list[ip] = {
            "status": "alive",
            "timestamp": time.time()
        }
        # ====================================================================

    def process_ack(self, source_id, seq):
        # print(f"Ack received by {self.node_id} from {source_id}")
        with self.list_lock:
            self.membership_list[source_id]["status"] = "alive"
            self.membership_list[source_id]["timestamp"] = time.time()
            with self.records_lock:
                for record in list(self.ping_records):
                    if record["target"] == source_id and record["seq"] == seq:
                        self.ping_records.remove(record) 
                        break
        with self.log_lock:
            log_membership_change(source_id, "ack - alive", self.log_file)

    def process_gossip(self, new_membership_list):
        with self.list_lock:
            # self.membership_list = load_membership_list(self.membership_file)
            for node_id, node_info in new_membership_list.items():
                if node_id == self.node_ip:
                    continue
                if node_id not in self.membership_list:
                    # If not, you can initialize it or handle it accordingly
                    self.membership_list[node_id] = node_info
                    if node_info["status"] == "alive":
                        with self.known_lock:
                            self.known_nodes.append(node_id)
                        with self.log_lock:
                            log_membership_change(node_id, "joined", self.log_file)
                    continue  # Skip to the next node in new_membership_list
                new_status = node_info["status"]
                current_status = self.membership_list[node_id]["status"]
                if new_status and node_info["timestamp"] > self.membership_list[node_id]["timestamp"]:
                    self.membership_list[node_id] = node_info
                    if new_status == "alive" and current_status != "alive":
                        if node_id not in self.known_nodes:
                            with self.known_lock:
                                self.known_nodes.append(node_id)
                        with self.log_lock:
                            log_membership_change(node_id, "joined", self.log_file)
                    elif new_status != "alive" and current_status == "alive":
                        if node_id in self.known_nodes:
                            with self.known_lock:
                                self.known_nodes.remove(node_id)
                        with self.log_lock:
                            log_membership_change(node_id, new_status, self.log_file)

    def send_leave(self):
        self.running = False
        
        with self.list_lock:
            # Mark the node as 'leave' in its own membership list
            self.membership_list[self.node_ip]["status"] = "leave"
            self.membership_list[self.node_ip]["timestamp"] = time.time()
            with self.log_lock:
                log_membership_change(self.node_ip, "leave", self.log_file)
            # print(f"Node {self.node_id} is leaving the cluster.")
            # Propagate the 'leave' status to other nodes
        with self.known_lock:
            if len(self.known_nodes) >= 3:
                # Select 3 unique nodes to gossip with
                target_nodes = random.sample(self.known_nodes, 3)
            else:
                # If less than 3 nodes, sample as many as possible without repeating
                target_nodes = self.known_nodes
        for target_node in target_nodes:
            self.send_gossip(target_node)
        self.shutdown()
        
    def shutdown(self):
        # Close the server socket
        # Optionally, wait for the threads to finish
        if self.server_thread.is_alive():
            self.server_thread.join()
        if self.gossip_thread.is_alive():
            self.gossip_thread.join()
        if self.ping_thread.is_alive():
            self.ping_thread.join()
        if self.ping_check_thread.is_alive():
            self.ping_check_thread.join()
        
    def get_seq(self):
        rand_source = time.time_ns()
        rand_gen = random.Random(rand_source)
        seq = rand_gen.randint(0, (1 << 15) - 2)
        return seq
    
    def get_membership_list(self):
        membership_info = []
        membership_info.append("================Membership List=================")
        for node_id, info in self.membership_list.items():
            if info['status']:
                formatted_info = f"id: {node_id} - status: {info['status']} - time: {info['timestamp']}"
                membership_info.append(formatted_info)
        membership_info.append("================================================")
        return "\n".join(membership_info)

    def get_id(self):
        return self.node_ip

def listen_for_commands():
    global gossip_node_instance
    while True:
        command = input("\nEnter command: ")  # Wait for user input
        if command == "leave":
            if gossip_node_instance is not None:
                gossip_node_instance.send_leave()
                print(f"Node {gossip_node_instance.node_ip} has joined the cluster.")
                gossip_node_instance = None
            else:
                print("You are not joined")
        elif command == "join":
            if gossip_node_instance is None:
                gossip_node_instance = GossipNode()
                print(f"Node {gossip_node_instance.node_ip} has joined the cluster.")
            else:
                print("Node is already in the cluster.")
        elif command == "showid":
            if gossip_node_instance is not None:
                print(gossip_node_instance.get_id())
            else:
                print("You are not joined")
        elif command == "showlist":
            if gossip_node_instance is not None:
                print(gossip_node_instance.get_membership_list())
            else:
                print("You are not joined")
        else:
            print("Wrong command, use either: join, leave, showid, showlist")

def main():
    command_thread = threading.Thread(target=listen_for_commands)
    command_thread.start()

if __name__ == "__main__":
    main()