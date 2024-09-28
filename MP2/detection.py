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
PORT = 9999

class GossipNode:
    def __init__(self, membership_file="membership_list.json"):
        self.node_ip = self.get_ip()
        self.membership_file = membership_file

        # Ensure membership list initialized
        self.membership_list = initialize_membership_list(self.membership_file)
        self.known_nodes = [node_ip for node_ip in self.membership_list if node_ip != self.node_ip]
        self.port = PORT
        self.running = True
        # self.known_nodes = {node_id: info for node_id, info in parsed_data.items() if node_id != self.node_id}

        self.lock = threading.Lock()

        # Generate the log file name based on node ID
        self.log_file = "mp2.log"
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
        self.server_socket.bind((self.node_ip, self.port))

        # print(f"Node {self.node_id} listening on {self.ip}:{self.port}")

        while self.running == True:
            message, (ip, port) = self.server_socket.recvfrom(1024)
            self.process_message(message, ip, port)

    def get_ip(self):
        hostname = socket.gethostname()
        local_ip = socket.gethostbyname(hostname)
        return local_ip
    
    def gossip(self):
        while self.running == True:
            time.sleep(random.uniform(1, 3))  # Gossip every 1 to 3 seconds
            if len(self.known_nodes) >= 3:
                # Select 3 unique nodes to gossip with
                target_nodes = random.sample(self.known_nodes, 3)
            else:
                # If less than 3 nodes, sample as many as possible without repeating
                target_nodes = self.known_nodes

            for target_node in target_nodes:
                self.send_gossip(target_node)

    def send_gossip(self, target_node):
        with self.lock:
            # self.membership_list = load_membership_list(self.membership_file)
            message = json.dumps({"type": "gossip", "membership_list": self.membership_list})
        gossip_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        gossip_socket.sendto(message.encode(), (target_node, PORT))
        gossip_socket.close()
        # print(f"Gossip sent from {self.node_id} to {target_node}")

    def ping(self):
        while self.running == True:
            time.sleep(random.uniform(2, 5))  # Ping every 2 to 5 seconds
            if self.known_nodes:
                target_node = random.choice(self.known_nodes)
                self.send_ping(target_node)

    def send_ping(self, target_node):
        seq = self.get_seq()
        ping_message = json.dumps({"type": "ping", "seq": seq})
        # Get unique number
        ping_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        ping_socket.sendto(ping_message.encode(), (target_node, PORT))
        ping_socket.close()
        # print(f"Ping sent from {self.node_id} to {target_node}")

        wait_second = FAIL_WAIT_TIME
        with self.lock:
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
            with self.lock:
                for record in list(self.ping_records):
                    if record["time"] <= 0:
                        targetId = record["target"]
                        self.ping_records.remove(record)
                        # print(f"Node {record['target']} marked as failure.")
                        self.membership_list[targetId]["status"] = "failure"
                        self.membership_list[targetId]["timestamp"] = time.time()
                        log_membership_change(targetId, "failure", self.log_file)
                    else:
                        record["time"] -= 1
    

    def process_message(self, message, ip, port):
        message = json.loads(message.decode())
        if message["type"] == "gossip":
            self.process_gossip(message["membership_list"])
        elif message["type"] == "ping":
            self.process_ping(message["source_id"], ip, port, message["seq"])
        elif message["type"] == "ack":
            self.process_ack(message["source_id"], message["seq"])
        else:
            raise ValueError("Wrong type in process_message()")  

    def process_ping(self, source_id, addr, port, seq):
        # print(f"Ping received by {self.node_id} from {source_id}")
        ack_message = json.dumps({"type": "ack", "source_id": self.node_ip, "seq": seq})
        ack_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        ack_socket.sendto(ack_message.encode(), (addr, port))
        ack_socket.close()

        # Try ================================================================
        self.membership_list[source_id]["status"] = "alive"
        self.membership_list[source_id]["timestamp"] = time.time()
        log_membership_change(source_id, "ping", self.log_file)
        # ====================================================================

    def process_ack(self, source_id, seq):
        # print(f"Ack received by {self.node_id} from {source_id}")
        with self.lock:
            self.membership_list[source_id]["status"] = "alive"
            self.membership_list[source_id]["timestamp"] = time.time()
            for record in list(self.ping_records):
                if record["target"] == source_id and record["seq"] == seq:
                    self.ping_records.remove(record) 
                    break
            log_membership_change(source_id, "ack - alive", self.log_file)

    def process_gossip(self, new_membership_list):
        with self.lock:
            # self.membership_list = load_membership_list(self.membership_file)
            for node_id, node_info in new_membership_list.items():
                # if node_id in self.membership_list and self.membership_list[node_id]["status"] in ["leave", "failure", ""]:
                #     self.membership_list[node_id] = node_info
                new_status = node_info["status"]
                current_status = self.membership_list[node_id]["status"]
                if new_status and current_status != new_status and node_info["timestamp"] > self.membership_list[node_id]["timestamp"]:
                    self.membership_list[node_id] = node_info
                    if new_status == "alive":
                        if node_id not in self.known_nodes:
                            self.known_nodes.append(node_id)
                        log_membership_change(node_id, "joined", self.log_file)
                    elif new_status == "alive" and new_status in ["failure", "leave"]:
                        if node_id in self.known_nodes:
                            self.known_nodes.remove(node_id)
                        log_membership_change(node_id, new_status, self.log_file)
                    else:
                        raise ValueError("Wrong type in process_gossip()")   
            # print(f"Updated membership list at {self.node_id}: {self.membership_file}")

    def send_leave(self):
        with self.lock:
            # Mark the node as 'leave' in its own membership list
            self.membership_list[self.node_ip]["status"] = "leave"
            self.membership_list[self.node_ip]["timestamp"] = time.time()
            log_membership_change(self.node_ip, "leave", self.log_file)
            # print(f"Node {self.node_id} is leaving the cluster.")
            # Propagate the 'leave' status to other nodes
            for target_node in self.known_nodes:
                self.send_gossip(target_node)
        self.shutdown()

    def shutdown(self):
        self.running = False

        # Close the server socket
        if hasattr(self, 'server_socket'):
            self.server_socket.close()

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
        membership_info.append("================Membership List================")
        for node_id, info in self.membership_list.items():
            formatted_info = f"id: {node_id} - ip: {info['ip']} - status: {info['status']} - time: {info['timestamp']}"
            membership_info.append(formatted_info)
        membership_info.append("================================================")
        return "\n".join(membership_info)

    def get_id(self):
        return self.node_ip

def listen_for_commands():
    global gossip_node_instance
    while True:
        command = input("Enter command: ")  # Wait for user input
        if command == "leave":
            if gossip_node_instance is not None:
                gossip_node_instance.send_leave()
                gossip_node_instance = None  # Clear the instance after leaving
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