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
SUS_WAIT_TIME = 10
gossip_node_instance = None
PORT = 7777

class GossipNode:
    def __init__(self, membership_file="MP2/membership_list.json"):
        self.node_ip = self.get_ip()
        self.membership_file = membership_file
        self.version = 1
        # Ensure membership list initialized
        self.membership_list = initialize_membership_list(self.node_ip, self.membership_file)

        self.known_nodes = {node_ip for node_ip in self.membership_list if node_ip != self.node_ip}
        self.running = True
        self.sus = False
        # self.known_nodes = {node_id: info for node_id, info in parsed_data.items() if node_id != self.node_id}
        self.list_lock = threading.Lock()
        self.log_lock = threading.Lock()
        self.known_lock = threading.Lock()
        self.records_lock = threading.Lock()
        self.socket_lock = threading.Lock()
        self.sus_lock = threading.Lock()
        self.sus_list_lock = threading.Lock()

        # Generate the log file name based on node ID
        self.log_file = "MP2/mp2.log"
        initialize_log_file(self.node_ip, self.version, self.log_file)

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
        self.sus_records = []

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
                    target_node = random.choice(list(self.known_nodes), 3)
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
                    target_node = random.choice(list(self.known_nodes))
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

        if not self.sus:
            wait_second = FAIL_WAIT_TIME
            record_to_check = {
                "target": target_node,
                "seq": seq,
                "time": wait_second
            }
            with self.records_lock:
                if not any(record["target"] == target_node for record in self.ping_records):
                    self.ping_records.append(record_to_check)
        else:
            wait_second = SUS_WAIT_TIME
            record_to_check = {
                "target": target_node,
                "time": wait_second
            }
            with self.sus_lock:
                if not any(record["target"] == target_node for record in self.sus_records):
                    self.sus_records.append(record_to_check)

    def check_ping_status(self):
        while self.running == True:
            time.sleep(1)
            if not self.sus:
                with self.records_lock:
                    for record in list(self.ping_records):
                        if record["time"] <= 0:
                            targetId = record["target"]
                            with self.known_lock:
                                self.known_nodes.discard(targetId)
                                self.ping_records.remove(record)
                            # print(f"Node {record['target']} marked as failure.")
                            with self.list_lock:
                                self.membership_list[targetId]["status"] = "failure"
                                self.membership_list[targetId]["timestamp"] = time.time()
                                version = self.membership_list[targetId]["version"]
                            with self.log_lock:
                                log_membership_change(targetId, "failure", version, self.log_file)
                        else:
                            record["time"] -= 1
            else:
                with self.sus_lock:
                    for record in list(self.sus_records): 
                        time_left = record["time"]  
                        targetId = record["target"]             
                        if time_left  <= 0:
                            self.known_nodes.discard(targetId)
                            self.sus_records.remove(record)
                            # print(f"Node {record['target']} marked as failure.")
                            with self.list_lock:
                                self.membership_list[targetId]["status"] = "failure"
                                self.membership_list[targetId]["timestamp"] = time.time()
                                version = self.membership_list[targetId]["version"]
                            with self.log_lock:
                                log_membership_change(targetId, "failure", version, self.log_file)
                        elif time_left == 5:
                            print(f"\n!!!!!!!{targetId} was suspected!!!!!!\n")
                            with self.list_lock:
                                self.membership_list[targetId]["status"] = "suspicion"
                                self.membership_list[targetId]["timestamp"] = time.time()
                                version = self.membership_list[targetId]["version"]
                            with self.log_lock:
                                log_membership_change(targetId, "suspicion", version, self.log_file)
                            record["time"] -= 1
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
        if not self.sus:
            with self.records_lock:
                self.ping_records = [record for record in self.ping_records if record["target"] != ip]
        else:
            with self.sus_lock:
                self.sus_records = [record for record in self.sus_records if record["target"] != ip]

        with self.list_lock:
            if ip not in self.membership_list:
                log_membership_change(ip, "joined", 1, self.log_file)
                self.membership_list[ip] = {
                    "status": "alive",
                    "timestamp": time.time()-86400,
                    "sus": self.sus,
                    "version": 1
                }
            else:
                self.membership_list[ip]["status"] = "alive"
                # self.membership_list[ip]["time"] = time.time()
                self.membership_list[ip]["version"] += 1
        with self.known_lock:
            self.known_nodes.add(ip)
        # ====================================================================

    def process_ack(self, source_id, seq):
        # print(f"Ack received by {self.node_id} from {source_id}")
        with self.list_lock:
            self.membership_list[source_id]["status"] = "alive"
            self.membership_list[source_id]["timestamp"] = time.time()
            if not self.sus:
                with self.records_lock:
                    for record in list(self.ping_records):
                        if record["target"] == source_id and record["seq"] == seq:
                            self.ping_records.remove(record) 
                            break
            else:
                with self.sus_lock:
                    for record in list(self.sus_records):
                        if record["target"] == source_id and record["seq"] == seq:
                            self.ping_records.remove(record) 
                            break

        # with self.log_lock:
        #     log_membership_change(source_id, "ack - alive", self.log_file)

    def process_gossip(self, new_membership_list):
        with self.list_lock:
            # self.membership_list = load_membership_list(self.membership_file)
            for node_id, node_info in new_membership_list.items():
                new_status = node_info["status"]
                if not new_status:
                    break
                new_version = node_info["version"]
                if node_id not in self.membership_list:
                    # If not, you can initialize it or handle it accordingly
                    self.membership_list[node_id] = node_info
                    if node_info["status"] == "alive":
                        with self.known_lock:
                            self.known_nodes.add(node_id)
                        with self.log_lock:
                            log_membership_change(node_id, "joined", new_version, self.log_file)
                    continue  # Skip to the next node in new_membership_list
                current_version = self.membership_list[node_id]["version"]
                current_status = self.membership_list[node_id]["status"]
                current_time = self.membership_list[node_id]["time"]
                new_time = node_info["time"]
                self_time = self.membership_list[self.node_ip]["time"]
                if node_info["sus"] and not self.sus and self_time < new_time:
                    self.sus = True
                    self.enable_sus(self)
                elif not node_info["sus"] and self.sus and self_time < new_time:
                    self.sus = False
                    self.disable_sus(self)
                    for _, info in self.membership_list.items():
                        if info["status"] == "sus":
                            info["status"] = "alive"
                newest_time = max(current_time, new_time)
                if current_version > new_version or (new_version == current_status and current_time > new_time):
                    break
                elif current_version < new_version:
                    self.membership_list[node_id] = new_version
                if self.node_ip == node_id:
                    self.version = new_version
                    break
                # self.membership_list[node_id] = node_info
                self.membership_list[node_id] = {
                    "status": new_status,
                    "timestamp": newest_time,
                    "version": new_version
                }
                if new_status == "sus" and current_status != "sus":
                    wait_second = SUS_WAIT_TIME + 5
                    with self.sus_lock:
                        record_to_check = {
                            "target": node_id,
                            "time": wait_second
                        }
                    with self.sus_lock:
                        if not any(record["target"] == node_id for record in self.sus_records):
                            self.sus_records.append(record_to_check)
                    with self.log_lock:
                        log_membership_change(node_id, "suspicion", new_version, self.log_file)
                elif new_status != "sus" and current_status == "sus":
                    if new_status == "alive":
                        if node_id in self.sus_records:
                            log_membership_change(node_id, "resume", new_version, self.log_file)
                            with self.sus_lock:
                                self.sus_records = [record for record in self.sus_records if record["target"] != node_id]
                    else:
                        with self.known_lock:
                            self.known_nodes.discard(node_id)
                        with self.sus_lock:
                            self.sus_records = [record for record in self.sus_records if record["target"] != node_id]
                            
                        with self.log_lock:
                            log_membership_change(node_id, new_status, new_version, self.log_file)
                elif new_status == "alive" and current_status != "alive":
                    with self.known_lock:
                        self.known_nodes.add(node_id)
                    with self.log_lock:
                        log_membership_change(node_id, "joined", new_version, self.log_file)
                elif new_status != "alive" and current_status == "alive":
                    with self.known_lock:
                        self.known_nodes.discard(node_id)
                    with self.log_lock:
                        log_membership_change(node_id, new_status, new_version, self.log_file)

    def send_leave(self):
        self.running = False
        with self.log_lock:
            log_membership_change(self.node_ip, "leave", self.version, self.log_file)
        with self.list_lock:
            # Mark the node as 'leave' in its own membership list
            self.membership_list[self.node_ip]["status"] = "leave"
            self.membership_list[self.node_ip]["timestamp"] = time.time()
        with self.known_lock:
            if len(self.known_nodes) >= 3:
                # Select 3 unique nodes to gossip with
                target_node = random.choice(list(self.known_nodes), 3)
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
    
    def enable_sus(self):
        self.sus = True
        with self.records_lock:
            self.ping_records = []

    def disable_sus(self):
        self.sus = False
        with self.sus_lock:
            self.sus_records = []
        for key, value in self.membership_list.items():
            if value.get('status') == "suspicion":
                value['status'] = "alive"
            
    def show_sus(self):
        sus_info = []
        sus_info.append("================Suspicion List=================")
        with self.sus_lock:
            for info in self.sus_records:
                id = info["target"]
                formatted_info = f"{id}"
                sus_info.append(formatted_info)
        sus_info.append("================================================")
        return "\n".join(sus_info)

    def get_membership_list(self):
        membership_info = []
        membership_info.append("================Membership List=================")
        with self.list_lock:
            for node_id, info in self.membership_list.items():
                if info['status']:
                    timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(info['timestamp']))
                    formatted_info = f"id: {node_id} - version: {info['version']} - status: {info['status']} - time: {timestamp}"
                    membership_info.append(formatted_info)
        membership_info.append("================================================")
        return "\n".join(membership_info)

    def get_id(self):
        return self.node_ip
    
    def get_sus(self):
        return self.sus

def listen_for_commands():
    global gossip_node_instance
    while True:
        command = input("\nEnter command: ")  # Wait for user input
        if command == "leave":
            if gossip_node_instance is not None:
                gossip_node_instance.send_leave()
                print(f"Node {gossip_node_instance.node_ip} has left the cluster.")
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
        elif command == "show_sus":
            if gossip_node_instance is not None:
                if gossip_node_instance.get_sus():
                    print(gossip_node_instance.show_sus())
                else:
                    print("You are not enable suspicion")
            else:
                print("You are not joined")
        elif command == "status_sus":
            if gossip_node_instance is not None:
                print(gossip_node_instance.get_sus())
            else:
                print("You are not joined")
        elif command == "enable_sus":
            if gossip_node_instance is not None:
                if gossip_node_instance.get_sus():
                    print("Already enable suspicion")
                else:
                    gossip_node_instance.enable_sus()
            else:
                print("You are not joined")
        elif command == "disable_sus":
            if gossip_node_instance is not None:
                if not gossip_node_instance.get_sus():
                    print("Already disable suspicion")
                else:
                    gossip_node_instance.disable_sus()
            else:
                print("You are not joined")
        else:
            print("Wrong command, use either: \njoin \nleave \nshowid \nshowlist \nenable_sus \ndisable_sus \nstatus_sus \nshow_sus")


def main():
    command_thread = threading.Thread(target=listen_for_commands)
    command_thread.start()

if __name__ == "__main__":
    main()