import time
import os

def log_membership_change(target_node_id, action, version, log_file):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    log_message = f"{timestamp} - Node: {target_node_id} Version: {version} - has {action}\n"
    with open(log_file, "a") as log:
        log.write(log_message)

def initialize_log_file(node_id, version, log_file_name):
    if os.path.exists(log_file_name):
        log_membership_change(node_id, "joined", version, log_file_name)
    else:
        with open(log_file_name, 'w') as log_file:
            pass
        log_membership_change(node_id, "joined", version, log_file_name)