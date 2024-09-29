import json
import os
import time


def initialize_membership_list(local_ip, membership_file):
    if os.path.exists(membership_file):
        with open(membership_file, 'r') as f:
            membership_list = json.load(f)
        membership_list[local_ip] = {
            "status": "alive",
            "timestamp": time.time(),
            "version": 1
        }
        return membership_list
    else:
        print(f"No membership file found at {membership_file}.")