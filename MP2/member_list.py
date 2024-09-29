import json
import os
import time

def initialize_membership_list(local_ip, membership_file="MP2/membership_list.json"):
    if os.path.exists(membership_file):
        with open(membership_file, 'r') as f:
            membership_list = json.load(f)
        membership_list[local_ip] = {
            "status": "alive",
            "timestamp": time.time()
        }
        return membership_list
    else:
        print(f"No membership file found at {membership_file}.")
