import json
import os

def initialize_membership_list(membership_file="membership_list.json"):
    if os.path.exists(membership_file):
        with open(membership_file, 'r') as f:
            membership_list = json.load(f)

        # for id in membership_list:
        #     if id == node_id:
        #         membership_list[id]["status"] = "alive"
        #     else:
        #         membership_list[id]["status"] = ""

        # with open(membership_file, 'w') as f:
        #     json.dump(membership_list, f, indent=4)
        return membership_list
    else:
        print(f"No membership file found at {membership_file}.")
