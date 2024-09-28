import socket
import threading
import time
import random
import json
import re
import sys
from member_list import initialize_membership_list
from log_update import log_membership_change, initialize_log_file

hostname = socket.gethostname()
local_ip = socket.gethostbyname(hostname)
print(type(local_ip))