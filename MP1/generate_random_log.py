import random
import os
import time
import string

random.seed(42)

def generate_random_message(length=50):
    return ''.join(random.choices(string.ascii_letters + string.digits + string.punctuation + ' ', k=length))

def generate_log_file(file_name="random.log", size_mb=2):
    size_bytes = size_mb * 1024 * 1024
    log_entries = []
    log_size = 0
    fixed_timestamp = "2024-09-15 12:00:00"
    
    while log_size < size_bytes:
        level = random.choice(['INFO', 'DEBUG', 'WARNING', 'ERROR'])
        message = generate_random_message(random.randint(10, 100))
        entry = f"{fixed_timestamp} - {level} - {message}\n"
        log_entries.append(entry)
        log_size += len(entry)
    
    with open(file_name, 'w') as f:
        f.writelines(log_entries)

log_file_name = "random_log_60MB.log"
generate_log_file(log_file_name, 60)

