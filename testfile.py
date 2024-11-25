import node
import sys
import threading
import time
import matplotlib.pyplot as plt
import socket
import json

def measure_time(operation, *args):
    start_time = time.time()
    operation(*args)
    end_time = time.time()
    return end_time - start_time

def send_request(command, timeout=5):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(timeout)
        s.connect(("127.0.0.1", 8081))
        s.send(json.dumps(command).encode())
        response = s.recv(4096).decode()
        return json.loads(response)

def insert_key_value(key, value):
    command = {"command": "store_key", "key": key, "value": value}
    send_request(command)

def retrieve_key_value(key):
    command = {"command": "retrieve_key", "key": key}
    send_request(command)

def delete_key(key):
    command = {"command": "delete_key", "key": key}
    send_request(command)

def performance_test():
    keys = [f"key{i}" for i in range(100)]
    values = [f"value{i}" for i in range(100)]

    insert_times = []
    retrieve_times = []
    delete_times = []

    with open('performance_times.txt', 'w') as f:
        for key, value in zip(keys, values):
            insert_time = measure_time(insert_key_value, key, value)
            insert_times.append(insert_time)
            f.write(f"Insert {key}: {insert_time}\n")

            retrieve_time = measure_time(retrieve_key_value, key)
            retrieve_times.append(retrieve_time)
            f.write(f"Retrieve {key}: {retrieve_time}\n")

            delete_time = measure_time(delete_key, key)
            delete_times.append(delete_time)
            f.write(f"Delete {key}: {delete_time}\n")

    plt.plot(insert_times, label='Insert')
    plt.plot(retrieve_times, label='Retrieve')
    plt.plot(delete_times, label='Delete')
    plt.xlabel('Operation Count')
    plt.ylabel('Time (seconds)')
    plt.legend()
    plt.title('Performance Test')
    plt.savefig('performance_test.png')  # Save the graph to a file
    plt.close()  # Ensure the plot is saved and closed properly

def automatic_test_from_file(file_path):
    with open(file_path, 'r') as file:
        commands = file.readlines()

    for command in commands:
        command = command.strip()
        print(f"\n> {command}")
        if command.startswith("insert|"):
            _, kv = command.split("|", 1)
            key, value = kv.split(":", 1)
            insert_key_value(key, value)
        elif command.startswith("get|"):
            _, key = command.split("|", 1)
            retrieve_key_value(key)
        elif command.startswith("delete|"):
            _, key = command.split("|", 1)
            delete_key(key)
    performance_test()
    os._exit(0)  # Exit after running all commands and saving the file

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python mainserver.py <ip> <port> [<known_ip> <known_port>]")
        sys.exit(1)

    ip, port = sys.argv[1], int(sys.argv[2])
    node.init_node(ip, port)
    
    if len(sys.argv) == 5:
        node.join((sys.argv[3], int(sys.argv[4])))
    else:
        node.join()

    for thread in [
        threading.Thread(target=node.serve_forever, daemon=True),
        threading.Thread(target=node.fix_fingers, daemon=True)
    ]:
        thread.start()

    automatic_test_from_file("xyz.txt")