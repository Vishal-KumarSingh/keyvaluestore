import threading

node_id = None
ip = None
port = None
m = 10
lock = threading.Lock()
table = []
finger_starts = []

def init_finger_table(node_node_id, node_ip, node_port, bits=10):
    global node_id, ip, port, m, finger_starts, table
    try:
        node_id = node_node_id
        ip = node_ip
        port = node_port
        m = bits
        finger_starts = [(node_id + 2**i) % (2**m) for i in range(m)]
        table = [(node_ip, node_port) for _ in range(m)]
        return True
    except Exception as e:
        print(f"Error initializing finger table: {e}")
        return False

def update_finger(index, node):
    with lock:
        if 0 <= index < m:
            table[index] = node
            return True
    return False

def get_finger(index):
    return table[index] if 0 <= index < m else None

def get_finger_start(index):
    return finger_starts[index] if 0 <= index < len(finger_starts) else None

def set_all_fingers(fingers):
    global table
    with lock:
        table = fingers
        return True
