import sys
import socket
import threading
import hashlib
import json
import time
import os
import fingertable as ft

__all__ = ['print_finger_table', 'init_node', 'join', 'store_key_value', 
           'retrieve_value', 'find_key_successor', 'is_key_owner', 
           'remote_store_key', 'remote_retrieve_key', 'remote_delete_key']

ip = None
port = None
node_id = None
m = 10
successor = None
predecessor = None
data_store = {}
data_store_file = ""
lock = threading.Lock()
is_standalone = False
DEBUG_MODE = False
CONNECTION_TIMEOUT = 1
MAX_RETRIES = 3
RETRY_DELAY = 0.2
DATA_STORE_DIR = "data_stores"
active_threads = []  # Add this line
max_concurrent_threads = 50  # Add this line if not already present

def init_node(node_ip, node_port, node_m=10): 
    global ip, port, node_id, m, successor, data_store_file
    global last_finger_update, successor_list
    
    try:
        ip = node_ip
        port = node_port
        m = node_m
        node_id = hash_function(f"{ip}:{port}")
        successor = (ip, port)
        last_finger_update = time.time()
        successor_list = []
        
        os.makedirs(DATA_STORE_DIR, exist_ok=True)
        data_store_file = os.path.join(DATA_STORE_DIR, f"node_data_{ip}_{port}.json")
        load_data_store()
        
        if not ft.init_finger_table(node_id, ip, port, m):
            raise RuntimeError("Failed to initialize finger table")
        return True
    except Exception as e:
        print(f"Error initializing node: {e}")
        raise

def hash_function(key, bits=10):
    return int(hashlib.sha1(key.encode()).hexdigest(), 16) % (2 ** bits)

def load_data_store():
    """Improved data store loading with better error handling"""
    global data_store, DATA_STORE_DIR
    try:
        # Create directory if it doesn't exist
        os.makedirs(DATA_STORE_DIR, exist_ok=True)
        
        # Ensure data_store_file is properly set
        if not data_store_file:
            print("Warning: data_store_file not set")
            return
            
        if os.path.exists(data_store_file):
            try:
                with open(data_store_file, 'r') as f:
                    loaded_data = json.load(f)
                    if isinstance(loaded_data, dict):
                        data_store = loaded_data
                        print(f"Loaded {len(data_store)} keys from {data_store_file}")
                    else:
                        print("Invalid data store format, creating new one")
                        data_store = {}
            except Exception as e:
                print(f"Error reading data store: {e}")
                data_store = {}
        else:
            print(f"Creating new data store at {data_store_file}")
            data_store = {}
            save_data_store()
            
    except Exception as e:
        print(f"Error in load_data_store: {e}")
        data_store = {}

def save_data_store():
    """Enhanced data store saving with atomic write"""
    if not data_store_file:
        print("Warning: data_store_file not set")
        return
        
    try:
        # Create temp file
        temp_file = f"{data_store_file}.tmp"
        with open(temp_file, 'w') as f:
            json.dump(data_store, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
            
        # Atomic rename
        os.replace(temp_file, data_store_file)
        
    except Exception as e:
        print(f"Error saving data store: {e}")
        if os.path.exists(temp_file):
            try:
                os.remove(temp_file)
            except:
                pass

def store_key_value(key, value):
    with lock:
        data_store[key] = value
        save_data_store()

def retrieve_value(key):
    """Retrieve a value from the data store"""
    with lock:
        if key in data_store:
            return data_store[key]
        raise KeyError("Key not found")

def remove_key(key):
    """Remove a key from the data store"""
    with lock:
        if key in data_store:
            del data_store[key]
            save_data_store()
        else:
            raise KeyError("Key not found")

def serve_forever():
    global active_threads  # Add this line
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((ip, port))
    server_socket.listen(5)

    while True:
        try:
            conn, addr = server_socket.accept()
            if len(active_threads) < max_concurrent_threads:
                thread = threading.Thread(target=handle_client_request, args=(conn,))
                thread.daemon = True
                active_threads.append(thread)
                thread.start()
            else:
                print("Thread pool full, dropping connection")
                conn.close()
        except Exception as e:
            print(f"Error in server: {e}")
            time.sleep(1)

def handle_client_request(conn):
    global predecessor, successor, active_threads, is_standalone
    
    try:
        conn.settimeout(5)
        data = conn.recv(4096).decode()
        if not data:
            return
            
        request = json.loads(data)
        response = {"status": "error", "message": "Invalid command"}

        # Handle store_key
        if request["command"] == "store_key":
            key = request["key"]
            value = request["value"]
            current_successor = find_key_successor(hash_function(key))
            
            if current_successor == (ip, port):
                store_key_value(key, value)
                response = {"status": "success", "message": "Key stored successfully"}
            else:
                response = remote_store_key(current_successor, key, value)

        # ...existing command handlers...

        elif request["command"] == "notify":
            possible_predecessor = tuple(request["predecessor"])
            
            # Handle first node in network (standalone)
            if is_standalone:
                predecessor = possible_predecessor
                successor = possible_predecessor
                is_standalone = False
                print(f"First connection: setting predecessor and successor to {possible_predecessor}")
                response = {"status": "notified", "old_predecessor": None}
                
            # Normal notify handling with improved checks
            elif possible_predecessor != (ip, port):
                should_update = False
                if predecessor is None:
                    should_update = True
                else:
                    pred_id = hash_function(f"{predecessor[0]}:{predecessor[1]}")
                    possible_pred_id = hash_function(f"{possible_predecessor[0]}:{possible_predecessor[1]}")
                    
                    if is_between_exclusive(possible_pred_id, pred_id, node_id):
                        should_update = True
                    elif pred_id == node_id:  # Handle self-reference case
                        should_update = True
                    elif not check_node_alive(predecessor):  # Handle dead predecessor
                        should_update = True
                
                if should_update:
                    old_predecessor = predecessor
                    predecessor = possible_predecessor
                    if DEBUG_MODE or old_predecessor != predecessor:
                        print(f"Updated predecessor to: {predecessor}")
                    response = {"status": "notified", "old_predecessor": old_predecessor}
                else:
                    response = {"status": "rejected"}
            else:
                response = {"status": "rejected"}

        elif request["command"] == "delete_key":
            try:
                key = request["key"]
                key_id = hash_function(key)
                if is_key_owner(key_id):
                    if key in data_store:
                        remove_key(key)
                        response = {"status": "success", "message": "Key deleted successfully"}
                    else:
                        response = {"status": "error", "message": "Key not found"}
                else:
                    successor = find_key_successor(key_id)
                    response = remote_delete_key(successor, key)
            except Exception as e:
                response = {"status": "error", "message": str(e)}

        elif request["command"] == "ping":
            response = {"status": "alive"}

        elif request["command"] == "find_successor":
            id_ = request["id"]
            succ = find_key_successor(id_)
            response = {"successor": succ}

        elif request["command"] == "get_predecessor":
            response = {"predecessor": predecessor}

        elif request["command"] == "get_successor_list":
            response = {"successor_list": successor_list}

        elif request["command"] == "retrieve_key":
            try:
                key = request["key"]
                
                # First check local data store regardless of ownership
                if key in data_store:
                    response = {"status": "success", "value": data_store[key]}
                else:
                    # If not in local store, check if we're the owner
                    key_id = hash_function(key)
                    if is_key_owner(key_id):
                        response = {"status": "error", "message": "Key not found"}
                    else:
                        successor = find_key_successor(key_id)
                        if successor != (ip, port):
                            response = remote_retrieve_key(successor, key)
                        else:
                            response = {"status": "error", "message": "Key not found"}
            except Exception as e:
                response = {"status": "error", "message": str(e)}

        # ...rest of the function...

        conn.send(json.dumps(response).encode())
    except socket.timeout:
        print("Request handling timed out")
    # except Exception as e:
    #     print(f"Error handling request: {e}")

        try:
            conn.send(json.dumps({"status": "error", "message": str(e)}).encode())
        except:
            pass
    finally:
        conn.close()
        if threading.current_thread() in active_threads:
            active_threads.remove(threading.current_thread())

def is_key_owner(key_id):
    """Simplified key ownership check without replication"""
    if predecessor is None or predecessor == (ip, port):
        return True
    
    pred_id = hash_function(f"{predecessor[0]}:{predecessor[1]}")
    node_hash = hash_function(f"{ip}:{port}")
    
    if pred_id < node_hash:
        return pred_id < key_id <= node_hash
    return key_id > pred_id or key_id <= node_hash

def find_key_successor(id_):
    """Find successor for a given id with better null checking"""
    try:
        # Handle case where successor is None or self
        if successor is None or successor == (ip, port):
            return (ip, port)
            
        succ_id = hash_function(f"{successor[0]}:{successor[1]}")
        if is_between_exclusive(id_, node_id, succ_id):
            return successor
        else:
            closest_node = find_nearest_preceding_node(id_)
            if closest_node == (ip, port):
                return successor if successor else (ip, port)
            return remote_find_successor(closest_node, id_)
    except Exception as e:
        print(f"Error in find_key_successor: {e}")
        return (ip, port)

def find_nearest_preceding_node(id_):
    """Find nearest preceding node with better error handling"""
    try:
        for i in range(m - 1, -1, -1):
            finger = ft.get_finger(i)
            if not finger or finger == (ip, port):
                continue
                
            try:
                finger_id = hash_function(f"{finger[0]}:{finger[1]}")
                if is_between_exclusive(finger_id, node_id, id_):
                    if check_node_alive(finger):
                        return finger
            except Exception:
                continue
    except Exception as e:
        print(f"Error in find_nearest_preceding_node: {e}")
    return (ip, port)

def handle_connection(node, command_dict, timeout=None):
    """Common connection handling function"""
    retries = MAX_RETRIES
    delay = RETRY_DELAY
    
    while retries > 0:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(timeout or CONNECTION_TIMEOUT)
                s.connect(node)
                s.send(json.dumps(command_dict).encode())
                data = s.recv(4096).decode()
                if not data:
                    raise ConnectionError("Empty response")
                return json.loads(data)
        except (socket.timeout, ConnectionRefusedError, json.JSONDecodeError) as e:
            retries -= 1
            if retries == 0:
                break
            time.sleep(delay)
            delay *= BACKOFF_FACTOR
        except Exception as e:
            break
    return None

def remote_find_successor(node, id_):
    """Update remote_find_successor to use handle_connection with better error handling"""
    if not check_node_alive(node):
        return (ip, port)
        
    response = handle_connection(node, {
        "command": "find_successor",
        "id": id_
    })
    
    if response and isinstance(response, dict) and "successor" in response:
        successor = response["successor"]
        if isinstance(successor, (list, tuple)) and len(successor) == 2:
            return tuple(successor)
    return (ip, port)

def remote_store_key(node, key, value, retries=3):
    for attempt in range(retries):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5)
                s.connect(node)
                request = {
                    "command": "store_key",
                    "key": key,
                    "value": value
                }
                s.send(json.dumps(request).encode())
                data = s.recv(4096).decode()
                if not data:
                    raise ConnectionError("Empty response received")
                response = json.loads(data)
                return response
        except json.JSONDecodeError:
            print(f"Invalid response from node {node}, attempt {attempt + 1}")
        except socket.timeout:
            print(f"Timeout while contacting node {node}, attempt {attempt + 1}")
        except Exception as e:
            print(f"Error storing key at node {node}, attempt {attempt + 1}: {e}")
        time.sleep(1)
    return {"status": "error", "message": "Request failed after multiple attempts"}

def remote_retrieve_key(node, key, retries=3):
    """Enhanced remote key retrieval with better error handling"""
    if not node or node == (ip, port):
        return {"status": "error", "message": "Invalid node"}

    for attempt in range(retries):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5)
                s.connect(node)
                request = {
                    "command": "retrieve_key",
                    "key": key
                }
                s.send(json.dumps(request).encode())
                data = s.recv(4096).decode()
                if not data:
                    raise ConnectionError("Empty response received")
                response = json.loads(data)
                if response.get("status") == "success":
                    return response
                elif attempt == retries - 1:
                    return response
        except Exception as e:
            if attempt == retries - 1:
                return {"status": "error", "message": f"Failed to retrieve key: {str(e)}"}
            time.sleep(0.5 * (attempt + 1))  # Exponential backoff
    return {"status": "error", "message": "Request failed after all retries"}

def remote_delete_key(node, key, retries=3):
    for attempt in range(retries):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(10)  # Increase timeout
                s.connect(node)
                request = {
                    "command": "delete_key",
                    "key": key
                }
                s.send(json.dumps(request).encode())
                data = s.recv(4096).decode()
                if not data:
                    raise ConnectionError("Empty response received")
                response = json.loads(data)
                return response
        except json.JSONDecodeError:
            print(f"Invalid response from node {node}, attempt {attempt + 1}")
        except socket.timeout:
            print(f"Timeout while contacting node {node}, attempt {attempt + 1}")
        except Exception as e:
            print(f"Error deleting key at node {node}, attempt {attempt + 1}: {e}")
        time.sleep(1)  # Add delay between retries
    return {"status": "error", "message": "Request failed after multiple attempts"}

def join(known_node=None):
    global successor, predecessor, is_standalone
    try:
        if known_node:
            # First try to find our successor
            new_successor = remote_find_successor(known_node, node_id)
            if not new_successor or not check_node_alive(new_successor):
                print("Failed to find/connect to successor")
                return False
                
            if new_successor == (ip, port):
                # If we're getting ourselves as successor, try the known node instead
                new_successor = known_node
                
            successor = new_successor
            predecessor = None  # Initially set to None
            is_standalone = False
            
            # Initialize finger table
            if not init_finger_table(known_node):
                print("Failed to initialize finger table")
                return False
            
            # Get predecessor from successor
            try:
                pred = remote_get_predecessor(successor)
                if pred and pred != (ip, port):
                    predecessor = pred
                    # Notify our predecessor
                    remote_notify(predecessor, (ip, port))
                
                # Always notify our successor
                remote_notify(successor, (ip, port))
                    
                # Force an immediate stabilization
                stabilize_immediate()
                    
            except Exception as e:
                print(f"Warning: Error during predecessor setup: {e}")
            
            print(f"Successfully joined network. Successor: {successor}, Predecessor: {predecessor}")
            return True
            
        else:
            successor = (ip, port)
            predecessor = (ip, port)
            is_standalone = True
            init_finger_table()
            print("Starting as standalone node")
            return True
            
    except Exception as e:
        print(f"Error joining network: {e}")
        successor = (ip, port)  # Fallback to self
        predecessor = (ip, port)
        return False

def check_node_alive(node, retries=MAX_RETRIES):
    """Check if a node is alive without printing errors"""
    if node == (ip, port):  # Don't check self
        return True
        
    for _ in range(retries):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(CONNECTION_TIMEOUT)
                s.connect(node)
                s.send(json.dumps({"command": "ping"}).encode())
                response = json.loads(s.recv(1024).decode())
                if response.get("status") == "alive":
                    return True
        except Exception:
            time.sleep(RETRY_DELAY)
    return False

def remote_get_predecessor(node):
    """Update remote_get_predecessor to use handle_connection"""
    if not check_node_alive(node):
        return None
        
    response = handle_connection(node, {
        "command": "get_predecessor"
    })
    
    if response and "predecessor" in response:
        return tuple(response["predecessor"])
    return None

def remote_notify(node, possible_predecessor, retries=MAX_RETRIES):
    """Improved notify with better error handling"""
    if not check_node_alive(node) or node == (ip, port):
        return False
    
    try:
        response = handle_connection(node, {
            "command": "notify",
            "predecessor": possible_predecessor
        })
        return response and response.get("status") == "notified"
    except Exception:
        return False    

def init_finger_table(known_node=None):
    if known_node:
        try:
            # Find successor for first finger
            first_finger = remote_find_successor(known_node, (node_id + 2**0) % (2**m))
            if not first_finger:
                print("Failed to get first finger, defaulting to self")
                ft.set_all_fingers([(ip, port)] * m)
                return False
                
            ft.update_finger(0, first_finger)
            
            # Initialize remaining fingers
            for i in range(1, m):
                try:
                    start = ft.get_finger_start(i)
                    if start is None:
                        continue
                        
                    prev_finger = ft.get_finger(i-1)
                    if not prev_finger:
                        continue
                        
                    if is_between(start, node_id, 
                        hash_function(f"{prev_finger[0]}:{prev_finger[1]}")):
                        ft.update_finger(i, prev_finger)
                    else:
                        new_finger = remote_find_successor(known_node, start)
                        if new_finger:
                            ft.update_finger(i, new_finger)
                except Exception as e:
                    print(f"Error initializing finger {i}: {e}")
                    ft.update_finger(i, (ip, port))
            return True
                    
        except Exception as e:
            print(f"Error initializing finger table: {e}")
            ft.set_all_fingers([(ip, port)] * m)
            return False
    else:
        ft.set_all_fingers([(ip, port)] * m)
        return True

def fix_fingers():
    global last_finger_update, successor
    i = 0
    while True:
        try:
            current_time = time.time()
            if current_time - last_finger_update > 30:
                if successor and successor != (ip, port) and check_node_alive(successor):
                    init_finger_table(successor)
                else:
                    ft.set_all_fingers([(ip, port)] * m)
                last_finger_update = current_time
            else:
                start = ft.get_finger_start(i)
                if start is not None:
                    try:
                        new_finger = find_key_successor(start)
                        if new_finger and isinstance(new_finger, tuple) and len(new_finger) == 2:
                            if check_node_alive(new_finger):
                                ft.update_finger(i, new_finger)
                            else:
                                ft.update_finger(i, (ip, port))
                        else:
                            ft.update_finger(i, (ip, port))
                    except Exception as e:
                        ft.update_finger(i, (ip, port))
                i = (i + 1) % m
        except Exception as e:
            ft.update_finger(i, (ip, port))
        time.sleep(1)

def is_between(id_, start, end):
    if start <= end:
        return start <= id_ <= end
    return id_ >= start or id_ <= end

def is_between_exclusive(id_, start, end):
    """Handle edge cases in is_between_exclusive"""
    try:
        if start < end:
            return start < id_ < end
        return id_ > start or id_ < end
    except Exception:
        return False

