import node
import sys
import threading

def menu():
    # Simplified menu display
    print("\n╔═══════════════════════════════════════════════════════╗")
    print("║              Chord DHT Key-Value Store                  ║")
    print("╠═══════╦═════════════════════════════════════════════════╣")
    print("║ Cmd # ║ Description                                     ║")
    print("╠═══════╬═════════════════════════════════════════════════╣")
    print("║   1   ║ insert|key:value  - Store a key-value pair      ║")
    print("║   2   ║ get|key          - Retrieve a value by key      ║")
    print("║   3   ║ delete|key       - Delete a key-value pair      ║")
    print("║   4   ║ finger           - Display finger table         ║")
    print("║   5   ║ info            - Display node information      ║")
    print("║   6   ║ exit            - Exit the program              ║")
    print("╚═══════╩═════════════════════════════════════════════════╝")

    while True:
        try:
            command = input("\n> ")

            if command.startswith("insert|"):
                _, kv = command.split("|", 1)
                key, value = kv.split(":", 1)
                successor = node.find_key_successor(node.hash_function(key))
                
                if successor == (node.ip, node.port):
                    node.store_key_value(key, value)
                    print("Success")
                else:
                    response = node.remote_store_key(successor, key, value)
                    print("Success" if response.get("status") == "success" 
                          else f"Error: {response.get('message')}")

            elif command.startswith("get|"):
                try:
                    _, key = command.split("|", 1)
                    try:
                        print(f"Value: {node.retrieve_value(key)}")
                    except KeyError:
                        successor = node.find_key_successor(node.hash_function(key))
                        response = node.remote_retrieve_key(successor, key)
                        print(f"Value: {response['value']}" if response.get("status") == "success"
                              else "Not found")
                except Exception as e:
                    print(f"Error: {e}")

            elif command.startswith("delete|"):
                try:
                    _, key = command.split("|", 1)
                    key_id = node.hash_function(key)
                    successor = node.find_key_successor(key_id)
                    response = node.remote_delete_key(successor, key)
                    print(f"Response: {response}")
                except Exception as e:
                    print(f"Error deleting key: {e}")

            elif command == "finger":
                node.print_finger_table()

            elif command == "info":
                print(f"\nNode ID: {node.node_id}")
                print(f"IP:Port: {node.ip}:{node.port}")
                print(f"Successor: {node.successor}")
                print(f"Predecessor: {node.predecessor}")
                print(f"Successor List: {node.successor_list}")

            elif command == "exit":
                import os
                os._exit(0)

            else:
                print("Invalid command")

        except Exception as e:
            print(f"Error: {e}")

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

    menu()
