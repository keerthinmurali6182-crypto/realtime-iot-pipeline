import socket
import time
import json
import random
from datetime import datetime

HOST = 'localhost'
PORT = 9999

def generate_mock_data():
    return {
        "device_id": f"sensor_{random.randint(1, 3)}",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "temperature": round(random.uniform(20.0, 35.0), 2),
        "humidity": round(random.uniform(40.0, 60.0), 2)
    }

def start_server():
    # Create a TCP/IP socket
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    server_socket.bind((HOST, PORT))
    server_socket.listen(1)
    print(f"📡 Data generator listening on {HOST}:{PORT}...")
    print("👉 Now you can start your Spark pipeline in the other terminal!")

    while True:
        try:
            # Wait for Spark to connect
            client_socket, client_address = server_socket.accept()
            print(f"🤝 Spark connected from {client_address}!")
            
            # Start streaming data to Spark
            while True:
                data = generate_mock_data()
                json_string = json.dumps(data) + "\n"  # Spark needs the newline character
                
                client_socket.sendall(json_string.encode('utf-8'))
                print(f"Sent: {json_string.strip()}")
                
                time.sleep(1) # Send data every 1 second
                
        except (ConnectionResetError, BrokenPipeError):
            print("⚠️ Spark disconnected. Waiting for reconnection...")
        except KeyboardInterrupt:
            print("\n🛑 Stopping data generator.")
            break
        finally:
            if 'client_socket' in locals():
                client_socket.close()
                
    server_socket.close()

if __name__ == "__main__":
    start_server()