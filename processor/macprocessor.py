import socket
import csv
import io

def receive_full_data(sock):
    data = b""
    try:
        sock.settimeout(5.0)
        while True:
            part = sock.recv(4096)
            if not part:
                break
            data += part
            if len(part) < 4096:
                break
    except socket.timeout:
        print("Timeout reached while waiting for data.")
    except Exception as e:
        print(f"An error occurred: {e}")
    return data

def connect_to_server(server_ip, port=65433):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((server_ip, port))
            print("Connected to server.")
            data = receive_full_data(sock)
            if data:
                # Assume the data is a CSV file content
                print("Received data:")
                file_stream = io.StringIO(data.decode('utf-8'))
                reader = csv.reader(file_stream)
                for row in reader:
                    print(row)
            else:
                print("No data received.")
    except socket.error as e:
        print(f"Could not connect to server {server_ip} on port {port}: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    SERVER_IP = '10.0.0.224'  # Ensure this is the correct IP address
    connect_to_server(SERVER_IP, 65433)
