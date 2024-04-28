import socket
import select

def start_server(host='0.0.0.0', port=65433):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((host, port))
    server_socket.listen()
    print(f"Server listening on {host}:{port}")

    clients = []
    data_queue = []
    while True:
        client_socket, addr = server_socket.accept()
        print(f"Client from {addr} connected.")
        clients.append(client_socket)

        # Check if both clients are connected
        if len(clients) == 2:
            print("Both clients have connected. Ready to relay data.")
            for client in clients:
                client.send(b"Ready to relay data.")
            
            # If there is queued data, send it
            while data_queue:
                for client in clients:
                    client.send(data_queue.pop(0))

        data = client_socket.recv(4096)
        if data:
            data_queue.append(data)  # Queue the data if not all clients are connected

if __name__ == "__main__":
    start_server()
