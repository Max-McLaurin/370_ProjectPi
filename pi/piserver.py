import socket
import select
from collections import deque
import json
import csv

def json_to_csv(data, filename='data.csv'):
    # Assume data is a list of dictionaries under 'quotes'
    keys = data[0]['quote']['USD'].keys()  # Get headers from the first entry
    with open(filename, 'w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=['timestamp'] + list(keys))
        writer.writeheader()
        for entry in data:
            row = {'timestamp': entry['timestamp']}
            row.update(entry['quote']['USD'])
            writer.writerow(row)

def start_server(host='0.0.0.0', port=65433):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((host, port))
    server_socket.listen(2)
    print(f"Server listening on {host}:{port}")

    sockets_list = [server_socket]
    client_address_map = {}
    clients = {"scraper": None, "processor": None}
    data_queue = deque()

    while True:
        read_sockets, _, exception_sockets = select.select(sockets_list, [], sockets_list)

        for notified_socket in read_sockets:
            if notified_socket == server_socket:
                client_socket, client_address = server_socket.accept()
                sockets_list.append(client_socket)
                client_address_map[client_socket] = client_address
                print(f"Accepted new connection from {client_address}")

                if not clients["scraper"]:
                    clients["scraper"] = client_socket
                    print(f"Scraper client connected: {client_address}")
                elif not clients["processor"]:
                    clients["processor"] = client_socket
                    print(f"Processor client connected: {client_address}")
                    # Send any queued data
                    while data_queue:
                        send_file(clients["processor"], data_queue.popleft())

            else:
                message = notified_socket.recv(4096)
                if message:
                    if notified_socket == clients["scraper"]:
                        data = json.loads(message.decode('utf-8'))
                        # Convert and save to CSV
                        json_to_csv(data['quotes'])
                        data_queue.append('data.csv')
                        if clients["processor"]:
                            send_file(clients["processor"], 'data.csv')
                else:
                    sockets_list.remove(notified_socket)
                    del client_address_map[notified_socket]
                    if notified_socket in [clients["scraper"], clients["processor"]]:
                        clients["scraper"] = None if notified_socket == clients["scraper"] else clients["scraper"]
                        clients["processor"] = None if notified_socket == clients["processor"] else clients["processor"]
                    notified_socket.close()

        for notified_socket in exception_sockets:
            sockets_list.remove(notified_socket)
            del client_address_map[notified_socket]
            notified_socket.close()

def send_file(client_socket, filename):
    with open(filename, 'rb') as file:
        client_socket.sendfile(file)
    print(f"Sent {filename} to processor.")

if __name__ == "__main__":
    start_server()



