import socket
import select
from collections import deque
import json
import csv
import zlib


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


def send_data_in_chunks_to_processor(client_socket, data, chunk_size=4096):
   compressed_data = zlib.compress(data.encode('utf-8'))
   print(f"Compressed data size: {len(compressed_data)} bytes")
   total_sent = 0
   while total_sent < len(compressed_data):
       sent = client_socket.send(compressed_data[total_sent:total_sent+chunk_size])
       if sent == 0:
           raise RuntimeError("Socket connection broken")
       total_sent += sent
   print("Data sent to processor in chunks.")
   client_socket.shutdown(socket.SHUT_WR)  # Optionally signal the end of data sending




def forward_data(client_socket, incoming_socket):
   try:
       while True:
           data = incoming_socket.recv(4096)
           if not data:
               break  # No more data to receive
           client_socket.sendall(data)  # Forward data as it is received
   except socket.error as e:
       print(f"Socket error: {e}")
   finally:
       incoming_socket.shutdown(socket.SHUT_RD)
       client_socket.shutdown(socket.SHUT_WR)
   print("Data forwarding completed.")








def receive_data_in_chunks(client_socket, buffer_size=4096):
   data = b""
   try:
       while True:
           part = client_socket.recv(buffer_size)
           if not part:
               break
           data += part
   except socket.error as e:
       print(f"Error receiving data: {e}")
       return None
   try:
       decompressed_data = zlib.decompress(data)
       print("Decompressed data size: {len(decompressed_data)} bytes")
       return decompressed_data.decode('utf-8')
   except zlib.error as e:
       print(f"Decompression error: {e}")
       return None








def start_server(host='0.0.0.0', port=65433):
   server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
   server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
   server_socket.bind((host, port))
   server_socket.listen(2)
   print(f"Server listening on {host}:{port}")




   sockets_list = [server_socket]
   client_address_map = {}
   clients = {"scraper": None, "processor": None}




   while True:
       read_sockets, _, exception_sockets = select.select(sockets_list, [], sockets_list)




       for notified_socket in read_sockets:
           if notified_socket == server_socket:
               client_socket, client_address = server_socket.accept()
               sockets_list.append(client_socket)
               client_address_map[client_socket] = client_address
               print(f"Accepted new connection from {client_address}")




               # Assign scraper or processor based on the order of connection
               if not clients["scraper"]:
                   clients["scraper"] = client_socket
               elif not clients["processor"]:
                   clients["processor"] = client_socket
           else:
               if notified_socket == clients["scraper"]:
                   data = receive_data_in_chunks(notified_socket)
                   # Process data and store/send it
                   if clients["processor"]:
                       send_file(clients["processor"], data)
               else:
                   notified_socket.close()
                   sockets_list.remove(notified_socket)
                   del client_address_map[notified_socket]
                   if notified_socket == clients["scraper"]:
                       clients["scraper"] = None
                   if notified_socket == clients["processor"]:
                       clients["processor"] = None




       for notified_socket in exception_sockets:
           sockets_list.remove(notified_socket)
           del client_address_map[notified_socket]
           notified_socket.close()




def send_file(client_socket, data, chunk_size=4096):
   print(f"Sending data to processor, size: {len(data)} bytes")
   index = 0
   while index < len(data):
       client_socket.sendall(data[index:index+chunk_size])
       index += chunk_size
   print("Data sent to processor.")






if __name__ == "__main__":
   start_server()