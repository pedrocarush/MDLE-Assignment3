import socket
import csv
import gzip
from sys import stdin
from datetime import datetime
from time import sleep


# Define the host and port
HOST = 'localhost'
PORT = 9999

# Create a socket object
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Bind the socket to the host and port
try:
    server_socket.bind((HOST, PORT))
except OSError as e:
    print(f'Oops, this address is probably already in use, trying port {PORT + 1}')
    server_socket.bind((HOST, PORT + 1))

# Listen for incoming connections
server_socket.listen(1)
print('Server is listening on {}:{}'.format(HOST, PORT))

# Accept a connection from a client
client_socket, client_address = server_socket.accept()
print('Accepted connection from {}:{}'.format(client_address[0], client_address[1]))

try:
    # Send data to client
    prev_time = None
    for row in csv.DictReader(stdin, delimiter=','):
        current_time = datetime.fromisoformat(row['timestamp'])
        
        if prev_time is not None:
            sleep((current_time - prev_time).total_seconds())
        prev_time = current_time

        client_socket.send((row['event'] + "\n").encode("utf-8"))
        
except KeyboardInterrupt:
    print('Quitting!')

except RuntimeError as e:
    print('Oops, something went wrong!')
    print(e)

finally:
    # Close the connection
    client_socket.close()
    server_socket.close()
