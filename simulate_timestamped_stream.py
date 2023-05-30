import socket
import csv
from sys import stdin
from datetime import datetime
from time import sleep

MAX_PORT_TRIES = 5

# Define the host and port
HOST = 'localhost'
PORT = 9999

# Create a socket object
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Bind the socket to the host and port
socket_bound = False
n_tries = 0
while not socket_bound and n_tries < MAX_PORT_TRIES:
    try:
        server_socket.bind((HOST, PORT))
        socket_bound = True
    except OSError as e:
        PORT += 1
        print(f'Oops, this address is probably already in use, trying port {PORT}')
    n_tries += 1

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
