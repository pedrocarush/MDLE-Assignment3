from collections import defaultdict
import socket
import csv
from sys import stdin
from datetime import datetime
from time import sleep

from plot_common import plot_top_5_over_time

MAX_PORT_TRIES = 5
TOP_N_ITEMS = 5
SAMPLE_TOP_EVERY_N_SECONDS = 5

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

counts_over_time = defaultdict(lambda: 0)
most_frequent_at_time = {}
try:
    # Send data to client
    prev_time = None
    prev_sample_time = datetime.now()
    for row in csv.DictReader(stdin, delimiter=','):
        current_time = datetime.fromisoformat(row['timestamp'])
        
        if prev_time is not None:
            sleep((current_time - prev_time).total_seconds())
            if (datetime.now() - prev_sample_time).total_seconds() > SAMPLE_TOP_EVERY_N_SECONDS:
                most_frequent_at_time[current_time] = sorted(counts_over_time.items(), key=lambda t: t[1], reverse=True)[:TOP_N_ITEMS]
                prev_sample_time = datetime.now()
        prev_time = current_time

        item = row['event']
        client_socket.send((f"{item},{current_time}\n").encode("utf-8"))
        counts_over_time[item] += 1
        
except KeyboardInterrupt:
    print('Quitting!')

except BrokenPipeError:
    print('Connection lost!')

except RuntimeError as e:
    print('Oops, something went wrong!')
    print(e)

finally:

    plot_top_5_over_time(most_frequent_at_time.items(), title_suffix=' (actual)', save_no_show=True)

    # Close the connection
    client_socket.close()
    server_socket.close()
