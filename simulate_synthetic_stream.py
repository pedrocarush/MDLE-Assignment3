import socket
import random
import matplotlib.pyplot as plt
from time import sleep
from typing import Generator
from datetime import datetime
from itertools import count

from plot_common import plot_ones_over_time

MAX_SLEEP_TIME = 2
MAX_PORT_TRIES = 5
MAX_BITS = 10000
HISTORY_SIZE = 1000

random.seed(0)

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

# Use whatever multiplier and modulus values, we don't care about the distribution of the bits
def bit_generator(seed: int = 0, multiplier: int = 1234567890, bias: int = 75320, modulus: int = 987654321) -> Generator[int, None, None]:
    for _ in range(MAX_BITS):
        yield seed & 1
        seed = (seed * multiplier + bias) % modulus

s = 0
timestamps = []
historic_bits = []
sums = []
try:
    # Send data to client
    prev_time = None
    for bit, timestamp_n in zip(bit_generator(), count()):
        timestamp = datetime.now()
        client_socket.send((f"{bit},{timestamp}\n").encode("utf-8"))
        sleep(random.random() * MAX_SLEEP_TIME)
        s += bit
        historic_bits.append(bit)
        if len(historic_bits) > HISTORY_SIZE:
            historic_bits.pop(0)
        timestamps.append(timestamp)
        sums.append(sum(historic_bits))
        
except KeyboardInterrupt:
    print('Quitting!')

except BrokenPipeError:
    print('Connection lost!')

except RuntimeError as e:
    print('Oops, something went wrong!')
    print(e)

finally:
    print('Total 1s:', s)
    plot_ones_over_time(list(zip(timestamps, sums)), title_suffix=' (actual)', save_no_show=True)

    # Close the connection
    client_socket.close()
    server_socket.close()
