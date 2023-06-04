import socket
import random
from time import sleep
from typing import Generator
from datetime import datetime
from itertools import count

MAX_SLEEP_TIME = 2
MAX_PORT_TRIES = 5
MAX_BITS = 10000
MAX_TIMESTAMP_N = 2**32 - 1

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
try:
    # Send data to client
    prev_time = None
    for bit, timestamp_n in zip(bit_generator(), count()):
        client_socket.send((f"{bit},{datetime.now()}\n").encode("utf-8"))
        sleep(random.random() * MAX_SLEEP_TIME)
        s += bit
        
except KeyboardInterrupt:
    print('Quitting!')

except RuntimeError as e:
    print('Oops, something went wrong!')
    print(e)

finally:
    print('Total 1s:', s)
    # Close the connection
    client_socket.close()
    server_socket.close()
