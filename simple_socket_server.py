import socket


# Define the host and port
HOST = 'localhost'
PORT = 9999

# Create a socket object
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Bind the socket to the host and port
server_socket.bind((HOST, PORT))

# Listen for incoming connections
server_socket.listen(1)
print('Server is listening on {}:{}'.format(HOST, PORT))

# Accept a connection from a client
client_socket, client_address = server_socket.accept()
print('Accepted connection from {}:{}'.format(client_address[0], client_address[1]))

try:
    # Send data to client
    while True:
        data = input()
        client_socket.send((data + "\n").encode("utf-8"))

except KeyboardInterrupt:
    pass

finally:
    # Close the connection
    client_socket.close()
    server_socket.close()
