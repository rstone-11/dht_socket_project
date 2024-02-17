import socket
import sys

portNum = sys.argv[1]

# Create a UDP socket
serverSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

# Bind the socket to the server address and port
server_address = ('', int(portNum)) # Use '' to bind to all available interfaces
serverSocket.bind(server_address)

print("UDP server up and listening")

# Listen for incoming datagrams
while True:
    data, address = serverSocket.recvfrom(4096)
    print(f"Received {data} from {address}")

    message_text = data.decode('utf-8')

    if message_text != 'register':
        sent = serverSocket.sendto(b"Welcome to the UDP server!", address)
        print(f"Sent {sent} bytes back to {address}")
        
    else: serverSocket.sendto(b"SUCCESS", address)