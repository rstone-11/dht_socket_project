import socket
import sys

serverIP = sys.argv[1]
serverPort = sys.argv[2] 

# Create a UDP socket
clientSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

server_address = (serverIP, int(serverPort)) # Replace 'localhost' with the server's IP if different

try:
    # Send data
    message = b'This is the message. It will be repeated.'
    print(f"Sending {message}")
    sent = clientSocket.sendto(message, server_address)

    # Receive response
    data, server = clientSocket.recvfrom(4096)
    print(f"Received {data}")

    while True:
        message = input("Message: ")
        clientSocket.sendto(message.encode(), server_address)

        data, server = clientSocket.recvfrom(4096)
        print(f"Received {data} from {server}")

finally:
    print("Closing socket")
    clientSocket.close()
