import socket
import sys
import random

serverIP = sys.argv[1]
serverPort = sys.argv[2] 
#port number range = [7000, 7499]
managerPort = random.randint(7001, 7499)
peerPort = random.randint(7001, 7499)
while managerPort == peerPort:
    peerPort = random.randint(7001, 7499)

# Create a UDP socket
clientSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
clientSocket.bind(('', managerPort))
assigned_ip1, assigned_port1 = clientSocket.getsockname()

peerSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
peerSocket.bind(('', peerPort))
assigned_ip2, assigned_port2 = peerSocket.getsockname()

hostname = socket.gethostname()
IP = socket.gethostbyname(hostname)

print(f"Manager socket bound to port {managerPort}, Peer socket bound to port: {peerPort}")
print(f"running on ip: {IP}")


server_address = (serverIP, int(serverPort)) # Replace 'localhost' with the server's IP if different

try:
    # Send data
    #message = b'This is the message. It will be repeated.'
    #print(f"Sending {message}")
    #sent = clientSocket.sendto(message, server_address)

    # Receive response
    #data, server = clientSocket.recvfrom(4096)
    #print(f"Received {data}")

    while True:
        message = input("Message to Manager: ")
        clientSocket.sendto(message.encode(), server_address)

        data, server = clientSocket.recvfrom(4096)
        print(f"Received {data} from {server}")

finally:
    print("Closing socket")
    clientSocket.close()
    peerSocket.close()
