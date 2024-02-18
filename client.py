import socket
import sys
import random
import json

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

tuples = []

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
        parts = message.split()
        clientSocket.sendto(message.encode(), server_address)

        if parts[0] == "register":
            data, server = clientSocket.recvfrom(4096)
            print(data.decode('utf-8'))

        elif parts[0] == "setup-dht":
            data, server = clientSocket.recvfrom(4096)
            print(data.decode('utf-8'))
            if data.decode('utf-8') == 'SUCCESS':
                n = int(parts[2])
                for i in range(n):
                    data, _ = clientSocket.recvfrom(4096)
                    peer_info = data.decode('utf-8')
                    dataTuple = json.loads(peer_info)
                    tuples.append(dataTuple)
                for peer_tuple in tuples:
                    print(peer_tuple)

        #data, server = clientSocket.recvfrom(4096)
        #print(f"Received {data} from {server}")

finally:
    print("Closing socket")
    clientSocket.close()
    peerSocket.close()
