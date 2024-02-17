import socket
import sys

portNum = sys.argv[1]
clients = []

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
    parts = message_text.split()  # Split by whitespace to get command parts

    # Process the command
    if parts[0] == "register" and len(parts) == 5:
        # Extract parameters from the command
        peer_name, ipv4_address, m_port, p_port = parts[1:]
        # Process registration...
        if any(client['peer_name'] == peer_name for client in clients):
                print(f"peer name is not unique")
                serverSocket.sendto(b"FAILURE",(ipv4_address, int(m_port)))
            # Peer name is not unique, so registration fails
        else:
            # If unique, add the new peer
            clients.append({
                'peer_name': peer_name,
                'ipv4_address': ipv4_address,
                'm_port': m_port,
                'p_port': p_port,
                'state': 'Free'
            })
            print(f"Registering peer: {peer_name}, Address: {ipv4_address}, M-port: {m_port}, P-port: {p_port}")
            # Add logic to store these details in a suitable data structure
            serverSocket.sendto(b"SUCCESS",(ipv4_address, int(m_port)))



    elif parts[0] == "setup-dht" and len(parts) == 4:
        peer_name, n, YYYY = parts[1:]
        # Process DHT setup...
        print(f"Setting up DHT for peer: {peer_name}, N: {n}, YYYY: {YYYY}")
        # Add corresponding logic



    elif parts[0] == "dht-complete" and len(parts) == 2:
        peer_name = parts[1]
        # Mark DHT as complete for this peer...
        print(f"DHT complete for peer: {peer_name}")
        # Implement necessary actions



    else:
        print("Unknown or malformed command.")





