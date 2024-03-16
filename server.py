import socket
import sys
import random
import json

portNum = sys.argv[1]
clients = []
selected_peers = []

#create a UDP socket
serverSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

#bind the socket to the server address and port
server_address = ('', int(portNum)) #use '' to bind to all available interfaces
serverSocket.bind(server_address)

print("UDP server up and listening")
hostname = socket.gethostname()
IP = socket.gethostbyname(hostname)
print(f"running on ip: {IP}")

#listen for incoming datagrams
while True:
    data, address = serverSocket.recvfrom(4096)

    message_text = data.decode('utf-8')
    parts = message_text.split()  #split by whitespace to get command parts

    #process the command
    if parts[0] == "register" and len(parts) == 5:
        #extract parameters from the command
        peer_name, ipv4_address, m_port, p_port = parts[1:]

        #process registration
        if any(client['peer_name'] == peer_name for client in clients):
                print(f"peer name is not unique")
                serverSocket.sendto(b"FAILURE",(ipv4_address, int(m_port)))
                continue
                #peer name is not unique, so registration fails
        else:
            #if unique, add the new peer
            clients.append({
                'peer_name': peer_name,
                'ipv4_address': ipv4_address,
                'm_port': int(m_port),
                'p_port': int(p_port),
                'state': 'Free'
            })
            print(f"Registering peer: {peer_name}, Address: {ipv4_address}, M-port: {m_port}, P-port: {p_port}")
            serverSocket.sendto(b"SUCCESS",(ipv4_address, int(m_port)))



    elif parts[0] == "setup-dht" and len(parts) == 4:
        peer_name, n, YYYY = parts[1:]
        n = int(n)
        
        #check if peer-name is registered
        if not any(client['peer_name'] == peer_name for client in clients):
             serverSocket.sendto(b"FAILURE", address)
             print(f"peer name is not registered")
             continue

        #n is at least 3
        if n < 3:
             serverSocket.sendto(b"FAILURE", address)
             print(f"n isn't at least 3")
             continue

        #at least n users are registered
        if len(clients) < n:
             serverSocket.sendto(b"FAILURE", address)
             print(f"there aren't {n} clients")
             continue

        #a dht has not already been setup
        if any(client['state'] != 'Free' for client in clients):
             serverSocket.sendto(b"FAILURE", address)
             print("a dht has already been setup")
             continue
        
        print(f"Setting up DHT for peer: {peer_name}, N: {n}, YYYY: {YYYY}")

        #manager sets state of peer-name to Leader of one who is specified in command
        for client in clients:
             if client['peer_name'] == peer_name:
                client['state'] = 'Leader'
                leader = client  
                leader_address = (client['ipv4_address'], client['m_port'])
                serverSocket.sendto(b"SUCCESS", (client['ipv4_address'], client['m_port']))
            
        #selects n-1 Free users from registered peers and changes each one’s state to InDHT
        selected_peers = [leader] + random.sample([client for client in clients if client['state'] == 'Free'], n-1)
        for peer in selected_peers[1:]:
             peer['state'] = 'InDHT'
     
        for peer in selected_peers:
             print(peer)

        #sends each peer in the dht individually to the leader
        tuples = [[peer['peer_name'], peer['ipv4_address'], peer['p_port']] for peer in selected_peers]
        for tuple in tuples:
             peer_info = json.dumps(tuple)
             serverSocket.sendto(peer_info.encode('utf-8'), leader_address)

        #waits for the dht-complete command
        data, address = serverSocket.recvfrom(4096)
        message_text = data.decode('utf-8')
        parts = message_text.split()  #split by whitespace to get command parts

        if parts[0] == "dht-complete" and len(parts) == 2:
          peer_name = parts[1]
          if selected_peers[0]['peer_name'] == peer_name:
               # Mark DHT as complete for this peer
               print(f"DHT complete for peer: {peer_name}")
               serverSocket.sendto(b"SUCCESS", address)

          else: serverSocket.sendto(b"FAILURE", address)
        else:
             serverSocket.sendto(b"FAILURE", address)

    elif parts[0] == "query-dht" and len(parts) == 2:
         #make sure dht is setup
         if not selected_peers:
              print('dht is not setup')
              serverSocket.sendto(b"FAILURE", address)
              continue

         #make sure S is registered and free
         name = parts[1]
         #print(f"looking for: {name} in clients")
         peer_S = None
         for client in clients:
              if client['peer_name'] == name:
                   peer_S = client

         if peer_S is None:
              print('client is not registered')
              serverSocket.sendto(b"FAILURE", address)
              continue

         if peer_S['state'] != 'Free':
              print('client is registered but not free')
              serverSocket.sendto(b"FAILURE", address)
              continue     

         #choose one of the peers in the DHT at random
         random_peer = random.choice(selected_peers)

         #sends a 3-tuple of this peers info back to S and S's name and ip
         r_tuple = random_peer['peer_name'], random_peer['ipv4_address'], random_peer['p_port'], peer_S['peer_name'], peer_S['ipv4_address'], len(selected_peers)
         r_tuple = json.dumps(r_tuple)
         serverSocket.sendto(r_tuple.encode('utf-8'), address)

    elif parts[0] == 'deregister' and len(parts) == 2:
         peer_t = None
         #make sure the peer is in a Free state
         peer_name = parts[1]
         for client in clients:
              if client['peer_name'] == peer_name:
                   peer_t = client
          
         #if so then remove peer from clients
         if peer_t is not None and peer_t['state'] == 'Free':
              clients.remove(peer_t)
              serverSocket.sendto(b"SUCCESS", address)
         #if not then return FAILURE
         else:
              serverSocket.sendto(b"FAILURE", address)
        
          
    else:
          print("Unknown command")
          serverSocket.sendto(b"FAILURE", address)
