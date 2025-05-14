import socket

HOST = "localhost"
PORT = 30003  # Depends on your dump1090 setup

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect((HOST, PORT))

while True:
    data = sock.recv(1024).decode("utf-8")
    print(data)  # You’ll parse and forward this later
