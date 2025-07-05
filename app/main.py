import socket
import threading

def sendMessage(c):
    #print("Entered")
    while b := c.recv(4096):
        if b=="":
            break   
        c.sendall(b"+PONG\r\n")

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    # print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    #print("Server started")

    while True:
        #print("New conn")
        c, _ = server_socket.accept()
        threading.Thread(target=sendMessage, args=(c,)).start()

if __name__ == "__main__":
    main()
