import socket
import threading
from collections import defaultdict
import time

def response_gen(decoded_data: list, key_store: dict[str, list]):
    print(decoded_data)
    command = decoded_data[2].lower()

# I will have to capture the clock and save it along as well

    match command:
        case "echo":
            response = (f"{decoded_data[3]}\r\n{decoded_data[4]}\r\n").encode()

        case "set":
            time_out = 0
            if len(decoded_data) == 12:
                time_out = time.time()+float(decoded_data[10])/1000
            key_store[decoded_data[4]] = [decoded_data[6], time_out]
            response = "+OK\r\n".encode()


        case "get":
            if decoded_data[4] in key_store and (key_store[decoded_data[4]][1] == 0 or (key_store[decoded_data[4]][1] >= time.time())):
                print(key_store[decoded_data[4]][1], time.time())
                value = key_store[decoded_data[4]][0] 
                response = (f"${len(value)}\r\n{value}\r\n").encode()
            else:
                print("entered")
                response = "$-1\r\n".encode()
        case _:
            response = ("+PONG\r\n").encode()
    return response


def handle_request(client_socket: socket.socket, key_store: defaultdict):
    try:
        while True:
            data = client_socket.recv(2048)
            if not data:
                break
            decoded = data.decode().split("\r\n")

            response = response_gen(decoded, key_store)
            client_socket.sendall(response)

    except Exception as e:
        print("exception: ", e)


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    # print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    keystore = defaultdict(int)
    #print("Server started")

    while True:
        #print("New conn")
        c, _ = server_socket.accept()
        threading.Thread(target=handle_request, args=(c,keystore,)).start()

if __name__ == "__main__":
    main()
