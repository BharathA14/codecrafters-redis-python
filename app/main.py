import socket
import threading
from collections import defaultdict
import time
import argparse

parser = argparse.ArgumentParser()

def response_gen(decoded_data: list, key_store: dict[str, list], args: dict[str, str]):
    print(decoded_data)
    command = decoded_data[2].lower()

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

        case "config":
            match decoded_data[6]:
                case "dir":
                    response = generate_op_string([decoded_data[6], args.dir]).encode() # type: ignore
                case "dbfilename":
                    response = generate_op_string([decoded_data[6], args.dbfilename]).encode() # type: ignore

        case _:
            response = ("+PONG\r\n").encode()
    return response


def generate_op_string(data:list)-> str:
    n = len(data)
    response=[f"*{n}\r\n"]
    for i in data:
        response.append(f"${len(i)}\r\n{i}\r\n")
    print(response)
    return ''.join(response)

def handle_request(client_socket: socket.socket, key_store: defaultdict, args: dict):
    try:
        while True:
            data = client_socket.recv(2048)
            if not data:
                break
            decoded = data.decode().split("\r\n")

            response = response_gen(decoded, key_store, args)
            client_socket.sendall(response)

    except Exception as e:
        print("exception: ", e)


def main(args):
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
        threading.Thread(target=handle_request, args=(c,keystore,args,)).start()

if __name__ == "__main__":
    parser.add_argument("--dir")
    parser.add_argument("--dbfilename")
    args = parser.parse_args()
    main(args)
