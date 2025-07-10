import socket
import threading
from collections import defaultdict
import time
import argparse
import app.rdb_parser as rdb_parser
import re

parser = argparse.ArgumentParser()
master_host = ""
master_host_port = ""
replica = False

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
                response = "$-1\r\n".encode()

        case "config":
            match decoded_data[6]:
                case "dir":
                    response = generate_resp_array([decoded_data[6], args.dir]).encode() # type: ignore
                case "dbfilename":
                    response = generate_resp_array([decoded_data[6], args.dbfilename]).encode() # type: ignore

        case "keys":
            matching_keys = []
            pattern = re.escape("*")
            for i in list(key_store.keys()):
                if re.search(pattern,decoded_data[4]):
                    matching_keys.append(i)
            return generate_resp_array(matching_keys).encode()
        
        case "info":
            if args.replicaof == "": # type: ignore
                response = encode_bulk_string("{role:master,master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb,master_repl_offset:0}").encode()
            else:
                response = encode_bulk_string("{role:slave}").encode()

        case "replconf":
            response = ("+OK\r\n").encode()
        case _:
            response = ("+PONG\r\n").encode()

    return response

def encode_bulk_string(i: str):
    return f"${len(i)}\r\n{i}\r\n"
    
def generate_resp_array(data:list)-> str:
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

def parse_master_config(config: str):
    global master_host, master_host_port
    if config != "":
        master_host, master_host_port = config.split(" ")
        master_host_port = int(master_host_port)

def is_replica():
    return master_host != ""

def add_wait_to_send_message(s:socket.socket, message: bytes):
    s.sendall(message)
    time.sleep(0.1)

def ping_master(s: socket.socket):
    add_wait_to_send_message(s,generate_resp_array(["PING"]).encode())
    
def ping_master_for_repl_conf(s: socket.socket, port: str):
    add_wait_to_send_message(s, generate_resp_array(["REPLCONF", "listening-port", port]).encode())
    add_wait_to_send_message(s, generate_resp_array(["REPLCONF", "capa", "psync2"]).encode())

def ping_master_for_sync(s:socket.socket):
    add_wait_to_send_message(s, generate_resp_array(["PSYNC", "?", "-1"]).encode())

def handshake_with_master(s: socket.socket, port: str):
    ping_master(s)
    ping_master_for_repl_conf(s, port)
    ping_master_for_sync(s)


def main(args):
    
    server_socket = socket.create_server(("localhost", int(args.port)), reuse_port=True)

    keystore = rdb_parser.read_file_and_construct_kvm(args.dir, args.dbfilename)
    parse_master_config(args.replicaof)

    if is_replica():
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.connect((master_host, int(master_host_port)))
        handshake_with_master(s, args.port)


    while True:
        c, _ = server_socket.accept()
        threading.Thread(target=handle_request, args=(c,keystore,args,)).start()

if __name__ == "__main__":
    parser.add_argument("--dir")
    parser.add_argument("--dbfilename")
    parser.add_argument("--port", default=6379)
    parser.add_argument("--replicaof", default = "")
    args = parser.parse_args()
    main(args)
