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
replica_connections = list()
master_connection_socket = None
master_replication_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
master_replication_offset = "0"


def response_gen(
    client_socket: socket.socket,
    raw_command: bytes,
    decoded_data: list,
    key_store: dict[str, list],
    args: dict[str, str],
    client_addr: str
):
    print("decoded in response gen:", decoded_data)
    command = decoded_data[2].lower()

    match command:
        case "echo":
            client_socket.sendall(
                f"{decoded_data[3]}\r\n{decoded_data[4]}\r\n".encode()
            )

        case "set":
            time_out = 0
            key = decoded_data[4]
            value = decoded_data[6]

            if len(decoded_data) == 12:
                time_out = time.time() + float(decoded_data[10]) / 1000
            key_store[key] = [value, time_out]

            if not is_replica():
                handle_replica_set(raw_command)
                client_socket.sendall("+OK\r\n".encode())
            else:
                if client_socket != master_connection_socket:
                    client_socket.sendall("+OK\r\n".encode())

        case "get":
            if decoded_data[4] in key_store and (
                key_store[decoded_data[4]][1] == 0
                or (key_store[decoded_data[4]][1] >= time.time())
            ):
                # print(key_store[decoded_data[4]][1], time.time())
                value = key_store[decoded_data[4]][0]
                client_socket.sendall(f"${len(value)}\r\n{value}\r\n".encode())
            else:
                client_socket.sendall("$-1\r\n".encode())

        case "config":
            match decoded_data[6]:
                case "dir":
                    client_socket.sendall(generate_resp_array([decoded_data[6], args.dir]).encode())  # type: ignore
                case "dbfilename":
                    client_socket.sendall(generate_resp_array([decoded_data[6], args.dbfilename]).encode())  # type: ignore

        case "keys":
            matching_keys = []
            pattern = re.escape("*")
            for i in list(key_store.keys()):
                if re.search(pattern, decoded_data[4]):
                    matching_keys.append(i)
            client_socket.sendall(generate_resp_array(matching_keys).encode())

        case "info":
            if args.replicaof == "":  # type: ignore
                response = generate_bulk_string(
                    "{"
                    + "role:master,master_replid:"
                    + master_replication_id
                    + ",master_repl_offset:"
                    + "0"
                    + "}"
                ).encode()
            else:
                response = generate_bulk_string("{role:slave}").encode()
            client_socket.sendall(response)

        case "replconf":
            if decoded_data[4].lower() == "listening-port":
                try:
                    replica_port = int(decoded_data[6])
                    print(f"Replica reported listening on port {replica_port}. Connecting...")

                    propagation_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    propagation_socket.connect((client_addr, replica_port))
                    
                    replica_connections.append(propagation_socket)
                    print(f"Successfully connected to replica at {client_addr}:{replica_port}. Connection stored.")

                except Exception as e:
                    print(f"Failed to connect to replica at {client_addr}:{replica_port}. Error: {e}")

            client_socket.sendall(("+OK\r\n").encode())

        case "psync":
            add_wait_to_send_message(
                client_socket,
                generate_bulk_string(
                    "FULLRESYNC "
                    + master_replication_id
                    + " "
                    + master_replication_offset
                ).encode(),
            )
            print(rdb_parser.send_rdb_file())
            add_wait_to_send_message(client_socket, rdb_parser.send_rdb_file())
        case _:
            client_socket.sendall(("+PONG\r\n").encode())


def handle_replica_set(command: bytes):
    global replica_connections
    print(f"Propagating command to {len(replica_connections)} replica(s)...")
    
    failed_connections = []

    for replica_socket in replica_connections:
        try:
            replica_socket.sendall(command)
        except (BrokenPipeError, OSError) as e:
            print(f"Failed to propagate to replica {replica_socket.getpeername()}. Error: {e}. Removing connection.")
            failed_connections.append(replica_socket)

    if failed_connections:
        replica_connections = [conn for conn in replica_connections if conn not in failed_connections]


def generate_bulk_string(i: str):
    return f"${len(i)}\r\n{i}\r\n"


def generate_resp_array(data: list) -> str:
    n = len(data)
    response = [f"*{n}\r\n"]
    for i in data:
        response.append(f"${len(i)}\r\n{i}\r\n")
    print(response)
    return "".join(response)


def handle_request(client_socket: socket.socket, key_store: defaultdict, args: dict, client_addr: str):
    try:
        while True:
            data = client_socket.recv(2048)
            if not data:
                break
            print("decoded data:", data.decode())
            decoded = data.decode().split("\r\n")

            response_gen(client_socket, data, decoded, key_store, args, client_addr)

    except Exception as e:
        print("exception: ", e)


def parse_master_config(config: str):
    global master_host, master_host_port
    if config != "":
        master_host, master_host_port = config.split(" ")
        master_host_port = int(master_host_port)


def is_replica():
    # print("master_host",master_host)
    return master_host != ""


def add_wait_to_send_message(s: socket.socket, message: bytes):
    s.sendall(message)
    time.sleep(0.1)


def ping_master(s: socket.socket):
    add_wait_to_send_message(s, generate_resp_array(["PING"]).encode())


def ping_master_for_repl_conf(s: socket.socket, port: str):
    add_wait_to_send_message(
        s, generate_resp_array(["REPLCONF", "listening-port", port]).encode()
    )
    add_wait_to_send_message(
        s, generate_resp_array(["REPLCONF", "capa", "psync2"]).encode()
    )


def ping_master_for_sync(s: socket.socket):
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

        global master_connection_socket
        master_connection_socket = s

        handshake_with_master(s, args.port)

    while True:
        c, (conn_host, _)= server_socket.accept()
        threading.Thread(
            target=handle_request,
            args=(
                c,
                keystore,
                args,
                conn_host
            ),
        ).start()


if __name__ == "__main__":
    parser.add_argument("--dir", default=".")
    parser.add_argument("--dbfilename", default="empty.rdb")
    parser.add_argument("--port", default=6379)
    parser.add_argument("--replicaof", default="")
    args = parser.parse_args()
    main(args)
