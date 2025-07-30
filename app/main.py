import dataclasses
import socket
import threading
import datetime
import argparse
import time
from datetime import timedelta
from typing import Any, Dict, Optional, cast, List

from app import rdb_parser

EMPTY_RDB = bytes.fromhex(
    "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
)
transaction_enabled = {}
transactions = {}
bl_pop_queue = {} # {key: [conn,...] }
bl_pop_lock = threading.Lock()


@dataclasses.dataclass
class Args:
    port: int
    replicaof: Optional[str]
    dir: str
    dbfilename: str


def parse_single(data: bytes):
    """Parse a single RESP command from data"""
    if not data:
        raise ValueError("Not enough data")
        
    first_byte = data[0:1]
    
    match first_byte:
        case b"*":
            # Array - find the first \r\n
            if b"\r\n" not in data:
                raise ValueError("Not enough data")
            first_line, remaining = data.split(b"\r\n", 1)
            count = int(first_line[1:].decode())
            
            value = []
            for _ in range(count):
                item, remaining = parse_single(remaining)
                value.append(item)
            return value, remaining
            
        case b"$":
            # Bulk string - find the first \r\n
            if b"\r\n" not in data:
                raise ValueError("Not enough data")
            first_line, remaining = data.split(b"\r\n", 1)
            length = int(first_line[1:].decode())
            
            if length == -1:  # Null bulk string
                return None, remaining
            if len(remaining) < length:  # Not enough data for content
                raise ValueError("Not enough data for bulk string")
            
            blk = remaining[:length]
            remaining = remaining[length:]
            
            # Check if next 2 bytes are \r\n and consume them if present
            if remaining.startswith(b"\r\n"):
                remaining = remaining[2:]
            
            return blk, remaining

        case b"+":
            # Simple string - find the first \r\n
            if b"\r\n" not in data:
                raise ValueError("Not enough data")
            first_line, remaining = data.split(b"\r\n", 1)
            return first_line[1:].decode(), remaining

        case b":":
            # Integer - find the first \r\n
            if b"\r\n" not in data:
                raise ValueError("Not enough data")
            first_line, remaining = data.split(b"\r\n", 1)
            return int(first_line[1:].decode()), remaining

        case b"-":
            # Error - find the first \r\n
            if b"\r\n" not in data:
                raise ValueError("Not enough data")
            first_line, remaining = data.split(b"\r\n", 1)
            return first_line[1:].decode(), remaining

        case _:
            raise RuntimeError(f"Parse not implemented: {first_byte}")

def parse_next(data: bytes):
    """Parse all RESP commands from data and return list of commands"""
    print("next_data", data[:100] + b"..." if len(data) > 100 else data)
    commands = []
    remaining_data = data
    
    while remaining_data:
        if not remaining_data:
            break
        first_byte = remaining_data[0:1]
        if first_byte not in [b"*", b"$", b"+", b":", b"-"]:
            print(f"Unknown command start: {first_byte}, remaining: {remaining_data[:20]}...")
            break
            
        try:
            command, remaining_data = parse_single(remaining_data)
            commands.append(command)
            print(f"Parsed command: {command}")
        except (ValueError, IndexError) as e:
            print(f"Parse error: {e}, stopping parse")
            break
    
    return commands, remaining_data


def encode_resp(
    data: Any, trailing_crlf: bool = True, encoded_list: bool = False
) -> bytes:
    if isinstance(data, bytes):
        return b"$%b\r\n%b%b" % (
            str(len(data)).encode(),
            data,
            b"\r\n" if trailing_crlf else b"",
        )
    if isinstance(data, str):
        if data[0] == "-":
            return b"%b\r\n" % (data.encode(),)
        return b"+%b\r\n" % (data.encode(),)
    if isinstance(data, int):
        return b":%b\r\n" % (str(data).encode())
    if data is None:
        return b"$-1\r\n"
    if isinstance(data, list):
        return b"*%b\r\n%b" % (
            str(len(data)).encode(),
            b"".join(map(encode_resp, data)),
        )

    raise RuntimeError(f"Encode not implemented: {data}")


db: Dict[Any, rdb_parser.Value] = {}


@dataclasses.dataclass
class Replication:
    master_replid: str
    master_repl_offset: int
    connected_replicas: List[socket.socket]


replication = Replication(
    master_replid="8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
    master_repl_offset=0,
    connected_replicas=[],
)


def handle_conn(args: Args, conn: socket.socket, is_replica_conn: bool = False):
    data = b""
    global transaction_enabled, transactions

    while data or (data := conn.recv(65536)):
        # Parse all commands from the data buffer
        commands, data = parse_next(data)
        
        for value in commands:
            trailing_crlf = True

            if conn not in transaction_enabled.keys():
                transaction_enabled[conn] = False
                transactions[conn] = []

            response = handle_command(args, value, conn, is_replica_conn, trailing_crlf)
            # print(response)

            if not transaction_enabled[conn] and response !="custom":
                encoded_resp = encode_resp(response, trailing_crlf)
                conn.send(encoded_resp)


def handle_neg_index(s: int, e: int, list_len: int):
    if s < 0:
        if abs(s) <= list_len:
            s = list_len + s
        else:
            s = 0
    if e < 0:
        if abs(e) <= list_len:
            e = list_len + e
        else:
            e = 0
    return s, e

def handle_blpop(k:str, t:datetime.datetime, conn: socket.socket):
    while True:
        if datetime.datetime.now() > t:
            conn.send(encode_resp(None))
            return
        else:
            with bl_pop_lock:
                if k in db.keys() and len(db[k].value) > 0:
                    conn = bl_pop_queue[k].pop(0)
                    conn.send(encode_resp([k,db[k].value[0]]))
                    db[k].value = db[k].value[1:]
                    return


def handle_command(
    args: Args,
    value: List,
    conn: socket.socket,
    is_replica_conn: bool = False,
    trailing_crlf: bool = True,
) -> str | None | List[Any]:
    global transaction_enabled, transactions
    response = "custom"
    # print(value, is_replica_conn)
    match value:
        case [b"PING"]:
            response = "PONG"
        case [b"ECHO", s]:
            response = s
        case [b"GET", k]:
            if not queue_transaction(value, conn):
                now = datetime.datetime.now()
                value = db.get(k)
                if value is None:
                    response = None
                elif value.expiry is not None and now >= value.expiry:
                    db.pop(k)
                    response = None
                else:
                    response = value.value
        case [b"INFO", b"replication"]:
            if args.replicaof is None:
                response = f"""\
role:master
master_replid:{replication.master_replid}
master_repl_offset:{replication.master_repl_offset}
""".encode()
            else:
                response = b"""role:slave\n"""
        case [b"REPLCONF", b"listening-port", port]:
            response = "OK"
        case [b"REPLCONF", b"capa", b"psync2"]:
            response = "OK"
        case [b"REPLCONF", b"GETACK", b"*"]:
            print("processed repl conf")
            response = ["REPLCONF", "ACK", "0"]
        case [b"PSYNC", replid, offset]:
            response = "custom"
            conn.send (encode_resp(
                f"FULLRESYNC "
                f"{replication.master_replid} " 
                f"{replication.master_repl_offset}"
            ))
            conn.send(encode_resp(EMPTY_RDB, False))
            # response = EMPTY_RDB
            # response = ["REPLCONF", "GETACK", "*"])
            replication.connected_replicas.append(conn)
            # print('sent')
        case [b"REPLCONF", b"GETACK", b"*"]:
            response = ["REPLCONF", "ACK", "0"]
        case [b"DISCARD"]:
            if transaction_enabled[conn]:
                transaction_enabled[conn] = False
                transactions[conn] = []
                response = "OK"
            else:
                response = "-ERR DISCARD without MULTI"
        case [b"MULTI"]:
            transaction_enabled[conn] = True
            conn.send(encode_resp("OK"))
        case [b"EXEC"]:
            if not transaction_enabled[conn]:
                response = "-ERR EXEC without MULTI"
            else:
                transaction_enabled[conn] = False
                if len(transactions[conn]) == 0:
                    return []
                response = handle_transaction(args, conn, is_replica_conn)
                transactions[conn] = []
        case [b"TYPE",k]:
            if k in db.keys():
                response = "string"
            else:
                response = "none"

        case [b'LLEN',k]:
            if k in db.keys():
                response = len(db[k].value)
            else:
                response = 0
        case [b'LPOP', k, *v]:
            if k in db.keys():
                if len(db.keys()) == 0:
                    response = None
                else:
                    if len(v) == 0:
                        response = db[k].value[0]
                        db[k].value = db[k].value[1:]
                    else:
                        if int(v[0]) >= len(db[k].value):
                            response = db[k].value
                            db[k].value = []
                        else:
                            response = db[k].value[:int(v[0])]
                            db[k].value = db[k].value[int(v[0]):]

        case [b'BLPOP', k, t]:
            global bl_pop_queue
            if k in bl_pop_queue.keys() or k not in db.keys() or (k in db.keys() and len(db[k].value) == 0):
                if k in bl_pop_queue.keys():
                    bl_pop_queue[k].append(conn)
                else:
                    bl_pop_queue[k] = [conn]

                dt = datetime.datetime.now()
                td = timedelta(seconds=float(t))
                if float(t) == 0:
                    td = timedelta(days=1)
                dt = dt.__add__(td)
                threading.Thread(
                    target=handle_blpop,
                    args=(k,dt,conn,),
                ).start()
                response = "custom"
                # once the list is empty make sure to destory the key from dict
            else:
                response = db[k].value[0]
                db[k].value = db[k].value[1:]

        case [b"LPUSH", k, *v]:
            if not queue_transaction(value, conn):
                for rep in replication.connected_replicas:
                    rep.send(encode_resp(value))
                if k in db.keys():
                    v = v[::-1]
                    v.extend(db[k].value)
                    db[k].value = v
                else:
                    db[k] = rdb_parser.Value(value=v[::-1], expiry=None)
                if not is_replica_conn:
                    response = len(db[k].value)

        case [b"RPUSH", k, *v]:
            if not queue_transaction(value, conn):
                for rep in replication.connected_replicas:
                    rep.send(encode_resp(value))
                if k in db.keys():
                    db[k].value.extend(v)
                else:
                    db[k] = rdb_parser.Value(value=v, expiry=None)
                if not is_replica_conn:
                    response = len(db[k].value)
        case [b"LRANGE", k, s, e]:
            if k in db.keys():
                int_s, int_e = handle_neg_index(int(s), int(e), len(db[k].value))
                response = db[k].value[int_s : int_e + 1]
            else:
                response = []
        case [b"INCR", k]:
            if not queue_transaction(value, conn):
                db_value = db.get(k)
                if db_value is None:
                    new_value = 1
                else:
                    try:
                        current_int = int(db_value.value.decode())
                        new_value = current_int + 1
                    except Exception:
                        response = "-ERR value is not an integer or out of range"
                        return response

                db[k] = rdb_parser.Value(
                    value=str(new_value).encode(),
                    expiry=None,
                )

                response = new_value
        case [b"SET", k, v, b"px", expiry_ms]:
            if not queue_transaction(value, conn):
                for rep in replication.connected_replicas:
                    rep.send(encode_resp(value))
                now = datetime.datetime.now()
                expiry_ms = datetime.timedelta(
                    milliseconds=int(expiry_ms.decode()),
                )
                db[k] = rdb_parser.Value(
                    value=v,
                    expiry=now + expiry_ms,
                )
                if not is_replica_conn:
                    response = "OK"
        case [b"SET", k, v]:
            if not queue_transaction(value, conn):
                for rep in replication.connected_replicas:
                    rep.send(encode_resp(value))
                db[k] = rdb_parser.Value(
                    value=v,
                    expiry=None,
                )
                if not is_replica_conn:
                    response = "OK"
        case _:
            raise RuntimeError(f"Command not implemented: {value}")

    return response


def queue_transaction(command: List, conn: socket.socket):
    global transaction_enabled, transactions
    if conn in transaction_enabled.keys() and transaction_enabled[conn] == True:
        transactions[conn].append(command)
        conn.send(encode_resp("QUEUED"))
        return True
    return False


def handle_transaction(args: Args, conn: socket.socket, is_replica_conn: bool):
    global transactions
    response = []
    for transaction in transactions[conn]:
        response.append(handle_command(args, transaction, conn, is_replica_conn))
    return response


def main(args: Args):
    global db
    db = rdb_parser.read_file_and_construct_kvm(args.dir, args.dbfilename)
    server_socket = socket.create_server(
        ("localhost", args.port),
        reuse_port=True,
    )
    if args.replicaof is not None:
        (host, port) = args.replicaof.split(" ")
        port = int(port)
        master_conn = socket.create_connection((host, port))

        t = threading.Thread(
            target=handle_conn,
            args=(args, master_conn, True),
            daemon=True,
        )
        # Handshake PING
        master_conn.send(encode_resp([b"PING"]))
        responses, _ = parse_next(master_conn.recv(65536))
        resp = responses[0] if responses else None
        assert resp == "PONG"
        # Handshake REPLCONF listening-port
        master_conn.send(
            encode_resp(
                [
                    b"REPLCONF",
                    b"listening-port",
                    str(args.port).encode(),
                ]
            )
        )
        responses, _ = parse_next(master_conn.recv(65536))
        resp = responses[0] if responses else None
        assert resp == "OK"
        # Handshake REPLCONF capabilities
        master_conn.send(encode_resp([b"REPLCONF", b"capa", b"psync2"]))
        responses, _ = parse_next(master_conn.recv(65536))
        resp = responses[0] if responses else None
        assert resp == "OK"

        # Handshake PSYNC
        master_conn.send(encode_resp([b"PSYNC", b"?", b"-1"]))

        #Handling this since the tcp messages are broken in different permutations
        responses, _ = parse_next(master_conn.recv(65536))
        count = len(responses)
        while True:
            if count == 2:
                print("1")
                break
            if count > 2:
                print("2")
                for command in responses[2:]:
                    handle_command(args, command, master_conn, False)
                break
            else:
                print("3")
                responses, _ = parse_next(master_conn.recv(65536))
                count += len(responses)

        print("PSYNC response", responses)

        t.start()
        print(f"Handshake with master completed: {resp=}")


    with server_socket:
        while True:
            (conn, _) = server_socket.accept()
            threading.Thread(
                target=handle_conn,
                args=(
                    args,
                    conn,
                    False,
                ),
                daemon=True,
            ).start()


if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--port", type=int, default=6379)
    args.add_argument("--replicaof", required=False)
    args.add_argument("--dir", default=".")
    args.add_argument("--dbfilename", default="empty.rdb")
    
    parsed_args = args.parse_args()
    
    args = Args(
        port=parsed_args.port,
        replicaof=parsed_args.replicaof,
        dir=parsed_args.dir,
        dbfilename=parsed_args.dbfilename
    )
    
    main(args)
