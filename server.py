"""
server.py

Author: Shurjo Majumder
ID: 169035249
E-mail: maju5249@mylaurier.ca

TCP echo / status / file-repository server (sockets only).

Wire protocol
-------------
Client -> server: UTF-8 text lines terminated by \'\\n\' (at most MAX_LINE_BYTES).

Server -> client: frames.
    "<KIND> <LENGTH>\\n" followed by exactly LENGTH payload bytes, KIND in {MSG, ERR}
    "FILE <LENGTH> <NAME>\\n" followed by exactly LENGTH raw file bytes

Session:
    1. After accept, the server sends either
         - MSG frame whose payload is the assigned name ("Client01", ...), or
         - ERR frame (server full) and closes the connection.
    2. The client replies with a line containing that name. Any other value -> ERR frame, close.
    3. Command loop (one reply frame per line received):
         "exit" / "quit" / "q" -> server closes the connection (no reply)
         "status"              -> MSG frame with the connection cache
         "list"                -> MSG frame with the repository file names, one per line;
                                  the next line is then interpreted as a file name
                                  (valid -> FILE frame, invalid -> ERR frame)
         anything else         -> MSG frame: "<line> ACK"
"""
from __future__ import annotations

import argparse
import dataclasses
import enum
import logging
import os
import pathlib
import socket
import sys
import threading
from datetime import datetime

logger = logging.getLogger("server")

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 5000
MAX_CLIENTS = 3
MAX_LINE_BYTES = 4096
CHUNK_BYTES = 64 * 1024
HANDSHAKE_TIMEOUT = 10.0
ACCEPT_POLL_SECONDS = 0.2
ACK_SUFFIX = "ACK"
EXIT_COMMANDS = frozenset({"exit", "quit", "q"})

REPO_DIR = pathlib.Path(__file__).resolve().parent / "content"


def get_content_list() -> set[str]:
    """
    Names of the regular files currently in the repository.

    Names containing CR/LF are excluded (they cannot be requested over a line protocol).
    Subdirectories and symlinks are excluded.
    """
    try:
        return {
            entry.name
            for entry in REPO_DIR.iterdir()
            if entry.is_file()
            and not entry.is_symlink()
            and "\n" not in entry.name
            and "\r" not in entry.name
        }
    except OSError:
        return set()


def client_name(client_id: int) -> str:
    return f"Client{client_id:02d}"


@enum.unique
class ClientState(enum.Enum):
    """
    Server client state enum.
    """
    ECHO = 0b0001
    FILE = 0b0010
    ERROR = 0b0100


@enum.unique
class ResponseKind(enum.Enum):
    MSG = "MSG"
    ERR = "ERR"
    FILE = "FILE"


class Client:
    """
    Client class for storing data relevant to the active client. One per thread.
    """
    _client_id: int
    _ip: str
    _port: int
    _username: str
    _stack: list[str]
    _state: ClientState

    def __init__(self, client_id: int, ip: str, port: int, username: str = "Client"):
        """
        Client class constructor.

        :param client_id: Integer identifying the client.
        :param ip: Client's IP address.
        :param port: Client's port number.
        :param username: Client's username.
        """
        self._client_id = client_id
        self._ip = ip
        self._port = port
        self._username = username
        self._stack = []
        self._state = ClientState.ECHO

    @property
    def client_id(self) -> int:
        return self._client_id

    @property
    def username(self) -> str:
        return self._username

    def state(self) -> tuple[ClientState, str]:
        """
        Returns the current state of the client.

        :return: The current state of the client, as well as the item on the top of the stack (if any, else return empty string).
        """
        return self._state, self._stack[-1] if self._stack else ""

    def update_state(self, client_data: str):
        """
        Updates the state of the client based on a PDA.

        :param client_data: Data from the client.
        :return:
        """
        match (self._state, client_data):
            # state transitions for ECHO
            case (ClientState.ECHO, "list"):
                self._stack.clear()
                self._state = ClientState.FILE
            case (ClientState.ECHO, _):
                self._stack.clear()
                self._state = ClientState.ECHO

            # state transitions for FILE
            case (ClientState.FILE, fileName) if fileName in get_content_list():
                self._stack.append(fileName)
                self._state = ClientState.ECHO
            case (ClientState.FILE, _):
                self._state = ClientState.ERROR

            # state transitions for ERROR
            case (ClientState.ERROR, fileName) if fileName in get_content_list():
                self._stack.append(fileName)
                self._state = ClientState.ECHO
            case (ClientState.ERROR, _):
                self._state = ClientState.ERROR

    def __str__(self) -> str:
        return f'{self._ip}\t{self._port}'

    def __repr__(self) -> str:
        return f"{self._ip}\t{self._username}"


@dataclasses.dataclass(frozen=True)
class ClientConnection:
    id: int
    ip: str
    session_start: datetime
    session_end: datetime | None

    @property
    def name(self) -> str:
        return client_name(self.id)

    def __str__(self) -> str:
        return f'{self.name}\t{self.ip}\t{self.session_start}\t{self.session_end if self.session_end else "$"}'

    def __repr__(self) -> str:
        return self.__str__()


class ConnectionTable:
    _singleton: ConnectionTable | None = None
    _lock: threading.Lock = threading.Lock()
    _max_connections: int
    _last_client_id: int
    _table: dict[str, ClientConnection]

    def add_connection(self, client_ip: str, session_start: datetime) -> ClientConnection | None:
        """
        Registers a new connection and assigns the next client id.

        :return: The new connection, or None if max_connections connections are already active
                 (nothing is added and no id is consumed).
        """
        with self._lock:
            if self._active_count_locked() >= self._max_connections:
                return None
            self._last_client_id += 1
            conn = ClientConnection(self._last_client_id, client_ip, session_start, None)
            self._table[conn.name] = conn
            return conn

    def get_connection(self, client_id: int) -> ClientConnection | None:
        with self._lock:
            return self._table.get(client_name(client_id))

    def drop_connection(self, client_id: int) -> ClientConnection | None:
        key = client_name(client_id)
        with self._lock:
            conn = self._table.get(key)
            if conn is None or conn.session_end is not None:
                return None
            ended = dataclasses.replace(conn, session_end=datetime.now())
            self._table[key] = ended
            return ended

    def borrow_cache_copy(self) -> dict[str, ClientConnection]:
        with self._lock:
            return dict(self._table)

    def active_count(self) -> int:
        with self._lock:
            return self._active_count_locked()

    def clear(self) -> None:
        """Resets the cache and the id counter (used by tests)."""
        with self._lock:
            self._table.clear()
            self._last_client_id = 0

    def _active_count_locked(self) -> int:
        return sum(1 for c in self._table.values() if c.session_end is None)

    @property
    def max_connections(self) -> int:
        return self._max_connections

    def __new__(cls, max_connections: int | None = None) -> ConnectionTable:
        """
        :param max_connections: Maximum number of simultaneously active connections. Fixed by the
                                first construction (MAX_CLIENTS if omitted) and never modified.
                                Later calls may omit it; passing a different value raises ValueError.
        """
        if cls._singleton is None:
            with cls._lock:
                if cls._singleton is None:
                    limit = MAX_CLIENTS if max_connections is None else max_connections
                    if limit < 1:
                        raise ValueError(f"max_connections must be >= 1, got {limit}")
                    self = super().__new__(cls)
                    self._max_connections = limit
                    self._table = {}
                    self._last_client_id = 0
                    cls._singleton = self
                    return self
        if max_connections is not None and max_connections != cls._singleton._max_connections:
            raise ValueError(f"ConnectionTable already created with max_connections="
                             f"{cls._singleton._max_connections}, got {max_connections}")
        return cls._singleton


@dataclasses.dataclass(frozen=True)
class Reply:
    """A server response: text (MSG/ERR) or a repository file to stream (FILE)."""
    kind: ResponseKind
    text: str = ""
    path: pathlib.Path | None = None


class ProtocolError(Exception):
    pass


def format_status() -> str:
    rows = sorted(ConnectionTable().borrow_cache_copy().values(), key=lambda c: c.id)
    return "\n".join(["NAME\tIP\tACCEPTED\tFINISHED", *map(str, rows)])


def respond(client: Client, user_input: str) -> Reply | None:
    """
    Computes the reply for one line of client input.

    :return: The reply, or None if the client asked to close the connection.
    """
    if user_input in EXIT_COMMANDS:
        return None
    if user_input == "status" and client.state()[0] == ClientState.ECHO:
        return Reply(ResponseKind.MSG, format_status())

    client.update_state(user_input)
    match client.state():
        case (ClientState.FILE, _):
            names = sorted(get_content_list())
            return Reply(ResponseKind.MSG, "\n".join(names) if names else "(repository is empty)")
        case (ClientState.ERROR, _):
            return Reply(ResponseKind.ERR, f"File not found: {user_input}. "
                                   f"Send a listed file name.")
        case (ClientState.ECHO, fileName) if fileName:
            return Reply(ResponseKind.FILE, path=REPO_DIR / fileName)
        case _:
            return Reply(ResponseKind.MSG, user_input + ACK_SUFFIX)


def _send_frame(sock: socket.socket, kind: ResponseKind, text: str) -> None:
    payload = text.encode("utf-8")
    sock.sendall(f"{kind.value} {len(payload)}\n".encode("ascii") + payload)


def _send_file(sock: socket.socket, path: pathlib.Path) -> int:
    try:
        f = open(path, "rb")
    except OSError:
        _send_frame(sock, ResponseKind.ERR, f"File unavailable: {path.name}")
        return 404
    with f:
        size = os.fstat(f.fileno()).st_size
        sock.sendall(f"{ResponseKind.FILE.value} {size} {path.name}\n".encode("utf-8"))
        sent = 0
        while sent < size:
            chunk = f.read(min(CHUNK_BYTES, size - sent))
            if not chunk:
                return 500  # file shrank mid-transfer; framing is broken, caller must close
            sock.sendall(chunk)
            sent += len(chunk)
    return 200


def send_response(sock: socket.socket, reply: Reply) -> int:
    """
    Sends one reply frame.

    :return: 200 on success, 404 if the file vanished before it could be opened
             (an ERR frame was sent instead), 500 on transport/IO failure.
    """
    try:
        if reply.kind == ResponseKind.FILE and reply.path is not None:
            return _send_file(sock, reply.path)
        _send_frame(sock, reply.kind, reply.text)
        return 200  # OK
    except OSError:
        return 500


def read_line(reader) -> str | None:
    """Reads one line; returns None on EOF. Raises ProtocolError if the line is too long."""
    raw = reader.readline(MAX_LINE_BYTES + 1)
    if not raw:
        return None
    if len(raw) > MAX_LINE_BYTES and not raw.endswith(b"\n"):
        raise ProtocolError("line too long")
    return raw.removesuffix(b"\n").removesuffix(b"\r").decode("utf-8", errors="replace")


def _session(client: Client, sock: socket.socket, reader) -> int:
    sock.settimeout(HANDSHAKE_TIMEOUT)
    try:
        send_response(sock, Reply(ResponseKind.MSG, client.username))
        declared = read_line(reader)
    except ProtocolError:
        send_response(sock, Reply(ResponseKind.ERR, "Invalid handshake"))
        return 1
    if declared != client.username:
        send_response(sock, Reply(ResponseKind.ERR, f"Name mismatch: expected {client.username}"))
        return 1
    sock.settimeout(None)
    logger.info("%s handshake complete (%r)", client.username, client)

    while True:
        try:
            line = read_line(reader)
        except ProtocolError:
            send_response(sock, Reply(ResponseKind.ERR, f"Line exceeds {MAX_LINE_BYTES} bytes"))
            return 1
        if line is None:
            return 0  # client closed without "exit"
        reply = respond(client, line)
        if reply is None:
            return 0
        if send_response(sock, reply) >= 500:
            return 1


def client_thread_main(client: Client, sock: socket.socket) -> int:
    table = ConnectionTable()
    status = 1
    with sock, sock.makefile("rb") as reader:
        try:
            status = _session(client, sock, reader)
        except OSError as exc:
            logger.info("%s connection error: %s", client.username, exc)
        finally:
            # Recorded before the socket closes so a freed slot is visible as soon as the peer sees EOF.
            ended = table.drop_connection(client.client_id)
            logger.info("%s finished: %s", client.username, ended)
    return status


def _reject(sock: socket.socket, message: str) -> None:
    with sock:
        try:
            sock.settimeout(2.0)
            _send_frame(sock, ResponseKind.ERR, message)
        except OSError:
            pass


def serve(listener: socket.socket, stop_event: threading.Event | None = None) -> None:
    """
    Accept loop. Runs until stop_event is set (or KeyboardInterrupt); closes the listener on return.
    The connection limit is the ConnectionTable's max_connections.
    """
    stop_event = stop_event or threading.Event()
    table = ConnectionTable()
    listener.settimeout(ACCEPT_POLL_SECONDS)
    try:
        while not stop_event.is_set():
            try:
                conn, addr = listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            ip, port = addr[0], addr[1]
            record = table.add_connection(ip, datetime.now())
            if record is None:
                logger.info("Rejected %s:%s (server full)", ip, port)
                _reject(conn, f"Server full: maximum of {table.max_connections} clients")
                continue
            logger.info("Accepted %s from %s:%s", record.name, ip, port)
            client = Client(record.id, ip, port, record.name)
            threading.Thread(target=client_thread_main, args=(client, conn),
                             name=record.name, daemon=True).start()
    finally:
        listener.close()


def main(max_connections: int = MAX_CLIENTS) -> int:
    global REPO_DIR
    parser = argparse.ArgumentParser(description="TCP echo/file server")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--repo", default=str(REPO_DIR), help="directory served by 'list'")
    parser.add_argument("--max-clients", type=int, default=max_connections)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    REPO_DIR = pathlib.Path(args.repo).resolve()
    REPO_DIR.mkdir(parents=True, exist_ok=True)
    try:
        ConnectionTable(args.max_clients)
    except ValueError as exc:
        parser.error(str(exc))

    listener = socket.create_server((args.host, args.port), backlog=8)
    logger.info("Listening on %s:%s (max %d clients, repo %s)",
                args.host, listener.getsockname()[1], args.max_clients, REPO_DIR)
    stop = threading.Event()
    try:
        serve(listener, stop)
    except KeyboardInterrupt:
        stop.set()
        logger.info("Shutting down")
    return 0


if __name__ == '__main__':
    sys.exit(main())