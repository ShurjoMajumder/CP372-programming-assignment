"""
Tests for server.py.

Author: Shurjo Majumder
ID: 169035249
E-mail: maju5249@mylaurier.ca

Run:  python -m unittest -v test_server        (or: pytest test_server.py)

The Conn helper below is test scaffolding that speaks the wire protocol documented in
server.py; it is not a client implementation.
"""
import functools
import os
import pathlib
import socket
import tempfile
import threading
import time
import unittest
import uuid
from datetime import datetime
from unittest import mock

import server
from server import Client, ClientConnection, ClientState, ConnectionTable, Reply, ResponseKind

KIND_MSG, KIND_ERR, KIND_FILE = ResponseKind.MSG, ResponseKind.ERR, ResponseKind.FILE


def wait_until(predicate, timeout=3.0, interval=0.01):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def fresh_table(testcase, max_connections=3):
    """Replaces the ConnectionTable singleton for the duration of the test."""
    patcher = mock.patch.object(ConnectionTable, "_singleton", None)
    patcher.start()
    testcase.addCleanup(patcher.stop)
    return ConnectionTable(max_connections)


def read_frame(f):
    """Returns (kind, file_name, payload). file_name is "" except for FILE frames."""
    header = f.readline()
    if not header:
        raise EOFError("connection closed")
    parts = header.decode("utf-8").rstrip("\n").split(" ", 2)
    kind, length = ResponseKind(parts[0]), int(parts[1])
    name = parts[2] if len(parts) == 3 else ""
    payload = f.read(length)
    assert len(payload) == length, "short payload"
    return kind, name, payload


class Conn:
    def __init__(self, host, port):
        self.sock = socket.create_connection((host, port), timeout=5)
        self.f = self.sock.makefile("rb")

    def send_line(self, text):
        self.sock.sendall(text.encode("utf-8") + b"\n")

    def frame(self):
        return read_frame(self.f)

    def text(self):
        kind, _, payload = self.frame()
        return kind, payload.decode("utf-8")

    def ask(self, line):
        self.send_line(line)
        return self.text()

    def at_eof(self):
        try:
            return self.f.read(1) == b""
        except ConnectionResetError:
            return True

    def close(self):
        try:
            self.f.close()
            self.sock.close()
        except OSError:
            pass


class RepoTestCase(unittest.TestCase):
    """Isolated repository directory + fresh ConnectionTable (MAX_CONNECTIONS) per test."""
    MAX_CONNECTIONS = 3

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = pathlib.Path(tmp.name)
        self.repo = self.root / "repo"
        self.repo.mkdir()
        patcher = mock.patch.object(server, "REPO_DIR", self.repo)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.table = fresh_table(self, self.MAX_CONNECTIONS)

    def add_file(self, name, data=b"data"):
        (self.repo / name).write_bytes(data)


# --------------------------------------------------------------------------- unit tests

class GetContentListTests(RepoTestCase):
    def test_lists_only_regular_files(self):
        self.add_file("a.txt")
        self.add_file("my file.txt")
        (self.repo / "sub").mkdir()
        self.assertEqual(server.get_content_list(), {"a.txt", "my file.txt"})

    def test_excludes_symlinks(self):
        self.add_file("a.txt")
        try:
            os.symlink(self.root / "elsewhere", self.repo / "link")
            (self.root / "elsewhere").write_bytes(b"x")
        except (OSError, NotImplementedError):
            self.skipTest("symlinks unavailable")
        self.assertEqual(server.get_content_list(), {"a.txt"})

    def test_missing_repo_directory_yields_empty_set(self):
        with mock.patch.object(server, "REPO_DIR", self.root / "does-not-exist"):
            self.assertEqual(server.get_content_list(), set())


class ClientStateTests(RepoTestCase):
    def setUp(self):
        super().setUp()
        self.add_file("a.txt")
        self.client = Client(1, "127.0.0.1", 5000, "Client01")

    def step(self, data):
        self.client.update_state(data)
        return self.client.state()

    def test_initial_state(self):
        self.assertEqual(self.client.state(), (ClientState.ECHO, ""))

    def test_list_enters_file_state(self):
        self.assertEqual(self.step("list"), (ClientState.FILE, ""))

    def test_file_state_valid_name(self):
        self.step("list")
        self.assertEqual(self.step("a.txt"), (ClientState.ECHO, "a.txt"))

    def test_file_state_invalid_name_goes_to_error(self):
        self.step("list")
        self.assertEqual(self.step("nope")[0], ClientState.ERROR)

    def test_error_state_persists_on_invalid_input(self):
        self.step("list")
        self.step("nope")
        self.assertEqual(self.step("still-nope")[0], ClientState.ERROR)

    def test_error_state_valid_name_recovers(self):
        self.step("list")
        self.step("nope")
        self.assertEqual(self.step("a.txt"), (ClientState.ECHO, "a.txt"))

    def test_list_outside_echo_is_only_a_file_name(self):
        self.step("list")
        self.assertEqual(self.step("list")[0], ClientState.ERROR)
        self.assertEqual(self.step("list")[0], ClientState.ERROR)

    def test_file_named_list_is_selectable_in_file_and_error_states(self):
        self.add_file("list")
        self.step("list")
        self.assertEqual(self.step("list"), (ClientState.ECHO, "list"))
        self.step("list")
        self.step("nope")
        self.assertEqual(self.step("list"), (ClientState.ECHO, "list"))

    def test_echo_input_clears_stack(self):
        self.step("list")
        self.step("a.txt")
        self.assertEqual(self.step("hello"), (ClientState.ECHO, ""))

    def test_file_name_without_list_is_plain_echo(self):
        self.assertEqual(self.step("a.txt"), (ClientState.ECHO, ""))


class ConnectionTableTests(unittest.TestCase):
    def setUp(self):
        self.table = fresh_table(self, 3)

    def test_singleton(self):
        self.assertIs(ConnectionTable(), ConnectionTable())

    def test_max_connections_is_fixed_at_construction(self):
        self.assertEqual(self.table.max_connections, 3)
        self.assertIs(ConnectionTable(), self.table)
        self.assertIs(ConnectionTable(3), self.table)
        with self.assertRaises(ValueError):
            ConnectionTable(5)
        with self.assertRaises(AttributeError):
            self.table.max_connections = 5
        self.assertEqual(self.table.max_connections, 3)

    def test_default_and_invalid_max_connections(self):
        with mock.patch.object(ConnectionTable, "_singleton", None):
            self.assertEqual(ConnectionTable().max_connections, server.MAX_CLIENTS)
        with mock.patch.object(ConnectionTable, "_singleton", None):
            for bad in (0, -1):
                with self.assertRaises(ValueError):
                    ConnectionTable(bad)

    def test_names_are_zero_padded_and_incremental(self):
        names = [self.table.add_connection("1.1.1.1", datetime.now()).name for _ in range(3)]
        self.assertEqual(names, ["Client01", "Client02", "Client03"])

    def test_new_connection_is_active(self):
        conn = self.table.add_connection("1.1.1.1", datetime.now())
        self.assertIsNone(conn.session_end)
        self.assertEqual(self.table.active_count(), 1)

    def test_max_connections_refuses_without_consuming_id(self):
        table = fresh_table(self, 2)
        table.add_connection("1.1.1.1", datetime.now())
        table.add_connection("1.1.1.2", datetime.now())
        self.assertIsNone(table.add_connection("1.1.1.3", datetime.now()))
        table.drop_connection(1)
        conn = table.add_connection("1.1.1.4", datetime.now())
        self.assertEqual(conn.id, 3)

    def test_drop_sets_end_time_once(self):
        self.table.add_connection("1.1.1.1", datetime.now())
        ended = self.table.drop_connection(1)
        self.assertIsNotNone(ended.session_end)
        self.assertEqual(self.table.get_connection(1), ended)
        self.assertIsNone(self.table.drop_connection(1))
        self.assertEqual(self.table.get_connection(1), ended)
        self.assertEqual(self.table.active_count(), 0)

    def test_drop_unknown_id(self):
        self.assertIsNone(self.table.drop_connection(99))
        self.assertIsNone(self.table.get_connection(99))

    def test_borrowed_copy_is_a_snapshot(self):
        self.table.add_connection("1.1.1.1", datetime.now())
        snap = self.table.borrow_cache_copy()
        snap.clear()
        self.assertEqual(len(self.table.borrow_cache_copy()), 1)

    def test_concurrent_adds_get_unique_ids(self):
        table = fresh_table(self, 100)
        results = []
        lock = threading.Lock()

        def worker():
            conn = table.add_connection("1.1.1.1", datetime.now())
            with lock:
                results.append(conn.id)

        threads = [threading.Thread(target=worker) for _ in range(50)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(sorted(results), list(range(1, 51)))

    def test_concurrent_adds_respect_max_connections(self):
        accepted = []
        lock = threading.Lock()

        def worker():
            conn = self.table.add_connection("1.1.1.1", datetime.now())
            if conn is not None:
                with lock:
                    accepted.append(conn.id)

        threads = [threading.Thread(target=worker) for _ in range(30)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(sorted(accepted), [1, 2, 3])

    def test_str_uses_dollar_for_active_sessions(self):
        start = datetime(2026, 1, 2, 3, 4, 5)
        self.assertEqual(str(ClientConnection(1, "9.9.9.9", start, None)),
                         f"Client01\t9.9.9.9\t{start}\t$")
        end = datetime(2026, 1, 2, 3, 5, 5)
        self.assertEqual(str(ClientConnection(2, "9.9.9.9", start, end)),
                         f"Client02\t9.9.9.9\t{start}\t{end}")


class RespondTests(RepoTestCase):
    def setUp(self):
        super().setUp()
        self.add_file("b.txt")
        self.add_file("a.txt")
        self.client = Client(1, "127.0.0.1", 5000, "Client01")

    def test_exit_commands_close(self):
        for cmd in ("exit", "quit", "q"):
            with self.subTest(cmd=cmd):
                self.assertIsNone(server.respond(self.client, cmd))

    def test_echo_appends_ack(self):
        self.assertEqual(server.respond(self.client, "hello"),
                         Reply(KIND_MSG, "hello" + server.ACK_SUFFIX))

    def test_status_contains_cache(self):
        ConnectionTable().add_connection("1.2.3.4", datetime.now())
        reply = server.respond(self.client, "status")
        self.assertEqual(reply.kind, KIND_MSG)
        self.assertIn("Client01\t1.2.3.4", reply.text)

    def test_status_does_not_alter_state(self):
        server.respond(self.client, "status")
        self.assertEqual(self.client.state(), (ClientState.ECHO, ""))

    def test_status_outside_echo_is_only_a_file_name(self):
        server.respond(self.client, "list")
        self.assertEqual(server.respond(self.client, "status").kind, KIND_ERR)
        self.add_file("status")
        self.assertEqual(server.respond(self.client, "status"),
                         Reply(KIND_FILE, path=self.repo / "status"))

    def test_list_is_sorted(self):
        self.assertEqual(server.respond(self.client, "list"), Reply(KIND_MSG, "a.txt\nb.txt"))

    def test_list_empty_repo(self):
        (self.repo / "a.txt").unlink()
        (self.repo / "b.txt").unlink()
        reply = server.respond(self.client, "list")
        self.assertEqual(reply.kind, KIND_MSG)
        self.assertEqual(reply.text, "(repository is empty)")

    def test_file_request(self):
        server.respond(self.client, "list")
        self.assertEqual(server.respond(self.client, "a.txt"),
                         Reply(KIND_FILE, path=self.repo / "a.txt"))

    def test_missing_file_request(self):
        server.respond(self.client, "list")
        reply = server.respond(self.client, "zzz")
        self.assertEqual(reply.kind, KIND_ERR)
        self.assertIn("zzz", reply.text)


class SendResponseTests(RepoTestCase):
    def test_vanished_file_yields_err_frame_and_404(self):
        a, b = socket.socketpair()
        self.addCleanup(a.close)
        self.addCleanup(b.close)
        b.settimeout(5)
        status = server.send_response(a, Reply(KIND_FILE, path=self.repo / "gone.txt"))
        self.assertEqual(status, 404)
        kind, _, payload = read_frame(b.makefile("rb"))
        self.assertEqual(kind, KIND_ERR)
        self.assertIn(b"gone.txt", payload)

    def test_closed_peer_yields_500(self):
        a, b = socket.socketpair()
        b.close()
        self.addCleanup(a.close)
        status = 200
        for _ in range(100):  # first sends may succeed into the kernel buffer
            status = server.send_response(a, Reply(KIND_MSG, "x" * 1000))
            if status != 200:
                break
        self.assertEqual(status, 500)


# --------------------------------------------------------------------------- live tests
#
# The protocol tests live in LiveProtocolTests and run in two modes:
#   LiveServerTests      every test starts its own server on an ephemeral port (always runs)
#   ExternalServerTests  tests an already running server (runs only if TEST_SERVER is set)

def embedded_only(test):
    @functools.wraps(test)
    def wrapper(self, *args, **kwargs):
        if not self.EMBEDDED:
            self.skipTest("requires a server started by the test")
        return test(self, *args, **kwargs)

    return wrapper


def client_number(name):
    return int(name.removeprefix("Client"))


def parse_status(text):
    """Returns (header_columns, {name: columns}) for a status payload."""
    lines = text.split("\n")
    return lines[0].split("\t"), {r.split("\t")[0]: r.split("\t") for r in lines[1:]}


class LiveProtocolTests:
    """
    Protocol tests shared by both modes. Subclasses (mixed in before a TestCase) provide
    start_server(), which sets host, port, max_clients and repo.
    """
    EMBEDDED = False
    host = "127.0.0.1"
    port = None
    max_clients = server.MAX_CLIENTS
    repo = None

    def setUp(self):
        super().setUp()
        self.conns = []
        self._created = []
        self.addCleanup(self._remove_created)
        self.start_server()
        self.addCleanup(self._close_conns)

    # ---- hooks / helpers

    def start_server(self):
        raise NotImplementedError

    def require_repo(self):
        """Skips the test if the server's repository directory is not known."""

    def _close_conns(self):
        for c in self.conns:
            c.close()

    def _remove_created(self):
        for path in reversed(self._created):
            try:
                if path.is_dir():
                    path.rmdir()
                else:
                    path.unlink(missing_ok=True)
            except OSError:
                pass

    def add_file(self, name, data=b"data", where=None):
        """Creates a file (default: in the repository); never overwrites; removed at test end."""
        self.require_repo()
        path = (where or self.repo) / name
        if path.exists():
            self.fail(f"refusing to overwrite existing {path}")
        path.write_bytes(data)
        self._created.append(path)
        return path

    def add_dir(self, name):
        self.require_repo()
        path = self.repo / name
        if path.exists():
            self.fail(f"refusing to reuse existing {path}")
        path.mkdir()
        self._created.append(path)
        return path

    def connect(self):
        c = Conn(self.host, self.port)
        self.conns.append(c)
        return c

    def login(self):
        # A running server may not have released the previous test's slots yet, so retry on "full".
        attempts = 1 if self.EMBEDDED else 60
        for _ in range(attempts):
            c = self.connect()
            kind, text = c.text()
            if kind == KIND_MSG:
                break
            c.close()
            time.sleep(0.05)
        self.assertEqual(kind, KIND_MSG, text)
        c.send_line(text)
        return c, text

    def rows(self, client):
        """Status cache rows as seen through an already logged-in client."""
        kind, text = client.ask("status")
        self.assertEqual(kind, KIND_MSG)
        return parse_status(text)[1]

    def wait_ended(self, observer, name):
        """True once the cache shows a finish time for `name` (queried through `observer`)."""

        def ended():
            row = self.rows(observer).get(name)
            return row is not None and row[3] != "$"

        return wait_until(ended)

    # ---- naming / handshake / capacity

    def test_clients_are_named_incrementally(self):
        names = [self.login()[1] for _ in range(min(3, self.max_clients))]
        numbers = [client_number(n) for n in names]
        for name in names:
            self.assertRegex(name, r"^Client\d{2,}$")
        self.assertEqual(numbers, list(range(numbers[0], numbers[0] + len(numbers))))

    def test_extra_client_rejected_and_not_cached(self):
        clients = [self.login() for _ in range(self.max_clients)]
        observer = clients[0][0]
        before = len(self.rows(observer))
        extra = self.connect()
        kind, text = extra.text()
        self.assertEqual(kind, KIND_ERR)
        self.assertIn("full", text.lower())
        self.assertTrue(extra.at_eof())
        self.assertEqual(len(self.rows(observer)), before)

    def test_slot_is_freed_after_exit_and_ids_not_reused(self):
        clients = [self.login() for _ in range(self.max_clients)]
        clients[0][0].send_line("exit")
        self.assertTrue(clients[0][0].at_eof())
        _, name = self.login()
        self.assertGreater(client_number(name), max(client_number(n) for _, n in clients))

    def test_name_mismatch_is_rejected(self):
        observer, _ = self.login()
        c = self.connect()
        kind, assigned = c.text()
        self.assertEqual(kind, KIND_MSG)
        c.send_line("NotMyName")
        kind, _ = c.text()
        self.assertEqual(kind, KIND_ERR)
        self.assertTrue(c.at_eof())
        self.assertTrue(self.wait_ended(observer, assigned))

    # ---- echo / ACK

    def test_echo_with_ack(self):
        c, _ = self.login()
        self.assertEqual(c.ask("hello world"), (KIND_MSG, "hello worldACK"))
        self.assertEqual(c.ask("second"), (KIND_MSG, "secondACK"))

    def test_echo_empty_and_unicode_lines(self):
        c, _ = self.login()
        self.assertEqual(c.ask(""), (KIND_MSG, "ACK"))
        self.assertEqual(c.ask("héllo ✓"), (KIND_MSG, "héllo ✓ACK"))

    def test_crlf_line_endings_accepted(self):
        c, _ = self.login()
        c.sock.sendall(b"hello\r\n")
        self.assertEqual(c.text(), (KIND_MSG, "helloACK"))

    # ---- status / cache

    def test_status_reports_active_and_finished_sessions(self):
        (c1, n1), (c2, n2) = self.login(), self.login()
        c2.send_line("exit")
        self.assertTrue(c2.at_eof())
        kind, text = c1.ask("status")
        self.assertEqual(kind, KIND_MSG)
        header, rows = parse_status(text)
        self.assertEqual(header, ["NAME", "IP", "ACCEPTED", "FINISHED"])
        self.assertLessEqual({n1, n2}, set(rows))
        self.assertEqual(rows[n1][1], c1.sock.getsockname()[0])
        self.assertEqual(rows[n1][3], "$")
        self.assertNotEqual(rows[n2][3], "$")
        datetime.fromisoformat(rows[n1][2])
        datetime.fromisoformat(rows[n2][3])

    def test_abrupt_disconnect_is_recorded(self):
        observer, _ = self.login()
        c, name = self.login()
        c.close()
        self.assertTrue(self.wait_ended(observer, name))

    # ---- closing

    def test_exit_aliases_close_connection(self):
        for cmd in ("exit", "quit", "q"):
            with self.subTest(cmd=cmd):
                c, _ = self.login()
                c.send_line(cmd)
                self.assertTrue(c.at_eof())

    def test_line_too_long_closes_connection(self):
        observer, _ = self.login()
        c, name = self.login()
        c.sock.sendall(b"x" * (server.MAX_LINE_BYTES + 10) + b"\n")
        try:
            kind, _ = c.text()
            self.assertEqual(kind, KIND_ERR)
        except (ConnectionResetError, EOFError):
            pass  # peer closed with unread data; RST may preempt the ERR frame
        self.assertTrue(self.wait_ended(observer, name))

    # ---- repository / file streaming

    def test_list_returns_repository_files(self):
        self.add_file("b.txt")
        self.add_file("my file.txt")
        self.add_dir("sub")
        c, _ = self.login()
        kind, text = c.ask("list")
        self.assertEqual(kind, KIND_MSG)
        self.assertEqual(text.split("\n"), ["b.txt", "my file.txt"])

    def test_file_is_streamed(self):
        self.add_file("a.txt", b"alpha")
        c, _ = self.login()
        c.ask("list")
        c.send_line("a.txt")
        self.assertEqual(c.frame(), (KIND_FILE, "a.txt", b"alpha"))
        self.assertEqual(c.ask("after"), (KIND_MSG, "afterACK"))

    def test_file_name_with_spaces(self):
        self.add_file("my file.txt", b"spaced")
        c, _ = self.login()
        c.ask("list")
        c.send_line("my file.txt")
        self.assertEqual(c.frame(), (KIND_FILE, "my file.txt", b"spaced"))

    def test_large_binary_file_round_trip(self):
        data = os.urandom(server.CHUNK_BYTES * 4 + 123)
        self.add_file("big.bin", data)
        c, _ = self.login()
        c.ask("list")
        c.send_line("big.bin")
        kind, name, payload = c.frame()
        self.assertEqual((kind, name), (KIND_FILE, "big.bin"))
        self.assertEqual(payload, data)
        self.assertEqual(c.ask("still alive"), (KIND_MSG, "still aliveACK"))

    def test_empty_file(self):
        self.add_file("empty.bin", b"")
        c, _ = self.login()
        c.ask("list")
        c.send_line("empty.bin")
        self.assertEqual(c.frame(), (KIND_FILE, "empty.bin", b""))
        self.assertEqual(c.ask("next"), (KIND_MSG, "nextACK"))

    def test_nonexistent_file_gives_error_and_retry_works(self):
        self.add_file("a.txt", b"alpha")
        c, _ = self.login()
        c.ask("list")
        kind, text = c.ask("nope.txt")
        self.assertEqual(kind, KIND_ERR)
        self.assertIn("nope.txt", text)
        c.send_line("a.txt")
        self.assertEqual(c.frame(), (KIND_FILE, "a.txt", b"alpha"))

    def test_list_after_error_is_only_a_file_name(self):
        self.add_file("a.txt")
        c, _ = self.login()
        c.ask("list")
        c.ask("nope.txt")
        self.assertEqual(c.ask("list")[0], KIND_ERR)
        c.send_line("a.txt")
        self.assertEqual(c.frame(), (KIND_FILE, "a.txt", b"data"))

    def test_file_named_list_can_be_requested(self):
        self.add_file("list", b"L")
        c, _ = self.login()
        self.assertEqual(c.ask("list"), (KIND_MSG, "list"))
        c.send_line("list")
        self.assertEqual(c.frame(), (KIND_FILE, "list", b"L"))
        self.assertEqual(c.ask("list"), (KIND_MSG, "list"))  # back in ECHO: "list" is the command

    def test_empty_repository(self):
        self.require_repo()
        c, _ = self.login()
        self.assertEqual(c.ask("list"), (KIND_MSG, "(repository is empty)"))
        self.assertEqual(c.ask("anything")[0], KIND_ERR)

    def test_path_traversal_is_refused(self):
        self.require_repo()
        secret = f"secret-{uuid.uuid4().hex}.txt"
        secret_path = self.add_file(secret, b"TOP SECRET", where=self.repo.parent)
        self.add_file("a.txt")
        c, _ = self.login()
        c.ask("list")
        for attempt in (f"../{secret}", str(secret_path), "..", "./a.txt", "sub/../a.txt"):
            with self.subTest(attempt=attempt):
                kind, text = c.ask(attempt)
                self.assertEqual(kind, KIND_ERR)
                self.assertNotIn("TOP SECRET", text)

    def test_directory_cannot_be_requested(self):
        self.add_dir("sub")
        c, _ = self.login()
        c.ask("list")
        self.assertEqual(c.ask("sub")[0], KIND_ERR)

    def test_file_name_without_list_is_echoed(self):
        self.add_file("a.txt")
        c, _ = self.login()
        self.assertEqual(c.ask("a.txt"), (KIND_MSG, "a.txtACK"))

    def test_status_in_file_mode_is_only_a_file_name(self):
        self.add_file("a.txt", b"alpha")
        c, _ = self.login()
        c.ask("list")
        self.assertEqual(c.ask("status")[0], KIND_ERR)
        self.add_file("status", b"s")
        c.send_line("status")
        self.assertEqual(c.frame(), (KIND_FILE, "status", b"s"))
        self.assertEqual(c.ask("status")[0], KIND_MSG)  # back in ECHO: status is the command

    def test_file_deleted_between_list_and_request(self):
        path = self.add_file("a.txt")
        c, _ = self.login()
        c.ask("list")
        path.unlink()
        self.assertEqual(c.ask("a.txt")[0], KIND_ERR)

    # ---- isolation / concurrency

    def test_client_state_is_per_client(self):
        self.add_file("a.txt", b"alpha")
        (c1, _), (c2, _) = self.login(), self.login()
        c1.ask("list")
        self.assertEqual(c2.ask("a.txt"), (KIND_MSG, "a.txtACK"))
        c1.send_line("a.txt")
        self.assertEqual(c1.frame(), (KIND_FILE, "a.txt", b"alpha"))

    def test_concurrent_file_transfers(self):
        data = os.urandom(server.CHUNK_BYTES * 3)
        self.add_file("big.bin", data)
        count = min(3, self.max_clients)
        clients = [self.login()[0] for _ in range(count)]
        results = [None] * count

        def worker(i):
            c = clients[i]
            c.ask("list")
            c.send_line("big.bin")
            results[i] = c.frame()

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(count)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(10)
        for r in results:
            self.assertEqual(r, (KIND_FILE, "big.bin", data))


class LiveServerTests(LiveProtocolTests, RepoTestCase):
    """Starts a private server (ephemeral port, fresh ConnectionTable and repository) per test."""
    EMBEDDED = True

    def start_server(self):
        self.max_clients = self.MAX_CONNECTIONS
        self.listener = socket.create_server(("127.0.0.1", 0))
        self.host, self.port = "127.0.0.1", self.listener.getsockname()[1]
        self.stop = threading.Event()
        self.thread = threading.Thread(target=server.serve,
                                       args=(self.listener, self.stop), daemon=True)
        self.thread.start()
        self.addCleanup(self._stop_server)

    def _stop_server(self):
        self.stop.set()
        self.thread.join(3)

    def session_ended(self, client_id):
        conn = ConnectionTable().get_connection(client_id)
        return conn is not None and conn.session_end is not None

    def test_names_start_at_client01_and_ids_are_not_reused(self):
        clients = [self.login() for _ in range(3)]
        self.assertEqual([n for _, n in clients], ["Client01", "Client02", "Client03"])
        clients[0][0].send_line("exit")
        self.assertTrue(clients[0][0].at_eof())
        self.assertEqual(self.login()[1], "Client04")

    def test_handshake_timeout_frees_slot(self):
        with mock.patch.object(server, "HANDSHAKE_TIMEOUT", 0.3):
            c = self.connect()
            c.text()  # receives name, never answers
            self.assertTrue(c.at_eof())
        self.assertTrue(wait_until(lambda: self.session_ended(1)))


@unittest.skipUnless(os.environ.get("TEST_SERVER"),
                     "set TEST_SERVER=host:port to test an already running server")
class ExternalServerTests(LiveProtocolTests, unittest.TestCase):
    """
    Runs the protocol tests against an already running server. Environment:
      TEST_SERVER             host:port of the server (required)
      TEST_SERVER_MAX_CLIENTS the server's client limit (default: server.MAX_CLIENTS; must be >= 2)
      TEST_SERVER_REPO        the server's repository directory on this machine; it must be EMPTY and
                              used by nothing else. Tests create files in it (never overwriting) and
                              remove them afterwards. Tests that need it are skipped when unset.
    The server should be otherwise idle: tests assume free client slots.
    """

    def start_server(self):
        host, _, port = os.environ["TEST_SERVER"].rpartition(":")
        self.host, self.port = host or "127.0.0.1", int(port)
        self.max_clients = int(os.environ.get("TEST_SERVER_MAX_CLIENTS", server.MAX_CLIENTS))
        if self.max_clients < 2:
            self.skipTest("TEST_SERVER_MAX_CLIENTS must be at least 2")
        repo = os.environ.get("TEST_SERVER_REPO")
        self.repo = pathlib.Path(repo).resolve() if repo else None
        if self.repo is not None and any(self.repo.iterdir()):
            self.fail(f"TEST_SERVER_REPO ({self.repo}) must be empty")

    def require_repo(self):
        if self.repo is None:
            self.skipTest("set TEST_SERVER_REPO to the running server's (empty) repository directory")


if __name__ == "__main__":
    unittest.main()
