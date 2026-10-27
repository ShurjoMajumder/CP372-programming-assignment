
import pathlib
import socket
import sys
from typing import NamedTuple

DEF_H = "127.0.0.1"
DEF_P = 5000
S_D = pathlib.Path("downloads")      
B_P = 64 * 1024                 
QUIT_COM = ("exit", "quit", "q")         


class Header(NamedTuple):
    kind: str       # MSG, ERR or FILE
    size: int       # # of bytes in payload
    label: str      # file name


class CliClient:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.sock = None
        self.rx = None
        self.my_name = "???"               # the server hands us a Client# name

    def open(self) -> None:
        """Open the TCP"""
        self.sock = socket.create_connection((self.host, self.port))
        self.rx = self.sock.makefile("rb")

    def close(self) -> None:
        for thing in (self.rx, self.sock):
            if thing is not None:
                try:
                    thing.close()
                except OSError:
                    pass                   

    def text(self, text: str) -> None:
        """Send one line to the server"""
        self.sock.sendall(text.encode("utf-8") + b"\n")

    def read_header(self) -> Header | None:
        raw = self.rx.readline()
        if not raw:
            return None
        
        kind, size, *tail = raw.decode("utf-8").rstrip("\n").split(" ", 2)
        return Header(kind, int(size), tail[0] if tail else "")

    def read_body_text(self, size: int) -> str:
        pieces = []
        missing = size
        while missing > 0:
            piece = self.rx.read(min(B_P, missing))
            if not piece:                  
                raise ConnectionError("server vanished mid-frame")
            pieces.append(piece)
            missing -= len(piece)
        return b"".join(pieces).decode("utf-8", errors="replace")

    def s_file(self, hdr: Header) -> pathlib.Path:
        S_D.mkdir(exist_ok=True)
        
        dest = S_D / pathlib.Path(hdr.label).name
        missing = hdr.size
        with open(dest, "wb") as out:
            while missing > 0:
                piece = self.rx.read(min(B_P, missing))
                if not piece:
                    raise ConnectionError("server vanished mid-file")
                out.write(piece)
                missing -= len(piece)
        return dest

    def handshake(self) -> bool:
        hdr = self.read_header()
        if hdr is None:
            print("Server closed the connection right away.")
            return False
        text = self.read_body_text(hdr.size)
        if hdr.kind == "ERR":
            print(f"Server said no: {text}")
            return False
        self.my_name = text
        self.text(self.my_name)
        print(f"Connected as {self.my_name}. Type 'exit' to leave.")
        return True

    def handle_reply(self) -> bool:
        hdr = self.read_header()
        if hdr is None:
            print("Server closed the connection.")
            return False
        if hdr.kind == "FILE":
            dest = self.s_file(hdr)
            print(f"<< got file '{hdr.label}' ({hdr.size} bytes) -> {dest}")
        else:
            print(f"<< [{hdr.kind}] {self.read_body_text(hdr.size)}")
        return True

    def repl(self) -> None:
        while True:
            try:
                typed = input(f"{self.my_name}> ")
            except (EOFError, KeyboardInterrupt):   # Ctrl+D / Ctrl+C to exit
                typed = "exit"
                print()

            self.text(typed)

            if typed in QUIT_COM:
                print("Bye!")
                return
            if not self.handle_reply():
                return


def main() -> None:
    host = sys.argv[1] if len(sys.argv) > 1 else DEF_H
    port = int(sys.argv[2]) if len(sys.argv) > 2 else DEF_P

    client = CliClient(host, port)
    try:
        client.open()
        if client.handshake():
            client.repl()
    except (ConnectionError, OSError) as err:
        print(f"Connection problem: {err}")
    finally:
        client.close()


if __name__ == "__main__":
    main()