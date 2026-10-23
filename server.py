import dataclasses
import enum
import sys
from datetime import datetime


@enum.unique
class ClientStates(enum.Enum):
    ECHO = 0
    LIST = 1
    FILE = 2


@dataclasses.dataclass
class Client:
    id: int
    ip: str
    port: int
    username: str
    state: ClientStates = ClientStates.ECHO

    def __str__(self) -> str:
        return f'{self.ip}\t{self.port}'

    def __repr__(self) -> str:
        return f"{self.ip}\t{self.username}"

    def update_state(self, client_data: str):
        if client_data == "list":
            self.state = ClientStates.LIST
        pass


@dataclasses.dataclass
class ClientConnection:
    id: int
    ip: str
    session_start: datetime
    session_end: datetime | None

    def __str__(self) -> str:
        return f'{self.id}\t{self.ip}\t{self.session_start}\t{self.session_end if self.session_end else "$"}'

    def __repr__(self) -> str:
        return self.__str__()


def handle_input(client: Client, client_data: str) -> str:
    client.update_state(client_data)
    match client.state:
        case ClientStates.ECHO:
            return client_data + "ACK"
        case ClientStates.LIST:
            ...
        case ClientStates.FILE:
            ...
    return ""


def send_response(client: Client | None, response: str) -> int:
    print(response + "ACK")
    return 200  # OK


def main(max_connections=10) -> int:
    running = True
    while running:
        user_input = input("Enter command: ")
        if user_input == "quit" or user_input == "exit" or user_input == "q":
            return 0
        send_response(None, user_input)

    return 0


if __name__ == '__main__':
    sys.exit(main())
