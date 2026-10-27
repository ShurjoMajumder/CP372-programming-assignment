---
title: CP372 Programming Assignment
author:
  - "Shurjo Majudmer (maju5249@mylaurier.ca)"
  - "Sacchin Purewall (pure1374@mylaurier.ca)"
date: 2026-10-26
geometry: "margin=3cm"
---

# Server Code

## General Overview

The overview of the server side code can be shown in the following figure.

![Startup Sequence](assets/overview.png)

## Life of A Client Session

Here is an overview of a client session lifecycle from birth to inevitable death. Each thread is
started by the main thread, which spawns it off if the server still has room. Then, a handshake 
is performed. This handshake merely serves the purpose of telling the client their `Client#` 
value. From there, the client is free to either have the server ack them endlessly, or they may 
request the server to send them the contents of whatever files are present in `content/`.

![Client Lifecycle](./assets/client_lifecycle.png)

Now, handling the commands does require some finesse, which is where the next section becomes 
relevant.

## The PDA Defining The Client's State

The following PDA (push down automata) is used to describe the state transitions the client 
object goes through over the course of a session. This PDA was formally proven to be correct, 
however the proof goes beyond the requirements for this report.

![Client PDA](assets/client_dfa.png)

Each state simply tells the relevant thread what kind of response to send, and whether 
content from a file should be sent. In ECHO mode, the server simply echoes what the 
client sent. In FILE mode, the server treats input as a file name, and checks if the 
file exists in the repository. If yes, the filename is pushed onto the stack and the 
server streams the file's contents, if not, an error is raised and the client must 
try again.

While the language recognized here is technically regular, thus a DFA would have sufficed, an 
equivalent DFA would have required 4 states (instead of just three). This would have significantly 
complicated the code implementation by adding the necessary transitions for the 4th state.

## The Connection Table

The connection table is merely a table tracking the connections to the server. The only 
complication in its design is that a lock is required to read/modify/write it.

# Client Code
