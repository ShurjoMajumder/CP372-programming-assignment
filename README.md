---
title: CP372 Programming Assignment
authors:
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

Here is an overview of a client session lifecycle from birth to inevitable death.

![Client Lifecycle](./assets/client_lifecycle.png)

## The DFA Defining The Client's State

The following DFA is used to describe the state transitions the client object goes 
through over the course of a session. This DFA was formally proven to be correct, 
however the proof goes beyond the requirements for this report.

![img.png](assets/client_dfa.png)

Each state simply tells the relevant thread what kind of response to send, and whether 
content from a file should be sent. In ECHO mode, the server simply echoes what the 
client sent. In FILE mode, the server treats input as a file name, and checks if the 
file exists in the repository. If yes, the filename is pushed onto the stack and the 
server streams the file's contents, if not, an error is raised and the client must 
try again.

While the use of a stack in this case would imply that this is a PDA, which is technically 
true, the language recognized is regular, thus we can simply use an equivalent DFA.

## The Connection Table

The connection table is merely a table tracking the connections to the server. The only 
complication in its design is that a lock is required to read/modify/write it.

# Client Code
