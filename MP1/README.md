# CS425 G87 MP1

## Overview

This project implements a basic client-server architecture for querying log files in a distributed system. The server listens for incoming requests from clients, processes the queries, and returns the relevant log entries.


## Structure

```bash
project_root/
├── MP1/vm1.log
└── MP2/vm1.log
```

## Prepare

```bash
    sudo dnf install python3-pip
```

```bash
    pip3 install python-dotenv
```

Also update the `.env` file with the IP address and port number of your server.

## How to run

```bash
    python MP1/server.py /path/to/folder     #example: python MP1/server.py MP2
```
```bash
    python MP1/client.py
```

## Unit test

```bash
    python cs425_g87/MP1_Python/test.py
```
