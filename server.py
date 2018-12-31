import sys
from file_db import FileDb
from sync_server import SyncServer

server = SyncServer(sys.stdin.buffer, sys.stdout.buffer, sys.argv[1], FileDb(sys.argv[2]))
while server.read_command():
    pass