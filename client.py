import argparse
import subprocess
import sys
import os
from sync_client import SyncClient
from file_finder import FileFinder
import util
from pprint import pprint

parser = argparse.ArgumentParser(description='Creates a backup')
parser.add_argument("-f", "--file-list", help="specifies the file list", required=True)
parser.add_argument("-c", "--command", help="specifies the command to run", required=True)
args = parser.parse_args()

proc = subprocess.Popen(args.command, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=sys.stderr)
client = SyncClient(proc)
server_files = client.get_file_db()
file_finder = FileFinder()
with open(args.file_list, "r") as filters_file:
    file_finder.add_from_text(filters_file)

root_dir = "/"

def process_local_file(path):
    if path in server_files:
        local_sha256 = util.sha256_file(open(path, 'rb'))
        server_file = server_files[path]
        if not 'dir' in server_file and local_sha256 == server_file['sha256']:
            print(f"Skipping {path} - already uploaded")
            return
    print(f"Uploading {path}")
    client.upload_file(path, os.open(os.path.join(root_dir, path), os.O_RDONLY))

def process_local_dir(path):
    if path in server_files:
        server_file = server_files[path]
        if 'dir' in server_file:
            print(f"Skipping {path} - dir already created")
            return
    print(f"Creating dir {path}")
    client.mkdir(path, os.open(os.path.join(root_dir, path), os.O_RDONLY))

file_finder.process(root_dir, process_local_file, process_local_dir)

proc.stdin.close()
proc.wait()