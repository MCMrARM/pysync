import argparse
import subprocess
import sys
import os
from sync_client import SyncClient
from file_finder import FileFinder
import util

parser = argparse.ArgumentParser(description='Creates a backup')
parser.add_argument("-f", "--file-list", help="specifies the file list", required=True)
parser.add_argument("-c", "--command", help="specifies the command to run", required=True)
parser.add_argument("--dry-run", help="don't copy files", action='store_true')
args = parser.parse_args()

proc = subprocess.Popen(args.command, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=sys.stderr)
client = SyncClient(proc)
server_files = client.get_file_db()
file_finder = FileFinder()
with open(args.file_list, "r") as filters_file:
    file_finder.add_from_text(filters_file)

root_dir = "/"

created_dirs = {}
total_uploaded_size = 0

def create_parent_dirs(path):
    path_dir = os.path.dirname(path)
    dirs_to_create = []
    while not path_dir in created_dirs and path_dir:
        dirs_to_create.append(path_dir)
        path_dir = os.path.dirname(path_dir)
    for dirp in reversed(dirs_to_create):
        print(f"Creating parent dir {dirp}")
        if not args.dry_run:
            client.mkdir(dirp, os.open(os.path.join(root_dir, dirp), os.O_RDONLY))

def process_local_file(path):
    global total_uploaded_size
    if path in server_files:
        local_sha256 = util.sha256_file(open(path, 'rb'))
        server_file = server_files[path]
        if not 'dir' in server_file and local_sha256 == server_file['sha256']:
            # print(f"Skipping {path} - already uploaded")
            return
    create_parent_dirs(path)
    full_path = os.path.join(root_dir, path)
    print(f"Uploading {full_path}")
    if os.path.islink(full_path):
        return
    total_uploaded_size += os.stat(full_path).st_size
    if not args.dry_run:
        client.upload_file(path, os.open(full_path, os.O_RDONLY))

def process_local_dir(path):
    if path in server_files:
        server_file = server_files[path]
        if 'dir' in server_file:
            # print(f"Skipping {path} - dir already created")
            return
    create_parent_dirs(path)
    full_path = os.path.join(root_dir, path)
    print(f"Creating dir {full_path}")
    if not args.dry_run:
        client.mkdir(path, os.open(full_path, os.O_RDONLY))
    created_dirs[path] = True

file_finder.process(root_dir, process_local_file, process_local_dir)

proc.stdin.close()
proc.wait()

print(f"Uploaded {int(total_uploaded_size/1024/1024)}MB")