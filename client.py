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
parser.add_argument("-m", "--size-and-time", help="assume files with same size and mtime are equal", action='store_true')
parser.add_argument("--dry-run", help="don't copy files", action='store_true')
args = parser.parse_args()

root_dir = "/"

proc = subprocess.Popen(args.command, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=sys.stderr)
client = SyncClient(proc)
server_files = client.get_file_db()
file_finder = FileFinder()
with open(args.file_list, "r") as filters_file:
    file_finder.add_from_text(root_dir, filters_file)

created_dirs = {}
local_dirs = {}
local_files = {}
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
            try:
                fh = os.open(os.path.join(root_dir, dirp), os.O_RDONLY)
            except PermissionError:
                print("Error opening parent directory", file=sys.stderr)
                return False
            client.mkdir(dirp, fh)
            os.close(fh)
    return True

def process_local_file(path):
    global total_uploaded_size
    local_files[path] = True
    full_path = os.path.join(root_dir, path)
    is_symlink = os.path.islink(full_path)
    symlink_to = None
    if is_symlink:
        symlink_to = os.readlink(full_path)
        if symlink_to[0] == '/': # absolute path
            symlink_to = '/' + os.path.relpath(symlink_to, root_dir)
    server_file = server_files.find_path(path)
    if server_file is not None:
        if server_file.is_symlink() and is_symlink and symlink_to == server_file.symlink:
            return
        if not server_file.is_directory() and not server_file.is_symlink() and not is_symlink:
            stat_info = os.stat(full_path)
            if stat_info.st_size == server_file.size and stat_info.st_mtime_ns == server_file.mtime:
                # print(f"Skipping {path} - already uploaded (time)")
                return

            try:
                with open(full_path, 'rb') as fh:
                    local_sha256 = util.sha256_file(fh)
            except PermissionError:
                print("Error opening file for SHA256 calculation", file=sys.stderr)
                return

            if local_sha256 == server_file.sha256:
                # print(f"Skipping {path} - already uploaded (sha256)")
                return
    if not create_parent_dirs(path):
        return
    if is_symlink:
        print(f"Symlinking {full_path} -> {symlink_to}")
        if not args.dry_run:
            client.symlink(path, symlink_to, full_path)
        return
    print(f"Uploading {full_path}")
    total_uploaded_size += os.stat(full_path).st_size
    if not args.dry_run:
        try:
            fh = os.open(full_path, os.O_RDONLY)
        except PermissionError:
            print("Error opening file", file=sys.stderr)
            return
        client.upload_file(path, fh)
        os.close(fh)

def process_local_dir(path):
    if path == '':
        return
    local_dirs[path] = True
    server_file = server_files.find_path(path)
    if server_file is not None:
        if server_file.is_directory():
            # print(f"Skipping {path} - dir already created")
            return
    if not create_parent_dirs(path):
        return
    full_path = os.path.join(root_dir, path)
    print(f"Creating dir {full_path}")
    if not args.dry_run:
        try:
            fh = os.open(full_path, os.O_RDONLY)
        except PermissionError:
            print("Error opening directory", file=sys.stderr)
            return
        client.mkdir(path, fh)
        os.close(fh)
    created_dirs[path] = True

file_finder.process(root_dir, process_local_file, process_local_dir)

def delete_files(el, current_path = ""):
    if el.children is not None:
        for chld in el.children.values():
            delete_files(chld, os.path.join(current_path, chld.name))
    if current_path not in local_files and current_path not in local_dirs and current_path != '':
        print(f"Deleting {current_path}")
        if not args.dry_run:
            client.delete(current_path)

delete_files(server_files.root)

print("Done")

proc.stdin.close()
proc.wait()

print(f"Uploaded {int(total_uploaded_size/1024/1024)}MB")