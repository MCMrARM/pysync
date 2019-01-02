import os
import json
import pickle
import util
import shutil
import errno
from file_db import FileDbEntry
import sys

class SyncServer:
    def __init__(self, inpipe, outpipe, rootdir, filedb):
        self.inpipe = inpipe
        self.outpipe = outpipe
        self.rootdir = rootdir
        self.filedb = filedb
        self.allowdelete = False
        self.permissions_file = 0o744
        self.permissions_dir = 0o755

    def read_line(self):
        line = self.inpipe.readline()
        if not line:
            return None
        return line.decode().rstrip('\n')

    def write_line(self, line):
        self.outpipe.write((line + '\n').encode())

    def get_path(self, path):
        path = path.lstrip('/')
        return os.path.join(self.rootdir, path)


    def read_command(self):
        line = self.read_line()
        if line is None:
            return False
        data = json.loads(line)
        op = data.get("op", None)
        if op == "upload":
            return self.read_upload_file(data)
        if op == "mkdir":
            return self.read_mkdir(data)
        if op == "symlink":
            return self.read_symlink(data)
        if op == "getdb":
            return self.read_getdb(data)
        if op == "delete":
            return self.read_delete(data)
        return False

    @staticmethod
    def _set_stat_and_xattr(fp, stat, xattrs):
        os.setxattr(fp, 'user.psy.stat', json.dumps(stat).encode(), follow_symlinks=False)
        for k, v in xattrs:
            os.setxattr(fp, 'user.psy.x.'.encode() + k, v, follow_symlinks=False)

    def read_mkdir(self, data):
        xattr_data = pickle.loads(self.inpipe.read(data['xattr_size']))

        fp = self.get_path(data['path'])
        if os.path.isfile(fp) and self.allowdelete:
            print(f"mkdir {fp} - already exists; deleting", file=sys.stderr)
            os.remove(fp)
        try:
            os.mkdir(fp, mode=self.permissions_dir)
        except FileExistsError:
            pass
        self._set_stat_and_xattr(fp, data['stat'], xattr_data)
        parent_dir = self.filedb.get_path(os.path.dirname(data['path']))
        ent = parent_dir.get_child(os.path.basename(data['path']))
        if ent is not None:
            if not ent.is_directory():
                raise ValueError('Dir already exists as a file in the db')
        else:
            ent = FileDbEntry(os.path.basename(data['path']), parent_dir)
        ent.set_directory()
        ent.mtime = data['stat']['mtime']
        self.filedb.append(ent)
        return True

    def read_symlink(self, data):
        xattr_data = pickle.loads(self.inpipe.read(data['xattr_size']))

        fp = self.get_path(data['path'])
        to_fp = data['to']
        if to_fp[0] == '/':
            to_fp = self.get_path(to_fp)
        if os.path.exists(fp):
            if os.path.islink(fp):
                os.remove(fp)
            elif os.path.isdir(fp) and self.allowdelete:
                shutil.rmtree(fp, ignore_errors=True)
            elif self.allowdelete:
                os.remove(fp)
        os.symlink(to_fp, fp)
        # self._set_stat_and_xattr(fp, data['stat'], xattr_data)

        parent_dir = self.filedb.get_path(os.path.dirname(data['path']))
        ent = FileDbEntry(os.path.basename(data['path']), parent_dir)
        ent.symlink = data['to']
        ent.mtime = data['stat']['mtime']
        self.filedb.append(ent)
        return True

    def read_upload_file(self, data):
        fp = self.get_path(data['path'])
        f = os.open(fp, os.O_WRONLY | os.O_CREAT, self.permissions_file)
        xattr_data = pickle.loads(self.inpipe.read(data['xattr_size']))

        if os.path.exists(fp) and self.allowdelete:
            if os.path.islink(fp):
                os.remove(fp)
            elif os.path.isdir(fp):
                shutil.rmtree(fp, ignore_errors=True)

        with os.fdopen(os.dup(f), 'wb') as output:
            util.copy_file_limited(self.inpipe, output, data['size'])
        self._set_stat_and_xattr(fp, data['stat'], xattr_data)

        parent_dir = self.filedb.get_path(os.path.dirname(data['path']))
        ent = FileDbEntry(os.path.basename(data['path']), parent_dir)
        ent.size = data['size']
        ent.mtime = data['stat']['mtime']
        with open(fp, 'rb') as fh:
            ent.sha256 = util.sha256_file(fh)
        self.filedb.append(ent)

        os.close(f)
        return True

    def read_delete(self, data):
        fp = self.get_path(data['path'])
        try:
            if os.path.isdir(fp):
                os.rmdir(fp)
            else:
                os.remove(fp)
        except FileNotFoundError:
            pass
        except OSError as e:
            if e.errno != errno.ENOTEMPTY:
                raise
        ent = self.filedb.get_path(data['path'])
        ent.set_removed()
        self.filedb.append(ent)
        return True

    def read_getdb(self, data):
        self.write_line(json.dumps({'count': len(self.filedb.db)}))
        for entry in self.filedb.db.values():
            if entry != self.filedb.root:
                self.outpipe.write(entry.encode())
        self.outpipe.flush()
        return True
