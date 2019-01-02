import json
import pickle
import os
import shutil
from file_db import FileDb, FileDbEntry

class SyncClient:
    def __init__(self, proc):
        self.proc = proc
        self.inpipe = self.proc.stdout
        self.outpipe = self.proc.stdin

    def read_line(self):
        line = self.inpipe.readline()
        if not line:
            return None
        return line.decode().rstrip('\n')

    def write_command(self, data):
        self.outpipe.write((json.dumps(data) + '\n').encode())
        self.outpipe.flush()

    @staticmethod
    def _get_file_stat(fd, follow_symlinks=True):
        f_stat = os.stat(fd, follow_symlinks=follow_symlinks)
        stat_data = {'mode': f_stat.st_mode, 'uid': f_stat.st_uid, 'gid': f_stat.st_gid,
                     'atime': f_stat.st_atime_ns, 'mtime': f_stat.st_mtime_ns}
        return stat_data

    @staticmethod
    def _get_xattrs(fd, follow_symlinks=True):
        ret = []
        for name in os.listxattr(fd, follow_symlinks=follow_symlinks):
            name = name.encode()
            ret.append((name, os.getxattr(fd, name, follow_symlinks=follow_symlinks)))
        return ret

    def mkdir(self, server_filename, fd):
        stat_data = self._get_file_stat(fd)
        xattr_data = pickle.dumps(self._get_xattrs(fd))
        self.write_command({'op': 'mkdir', 'path': server_filename, 'stat': stat_data, 'xattr_size': len(xattr_data)})
        self.outpipe.write(xattr_data)
        self.outpipe.flush()

    def symlink(self, server_filename, server_to, local_filename):
        stat_data = self._get_file_stat(local_filename, False)
        xattr_data = pickle.dumps(self._get_xattrs(local_filename, False))
        self.write_command({'op': 'symlink', 'path': server_filename, 'to': server_to,
                            'stat': stat_data, 'xattr_size': len(xattr_data)})
        self.outpipe.write(xattr_data)
        self.outpipe.flush()

    def upload_file(self, server_filename, fd):
        stat_data = self._get_file_stat(fd)
        file_size = os.stat(fd).st_size
        xattr_data = pickle.dumps(self._get_xattrs(fd))
        self.write_command({'op': 'upload', 'path': server_filename, 'stat': stat_data, 'size': file_size,
                            'xattr_size': len(xattr_data)})
        self.outpipe.write(xattr_data)
        with os.fdopen(os.dup(fd), 'rb') as source:
            shutil.copyfileobj(source, self.outpipe, file_size)
        self.outpipe.flush()

    def delete(self, server_filename):
        self.write_command({'op': 'delete', 'path': server_filename})

    def get_file_db(self):
        self.write_command({'op': 'getdb'})
        data = json.loads(self.read_line())
        ret = FileDb(None)
        for i in range(1, data['count']):
            ent = FileDbEntry.decode(self.inpipe, ret.db)
            if ent.is_removed():
                raise ValueError('Server db data cannot contain removed entries')
            ret.db[ent.id] = ent
            ent.parent.add_child(ent)
        return ret
