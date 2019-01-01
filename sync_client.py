import json
import pickle
import os
import shutil
import xattr

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
    def _get_file_stat(fd):
        f_stat = os.stat(fd)
        stat_data = {'mode': f_stat.st_mode, 'uid': f_stat.st_uid, 'gid': f_stat.st_gid,
                     'atime': f_stat.st_atime, 'mtime': f_stat.st_mtime}
        return stat_data

    def mkdir(self, server_filename, fd):
        stat_data = self._get_file_stat(fd)
        xattr_data = pickle.dumps(xattr.get_all(fd, nofollow=True))
        self.write_command({'op': 'mkdir', 'path': server_filename, 'stat': stat_data, 'xattr_size': len(xattr_data)})
        self.outpipe.write(xattr_data)
        self.outpipe.flush()

    def upload_file(self, server_filename, fd):
        stat_data = self._get_file_stat(fd)
        file_size = os.stat(fd).st_size
        xattr_data = pickle.dumps(xattr.get_all(fd, nofollow=True))
        self.write_command({'op': 'upload', 'path': server_filename, 'stat': stat_data, 'size': file_size,
                            'xattr_size': len(xattr_data)})
        self.outpipe.write(xattr_data)
        shutil.copyfileobj(os.fdopen(fd, 'rb'), self.outpipe, file_size)
        self.outpipe.flush()

    def get_file_db(self):
        self.write_command({'op': 'getdb'})
        ret = {}
        while True:
            line = self.read_line()
            if not line or line == "":
                break
            line = json.loads(line)
            ret[line['name']] = line
        return ret
