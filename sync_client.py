import json
import os
import shutil

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

    def mkdir(self, server_filename, fd):
        f_stat = os.stat(fd)
        stat_data = {'mode': f_stat.st_mode, 'uid': f_stat.st_uid, 'gid': f_stat.st_gid,
                     'atime': f_stat.st_atime, 'mtime': f_stat.st_mtime}
        self.write_command({'op': 'mkdir', 'path': server_filename, 'stat': stat_data})

    def upload_file(self, server_filename, fd):
        f_stat = os.stat(fd)
        stat_data = {'mode': f_stat.st_mode, 'uid': f_stat.st_uid, 'gid': f_stat.st_gid,
                     'atime': f_stat.st_atime, 'mtime': f_stat.st_mtime}
        self.write_command({'op': 'upload', 'path': server_filename, 'stat': stat_data, 'size': f_stat.st_size})
        shutil.copyfileobj(os.fdopen(fd, 'rb'), self.outpipe, f_stat.st_size)

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
