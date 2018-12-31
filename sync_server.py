import os
import json
import util

class SyncServer:
    def __init__(self, inpipe, outpipe, rootdir, filedb):
        self.inpipe = inpipe
        self.outpipe = outpipe
        self.rootdir = rootdir
        self.filedb = filedb

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
        if op == "getdb":
            return self.read_getdb(data)
        return False

    def read_mkdir(self, data):
        try:
            os.mkdir(self.get_path(data['path']))
        except FileExistsError:
            pass
        self.filedb.append({'name': data['path'], 'dir': True})
        return True

    def read_upload_file(self, data):
        f = os.open(self.get_path(data['path']), os.O_WRONLY | os.O_CREAT)
        util.copy_file_limited(self.inpipe, os.fdopen(f, 'wb'), data['size'])
        self.filedb.append({'name': data['path'], 'sha256': util.sha256_file(open(data['path'], 'rb'))})
        return True

    def read_getdb(self, data):
        for entry in self.filedb.db.values():
            self.write_line(json.dumps(entry))
        self.write_line("")
        self.outpipe.flush()
        return True
