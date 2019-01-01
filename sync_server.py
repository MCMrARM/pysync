import os
import json
import pickle
import util
import xattr

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
        if op == "symlink":
            return self.read_symlink(data)
        if op == "getdb":
            return self.read_getdb(data)
        return False

    @staticmethod
    def _set_stat_and_xattr(fp, stat, xattrs):
        # xattr.set(fp, 'user.psy.stat', json.dumps(stat).encode(), nofollow=True)
        # for k, v in xattrs:
        #     xattr.set(fp, 'user.psy.x.'.encode() + k, v, nofollow=True)
        pass

    def read_mkdir(self, data):
        xattr_data = pickle.loads(self.inpipe.read(data['xattr_size']))

        fp = self.get_path(data['path'])
        try:
            os.mkdir(fp)
        except FileExistsError:
            pass
        self._set_stat_and_xattr(fp, data['stat'], xattr_data)
        self.filedb.append({'name': data['path'], 'dir': True})
        return True

    def read_symlink(self, data):
        xattr_data = pickle.loads(self.inpipe.read(data['xattr_size']))

        fp = self.get_path(data['path'])
        to_fp = data['to']
        if to_fp[0] == '/':
            to_fp = self.get_path(to_fp)
        if os.path.exists(fp):
            os.remove(fp)
        os.symlink(to_fp, fp)
        self._set_stat_and_xattr(fp, data['stat'], xattr_data)
        self.filedb.append({'name': data['path'], 'symlink': data['to']})
        return True

    def read_upload_file(self, data):
        fp = self.get_path(data['path'])
        f = os.open(fp, os.O_WRONLY | os.O_CREAT)
        xattr_data = pickle.loads(self.inpipe.read(data['xattr_size']))
        util.copy_file_limited(self.inpipe, os.fdopen(f, 'wb'), data['size'])
        self._set_stat_and_xattr(fp, data['stat'], xattr_data)
        self.filedb.append({'name': data['path'], 'sha256': util.sha256_file(open(data['path'], 'rb'))})
        return True

    def read_getdb(self, data):
        for entry in self.filedb.db.values():
            self.write_line(json.dumps(entry))
        self.write_line("")
        self.outpipe.flush()
        return True
