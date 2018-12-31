import json
import os

class FileDb:
    def __init__(self, file_path):
        self.db = {}
        self.file_path = file_path
        self.unneeded_records = 0
        self.append_handle = None
        self.load()

    def load(self):
        if not os.path.exists(self.file_path):
            return
        self.db = {}
        self.unneeded_records = 0
        self._close_append_handle()
        with open(self.file_path, 'r') as file:
            for line in file:
                if line == '':
                    continue
                try:
                    l = json.loads(line)
                except ValueError:
                    continue
                if l['name'] in self.db:
                    self.unneeded_records += 1
                self.db[l['name']] = l
        self._maybe_compact()

    def _close_append_handle(self):
        if self.append_handle is not None:
            self.append_handle.close()
            self.append_handle = None

    def _maybe_compact(self):
        if self.unneeded_records >= 1000:
            self.rewrite()

    def rewrite(self):
        self._close_append_handle()
        with open(self.file_path + ".tmp", 'w') as file:
            for v in self.db.values():
                file.write(json.dumps(v))
        os.rename(self.file_path + ".tmp", self.file_path)

    def append(self, entry):
        if entry['name'] in self.db:
            self.unneeded_records += 1
        self.db[entry['name']] = entry
        if self.append_handle is None:
            if not os.path.exists(self.file_path):
                self.rewrite()
                return
            self.append_handle = open(self.file_path, 'a')
        self.append_handle.write('\n' + json.dumps(entry))
        self._maybe_compact()