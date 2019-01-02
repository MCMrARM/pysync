import os
import sys
import time
from util import *

class FileDbEntry:
    FLAG_DIRECTORY = 1
    FLAG_REMOVED = 2

    ENTRY_META_SIZE = 1
    ENTRY_META_MTIME = 2
    ENTRY_META_SHA256 = 3
    ENTRY_META_SYMLINK = 4

    def __init__(self, name = None, parent = None):
        self.id = None
        self.name = name
        self.parent = parent
        self.children = None
        self.size = None
        self.mtime = None
        self.sha256 = None
        self.symlink = None
        self.flags = 0

    def reset_meta(self):
        if self.is_directory():
            if self.children is not None and len(self.children) > 0:
                raise ValueError('Directory not empty')
        self.children = None
        self.size = None
        self.mtime = None
        self.sha256 = None
        self.symlink = None
        self.flags = 0

    def add_child(self, val):
        if not self.flags & self.FLAG_DIRECTORY:
            raise ValueError('Cannot add a children to a file')
        if self.children is None:
            self.children = {}
        self.children[val.name] = val

    def remove_child(self, val):
        self.children.pop(val.name)

    def get_child(self, val):
        if self.children is None:
            return None
        return self.children.get(val, None)

    def set_directory(self):
        self.flags |= self.FLAG_DIRECTORY

    def is_directory(self):
        return (self.flags & self.FLAG_DIRECTORY) != 0

    def set_removed(self):
        if self.children is not None and len(self.children) > 0:
            raise ValueError('Directory not empty')
        self.flags |= self.FLAG_REMOVED

    def is_removed(self):
        return (self.flags & self.FLAG_REMOVED) != 0

    def is_symlink(self):
        return self.symlink is not None

    def encode(self):
        buf = bytearray()
        # varint id, varint parent
        encode_varint(buf, self.id)
        encode_varint(buf, self.parent.id)
        # varint name_len, followed by data
        name_encoded = self.name.encode()
        encode_varint(buf, len(name_encoded))
        buf += name_encoded
        # byte flags
        buf.append(self.flags)
        # metadata (byte type, ...)
        if self.sha256 is not None:
            buf.append(self.ENTRY_META_SHA256)
            encode_varint(buf, len(self.sha256))
            buf += self.sha256
        if self.size is not None:
            buf.append(self.ENTRY_META_SIZE)
            encode_varint(buf, self.size)
        if self.mtime is not None:
            buf.append(self.ENTRY_META_MTIME)
            encode_varint(buf, self.mtime)
        if self.symlink is not None:
            buf.append(self.ENTRY_META_SYMLINK)
            symlink_encoded = self.symlink.encode()
            encode_varint(buf, len(symlink_encoded))
            buf += symlink_encoded
        buf.append(0) # end of meta
        return buf

    @staticmethod
    def decode(stream, parents):
        try:
            file_id = decode_varint_stream(stream)
        except EOFError:
            return None
        parent = decode_varint_stream(stream)
        if parent not in parents:
            raise ValueError('Invalid parent: ' + str(parent))
        name = decode_varint_prefixed_bytes(stream).decode()
        ent = FileDbEntry(name, parents[parent])
        ent.id = file_id
        ent.flags = ord(read_exactly(stream, 1))
        while True:
            meta_type = stream.read(1)
            if len(meta_type) == 0:
                raise EOFError()
            meta_type = ord(meta_type)
            if meta_type == 0:
                break
            if meta_type == FileDbEntry.ENTRY_META_SHA256:
                ent.sha256 = decode_varint_prefixed_bytes(stream)
            elif meta_type == FileDbEntry.ENTRY_META_SIZE:
                ent.size = decode_varint_stream(stream)
            elif meta_type == FileDbEntry.ENTRY_META_MTIME:
                ent.mtime = decode_varint_stream(stream)
            elif meta_type == FileDbEntry.ENTRY_META_SYMLINK:
                ent.symlink = decode_varint_prefixed_bytes(stream).decode()
            else:
                raise ValueError('Invalid metadata: ' + str(meta_type))
        return ent

class FileDb:
    def __init__(self, file_path):
        self.file_path = file_path
        self.unneeded_records = 0
        self.next_id = 1
        self.append_handle = None
        self.root = FileDbEntry()
        self.root.set_directory()
        self.root.id = 0
        self.db = {0: self.root}
        self.load()

    def load(self):
        if self.file_path is None or not os.path.exists(self.file_path):
            return
        self.root = FileDbEntry()
        self.root.set_directory()
        self.root.id = 0
        self.db = {0: self.root}
        self.unneeded_records = 0
        self.next_id = 0
        self._close_append_handle()
        with open(self.file_path, 'rb') as file:
            while True:
                try:
                    ent = FileDbEntry.decode(file, self.db)
                    if ent is None:
                        break
                    if ent.id in self.db:
                        if self.db[ent.id].parent.id != ent.parent.id:
                            raise ValueError(f"Parent is invalid")
                        ent.children = self.db[ent.id].children
                        self.unneeded_records += 1
                    if ent.is_removed():
                        self.db.pop(ent.id, None)
                        ent.parent.remove_child(ent)
                        if self.next_id == ent.id + 1:
                            self.next_id = ent.id
                    else:
                        self.db[ent.id] = ent
                        ent.parent.add_child(ent)
                        if ent.id >= self.next_id:
                            self.next_id = ent.id + 1
                except (ValueError, EOFError) as e:
                    print(e, file=sys.stderr)
                    break
        self._maybe_compact()

    def close(self):
        self._close_append_handle()
        self.file_path = None
        self.db = None

    def _close_append_handle(self):
        if self.append_handle is not None:
            self.append_handle.close()
            self.append_handle = None

    def _maybe_compact(self):
        if self.unneeded_records >= 100000:
            self.rewrite()

    def rewrite(self):
        self._close_append_handle()
        self.unneeded_records = 0
        with open(self.file_path + ".tmp", 'wb') as file:
            for v in self.db.values():
                if v == self.root:
                    continue
                file.write(v.encode())
        os.rename(self.file_path + ".tmp", self.file_path)

    def find_path(self, path):
        entry = self.root
        if path == '':
            return entry
        for el in path.split('/'):
            if entry.children is None or el not in entry.children:
                return None
            entry = entry.children[el]
        return entry

    def get_path(self, path):
        ret = self.find_path(path)
        if ret is None:
            raise KeyError('Invalid path')
        return ret

    def append(self, entry):
        if entry.parent is None:
            raise ValueError('Entry parent can not be None')
        if entry.id in self.db:
            self.unneeded_records += 1
        if entry.id is None:
            if entry.parent.children is not None and entry.name in entry.parent.children:
                other_entry = entry.parent.children[entry.name]
                other_entry.reset_meta()
                other_entry.set_removed()
                entry.id = other_entry.id
                other_entry.id = None
            else:
                entry.id = self.next_id
                self.next_id += 1
            entry.parent.add_child(entry)
            self.db[entry.id] = entry
        if entry.is_removed():
            if entry.id not in self.db:
                return
            self.db.pop(entry.id, None)
            entry.parent.remove_child(entry)

        if self.append_handle is None:
            if not os.path.exists(self.file_path):
                self.rewrite()
                return
            self.append_handle = open(self.file_path, 'ab')
        self.append_handle.write(entry.encode())
        self._maybe_compact()