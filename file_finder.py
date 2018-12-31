import os

class FileFinderDir:
    def __init__(self, parent = None):
        self.parent = parent
        self.subdirs = {}
        self.include = -1
        self.exclude = -1
        self.wildcards = []


class FileFinder:
    def __init__(self):
        self.dirs = {}
        self.next_rule_id = 0

    def get_dir(self, path):
        path = path.split('/')
        path = list(filter(None, path))
        if len(path) == 0:
            raise Exception("Invalid path")
        if path[0] not in self.dirs:
            self.dirs[path[0]] = FileFinderDir()
        d = self.dirs[path[0]]
        for el in path[1:]:
            if el not in d.subdirs:
                d.subdirs[el] = FileFinderDir()
            d = d.subdirs[el]
        return d

    def add_filter(self, what, include = True):
        first_wildcard = what.find("*")
        shared_part = what
        wildcard_part = None
        if first_wildcard != -1:
            shared_part_end = what.rfind("/", 0, first_wildcard) + 1
            shared_part = what[:shared_part_end]
            wildcard_part = what[shared_part_end:]
        shared_part = os.path.realpath(shared_part)
        shared_part_dir = self.get_dir(shared_part)
        if wildcard_part is not None and len(wildcard_part) > 0:
            raise Exception("Wildcards are not supported right now")
        if include:
            shared_part_dir.include = self.next_rule_id
        else:
            shared_part_dir.exclude = self.next_rule_id
        self.next_rule_id += 1

    def add_from_text(self, lines):
        for line in lines:
            line = line.rstrip('\n')
            if line[:2] == "+ ":
                self.add_filter(line[2:], True)
            if line[:2] == "- ":
                self.add_filter(line[2:], False)

    @staticmethod
    def _process_simple_dir(path, file_cb, dir_cb):
        for e in os.scandir(path):
            if e.is_dir():
                dir_cb(e.path)
            else:
                file_cb(e.path)

    @staticmethod
    def _process_parent_dirs(dstack, path, dir_cb):
        call_vals = []
        for idx, (d, added) in enumerate(reversed(dstack)):
            if added:
                break
            path = os.path.dirname(path)
            call_vals.append(path)
            dstack[-idx - 1] = (d, True)
        for val in reversed(call_vals):
            dir_cb(val)

    def _process(self, d, dstack, path, include_id, exclude_id, file_cb, dir_cb):
        if not os.path.exists(path):
            return
        include_id = max(include_id, d.include)
        exclude_id = max(exclude_id, d.exclude)
        if include_id > exclude_id:
            # include this dir
            self._process_parent_dirs(dstack, path, dir_cb)
            dir_cb(path)
            for e in os.scandir(path):
                if e.is_dir() and e.name in d.subdirs:
                    self._process(d.subdirs[e.name], dstack, e.path, include_id, exclude_id, file_cb, dir_cb)
                elif e.is_dir():
                    dir_cb(e.path)
                    self._process_simple_dir(e.path, file_cb, dir_cb)
                else:
                    file_cb(e.path)
        else:
            dstack.append((d, False))
            for sname, sd in d.subdirs.items():
                self._process(sd, dstack, os.path.join(path, sname), include_id, exclude_id, file_cb, dir_cb)
            dstack.pop()

    def process(self, root_path, file_cb, dir_cb):
        for name, d in self.dirs.items():
            self._process(d, [], os.path.join(root_path, name), -1, -1, file_cb, dir_cb)
