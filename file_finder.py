import os
import re

class FileFinderDir:
    def __init__(self, parent = None):
        self.parent = parent
        self.subdirs = {}
        self.include = -1
        self.exclude = -1
        self.wildcards = []

class FileFinderWildcard:
    def __init__(self, regex, include, index):
        self.regex = re.compile(regex)
        self.include = -1
        self.exclude = -1
        if include:
            self.include = index
        else:
            self.exclude = index


class FileFinder:
    def __init__(self):
        self.root_dir = FileFinderDir()
        self.next_rule_id = 0

    def get_dir(self, path):
        path = path.split('/')
        path = list(filter(None, path))
        d = self.root_dir
        for el in path:
            if el not in d.subdirs:
                d.subdirs[el] = FileFinderDir()
            d = d.subdirs[el]
        return d

    def build_wildcard_regex(self, wildcard):
        i = 0
        regex = "^"
        while True:
            j = wildcard.find('*', i)
            if j == -1:
                break
            regex += re.escape(wildcard[i:j])
            if j + 1 < len(wildcard) and wildcard[j + 1] == '*':
                # match anything
                regex += '.*'
                i = j + 2
            else:
                # match within the dir
                regex += '[^\/]*'
                i = j + 1
        regex += re.escape(wildcard[i:])
        regex += "($|\/)"
        return regex

    def add_filter(self, root_path, what, include = True):
        first_wildcard = what.find("*")
        shared_part = what
        wildcard_part = None
        if first_wildcard != -1:
            shared_part_end = what.rfind("/", 0, first_wildcard) + 1
            shared_part = what[:shared_part_end]
            wildcard_part = what[shared_part_end:]
        shared_part = os.path.realpath(os.path.join(root_path, shared_part))
        shared_part_dir = self.get_dir(shared_part)
        if wildcard_part is not None and len(wildcard_part) > 0:
            wildcard_regex = self.build_wildcard_regex(wildcard_part)
            shared_part_dir.wildcards.append(FileFinderWildcard(wildcard_regex, include, self.next_rule_id))
        elif include:
            shared_part_dir.include = self.next_rule_id
        else:
            shared_part_dir.exclude = self.next_rule_id
        self.next_rule_id += 1

    def add_from_text(self, root_path, lines):
        for line in lines:
            line = line.rstrip('\n')
            if line[:2] == "+ ":
                self.add_filter(root_path, line[2:], True)
            if line[:2] == "- ":
                self.add_filter(root_path, line[2:], False)

    @staticmethod
    def _process_simple_dir(root_path, path, wildcards, include_id, exclude_id, file_cb, dir_cb):
        dir_cb(path)
        for e in os.scandir(os.path.join(root_path, path)):
            e_path = os.path.join(path, e.name)
            e_include_id, e_exclude_id = FileFinder._process_wildcards(e_path, wildcards, include_id, exclude_id)
            if e_include_id <= e_exclude_id:
                continue

            if not e.is_symlink() and e.is_dir():
                FileFinder._process_simple_dir(root_path, e_path, wildcards, include_id, exclude_id, file_cb, dir_cb)
            else:
                file_cb(e_path)

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

    @staticmethod
    def _process_wildcards(dpath, wildcards, include_id, exclude_id):
        for (wlen, w) in wildcards:
            if max(w.include, w.exclude) < max(include_id, exclude_id):
                continue
            if w.regex.match(dpath[wlen:]):
                include_id = max(include_id, w.include)
                exclude_id = max(exclude_id, w.exclude)
        return include_id, exclude_id

    def _process(self, d, dstack, root_path, dpath, include_id, exclude_id, wildcards, file_cb, dir_cb):
        full_path = os.path.join(root_path, dpath)
        if not os.path.exists(full_path) or os.path.islink(full_path):
            return
        include_id = max(include_id, d.include)
        exclude_id = max(exclude_id, d.exclude)
        wildcards = wildcards + list(map(lambda w: (len(dpath) + 1, w), d.wildcards))
        include_id, exclude_id = self._process_wildcards(dpath, wildcards, include_id, exclude_id)
        if include_id > exclude_id:
            # include this dir
            self._process_parent_dirs(dstack, dpath, dir_cb)
            dir_cb(dpath)
            for e in os.scandir(full_path):
                e_path = os.path.join(dpath, e.name)
                if not e.is_symlink() and e.is_dir() and e.name in d.subdirs:
                    self._process(d.subdirs[e.name], dstack, root_path, e_path,
                                  include_id, exclude_id, wildcards, file_cb, dir_cb)
                    continue
                e_include_id, e_exclude_id = self._process_wildcards(e_path, wildcards, include_id, exclude_id)
                if e_include_id <= e_exclude_id:
                    continue

                if not e.is_symlink() and e.is_dir():
                    self._process_simple_dir(root_path, e_path, wildcards, e_include_id, e_exclude_id, file_cb, dir_cb)
                else:
                    file_cb(e_path)
        else:
            dstack.append((d, False))
            for sname, sd in d.subdirs.items():
                self._process(sd, dstack, root_path, os.path.join(dpath, sname),
                              include_id, exclude_id, wildcards, file_cb, dir_cb)
            dstack.pop()

    def process(self, root_path, file_cb, dir_cb):
        self._process(self.root_dir, [], root_path, "", -1, -1, [], file_cb, dir_cb)
