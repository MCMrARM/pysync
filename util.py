import hashlib

def copy_file_limited(src, dest, limit):
    if limit == 0:
        return
    b  = bytearray(128 * 1024)
    mv = memoryview(b)
    while True:
        if len(mv) > limit:
            mv = mv[:limit]
        n = src.readinto(mv)
        if not n:
            break
        if n > limit:
            n = limit
        if n < len(b):
            dest.write(mv[:n])
        else:
            dest.write(mv)
        limit -= n
        if limit == 0:
            return

def sha256_file(file):
    h  = hashlib.sha256()
    b  = bytearray(128 * 1024)
    mv = memoryview(b)
    while True:
        n = file.readinto(mv)
        if not n:
            break
        if n < len(b):
            h.update(mv[:n])
        else:
            h.update(mv)
    return h.hexdigest()