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
    return h.digest()

def read_exactly(stream, rlen):
    ret = stream.read(rlen)
    if rlen != len(ret):
        raise EOFError()
    return ret


def encode_varint(buf, val):
    while True:
        wr = val & 0x7f
        val >>= 7
        if val:
            buf.append(wr | 0x80)
        else:
            buf.append(wr)
            return

# https://github.com/fmoo/python-varint/blob/master/varint.py
def decode_varint_stream(stream):
    shift = 0
    result = 0
    while True:
        i = ord(read_exactly(stream, 1))
        result |= (i & 0x7f) << shift
        shift += 7
        if not (i & 0x80):
            break

    return result

def decode_varint_prefixed_bytes(stream):
    rlen = decode_varint_stream(stream)
    return read_exactly(stream, rlen)

