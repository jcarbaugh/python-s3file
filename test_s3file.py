import io
import os
import sys
import uuid

import pytest
from boto.s3.connection import S3Connection
from boto.s3.key import Key

from s3file import s3open

PY3 = sys.version_info[0] == 3
ENC = 'utf-8'

LOREM = "Lorem ipsum dolor sit amet, consectetur adipiscing elit."
LOREM_LINES = (LOREM + '\n') * 10
BOREM = bytes(LOREM, encoding=ENC) if PY3 else LOREM


# utility methods

def get_url(bucket, path):
    url_fmt = "http://%s.s3.amazonaws.com/%s"
    return url_fmt % (bucket.name, path.lstrip("/"))


# fixtures

@pytest.fixture(scope="module")
def bucket():
    conn = S3Connection()
    session_id = uuid.uuid4().hex[:8]
    bucket = conn.create_bucket("s3file_{}".format(session_id))
    yield bucket
    for key in bucket:
        key.delete()
    bucket.delete()


# the tests

def test_keys():
    aws_id = os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret = os.environ.get('AWS_SECRET_ACCESS_KEY')
    conn = S3Connection(aws_id, aws_secret)
    buckets = conn.get_all_buckets()
    assert buckets is not None


def test_context_manager(bucket):
    path = 'test_write_cm.txt'
    with s3open(get_url(bucket, path)) as f:
        f.write(LOREM)
    k = Key(bucket, path)
    assert k.get_contents_as_string(encoding=ENC) == LOREM


def test_write(bucket):
    path = 'test_write.txt'
    with s3open(get_url(bucket, path)) as f:
        f.write(LOREM)
    k = Key(bucket, path)
    assert k.get_contents_as_string(encoding=ENC) == LOREM


def test_write_binary(bucket):
    path = 'test_write.bin'
    with s3open(get_url(bucket, path), 'b') as f:
        f.write(BOREM)
    k = Key(bucket, path)
    assert k.get_contents_as_string() == BOREM


def test_write_binary_large(bucket):
    path = 'test_large_binary_write.bin'
    bs = bytes(BOREM)
    for i in range(0, 10):
        bs += bs
    with s3open(get_url(bucket, path), 'b') as f:
        f.write(bs)
    k = Key(bucket, path)
    assert k.get_contents_as_string() == bs


def test_read(bucket):
    path = 'test_read.txt'
    k = Key(bucket, path)
    k.set_contents_from_string(LOREM)
    with s3open(get_url(bucket, path)) as f:
        assert f.read() == LOREM


def test_read_binary(bucket):
    path = 'test_read.txt'
    k = Key(bucket, path)
    k.set_contents_from_string(BOREM)
    with s3open(get_url(bucket, path), 'b') as f:
        assert f.read() == BOREM


def test_read_binary_large(bucket):
    path = 'test_large_binary_read.bin'
    bs = bytes(BOREM)
    for i in range(0, 10):
        bs += bs
    k = Key(bucket, path)
    k.set_contents_from_string(bs)
    with s3open(get_url(bucket, path), 'b') as f:
        assert f.read() == bs


def test_tell(bucket):
    url = get_url(bucket, 'test_tell.txt')
    with s3open(url) as f:
        f.write(LOREM)
    with s3open(url) as f:
        assert f.read(8) == LOREM[:8]
        assert f.tell() == 8


def test_readlines(bucket):
    path = 'test_readlines.txt'
    url = get_url(bucket, path)
    lines = LOREM_LINES
    res = "".join(lines)
    k = Key(bucket, path)
    k.set_contents_from_string(res)
    with s3open(url) as f:
        rlines = f.readlines()
        rres = "".join(rlines)
    assert res == rres


def test_readlines_iter(bucket):
    path = 'test_readlines_iter.txt'
    url = get_url(bucket, path)
    k = Key(bucket, path)
    k.set_contents_from_string("".join(LOREM_LINES))
    with s3open(url) as f:
        for i, l in enumerate(f):
            assert LOREM_LINES[i] == l


def test_writelines(bucket):
    path = "test_writelines.txt"
    url = get_url(bucket, path)
    with s3open(url) as f:
        lines = LOREM_LINES
        res = "".join(lines)
        f.writelines(lines)
    k = Key(bucket, path)
    assert k.get_contents_as_string(encoding=ENC) == res


def test_readline(bucket):
    path = 'test_readline.txt'
    url = get_url(bucket, path)
    lines = LOREM_LINES
    res = "".join(lines)
    k = Key(bucket, path)
    k.set_contents_from_string(res)
    with s3open(url) as f:
        rline = f.readline()
    assert rline == LOREM + '\n'


def test_closed(bucket):
    url = get_url(bucket, 'test_closed.txt')
    with s3open(url) as f:
        assert not f.closed
    assert f.closed


def test_name(bucket):
    path = 'test_name.txt'
    url = get_url(bucket, path)
    with s3open(url) as f:
        assert "s3://" + bucket.name + "/" + path == f.name


def test_flush(bucket):
    path = 'test_flush.txt'
    url = get_url(bucket, path)
    fl = LOREM + "\n" + LOREM + "\n"
    fl2 = fl + fl
    with s3open(url) as f:
        f.write(fl)
        f.flush()
        k = Key(bucket, path)
        assert k.get_contents_as_string(encoding=ENC) == fl
        f.write(fl)
    assert k.get_contents_as_string(encoding=ENC) == fl2


def test_seek(bucket):
    path = 'test_seek.txt'
    url = get_url(bucket, path)

    lines = LOREM_LINES
    res = "".join(lines)

    res = LOREM * 10

    k = Key(bucket, path)
    k.set_contents_from_string(res)

    with s3open(url) as f:
        f.seek(0, io.SEEK_SET)
        assert f.read(1) == res[0]
        f.seek(1)
        assert f.read(8) == res[1:9]
        f.seek(2, io.SEEK_SET)
        assert f.read(8) == res[2:10]
        f.seek(-1, io.SEEK_CUR)
        assert f.read(9) == res[9:18]
        f.seek(-10, io.SEEK_END)
        assert f.read(10) == res[-10:]


def test_truncate(bucket):
    path = 'test_truncate.txt'
    url = get_url(bucket, path)
    lines = LOREM_LINES
    res = "".join(lines)
    k = Key(bucket, path)
    k.set_contents_from_string(res)
    with s3open(url) as f:
        # not convinced we should do this but down to the trucate in the write
        f.read()
        f.truncate(3)
    with s3open(url) as f:
        assert f.read() == res[:3]
        f.seek(1, 0)
        f.truncate()
    with s3open(url) as f:
        assert f.read() == res[:1]


def test_xreadlines(bucket):
    with pytest.raises(NotImplementedError):
        url = get_url(bucket, 'test_xreadlines.txt')
        with s3open(url) as f:
            f.xreadlines()
