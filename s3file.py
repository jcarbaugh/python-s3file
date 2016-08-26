import mimetypes
import os
import datetime
import sys
from io import BytesIO, StringIO

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

PY3 = sys.version_info[0] == 3

TEXT_MODE = 't'
BINARY_MODE = 'b'

__version__ = '2.0'


def s3open(*args, **kwargs):
    """ Convenience method for creating S3File object.
    """
    return S3File(*args, **kwargs)


class S3File(object):

    def __init__(self, url, mode='', key=None, secret=None,
                 expiration_days=0, private=False,
                 content_type=None, encoding='utf-8', create=True):

        from boto.s3.connection import S3Connection
        from boto.s3.key import Key

        self.mode = BINARY_MODE if 'b' in mode else TEXT_MODE
        self.encoding = encoding

        self.url = urlparse(url)
        self.expiration_days = expiration_days
        self.buffer = BytesIO()

        self.private = private
        self.closed = False
        self._readreq = True
        self._writereq = False
        self.content_type = \
            content_type or mimetypes.guess_type(self.url.path)[0]

        bucket = self.url.netloc
        if bucket.endswith('.s3.amazonaws.com'):
            bucket = bucket[:-17]

        self.client = S3Connection(key, secret)

        self.name = "s3://" + bucket + self.url.path

        if create:
            self.bucket = self.client.create_bucket(bucket)
        else:
            self.bucket = self.client.get_bucket(bucket, validate=False)

        self.key = Key(self.bucket)
        self.key.key = self.url.path.lstrip("/")
        self.buffer.truncate(0)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def __iter__(self):
        for l in self.buffer.readlines():
            yield l

    def _coerce_bytes(self, x):
        if PY3:
            return bytes(x, self.encoding) if isinstance(x, str) else x
        else:
            return bytes(x).encode(self.encoding) \
                if isinstance(x, basestring) else x  # noqa

    def _coerce_mode(self, x):
        if self.mode == TEXT_MODE and isinstance(x, bytes):
            return x.decode(self.encoding)
        # elif self.mode == BINARY_MODE and isinstance(x, strtype):
        #     return bytes(x, self.encoding)
        return x

    def _remote_read(self):
        """ Read S3 contents into internal file buffer.
            Once only
        """
        if self._readreq:
            self.buffer.truncate(0)
            if self.key.exists():
                params = {'encoding': self.encoding} \
                    if self.mode == TEXT_MODE else {}
                data = self.key.get_contents_as_string(**params)
                print(type(self.buffer))
                print(type(self.buffer.write))
                print(type(self._coerce_bytes(data)))
                self.buffer.write(self._coerce_bytes(data))
            self.buffer.seek(0)
            self._readreq = False

    def _remote_write(self):
        """ Write file contents to S3 from internal buffer.
        """
        if self._writereq:
            self.truncate(self.tell())

            headers = {
                "x-amz-acl":  "private" if self.private else "public-read"
            }

            if self.content_type:
                headers["Content-Type"] = self.content_type

            if self.expiration_days:
                now = datetime.datetime.utcnow()
                then = now + datetime.timedelta(self.expiration_days)
                headers["Expires"] = then.strftime("%a, %d %b %Y %H:%M:%S GMT")
                headers["Cache-Control"] = \
                    'max-age=%d' % (self.expiration_days * 24 * 3600,)

            self.key.set_contents_from_file(
                self.buffer, headers=headers, rewind=True)

    def close(self):
        """ Close the file and write contents to S3.
        """
        self._remote_write()
        self.buffer.close()
        self.closed = True

    # pass-through methods

    def flush(self):
        self._remote_write()

    def next(self):
        self._remote_read()
        return self.buffer.next()

    def read(self, size=-1):
        self._remote_read()
        return self._coerce_mode(self.buffer.read(size))

    def readline(self, size=-1):
        self._remote_read()
        return self._coerce_mode(self.buffer.readline(size))

    def readlines(self, sizehint=-1):
        self._remote_read()
        return [self._coerce_mode(l) for l in self.buffer.readlines(sizehint)]

    def xreadlines(self):
        raise NotImplementedError(
            'xreadlines has been deprecated since Python 2.3'
            'and is no longer supported by s3file')

    def seek(self, offset, whence=os.SEEK_SET):
        self.buffer.seek(offset, whence)
        # if it looks like we are moving in the file and we have not written
        # anything then we probably should read the contents
        if self.tell() != 0 and self._readreq and not self._writereq:
                self._remote_read()
                self.buffer.seek(offset, whence)

    def tell(self):
        return self.buffer.tell()

    def truncate(self, size=None):
        self._writereq = True
        self.buffer.truncate(size or self.tell())

    def write(self, s):
        self._writereq = True
        self.buffer.write(self._coerce_bytes(s))

    def writelines(self, sequence):
        self._writereq = True
        self.buffer.writelines(self._coerce_bytes(l) for l in sequence)
