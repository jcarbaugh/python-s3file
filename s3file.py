from urlparse import urlparse
import cStringIO
import mimetypes
import os
import datetime

__version__ = '1.0'


def s3open(*args, **kwargs):
    """ Convenience method for creating S3File object.
    """
    return S3File(*args, **kwargs)


class S3File(object):

    def __init__(self, url, key=None, secret=None, expiration_days=0, private=False, content_type=None):
        from boto.s3.connection import S3Connection
        from boto.s3.key import Key

        self.url = urlparse(url)
        self.expiration_days = expiration_days
        self.buffer = cStringIO.StringIO()
        self.private = private
        self.content_type = content_type or mimetypes.guess_type(self.url.path)[0]

        bucket = self.url.netloc
        if bucket.endswith('.s3.amazonaws.com'):
            bucket = bucket[:-17]

        self.client = S3Connection(key, secret)
        self.bucket = self.client.create_bucket(bucket)
        self.key = Key(self.bucket)
        self.key.key = self.url.path.lstrip("/")

        self._remote_read()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def _remote_read(self):
        """ Read S3 contents into internal file buffer.
        """
        self.buffer.truncate(0)
        if self.key.exists():
            self.key.get_contents_to_file(self.buffer)
        self.buffer.seek(0)

    def _remote_write(self):
        """ Write file contents to S3 from internal buffer.
        """
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
            headers["Cache-Control"] = 'max-age=%d' % (self.expiration_days * 24 * 3600),

        self.key.set_contents_from_file(self.buffer, headers=headers)

    def close(self):
        """ Close the file and write contents to S3.
        """
        value = self.buffer.getvalue()
        self._remote_write()
        self.buffer.close()

    # pass-through methods

    def flush(self):
        self._remote_write()

    def next(self):
        return self.buffer.next()

    def read(self, size=-1):
        return self.buffer.read(size)

    def readline(self, size=-1):
        return self.buffer.readline(size)

    def readlines(self, sizehint=-1):
        return self.buffer.readlines(sizehint)

    def xreadlines(self):
        return self.buffer.xreadlines()

    def seek(self, offset, whence=os.SEEK_SET):
        self.buffer.seek(offset, whence)

    def tell(self):
        return self.buffer.tell()

    def truncate(self, size=None):
        self.buffer.truncate(size or self.tell())

    def write(self, s):
        self.buffer.write(s)

    def writelines(self, sequence):
        self.buffer.writelines(sequence)
