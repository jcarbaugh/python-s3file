from boto.s3.connection import S3Connection
from boto.s3.key import Key
from optparse import OptionParser
from s3file import s3open
import random
import unittest

LOREM = "Lorem ipsum dolor sit amet, consectetur adipiscing elit."


class TestS3File(unittest.TestCase):

    def __init__(self, testname, key=None, secret=None):
        super(TestS3File, self).__init__(testname)
        self.key = key
        self.secret = secret

    def setUp(self):
        conn = S3Connection(self.key, self.secret)
        session_id = "%06d" % random.randint(0, 999999)
        session_key = self.key.lower() if self.key else session_id
        self.bucket = conn.create_bucket("s3file_%s_%s" % (session_id, session_key))

    def get_url(self, path):
        url_fmt = "http://%s.s3.amazonaws.com/%s"
        return url_fmt % (self.bucket.name, path.lstrip("/"))

    def test_context_manager(self):
        path = 'test_write_cm.txt'

        with s3open(self.get_url(path), key=self.key, secret=self.secret) as f:
            f.write(LOREM)

        k = Key(self.bucket, path)
        self.assertEqual(k.get_contents_as_string(), LOREM)

    def test_write(self):
        path = 'test_write.txt'

        # write using s3file
        f = s3open(self.get_url(path), key=self.key, secret=self.secret)
        f.write(LOREM)
        f.close()

        # check contents using boto
        k = Key(self.bucket, path)
        self.assertEqual(k.get_contents_as_string(), LOREM)

    def test_read(self):
        path = 'test_read.txt'

        # write using boto
        k = Key(self.bucket, path)
        k.set_contents_from_string(LOREM)

        # check contents using s3file
        f = s3open(self.get_url(path), key=self.key, secret=self.secret)
        self.assertEqual(f.read(), LOREM)
        f.close()

    def test_tell(self):
        url = self.get_url('test_tell.txt')
        f = s3open(url, key=self.key, secret=self.secret)
        f.write(LOREM)
        f.close()

        f = s3open(url, key=self.key, secret=self.secret)
        self.assertEqual(f.read(8), LOREM[:8])
        self.assertEqual(f.tell(), 8)

    def lorem_est(self):
	lor = LOREM + "\n"
	lines = [lor, lor[1:]+lor[:1], lor[2:]+lor[:2], lor[3:]+lor[:3],
		lor[4:]+lor[:4], lor[5:]+lor[:5], lor[6:]+lor[:6]]
	return lines

    def test_readlines(self):
	path = 'test_readlines.txt'
	url = self.get_url(path)
	lines = self.lorem_est()
	res = "".join(lines)
        k = Key(self.bucket, path)
        k.set_contents_from_string(res)
        f = s3open(url, key=self.key, secret=self.secret)
	rlines = f.readlines()
	rres = "".join(rlines)
	f.close()
        self.assertEqual(res, rres)

    def test_writelines(self):
	path = "test_writelines.txt"
	url = self.get_url(path)
        f = s3open(url, key=self.key, secret=self.secret)
	lines = self.lorem_est()
	res = "".join(lines)
	f.writelines(lines)
	f.close()
        k = Key(self.bucket, path)
        self.assertEqual(k.get_contents_as_string(), res)

    def test_readline(self):
	path = 'test_readline.txt'
	url = self.get_url(path)
	lines = self.lorem_est()
	res = "".join(lines)
        k = Key(self.bucket, path)
        k.set_contents_from_string(res)
        f = s3open(url, key=self.key, secret=self.secret)
	rline = f.readline()
	f.close()
        self.assertEqual(rline, LOREM + '\n')

    def test_closed(self):
	path = 'test_closed.txt'
	url = self.get_url(path)
        f = s3open(url, key=self.key, secret=self.secret)
	self.assertEqual(False, f.closed)
	f.close()
	self.assertEqual(True, f.closed)

    def test_name(self):
	path = 'test_name.txt'
	url = self.get_url(path)
        f = s3open(url, key=self.key, secret=self.secret)
	self.assertEqual("s3://" + self.bucket.name + "/" + path, f.name)
	f.close()

    def test_flush(self):
	path = 'test_flush.txt'
	url = self.get_url(path)
	fl = LOREM + "\n" + LOREM + "\n"
	fl2 = fl + fl
        f = s3open(url, key=self.key, secret=self.secret)
	f.write(fl)
	f.flush()
        k = Key(self.bucket, path)
        self.assertEqual(k.get_contents_as_string(), fl)
	f.write(fl)
	f.close()
        self.assertEqual(k.get_contents_as_string(), fl2)

    def test_xreadlines(self):
	path = 'test_xreadlines.txt'
	url = self.get_url(path)
	lines = self.lorem_est()
	res = "".join(lines)
        k = Key(self.bucket, path)
        k.set_contents_from_string(res)
        f = s3open(url, key=self.key, secret=self.secret)
	rres = ""
	for lin in f.xreadlines():
	    rres += lin
	f.close()
        self.assertEqual(res, rres)

    def test_seek(self):
	# needs start, relative, end
	path = 'test_seek.txt'
	url = self.get_url(path)
	lines = self.lorem_est()
	res = "".join(lines)
        k = Key(self.bucket, path)
        k.set_contents_from_string(res)
        f = s3open(url, key=self.key, secret=self.secret)
	f.seek(2,0)
	self.assertEqual(f.read(8), res[2:10])
	f.seek(1)
	self.assertEqual(f.read(8), res[1:9])
	f.seek(-1,1)
	self.assertEqual(f.read(9), res[8:17])
	f.seek(-10,2)
	self.assertEqual(f.read(10), res[-10:])
	f.close()

    def test_truncate(self):
	path = 'test_truncate.txt'
	url = self.get_url(path)
	lines = self.lorem_est()
	res = "".join(lines)
        k = Key(self.bucket, path)
        k.set_contents_from_string(res)
        f = s3open(url, key=self.key, secret=self.secret)
	dummy = f.read() # not convinced we should do this but down to the trucate in the write
	f.truncate(3)
	f.close()

        t = s3open(url, key=self.key, secret=self.secret)
	self.assertEqual(t.read(), res[:3])
	t.seek(1,0)
	t.truncate()
	t.close()

        f = s3open(url, key=self.key, secret=self.secret)
	self.assertEqual(f.read(), res[:1])
	f.close()

    def _bin_str(self):
	bs = ""
	for i in xrange(0,256):
		bs += chr(i)
	return bs

    def test_binary_write(self):
        path = 'test_binary_write.txt'
	bs = self._bin_str()
        f = s3open(self.get_url(path), key=self.key, secret=self.secret)
        f.write(bs)
        f.close()
        k = Key(self.bucket, path)
        self.assertEqual(k.get_contents_as_string(), bs)

    def test_large_binary_write(self):
        path = 'test_large_binary_write.txt'
	bs = self._bin_str()
	for i in  xrange(0, 10):
		bs += bs
        f = s3open(self.get_url(path), key=self.key, secret=self.secret)
        f.write(bs)
        f.close()
        k = Key(self.bucket, path)
        self.assertEqual(k.get_contents_as_string(), bs)

    def test_binary_read(self):
        path = 'test_binary_read.txt'
	bs = self._bin_str()
        k = Key(self.bucket, path)
        k.set_contents_from_string(bs)
        f = s3open(self.get_url(path), key=self.key, secret=self.secret)
        self.assertEqual(f.read(), bs)
        f.close()

    def test_large_binary_read(self):
        path = 'test_large_binary_read.txt'
	bs = self._bin_str()
	for i in  xrange(0, 10):
		bs += bs
        k = Key(self.bucket, path)
        k.set_contents_from_string(bs)
        f = s3open(self.get_url(path), key=self.key, secret=self.secret)
        self.assertEqual(f.read(), bs)
        f.close()

    def tearDown(self):
        for key in self.bucket:
            key.delete()
        self.bucket.delete()


if __name__ == '__main__':

    op = OptionParser()
    op.add_option("-k", "--key", dest="key", help="AWS key (optional if boto config exists)", metavar="KEY")
    op.add_option("-s", "--secret", dest="secret", help="AWS secret key (optional if boto config exists)", metavar="SECRET")

    (options, args) = op.parse_args()

    suite = unittest.TestSuite()
    suite.addTest(TestS3File("test_write", options.key, options.secret))
    suite.addTest(TestS3File("test_read", options.key, options.secret))
    suite.addTest(TestS3File("test_tell", options.key, options.secret))
    suite.addTest(TestS3File("test_context_manager", options.key, options.secret))
    suite.addTest(TestS3File("test_readlines", options.key, options.secret))
    suite.addTest(TestS3File("test_writelines", options.key, options.secret))
    suite.addTest(TestS3File("test_readline", options.key, options.secret))
    suite.addTest(TestS3File("test_closed", options.key, options.secret))
    suite.addTest(TestS3File("test_name", options.key, options.secret))
    suite.addTest(TestS3File("test_flush", options.key, options.secret))
    suite.addTest(TestS3File("test_xreadlines", options.key, options.secret))
    suite.addTest(TestS3File("test_seek", options.key, options.secret))
    suite.addTest(TestS3File("test_truncate", options.key, options.secret))
    suite.addTest(TestS3File("test_binary_write", options.key, options.secret))
    suite.addTest(TestS3File("test_large_binary_write", options.key, options.secret))
    suite.addTest(TestS3File("test_binary_read", options.key, options.secret))
    suite.addTest(TestS3File("test_large_binary_read", options.key, options.secret))

    unittest.TextTestRunner().run(suite)
