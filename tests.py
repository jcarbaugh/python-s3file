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

    unittest.TextTestRunner().run(suite)
