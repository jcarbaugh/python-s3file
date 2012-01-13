=============
python-s3file
=============

Read and write files to S3 using a file-like object. Refer to S3 buckets and keys using full URLs.

More tests and docs are needed.

Requirements
============

boto

Usage
=====

Basic usage::

	from s3file import s3open

	f = s3open("http://mybucket.s3.amazonaws.com/myfile.txt")
	f.write("Lorem ipsum dolor sit amet...")
	f.close()

``with`` statement::

	with s3open(path) as remote_file:
		remote_file.write("blah blah blah")

S3 authentication key and secret may be passed into the ``s3open`` method or stored in the `boto config file <http://code.google.com/p/boto/wiki/BotoConfig>`_.::

	f = s3open("http://mybucket.s3.amazonaws.com/myfile.txt", key, secret)

Other parameters to s3open include:

expiration_days
	Sets the number of days that the remote file should be cached by clients. Default is 0, not cached.

private
	If True, sets the file to be private. Defaults to False, publicly readable.

content_type
	The content_type of the file will be guessed from the URL, but you can explicitly set it by passing a content_type value.