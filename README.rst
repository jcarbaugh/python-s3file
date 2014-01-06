=============
python-s3file
=============

Read and write files to S3 using a file-like object. Refer to S3 buckets and keys using full URLs.

The underlying mechanism is a lazy read and write using ``cStringIO`` as the file emulation. This is an in memory buffer so is not suitable for large files (larger than your memory).

As S3 only supports reads and writes of the whole key, the S3 key will be read in its entirety and written on ``close``. Starting from release 1.2 this read and write are deferred until required and the key is only read from if the file is read from or written within and only updated if a write operation has been carried out on the buffer contents.


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

create
	**New in version 1.1** If False, assume bucket exists and bypass validation. Riskier, but can speed up writing. Defaults to True.
