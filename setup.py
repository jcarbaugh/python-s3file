from distutils.core import setup
from s3file import __version__

long_description = open('README.rst').read()

setup(name="python-s3file",
      version=__version__,
      py_modules=["s3file"],
      description="Read and write to S3 by opening a URL",
      author="Jeremy Carbaugh",
      author_email = "jcarbaugh@gmail.com",
      license='BSD',
      url="http://github.com/jcarbaugh/python-s3file/",
      long_description=long_description,
      platforms=["any"],
      classifiers=["Development Status :: 4 - Beta",
                   "Intended Audience :: Developers",
                   "License :: OSI Approved :: BSD License",
                   "Natural Language :: English",
                   "Operating System :: OS Independent",
                   "Programming Language :: Python",
                   "Topic :: Software Development :: Libraries :: Python Modules",
                   ],
      )