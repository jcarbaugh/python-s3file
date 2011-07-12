from distutils.core import setup
from s3file import __version__

long_description = open('README.rst').read()

setup(
    name="python-s3file",
    version=__version__,
    description="Read and write to Amazon S3 using a file-like object",
    author="Jeremy Carbaugh",
    author_email = "jcarbaugh@gmail.com",
    license='BSD',
    url="http://github.com/jcarbaugh/python-s3file/",
    long_description=long_description,
    py_modules=["s3file"],
    install_requires=['boto'],
    platforms=["any"],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)