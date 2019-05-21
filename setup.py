import os
from os.path import abspath, dirname, join
from typing import List

from setuptools import setup


def get_requires() -> List[str]:
    with open('requirements.txt') as reqs_file:
        reqs = reqs_file.read().split(os.sep)
    return reqs


# Read the README markdown data from README.md
with open(abspath(join(dirname(__file__), 'README.md')), 'rb') as readme_file:
    __readme__ = readme_file.read().decode('utf-8')

setup(
    name='gcp-helpers',
    version='0.0.1',
    description='Helpers for Google Cloud Platform',
    long_description=__readme__,
    long_description_content_type='text/markdown',
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.7',
        'Environment :: Console'
    ],
    keywords='botleague gcp',
    url='http://github.com/botleague/botleague_gcp',
    author='Craig Quiter',
    author_email='craig@deepdrive.io',
    license='MIT',
    packages=['gcp_helpers'],
    zip_safe=True,
    python_requires='>=3.7',
    install_requires=get_requires()
)
