import os
from os.path import abspath, dirname, join
from typing import List, Tuple

from setuptools import setup

from botleague_helpers import VERSION


def get_requires() -> Tuple[List[str], List[str]]:
    with open('requirements.txt') as reqs_file:
        reqs = reqs_file.read().split(os.linesep)
    dep_links = []
    final_reqs = []
    for req in reqs:
        if not req.startswith('git+git://github.com/'):
            final_reqs.append(req)
        else:
            req = req.replace('git+git', 'https')
            url, egg = req.split('#egg=')
            dep_link = url + '/tarball/master'
            if egg:
                dep_link += '#egg=' + egg
            dep_links.append(dep_link)
    return final_reqs, dep_links


# Read the README markdown data from README.md
with open(abspath(join(dirname(__file__), 'README.md')), 'rb') as readme_file:
    __readme__ = readme_file.read().decode('utf-8')

requires, dependency_links = get_requires()

setup(
    name='botleague-helpers',
    version=VERSION,
    description='Helpers for BotLeague',
    long_description=__readme__,
    long_description_content_type='text/markdown',
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.7',
        'Environment :: Console'
    ],
    keywords='botleague',
    url='http://github.com/deepdrive/botleague-helpers',
    author='Craig Quiter',
    author_email='craig@deepdrive.io',
    license='MIT',
    packages=['botleague_helpers'],
    zip_safe=True,
    python_requires='>=3.7',
    install_requires=requires,
    dependency_links=dependency_links,
)
