# =================================================================
#
# Terms and Conditions of Use
#
# Unless otherwise noted, computer program source code of this
# distribution is covered under Crown Copyright, Government of
# Canada, and is distributed under the MIT License.
#
# The Canada wordmark and related graphics associated with this
# distribution are protected under trademark law and copyright law.
# No permission is granted to use them outside the parameters of
# the Government of Canada's corporate identity program. For
# more information, see
# http://www.tbs-sct.gc.ca/fip-pcim/index-eng.asp
#
# Copyright title to all 3rd party software distributed with this
# software is held by the respective copyright holders as noted in
# those files. Users are asked to read the 3rd Party Licenses
# referenced with those assets.
#
# Copyright (c) 2015 Government of Canada
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

import os
import io
import re
from setuptools import Command, find_packages, setup

# set dependencies
with open('requirements.txt') as ff:
    INSTALL_REQUIRES = [line.strip() for line in ff]

KEYWORDS = [
    'woudc',
    'ozone',
    'uv',
    'ultra-violet',
    'WMO',
    'Extended CSV'
]

DESCRIPTION = '''Python package providing read/write support of the
WOUDC Extended CSV format.'''

try:
    import pypandoc
    LONG_DESCRIPTION = pypandoc.convert('README.md', 'rst')
except(IOError, ImportError, OSError):
    with open('README.md') as f:
        LONG_DESCRIPTION = f.read()

CONTACT = 'Meteorological Service of Canada'

EMAIL = 'thinesh.sornalingam@canada.ca'

SCRIPTS = []

URL = 'https://github.com/woudc/woudc-extcsv'


class PyTest(Command):
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        import subprocess
        import sys
        errno = subprocess.call([sys.executable, 'tests/run_tests.py'])
        raise SystemExit(errno)


def read(filename, encoding='utf-8'):
    """read file contents"""
    full_path = os.path.join(os.path.dirname(__file__), filename)
    with io.open(full_path, encoding=encoding) as fh:
        contents = fh.read().strip()
    return contents


def get_package_version():
    """get version from top-level package init"""
    version_file = read('woudc_extcsv/__init__.py')
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError('Unable to find version string.')


setup(
    name='woudc-extcsv',
    version=get_package_version(),
    description=DESCRIPTION.strip(),
    long_description=LONG_DESCRIPTION,
    license='MIT',
    platforms='all',
    keywords=' '.join(KEYWORDS),
    author=CONTACT,
    author_email=EMAIL,
    maintainer=CONTACT,
    maintainer_email=EMAIL,
    url=URL,
    install_requires=INSTALL_REQUIRES,
    packages=find_packages('.'),
    package_data={'woudc_extcsv': ['table_configuration.csv']},
    scripts=SCRIPTS,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Scientific/Engineering :: Atmospheric Science'
    ],
    cmdclass={'test': PyTest},
    test_suite='tests.run_tests'
)
