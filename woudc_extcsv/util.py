# =================================================================
#
# Terms and Conditions of Use
#
# Unless otherwise noted, computer program source code of this
# distribution # is covered under Crown Copyright, Government of
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
# Copyright (c) 2022 Government of Canada
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

import logging

LOGGER = logging.getLogger(__name__)


def non_content_line(line):
    """
    Returns True iff <line> represents a non-content line of an Extended CSV
    file, i.e. a blank line or a comment.

    :param line: List of comma-separated components in an input line.
    :returns: `bool` of whether the line contains no data.
    """

    if len(line) == 0:
        return True
    elif len(line) == 1:
        first = line[0].strip()
        return len(first) == 0 or first.startswith('*')
    else:
        return line[0].strip().startswith('*')


def parse_integer_range(bounds_string):
    """
    Returns an integer lower bound and upper bound of the range defined within
    <bounds_string>. Formats accepted include 'n', 'n+', and 'm-n'.

    :param bounds_string: String representing a range of integers
    :return: Pair of integer lower bound and upper bound on the range
    """

    if bounds_string.endswith('+'):
        lower_bound = int(bounds_string[:-1])
        upper_bound = float('inf')
    elif bounds_string.count('-') == 1:
        lower_bound, upper_bound = map(int, bounds_string.split('-'))
    else:
        lower_bound = int(bounds_string)
        upper_bound = lower_bound

    return (lower_bound, upper_bound)


def _table_index(table, index):
    """
    Helper function to return table index.
    """

    sep = '_'
    if index > 1:
        return '{}{}{}'.format(table, sep, str(index))
    else:
        return table
