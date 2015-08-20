# -*- coding: ISO-8859-15 -*-
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

"""This module contains all classes and functions core to WOUDC"""

import os
import csv
import re
import logging
from StringIO import StringIO
from woudc import errors
#from woudc.util import update_report

LOGGER = logging.getLogger(__name__)

class WOUDCextCSVReader(object):
    """Objectifies incoming extended CSV file"""

    def __init__(self, file_contents, report=None, ipath=None):
        """
        Read WOUDC extCSV file and objectify
        """
        self.file_path = ipath
        self.sections = {}
        self.metadata_tables = []
        self.data_tables = []
        self.all_tables = []
        self.comments = {}
        self.updated = False
        self.mtime = None
        self.error = []
        # get file modified time
        try:
            self.mtime = os.path.getmtime(self.file_path)
        except Exception, err:
            msg = str(err)
            LOGGER.error('Unable to get modified time of file: %s. Due to: %s',
                self.file_path, msg)
        LOGGER.info('Processing file: %s', self.file_path)
        blocks = re.split('#', file_contents)
        if len(blocks) == 0:
            msg = 'File located at: %s is malformed.' % self.file_path
            LOGGER.error(msg)
            raise errors.BPSMalformedExtCSVError(msg)
        # get rid of first element of cruft
        head_comment = blocks.pop(0)
        c = StringIO(head_comment.strip())
        for line in c:
            if all([
                line.strip() != '',
                line.strip() != os.linesep,
                line[0] != '*'
                ]):
                self.errors.append(9)
        self.table_count = {}
        for b in blocks:
            # determine delimiter
            if '::' in b:
                b.replace('::', ',')
                self.errors.append(5)
            if ';' in b:
                b.replace(';', ',')
                self.errors.append(5)
            if '$' in b:
                b.replace('$', ',')
                self.errors.append(5)
            if '%' in b:
                b.replace('%', ',')
                self.errors.append(5)
            if any([
                '|' in b,
                '/' in b,
				'\\' in b
                ]):
                msg = 'Invalid delimiter detected in file: %s' % self.file_path
                LOGGER.error(msg)
                if report is not None:
                    self.errors.append(6)
                    #continue
            try:
                s = StringIO(b.strip())
                c = csv.reader(s)
                header = (c.next()[0]).strip()
            except Exception, err:
                msg = 'File located at: %s is malformed.' % self.file_path
                LOGGER.error(msg)
                raise errors.BPSMalformedExtCSVError(msg)
            if header not in [
                'PROFILE',
                'DAILY',
                'GLOBAL',
                'DIFFUSE',
                #'MONTHLY',
                'OZONE_PROFILE',
                'N14_VALUES',
                'C_PROFILE',
                'OBSERVATIONS',
                'PUMP_CORRECTION',
                'SIMULTANEOUS',
                'DAILY_SUMMARY',
                'DAILY_SUMMARY_NSF',
                'SAOZ_DATA_V2',
                'GLOBAL_DAILY_TOTALS'
                ]: # metadata
                if header not in self.sections:
                    self.sections[header] = {}
                    self.metadata_tables.append(header)
                    self.table_count[header] = 1
                    self.all_tables.append(header)
                else:
                    self.table_count[header] = self.table_count[header] + 1
                    header = '%s%s' % (header, self.table_count[header])
                    self.sections[header] = {}
                    self.metadata_tables.append(header)
                self.sections[header]['_raw'] = b.strip()
                try:
                    fields = c.next()
                    # archive fix start
                    fields = filter(lambda a: a != '', fields)
                    # archive fix end
                    #if any('*' in s for s in fields):
                    if len(fields[0]) > 0:
                        if fields[0][0] == '*':
                            self.errors.append(8)
                except StopIteration:
                    msg = 'Extended CSV table %s has no fields' % header
                    LOGGER.info(msg)
                    if report is not None:
                        self.errors.append({140:{'TABLE_NAME': header}})
                        self.errors.append(140)
                values = None
                try:
                    values = c.next()
                    if len(values[0]) > 0:
                        if values[0][0] == '*':
                            self.errors.append(8)
                except StopIteration:
                    msg = 'Extended CSV table %s has no values' % header
                    LOGGER.info(msg)
                    if report is not None:
                        self.errors.append({140:{'TABLE_NAME': header}})
                    continue
                try:
                    anything_more = (c.next()[0]).strip()
                    if all([
                        anything_more is not None,
                        anything_more != '',
                        anything_more != os.linesep,
                        '*' not in anything_more,
                        ]):
                        if report is not None:
                            self.errors(3)
                            self.errors.append({140:{'TABLE_NAME': header}})
                except Exception, err:
                    pass
                if len(values) > len(fields):
                    self.errors(3)
                    continue
                i = 0
                for field in fields:
                    field = field.strip()
                    try:
                        self.sections[header][field] = (values[i]).strip()
                        i += 1
                    except (KeyError, IndexError):
                        self.sections[header][field] = None
                        msg = 'Corrupt format. Section: %s. Skipping.' % header
                        LOGGER.debug(msg)
            else:  # payload
                buf = StringIO(None)
                w = csv.writer(buf)
                columns = None
                for row in c:
                    if columns is None:
                        columns = row
                    if all([
                        row != '',
                        row is not None,
                        row != [],
                    ]):
                        if '*' not in row[0]:
                            w.writerow(row)
                        else:
                            if columns[0].lower() == 'time':
                                self.errors(21)
                if header not in self.sections:
                    #self.sections[header] = {'_raw': buf.getvalue()}
                    self.all_tables.append(header)
                    self.data_tables.append(header)
                    self.table_count[header] = 1
                else:
                    self.table_count[header] = self.table_count[header] + 1
                    header = '%s%s' % (header, self.table_count[header])
                    self.sections[header] = {}
                    self.data_tables.append(header)
                self.sections[header] = {'_raw': buf.getvalue()}    
                # self.sections[header] = {
                    # '_raw': self.sections[header]['_raw'] +
                    # buf.getvalue()[80:]
                    # }
        # objectify comments found in file
        # preserve order of occurance
        hash_detected = False
        table = None
        comments_list = []
        table_count = {}
        if ipath is not None:
            for line in open(ipath):
                if '#' in line:  # table detected
                    if hash_detected is False:
                        self.comments['header_comments'] = comments_list
                        comments_list = []
                        table = line[1:].strip()
                        if table in table_count.keys():
                            table_count[table] = table_count[table] + 1
                            table = '%s_%s' % (table, table_count[table])
                        else:
                            table_count[table] = 1
                        hash_detected = True
                        continue
                    self.comments[table] = comments_list
                    table = line[1:].strip()
                    if table in table_count.keys():
                        table_count[table] = table_count[table] + 1
                        table = '%s_%s' % (table, table_count[table])
                    else:
                        table_count[table] = 1
                    comments_list = []
                    continue
                # comments are preseced by '*' in column 0 of each line
                if line[0] == '*':  # comment detected,
                    comments_list.append(line.strip('\n'))
            self.comments[table] = comments_list

            
    def __eq__(self, other): 
        """equals method"""
        return self.__dict__ == other.__dict__
