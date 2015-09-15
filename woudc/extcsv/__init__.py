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
__version__ = '0.0.1'

import os
import re
import csv
import logging
from StringIO import StringIO
from collections import OrderedDict
from extcsv.util import validate_extcsv, serialize_extcsv
#from woudc import errors
#from woudc.util import update_report


LOGGER = logging.getLogger(__name__)

class WOUDCextCSVWriter(object):
    """Generic extended CSV writer object, used for serialization"""
    
    def __init__(self, ds=None, template=False):
        """
        Initialize a WOUDCextCSVWriter object.
        
        :param ds: an OrderedDict of WOUDC tables, fields, values and commons with which
                    your object will be instatiated.
        :param template: set True if you want the default/common WOUDC extendedCSV tables
                        and fields instantiated in your object.
        
        :returns: WOUDCextCSVWriter object
        """
        
        self._filename = None
        self._file_comments = []
        self._extcsv_ds = OrderedDict()
        # init object with requested data structure
        if ds is not None:
            for table, fields in ds.iteritems():
                t_toks = table.split('$')
                t = t_toks[0]
                t_index = t_toks[1]
                self.add_table(t, index=t_index)
                for field, values in fields.iteritems():
                    self.add_field(t, field, index=t_index)
                    for value in values:
                        self.add_data(t, value, field, index=t_index)
        # init object with common/metadata tables and fields, if template is requested
        if template:
            self.add_field('CONTENT', 'Class,Category,Level,Form')
            self.add_field('DATA_GENERATION', 'Date,Agency,Version,ScientificAuthority')
            self.add_field('PLATFORM', 'Type,ID,Name,Country,GAW_ID')
            self.add_field('INSTRUMENT', 'Name,Model,Number')
            self.add_field('LOCATION', 'Latitude,Longitude,Height')
            self.add_field('TIMESTAMP', 'UTCOffset,Date,Time')


    @property
    def filename(self):
        """
        Get filename
        
        :returns: this objects's file name
        """
        
        return self._filename
        
    @filename.setter
    def filename(self, value):
        """
        Set filename
        
        :param value: name of file
        """
        
        self._filename = value
        
    @property
    def file_comments(self):
        """
        get file comments
        """
        return self._file_comments

        
    @property
    def extcsv_ds(self):
        """
        get internal data structure.
        """
        return self._extcsv_ds
        

    def add_comment(self, comment):
        """add file level comments"""
        self.file_comments.append(comment)
        
        
    def add_table(self,table, table_comment=None, index=1):
        """
        add table
        """
        table_n = self.table_index(table, index)
        if table_n not in self.extcsv_ds.keys():
            self.extcsv_ds[table_n] = OrderedDict()
            self.extcsv_ds[table_n]['comments'] = []
            msg = 'Table: %s at index: %s added.' % (table, index)
            LOGGER.info(msg)
            if table_comment is not None:
                self.extcsv_ds[table_n]['comments'].append(table_comment)
        else:
            msg = 'Table: %s at index: %s already exists' % (table, index)
            print msg
            LOGGER.error(msg)
            

    def add_field(self, table, field, index=1, delim=','):
        """
        add field
        """
        table_n = self.table_index(table, index)
        # add table if not present
        if table_n not in self.extcsv_ds.keys():
            self.add_table(table, index=index)
        if delim not in field: # vertical insert
            if field not in self.extcsv_ds[table_n].keys():
                self.extcsv_ds[table_n][field] = []
                msg = 'Field: %s was added to Table: %s at index: %s' % (field, table, index)
                LOGGER.info(msg)
            else:
                msg = 'Field: %s already persent in Table: %s at index: %s' % (field, table, index)
                LOGGER.error(msg)
        else: # horizontal insert
            str_obj = StringIO(field)
            csv_reader = csv.reader(str_obj, delimiter=delim)
            fields = csv_reader.next()
            for field in fields:
                if field not in self.extcsv_ds[table_n].keys():
                    self.extcsv_ds[table_n][field] = []
                    msg = 'Field: %s was added to Table: %s at index: %s' % (field, table, index)
                    LOGGER.info(msg)
                else:
                    msg = 'Field: %s already persent in Table: %s at index: %s' % (field, table, index)
                    LOGGER.error(msg)
                

    def add_data(self, table, data, field=None, index=1, delim=','):
        """
        add data
        """
        table_n = self.table_index(table, index)
        # add table if not present
        if table_n not in self.extcsv_ds.keys():
            self.add_table(table, index=index)
        # add field if not present
        if field is not None:
            field_l = field.split(delim)
            for f in field_l:
                if f not in self.extcsv_ds[table_n].keys():
                    self.add_field(table, f, index=index)
        if all([
            delim not in data,
            field is not None,
            ]): # vertical insert
            if delim not in field:
                if not isinstance(data, list):
                    try:
                        self.extcsv_ds[table_n][field].append(str(data))
                        msg = 'Data added to Field: %s of Table: %s at index: %s' % (field, table, index)
                        LOGGER.info(msg)
                    except Exception, err:
                        msg = 'Unable to add data: %s, to table: %s at index: %s, due to: %s. Data skipped.' % (data, table, index, str(err))
                        LOGGER.error(msg)
                else:
                    try:
                        self.extcsv_ds[table_n][field] += data
                        msg = 'Data added to Field: %s of Table: %s at index: %s' % (field, table, index)
                        LOGGER.info(msg)
                    except Exception, err:
                        msg = 'Unable to add data: %s, to table: %s at index: %s, due to: %s. Data skipped.' % (data, table, index, str(err))
                        LOGGER.error(msg)
            else:
                msg = 'Multiple fields submitted but value(s) for single field provided. Data: %s skipped.' % (data)
                LOGGER.error(msg)
        elif delim in data:
            # horizontal insert
            str_obj = StringIO(data)
            csv_reader = csv.reader(str_obj, delimiter=delim)
            data_l = csv_reader.next()
            if len(data_l) > len(self.extcsv_ds[table_n].keys()):
                msg = 'Mismatching number of values to fields of table: %s at index: %s. Data skipped' % (table, index)
                LOGGER.error(msg)
            else:
                for data in data_l:
                    data_index = data_l.index(data) + 1
                    try:
                        field = self.extcsv_ds[table_n].keys()[data_index]
                    except IndexError, err:
                        msg = 'Number of data values exceed field count for table: %s, at index: %s. Skipping value: %s.' % (table, index, data)
                        LOGGER.error(msg)
                    self.extcsv_ds[table_n][field].append(str(data))
        else:
            msg = 'Invalid entry attempted. Multiple values submitted for single field. Data: [%s] skipped.' % data
            LOGGER.error(msg)


    def remove_table(self, table, index=1):
        """
        remove table
        """
        table_n = self.table_index(table, index)
        try:
            del self.extcsv_ds[table_n]
            msg = 'Removed Table: %s at index: %s' % (table, index)
            LOGGER.info(msg)
        except Exception, err:
            msg = 'Unable to delete Table: %s at index: %s, due to: %s' % (table, index, str(err))
            LOGGER.error(msg)
        
        
    def remove_field(self, table, field, index=1):
        """
        remove field from table, including it's data
        """
        table_n = self.table_index(table, index)
        try:
            del self.extcsv_ds[table_n][field]
            msg = 'Removed Field: %s of Table: %s at index: %s' % (field, table, index)
            LOGGER.info(msg)
        except Exception, err:
            msg = 'Unable to remove Field: %s of Table: %s at index: %s, due to: %s' % (field, table, index, str(err))
            LOGGER.error(msg)
            
            
    def remove_data(self, table, field, data=None, index=1, d_index=None, all_occurances=False):
        """
        remove data
        """
        table_n = self.table_index(table, index)
        if all([
            d_index is None,
            data is not None
            ]): # remove first occurance
            try:
                self.extcsv_ds[table_n][field].remove(data)
                msg = 'First occurance of data: %s from Field: %s of Table: %s at index: %s was removeed' %\
                    (data, field, table, index)
                LOGGER.info(msg)
            except ValueError:
                msg = 'Value: %s was not found in field: %s of table: %s at index: %s' % (data, field, table, index)
                LOGGER.error(msg)
        if d_index is not None: # remove by index
            try:
                self.extcsv_ds[table_n][field].pop(d_index)
                msg = 'Data at index: %s from Field: %s of Table: %s at index: %s was removed' %\
                    (d_index, field, table, index)
                LOGGER.info(msg)
            except IndexError:
                msg = 'No data found at position: %s in field: %s of table: %s at index: %s' % (d_index, field, table, index)
                LOGGER.error(msg)
        if all([
            data is not None,
            all_occurances
            ]): # remove all
            self.extcsv_ds[table_n][field] = filter(lambda a: a != data, self.extcsv_ds[table_n][field])
            msg = 'All occurance of data: %s from Field: %s of table: %s at index: %s removed.' %\
                (data, field, table, index)
            LOGGER.info(msg)
            
    def clear_file(self):
        """
        clear all tables
        """
        try:
            self.extcsv_ds.clear()
            LOGGER.info('Extended CSV cleared.')
        except Exception, err:
            msg = 'Could not clear extended CSV, due to: %s' % str(err)
            LOGGER.error(msg)
        
    def clear_table(self, table, index=1):
        """
        Clear table (all fields except table commenets)
        """
        table_n = self.table_index(table, index)
        try:
            # back up comments
            t_comments = self.extcsv_ds[table_n]['comments']
            self.extcsv_ds[table_n].clear()
            # put back commenets
            self.extcsv_ds[table_n]['comments'] = t_comments
            msg = 'Table: %s at index: %s cleared.' % (table, index)
            LOGGER.info(msg)
        except Exception, err:
            msg = 'Could not clear table: %s at index: %s, due to: %s' % (table, index, str(err))
            LOGGER.error(msg)
        
    def clear_field(self, table, field, index=1):
        """
        Clear field
        """
        table_n = self.table_index(table, index)
        try:
            self.extcsv_ds[table_n][field] = []
            msg = 'Field: %s of table: %s at index: %s' % (field, table, index)
            LOGGER.info(msg)
        except Exception, err:
            msg = 'Could not clear field: %s of table: %s at index: %s, due to: %s' % (field, table, index, str(err))
            LOGGER.error(msg)
                
    def inspect_table(self, table, index=1):
        """
        return table
        """
        table_n = self.table_index(table, index)
        return self.extcsv_ds[table_n]


    def inspect_field(self, table, field, index=1):
        """
        return field
        """
        table_n = self.table_index(table, index)
        return self.extcsv_ds[table_n][field]
        
        
    def serialize(self, path=None):
        """
        write out extcsv object to file
        """
        validate = validate_extcsv(self.extcsv_ds)
        bad = validate[0]
        if not bad:
            # object is good, write it out
            try:
                w_path = serialize_extcsv(self, path)
                LOGGER.info('File written to: %s', w_path)
            except Exception, err:
                msg = 'ExtCSV cannot be serialized, due to: %s' % str(err)
                LOGGER.error(msg)
        else:
            # object is bad, don't write out file
            # log violations
            violations = validate[1]
            msg = violations
            LOGGER.error(msg)
            print msg

    def table_index(self, table, index):
        """
        helper
        return table index str
        """
        sep = '$'
        return '%s%s%s' % (table, sep, index)
        
        
    def get_ds(self):
        """
        return internal data structure
        for testing only
        """
        print self.extcsv_ds
        

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
        
