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

import csv
import logging
import os
import re
from sets import Set
from StringIO import StringIO
from collections import OrderedDict

__version__ = '0.1.1'

LOGGER = logging.getLogger(__name__)

__dirpath = os.path.dirname(os.path.realpath(__file__))

TABLE_CONFIGURATION = os.path.join(__dirpath, 'table_configuration.csv')


class Reader(object):
    """WOUDC Extended CSV reader"""

    def __init__(self, content):
        """
        Parse WOUDC Extended CSV into internal data structure

        :param content: string buffer of content
        """

        header_fields = [
            'PROFILE',
            'DAILY',
            'GLOBAL',
            'DIFFUSE',
            # 'MONTHLY',
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
        ]

        self.sections = {}
        self.metadata_tables = []
        self.data_tables = []
        self.all_tables = []
        self.comments = {}
        self.updated = False
        self.errors = []

        LOGGER.info('processing Extended CSV')
        blocks = re.split('#', content)
        if len(blocks) == 0:
            msg = 'no tables found'
            LOGGER.error(msg)
        # get rid of first element of cruft
        head_comment = blocks.pop(0)
        c = StringIO(head_comment.strip())
        for line in c:
            if all([line.strip() != '', line.strip() != os.linesep,
                    line[0] != '*']):
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
            if any(['|' in b, '/' in b, '\\' in b]):
                msg = 'invalid delimiter'
                LOGGER.error(msg)
                self.errors.append(6)
            try:
                s = StringIO(b.strip())
                c = csv.reader(s)
                header = (c.next()[0]).strip()
            except Exception as err:
                msg = 'Extended CSV malformed'
                LOGGER.error(msg)
            if header not in header_fields:  # metadata
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
                    if len(fields[0]) > 0:
                        if fields[0][0] == '*':
                            self.errors.append(8)
                except StopIteration:
                    msg = 'Extended CSV table %s has no fields' % header
                    LOGGER.info(msg)
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
                    self.errors.append(140)
                    continue
                try:
                    anything_more = (c.next()[0]).strip()
                    if all([anything_more is not None, anything_more != '',
                            anything_more != os.linesep,
                            '*' not in anything_more]):
                        self.errors.append(140)
                except Exception as err:
                    LOGGER.warning(err)
                if len(values) > len(fields):
                    self.errors.append(3)
                    continue
                i = 0
                for field in fields:
                    field = field.strip()
                    try:
                        self.sections[header][field] = (values[i]).strip()
                        i += 1
                    except (KeyError, IndexError):
                        self.sections[header][field] = None
                        msg = 'corrupt format section %s skipping' % header
                        LOGGER.debug(msg)
            else:  # payload
                buf = StringIO(None)
                w = csv.writer(buf)
                columns = None
                for row in c:
                    if columns is None:
                        columns = row
                    if all([row != '', row is not None, row != []]):
                        if '*' not in row[0]:
                            w.writerow(row)
                        else:
                            if columns[0].lower() == 'time':
                                self.errors.append(21)
                if header not in self.sections:
                    self.all_tables.append(header)
                    self.data_tables.append(header)
                    self.table_count[header] = 1
                else:
                    self.table_count[header] = self.table_count[header] + 1
                    header = '%s%s' % (header, self.table_count[header])
                    self.sections[header] = {}
                    self.data_tables.append(header)
                self.sections[header] = {'_raw': buf.getvalue()}
        # objectify comments found in file
        # preserve order of occurence
        hash_detected = False
        table = None
        comments_list = []
        table_count = {}
        for line in content.splitlines():
            if '#' in line:  # table detected
                if not hash_detected:
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
            # comments are prefixed by '*' in column 0 of each line
            if line.startswith('*'):  # comment detected,
                comments_list.append(line.strip('\n'))
        self.comments[table] = comments_list

    def __eq__(self, other):
        """
        equals builtin

        :param other: object to be compared
        :returns: boolean result of comparison
        """

        return self.__dict__ == other.__dict__


class Writer(object):
    """WOUDC Extended CSV writer"""

    def __init__(self, ds=None, template=False):
        """
        Initialize a WOUDC Extended CSV writer

        :param ds: OrderedDict of WOUDC tables, fields, values and commons
        :param template: boolean to set default / common Extended CSV tables
        :returns: Writer object
        """

        self._file_comments = []
        self._extcsv_ds = OrderedDict()
        # init object with requested data structure
        if ds is not None:
            for table, fields in ds.items():
                t_toks = table.split('$')
                t = t_toks[0]
                t_index = t_toks[1]
                self.add_table(t, index=t_index)
                for field, values in fields.items():
                    self.add_field(t, field, index=t_index)
                    for value in values:
                        self.add_data(t, value, field, index=t_index)
        if template:  # init object with default metadata tables and fields
            self.add_field('CONTENT', 'Class,Category,Level,Form')
            datagen_fields = 'Date,Agency,Version,ScientificAuthority'
            self.add_field('DATA_GENERATION', datagen_fields)
            self.add_field('PLATFORM', 'Type,ID,Name,Country,GAW_ID')
            self.add_field('INSTRUMENT', 'Name,Model,Number')
            self.add_field('LOCATION', 'Latitude,Longitude,Height')
            self.add_field('TIMESTAMP', 'UTCOffset,Date,Time')

    @property
    def file_comments(self):
        """
        Get file comments

        :returns: return file level comments
        """

        return self._file_comments

    @property
    def extcsv_ds(self):
        """
        Get internal data structure

        :returns: object's internal data structure
        """

        return self._extcsv_ds

    def add_comment(self, comment):
        """
        Add file level comments

        :param comment: file level comment to be added.
        """

        self.file_comments.append(comment)

    def add_table_comment(self, table, comment, index=1):
        """
        Add table level comments

        :param table: table name
        :param index: table index or grouping
        :param comment: table level comment to be added.
        """

        table_n = _table_index(table, index)
        self.extcsv_ds[table_n]['comments'].append(comment)

    def add_table(self, table, table_comment=None, index=1):
        """
        Add table to extcsv

        :param table: table name
        :param table_comment: table comment
        :param index: table index or grouping
        """

        table_n = _table_index(table, index)
        if table_n not in self.extcsv_ds.keys():
            self.extcsv_ds[table_n] = OrderedDict()
            self.extcsv_ds[table_n]['comments'] = []
            msg = 'Table: %s at index: %s added.' % (table, index)
            LOGGER.info(msg)
            if table_comment is not None:
                self.extcsv_ds[table_n]['comments'].append(table_comment)
        else:
            msg = 'Table %s index %s already exists' % (table, index)
            LOGGER.error(msg)

    def add_field(self, table, field, index=1, delimiter=','):
        """
        Add field to Extended CSV table

        :param table: table name
        :param field: field name
        :param index: table index or grouping
        :param delimiter: delimiter for multiple fields/values
        """

        table_n = _table_index(table, index)
        # add table if not present
        if table_n not in self.extcsv_ds.keys():
            self.add_table(table, index=index)
        if delimiter not in field:  # vertical insert
            if field not in self.extcsv_ds[table_n].keys():
                self.extcsv_ds[table_n][field] = []
                msg = 'field %s added to table %s index %s' % \
                      (field, table, index)
                LOGGER.info(msg)
            else:
                msg = 'field %s already exists in table %s index %s' % \
                      (field, table, index)
                LOGGER.error(msg)
        else:  # horizontal insert
            str_obj = StringIO(field)
            csv_reader = csv.reader(str_obj, delimiter=delimiter)
            fields = csv_reader.next()
            for field in fields:
                if field not in self.extcsv_ds[table_n].keys():
                    self.extcsv_ds[table_n][field] = []
                    msg = 'field %s added to table %s index %s' % \
                          (field, table, index)
                    LOGGER.info(msg)
                else:
                    msg = 'field %s already exists in table %s at index %s' % \
                          (field, table, index)
                    LOGGER.error(msg)

    def add_data(self, table, data, field=None, index=1, delimiter=',',
                 table_comment=None):
        """
        Add data to Extended CSV table field

        :param table: table name
        :param field: field name
        :param index: table index or grouping
        :param delimiter: delimiter for multiple fields / values
        """

        table_n = _table_index(table, index)
        # add table if not present
        if table_n not in self.extcsv_ds.keys():
            self.add_table(table, index=index)
        # add table comment
        if table_comment is not None:
            self.add_table_comment(table, table_comment, index)
        # add field if not present
        if field is not None:
            field_l = field.split(delimiter)
            for f in field_l:
                if f not in self.extcsv_ds[table_n].keys():
                    self.add_field(table, f, index=index)
        if all([delimiter not in data, field is not None]):  # vertical insert
            if delimiter not in field:
                if not isinstance(data, list):
                    try:
                        self.extcsv_ds[table_n][field].append(str(data))
                        msg = 'data added to field %s table %s index %s' % \
                              (field, table, index)
                        LOGGER.info(msg)
                    except Exception as err:
                        msg = 'unable to add data %s' % err
                        LOGGER.error(msg)
                else:
                    try:
                        self.extcsv_ds[table_n][field] += data
                        msg = 'data added to field %s table %s index %s' % \
                              (field, table, index)
                        LOGGER.info(msg)
                    except Exception as err:
                        msg = 'unable to add data %s' % err
                        LOGGER.error(msg)
            else:
                msg = 'multiple fields / single value detected; skipping'
                LOGGER.error(msg)
        elif delimiter in data:
            # horizontal insert
            str_obj = StringIO(data)
            csv_reader = csv.reader(str_obj, delimiter=delimiter)
            data_l = csv_reader.next()
            if len(data_l) > len(self.extcsv_ds[table_n].keys()):
                msg = 'fields / values mismatch; skipping'
                LOGGER.error(msg)
            else:
                data_index = 0
                for data in data_l:
                    data_index += 1
                    try:
                        field = self.extcsv_ds[table_n].keys()[data_index]
                    except IndexError as err:
                        msg = 'number of data values exceed field count'
                        LOGGER.error(msg)
                    self.extcsv_ds[table_n][field].append(str(data))
        else:
            msg = 'multiple values / single field detected; skipping'
            LOGGER.error(msg)

    def remove_table(self, table, index=1):
        """
        Remove table from Extended CSV

        :param table: table name
        :param index: table index or grouping
        """

        table_n = _table_index(table, index)
        try:
            del self.extcsv_ds[table_n]
            msg = 'removed table %s index %s' % (table, index)
            LOGGER.info(msg)
        except Exception as err:
            msg = 'unable to delete table %s' % err
            LOGGER.error(msg)

    def remove_field(self, table, field, index=1):
        """
        Remove field (and data) from table

        :param table: table name
        :param field: field name
        :param index: table index or grouping
        """

        table_n = _table_index(table, index)
        try:
            del self.extcsv_ds[table_n][field]
            msg = 'removed field %s table %s index %s' % (field, table, index)
            LOGGER.info(msg)
        except Exception as err:
            msg = 'unable to remove field %s' % err
            LOGGER.error(msg)

    def remove_data(self, table, field, data=None, index=1, d_index=None,
                    all_occurences=False):
        """
        Remove data from Extended CSV table field

        :param table: table name
        :param field: field name
        :param data: data to be removed
        :param index: table index or grouping
        :param d_index: index of data in a multi value field
                        (i.e. profile field)
        :param all_occurences: remove all occurences of matching data
                               from a table field (default is False).
        """

        table_n = _table_index(table, index)
        if all([d_index is None, data is not None]):  # remove first occurence
            try:
                self.extcsv_ds[table_n][field].remove(data)
                msg = 'data %s field %s table %s index %s removed' % \
                      (data, field, table, index)
                LOGGER.info(msg)
            except ValueError:
                msg = 'value %s not found' % data
                LOGGER.error(msg)
        if d_index is not None:  # remove by index
            try:
                self.extcsv_ds[table_n][field].pop(d_index)
                msg = 'data at index %s field %s table %s index %s removed' % \
                      (d_index, field, table, index)
                LOGGER.info(msg)
            except IndexError:
                msg = 'no data found pos %s field %s table %s index %s' % \
                      (d_index, field, table, index)
                LOGGER.error(msg)
        if all([data is not None, all_occurences]):  # remove all
            LOGGER.info('removing all occurences')
            val = filter(lambda a: a != data, self.extcsv_ds[table_n][field])
            self.extcsv_ds[table_n][field] = val
            msg = 'data %s field %s table %s index %s removed' % \
                  (data, field, table, index)
            LOGGER.info(msg)

    def clear_file(self):
        """
        Remove all tables from Extended CSV
        """

        try:
            self.extcsv_ds.clear()
            LOGGER.info('Extended CSV cleared')
        except Exception as err:
            msg = 'Could not clear Extended CSV: %s' % err
            LOGGER.error(msg)

    def clear_table(self, table, index=1):
        """
        Clear table (all fields except table comments)

        :param table: table name
        :param index: index name
        """

        table_n = _table_index(table, index)
        try:
            # back up comments
            t_comments = self.extcsv_ds[table_n]['comments']
            self.extcsv_ds[table_n].clear()
            # put back commenets
            self.extcsv_ds[table_n]['comments'] = t_comments
            msg = 'table %s index %s cleared' % (table, index)
            LOGGER.info(msg)
        except Exception as err:
            msg = 'could not clear table %s' % err
            LOGGER.error(msg)

    def clear_field(self, table, field, index=1):
        """
        Clear all values from field

        :param table: table name
        :param field: field name
        :param index: index name
        """

        table_n = _table_index(table, index)
        try:
            self.extcsv_ds[table_n][field] = []
            msg = 'field %s table %s index %s cleared' % (field, table, index)
            LOGGER.info(msg)
        except Exception as err:
            msg = 'could not clear field %s' % err
            LOGGER.error(msg)

    def inspect_table(self, table, index=1):
        """
        Return the contents of an Extended CSV table

        :param table: table name
        :param index: index name
        :returns: dictionary table field(s) and associated value(s)
        """

        table_n = _table_index(table, index)
        return self.extcsv_ds[table_n]

    def inspect_field(self, table, field, index=1):
        """
        Return the content of a field

        :param table: table name
        :param field: field name
        :param index: table index
        :returns: list of value(s) at extcsv table field
        """

        table_n = _table_index(table, index)
        return self.extcsv_ds[table_n][field]

    def validate(self):
        """
        Validate extcsv for common/metadata tables and fields

        :returns: list of error codes and violations
        """

        error = False
        violations = []
        rules = table_configuration_lookup('common')
        for rule in rules:
            table = rule['table']
            table = '%s$1' % table[1:]
            table_required = rule['table_required']
            fields = rule['fields'].split(',')
            fields = [x.lower().strip() for x in fields]
            optional_fields = rule['optional_fields'].split(',')
            optional_fields = [x.lower() for x in optional_fields]
            for f in optional_fields:
                if f in fields:
                    fields.remove(f)
            # check required table
            if all([table not in self.extcsv_ds.keys(),
                    table_required == 'required']):
                violations.append(_violation_lookup(1, table))
                error = True
                for field in fields:
                    if field not in optional_fields:
                        error = True
                        violations.append(_violation_lookup(3, field))
            else:
                # check required fields
                fields_in = self.extcsv_ds[table]
                fields_in = [x.lower() for x in fields_in]
                a = Set(fields_in)
                b = Set(fields)
                c = a ^ b
                while len(c) > 0:
                    item = c.pop()
                    if all([item not in optional_fields,
                            item in fields_in,
                            item != 'comments']):  # unrecognized field name
                        error = True
                        violations.append(_violation_lookup(4, item))
                    if item in fields:
                        error = True
                        violations.append(_violation_lookup(3, item))

            if all([rule['incompatible_table'] is not None,
                    rule['incompatible_table'] in self.extcsv_ds.keys()]):
                error = True
                violations.append(_violation_lookup(2, table))

        return [error, violations]

    def serialize(self):
        """
        Write Extended CSV object to string

        :returns: StringIO object of file content
        """

        mem_file = StringIO()
        # write to string buffer
        csv_writer = csv.writer(mem_file)
        if len(self.file_comments) != 0:
            for comment in self.file_comments:
                mem_file.write('* %s%s' % (comment, os.linesep))
            mem_file.write('%s' % os.linesep)
        for table, fields in self.extcsv_ds.items():
            mem_file.write('#%s%s' % (table[0: table.index('$')], os.linesep))
            t_comments = fields['comments']
            row = fields.keys()[1:]
            csv_writer.writerow(row)
            values = fields.values()[1:]
            max_len = len(max(values, key=len))
            for i in range(0, max_len):
                row = []
                for value in values:
                    try:
                        row.append(value[i])
                    except IndexError:
                        row.append('')
                # clean up row
                for j in range(len(row) - 1, 0, -1):
                    if row[j] != '':
                        break
                    else:
                        row.pop(j)
                csv_writer.writerow(row)
            if len(t_comments) > 0:
                for comment in t_comments:
                    mem_file.write('* %s%s' % (comment, os.linesep))
            len1 = self.extcsv_ds.keys().index(table)
            len2 = len(self.extcsv_ds.keys()) - 1
            if len1 != len2:
                mem_file.write('%s' % os.linesep)

        return mem_file

# table name and index separator
sep = '$'


def _table_index(table, index):
    """
    Helper function to return table index.
    """

    return '%s%s%s' % (table, sep, index)


def _violation_lookup(code, rpl_str=None):
    """
    Helper function to look up error message given
    violation code

    :param code: violation code
    :param rpl_str: optional replacement string
    :returns: violation message
    """

    violations_map = {
        1: '{Missing required table name: $$$.}',
        2: '{Table name: $$$ is not from approved list.}',
        3: '{Missing required field name: $$$.}',
        4: '{Field name: $$$, is not from approved list.}',
        5: '{Improper delimiter found (";" or ":" or "$" or "%"),\
        corrected to "," (comma).}',
        6: '{Unknown delimiter found.  Delimiter must be "," (comma).}',
        8: '{Remarks - cannot be between TABLE names and Field names nor\
        between Field names and values of field.}',
        9: '{Cannot identify data, possibly a remark, \
        but no asterisk (*) used.}',
        140: '{Incorrectly formatted table: $$$. \
        Table does not contain exactly 3 lines.}'
    }

    if sep in rpl_str:
        rpl_str = rpl_str[:rpl_str.index(sep)]

    if rpl_str is not None:
        msg = violations_map[code].replace('$$$', rpl_str)
        return msg
    else:
        return violations_map[code]


def table_configuration_lookup(dataset, level='n/a', form='n/a',
                               all_tables=False):
    """
    Helper function to lookup table + field presence rules

    TODO: document
    """

    rules = []
    all_tb = []
    rule = {
        'table': None,
        'table_required': None,
        'occurence': None,
        'incompatible_table': None,
        'fields': None,
        'field_requird': None,
        'optional_fields': None
    }
    try:
        with open(TABLE_CONFIGURATION) as out_file:
            csv_reader = csv.reader(out_file, delimiter='|')
            for row in csv_reader:
                rule = {
                    'table': None,
                    'table_required': None,
                    'occurence': None,
                    'incompatible_table': None,
                    'fields': None,
                    'all_field_required': None,
                    'optional_fields': None
                }
                ds = row[0].strip()
                ll = row[1].strip()
                fm = row[2].strip()
                tb = row[3].strip()
                req_tb = row[4].strip()
                ocr = row[5].strip()
                inc = row[6].strip()
                flds = row[7].strip()
                req_fld = row[8].strip()
                opt_flds = row[9].strip()
                if all([dataset == ds, level == ll, form == fm]):
                    rule['table'] = tb
                    rule['table_required'] = req_tb
                    rule['occurence'] = ocr
                    rule['incompatible_table'] = inc
                    rule['fields'] = flds
                    rule['all_field_required'] = req_fld
                    rule['optional_fields'] = opt_flds
                    rules.append(rule)
                if all_tables:
                    all_tb.append(str(tb))
                    if all([inc != '', inc != u'n/a']):
                        all_tb.append(str(inc))
    except Exception as err:
        msg = str(err)
        LOGGER.error(msg)

    if all_tables:
        return all_tb
    else:
        return rules


# module level entry points / helper functions

def load(filename):
    """
    Load Extended CSV from file

    :param filename: filename
    :returns: Extended CSV data structure
    """

    with open(filename) as ff:
        return Reader(ff.read())


def loads(strbuf):
    """
    Load Extended CSV from string

    :param strbuf: string representation of Extended CSV
    :returns: Extended CSV data structure
    """

    return Reader(strbuf)


def dump(extcsv_obj, filename):
    """
    Dump Extended CSV object to file

    :param extcsv_obj: Extended CSV object
    :param filename: filename
    :returns: void, writes file to disk
    """
    LOGGER.info('Dumping Extended CSV object to file: %s' % filename)
    with open(filename, 'wb') as ff:
        ff.write(_dump(extcsv_obj))


def dumps(extcsv_obj):
    """
    Dump Extended CSV object to string representation

    :param extcsv_obj: Extended CSV object
    :returns: string
    """

    return _dump(extcsv_obj)


def _dump(extcsv_obj):
    """
    Internal helper function to dump Extended CSV object to
    string representation

    :param extcsv_obj: Extended CSV object
    :returns: string representation of Extended CSV
    """

    validate = extcsv_obj.validate()
    bad = validate[0]

    if bad:  # validation errors found
        msg = 'Could not serialize object to string.  Violations found: %s' % \
              ','.join(validate[1])
        LOGGER.error(msg)
        raise RuntimeError(msg)

    # object is good, dump to string
    try:
        LOGGER.info('Serializing object to string')
        return extcsv_obj.serialize().getvalue()
    except Exception as err:
        msg = 'Extended CSV cannot be serialized %s' % err
        LOGGER.error(msg)
        raise RuntimeError(msg)
