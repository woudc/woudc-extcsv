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

import csv
import json
import io
import os
import yaml

import re
import jsonschema
import logging

from io import StringIO
from datetime import datetime, time
from collections import OrderedDict

from woudc_extcsv.util import (parse_integer_range, _table_index,
                               non_content_line)


__version__ = '0.5.0'

__dirpath = os.path.dirname(os.path.realpath(__file__))

WDR_TABLE_SCHEMA = os.path.join(__dirpath, 'tables-schema.json')
WDR_TABLE_CONFIG = os.path.join(__dirpath, 'tables-backfilling.yml')
WDR_ERROR_CONFIG = os.path.join(__dirpath, 'errors-backfilling.csv')

LOGGER = logging.getLogger(__name__)

with open(WDR_TABLE_SCHEMA) as table_schema_file:
    table_schema = json.load(table_schema_file)
with open(WDR_TABLE_CONFIG) as table_definitions:
    DOMAINS = yaml.safe_load(table_definitions)
with open(WDR_ERROR_CONFIG) as error_definitions:
    reader = csv.reader(error_definitions, escapechar='\\')
    next(reader)  # Skip header line.
    ERRORS = OrderedDict()
    for row in reader:
        error_code = int(row[0])
        ERRORS[error_code] = row[1:3]


try:
    jsonschema.validate(DOMAINS, table_schema)
except jsonschema.SchemaError as err:
    LOGGER.critical('Failed to read table definition schema:'
                    ' cannot process incoming files')
    raise err
except jsonschema.ValidationError as err:
    LOGGER.critical('Failed to read table definition file:'
                    ' cannot process incoming files')
    raise err


def dump(extcsv_obj, filename):
    """
    Dump Reader Extended CSV object to file

    :param extcsv_obj: Reader Extended CSV object
    :param filename: filename
    :returns: void, writes file to disk
    """
    LOGGER.info('Dumping Extended CSV object to file: %s' % filename)
    with open(filename, 'w') as ff:
        ff.write(_dump(extcsv_obj))


def dumps(extcsv_obj):
    """
    Dump Writer Extended CSV object to string representation

    :param extcsv_obj: Writer Extended CSV object
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

    validate = [extcsv_obj.metadata_validator(),
                extcsv_obj.dataset_validator()]
    bad = validate[0] and validate[1]

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


def load(filename, reader=True):
    """
    Load Extended CSV from file

    :param filename: filename
    :returns: Extended CSV data structure
    """

    try:
        with io.open(filename, encoding='utf-8') as ff:
            content = ff.read()
            if not reader:
                return ExtendedCSV(content)
            else:
                return Reader(content)
    except UnicodeError as err:
        LOGGER.warning(err)
        msg = 'Unable to read {} with utf8 encoding. Attempting to read' \
              ' with latin1 encoding.'.format(filename)
        LOGGER.info(msg)
        with io.open(filename, encoding='latin1') as ff:
            content = ff.read()
            if not reader:
                return ExtendedCSV(content)
            else:
                return Reader(content)


def loads(strbuf):
    """
    Load Extended CSV from string

    :param strbuf: string representation of Extended CSV
    :returns: Extended CSV data structure
    """

    return Reader(strbuf)


class ExtendedCSV(object):
    """

    WOUDC Extended CSV parser

    https://guide.woudc.org/en/#chapter-3-standard-data-format

    """

    def __init__(self, content, reporter=None):
        """
        Read WOUDC Extended CSV file

        :param content: buffer of Extended CSV data
        :returns: `ExtendedCSV` object
        """

        self.extcsv = {}
        self._raw = None

        self._table_count = {}
        self._line_num = {}

        self.file_comments = []
        self.warnings = []
        self.errors = []
        self.reports = reporter

        self._noncore_table_schema = None
        self._observations_table = None

        LOGGER.debug('Reading into csv')
        self._raw = content
        self._raw = content.lstrip('\ufeff')
        reader = csv.reader(StringIO(self._raw))

        LOGGER.debug('Parsing object model')
        parent_table = None
        lines = enumerate(reader, 1)

        success = True
        for line_num, row in lines:
            separators = []
            for bad_sep in ['::', ';', '$', '%', '|', '\\']:
                if not non_content_line(row) and bad_sep in row[0]:
                    separators.append(bad_sep)

            for separator in separators:
                comma_separated = row[0].replace(separator, ',')
                row = next(csv.reader(StringIO(comma_separated)))

                if not self._add_to_report(104, line_num, separator=separator):
                    success = False

            if len(row) == 1 and row[0].startswith('#'):  # table name
                parent_table = ''.join(row).lstrip('#').strip()

                try:
                    LOGGER.debug('Found new table {}'.format(parent_table))
                    ln, fields = next(lines)

                    while non_content_line(fields):
                        if not self._add_to_report(103, ln):
                            success = False
                        ln, fields = next(lines)

                    parent_table = self.init_table(parent_table, fields,
                                                   line_num)
                except StopIteration:
                    if not self._add_to_report(206, line_num,
                                               table=parent_table):
                        success = False
            elif len(row) > 0 and row[0].startswith('*'):  # comment
                LOGGER.debug('Found comment')
                self.file_comments.append(row)
                continue
            elif non_content_line(row):  # blank line
                LOGGER.debug('Found blank line')
                continue
            elif parent_table is not None and not non_content_line(row):
                if not self.add_values_to_table(parent_table, row, line_num):
                    success = False
            else:
                if not self._add_to_report(211, line_num, row=','.join(row)):
                    success = False

        if not success:
            raise NonStandardDataError(self.errors)

    def _add_to_report(self, error_code, line=None, **kwargs):
        """
        Submit a warning or error of code <error_code> to the report generator,
        with was found at line <line> in the input file. Uses keyword arguments
        to detail the warning/error message.

        :returns: False iff the error is serious enough to abort parsing,
        i.e. True iff the file can continue parsing.
        """

        if self.reports is not None:
            message, severe = self.reports.add_message(error_code, line,
                                                       **kwargs)
        else:
            severe = ERRORS[error_code][0] == 'Error'
            message = ERRORS[error_code][1]
            while '{' in message:
                for i in range(0, len(message), 1):
                    if message[i] == '{':
                        key_start = i+1
                    elif message[i] == '}':
                        curr_key = message[key_start:i]
                        message = message[0:key_start-1] + \
                            str(kwargs[curr_key]) + \
                            message[i+1:len(message)]
                        break

        if severe:
            LOGGER.error(message)
            self.errors.append(message)
        else:
            LOGGER.warning(message)
            self.warnings.append(message)

        return not severe

    def add_comment(self, comment):
        """
        Add file-level comments

        :param comment: file level comment to be added.
        """

        self.file_comments.append(comment)
        LOGGER.info('added file-level comment')

    def add_table_comment(self, table, comment, index=1):
        """
        Add table-level comments

        :param table: table name
        :param index: table index or grouping
        :param comment: table level comment to be added.
        """

        table_n = _table_index(table, index)
        self.extcsv[table_n]['comments'].append(comment)
        LOGGER.info('added table-level comment')

    def line_num(self, table):
        """
        Returns the line in the source file at which <table> started.
        If there is no table in the file named <table>, returns None instead.

        :param table: name of an Extended CSV table.
        :returns: line number where the table occurs, or None if
                  the table never occurs.
        """

        return self._line_num.get(table, None)

    def update_line_num(self):
        """
        Updates the dictionary of lines in the source file at which
        <table> started.

        :returns: line number where the table occurs, or None if
                  the table never occurs.
        """

        return self._line_num

    def table_count(self, table_type):
        """
        Returns the number of tables named <table_type> in the source file.

        :param table_type: name of an Extended CSV table without suffixes.
        :returns: number of tables named <table_type> in the input file.
        """

        return self._table_count.get(table_type, 0)

    def init_table(self, table_name, fields, line_num):
        """
        Record an empty Extended CSV table named <table_name> with
        fields given in the list <fields>, which starts at line <line_num>.

        May change the name of the table if a table named <table_name>
        already exists. Returns the table name that ends up being used.

        :param table_name: name of the new table
        :param fields: list of column names in the new table
        :param line_num: line number of the table's header (its name)
        :returns: final name for the new table
        """

        if table_name not in self._table_count:
            self._table_count[table_name] = 1
        else:
            updated_count = self._table_count[table_name] + 1
            self._table_count[table_name] = updated_count
            table_name += '_' + str(updated_count)

        self.extcsv[table_name] = OrderedDict()
        self._line_num[table_name] = line_num

        self.extcsv[table_name]['comments'] = []
        for field in fields:
            self.extcsv[table_name][field.strip()] = []

        msg = 'added table {}'.format(table_name)
        LOGGER.info(msg)

        return table_name

    def add_field_to_table(self, table_name, fields, index=1):
        """
        Record an empty column(s) in Extended CSV table named <table_name>
        with field names given in the list <fields>.

        May reject a field in <fields> if the field already exists in
        <table_name>. Returns the field names that ends up being used.

        :param table_name: name of the table
        :param fields: list of column names in the new table
        :param line_num: line number of the table's header (its name)
        :returns: field names added to the table
        """

        added_fields = []
        _table_name = _table_index(table_name, index)
        if _table_name not in self._line_num:
            return added_fields

        for field in fields:
            if field not in self.extcsv[_table_name]:
                self.extcsv[_table_name][field.strip()] = []
                added_fields += [field]
                msg = 'field {} added to table {} index {}' \
                    .format(field, _table_name, index)
                LOGGER.info(msg)
            else:
                msg = 'field {} already exists in table {} index {}' \
                        .format(field, _table_name, index)
                LOGGER.error(msg)

        return added_fields

    def add_values_to_table(self, table_name, values, line_num, fields=None,
                            index=1, horizontal=True):
        """
        Add the raw strings in <values> to the bottom of the columns
        in the tabled named <table_name>.

        Returns whether the operation was successful (no errors occurred).

        :param table_name: name of the table the values fall under
        :param values: list of values from one row in the table
        :param line_num: line number the row occurs at
        :param horizontal: True if horizontal insert, false if vertical
        :returns: `bool` of whether the operation was successful
        """
        success = True
        _table_name = _table_index(table_name, index)

        if horizontal:  # horizontal insert
            if fields is not None:
                all_fields = list(self.extcsv[_table_name].keys())[1:]
                for f in all_fields:
                    if f not in fields:
                        fields.append(f)
                        values.append('')
            else:
                fields = list(self.extcsv[_table_name].keys())[1:]
                fillins = len(fields) - len(values)

                if fillins < 0:
                    if not self._add_to_report(
                            212, line_num, table=_table_name):
                        success = False

                values.extend([''] * fillins)
                values = values[:len(fields)]

            for field, value in zip(fields, values):
                self.extcsv[table_name][field].append(value.strip())

        else:  # vertical insert
            if len(fields) == 1:
                for value in values:
                    self.extcsv[table_name][fields[0]].append(value)
            else:
                for (field, value) in zip(fields, values):
                    self.extcsv[table_name][field].append(value)

        return success

    def remove_table(self, table_type, index=1):
        """
        Remove a table from the memory of this Extended CSV instance.
        Does not alter the source file in any way.

        :param table_name: name of the table to delete.
        :returns: void
        """

        table_name = _table_index(table_type, index)

        self.extcsv.pop(table_name)
        self._line_num.pop(table_name)

        if self._table_count[table_type] > 1:
            self._table_count[table_type] -= 1
        else:
            self._table_count.pop(table_type)

        msg = 'removed table {}'.format(table_name)
        LOGGER.info(msg)

    def remove_field(self, table, field, index=1):
        """
        Remove field (and data) from table

        :param table: table name
        :param field: field name
        :param index: table index or grouping
        """

        table_n = _table_index(table, index)
        try:
            del self.extcsv[table_n][field]
            msg = 'removed field %s table %s index %s' % (field, table, index)
            LOGGER.info(msg)
        except Exception as err:
            msg = 'unable to remove field %s' % err
            LOGGER.error(msg)

    def remove_values_from_table(self, table, field, data=None, index=1,
                                 d_index=None, all_occurences=False):
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
                self.extcsv[table_n][field].remove(data)
                msg = 'data %s field %s table %s index %s removed' % \
                      (data, field, table, index)
                LOGGER.info(msg)
            except ValueError:
                msg = 'value %s not found' % data
                LOGGER.error(msg)
        if d_index is not None:  # remove by index
            try:
                self.extcsv[table_n][field].pop(d_index)
                msg = 'data at index %s field %s table %s index %s removed' % \
                      (d_index, field, table, index)
                LOGGER.info(msg)
            except IndexError:
                msg = 'no data found pos %s field %s table %s index %s' % \
                      (d_index, field, table, index)
                LOGGER.error(msg)
        if all([data is not None, all_occurences]):  # remove all
            LOGGER.info('removing all occurences')
            val = filter(lambda a: a != data, self.extcsv[table_n][field])
            self.extcsv[table_n][field] = list(val)
            msg = 'data %s field %s table %s index %s removed' % \
                  (data, field, table, index)
            LOGGER.info(msg)

    def clear_file(self):
        """
        Remove all tables from Extended CSV Writer
        """

        try:
            self.extcsv.clear()
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
            t_comments = self.extcsv[table_n]['comments']
            self.extcsv[table_n].clear()
            # put back commenets
            self.extcsv[table_n]['comments'] = t_comments
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
            self.extcsv[table_n][field] = []
            msg = 'field %s table %s index %s cleared' % (field, table, index)
            LOGGER.info(msg)
        except Exception as err:
            msg = 'could not clear field %s' % err
            LOGGER.error(msg)

    def typecast_value(self, table, field, value, line_num):
        """
        Returns a copy of the string <value> converted to the expected type
        for a column named <field> in table <table>, if possible, or returns
        the original string otherwise.

        :param table: name of the table where the value was found
        :param field: name of the column
        :param value: string containing a value
        :param line_num: line number where the value was found
        :returns: value cast to the appropriate type for its column
        """

        if value == '':  # Empty CSV cell
            return None

        lowered_field = field.lower()

        try:
            if lowered_field == 'time':
                return self.parse_timestamp(table, value, line_num)
            elif lowered_field == 'date':
                return self.parse_datestamp(table, value, line_num)
            elif lowered_field == 'utcoffset':
                return self.parse_utcoffset(table, value, line_num)
        except Exception as err:
            self._add_to_report(335, line_num, table=table, field=field,
                                reason=err)
            return value

        try:
            if '.' in value:  # Check float conversion
                return float(value)
            elif len(value) > 1 and value.startswith('0'):
                return value
            else:  # Check integer conversion
                return int(value)
        except Exception:  # Default type to string
            return value

    def parse_timestamp(self, table, timestamp, line_num):
        """
        Return a time object representing the time contained in string
        <timestamp> according to the expected HH:mm:SS format with optional
        'am' or 'pm' designation.

        Corrects common formatting errors and performs very simple validation
        checks. Raises ValueError if the string cannot be parsed.

        The other parameters are used for error reporting.

        :param table: name of table the value was found under.
        :param timestamp: string value taken from a Time column.
        :param line_num: line number where the value was found.
        :returns: the timestamp converted to a time object.
        """

        success = True

        if timestamp[-2:] in ['am', 'pm']:
            noon_indicator = timestamp[-2:]
            timestamp = timestamp[:-2].strip()
        else:
            noon_indicator = None

        separators = re.findall(r'[^\w\d]', timestamp)
        bad_seps = set(separators) - set(':')

        for separator in bad_seps:
            if not self._add_to_report(109, line_num, table=table,
                                       separator=separator):
                success = False

            timestamp = timestamp.replace(separator, ':')

        tokens = timestamp.split(':')
        hour = tokens[0] or '00'
        minute = tokens[1] or '00' if len(tokens) > 1 else '00'
        second = tokens[2] or '00' if len(tokens) > 2 else '00'

        hour_numeric = minute_numeric = second_numeric = None

        try:
            hour_numeric = int(hour)
        except ValueError:
            if not self._add_to_report(301, line_num, table=table,
                                       component='hour'):
                success = False
        try:
            minute_numeric = int(minute)
        except ValueError:
            if not self._add_to_report(301, line_num, table=table,
                                       component='minute'):
                success = False
        try:
            second_numeric = int(second)
        except ValueError:
            if not self._add_to_report(301, line_num, table=table,
                                       component='second'):
                success = False

        if not success:
            raise ValueError('Parsing errors encountered in #{}.Time'
                             .format(table))

        if noon_indicator == 'am' and hour_numeric == 12:
            if not self._add_to_report(110, line_num, table=table):
                success = False
            hour_numeric = 0
        elif noon_indicator == 'pm' and hour_numeric not in [12, None]:
            if not self._add_to_report(110, line_num, table=table):
                success = False
            hour_numeric += 12

        if second_numeric is not None and second_numeric not in range(0, 60):
            if not self._add_to_report(340, line_num, table=table,
                                       component='second',
                                       lower='00', upper='59'):
                success = False

            while second_numeric >= 60 and minute_numeric is not None:
                second_numeric -= 60
                minute_numeric += 1
        if minute_numeric is not None and minute_numeric not in range(0, 60):
            if not self._add_to_report(340, line_num, table=table,
                                       component='minute',
                                       lower='00', upper='59'):
                success = False

            while minute_numeric >= 60 and hour_numeric is not None:
                minute_numeric -= 60
                hour_numeric += 1
        if hour_numeric is not None and hour_numeric not in range(0, 24):
            if not self._add_to_report(340, line_num, table=table,
                                       component='hour',
                                       lower='00', upper='23'):
                success = False

        if not success:
            raise ValueError('Parsing errors encountered in #{}.Time'
                             .format(table))
        else:
            return time(hour_numeric, minute_numeric, second_numeric)

    def parse_datestamp(self, table, datestamp, line_num):
        """
        Return a date object representing the date contained in string
        <datestamp> according to the expected YYYY-MM-DD format.

        Corrects common formatting errors and performs very simple validation
        checks. Raises ValueError if the string cannot be parsed.

        The other parameters are used for error reporting.

        :param table: name of table the value was found under.
        :param datestamp: string value taken from a Date column.
        :param line_num: line number where the value was found.
        :returns: datestamp converted to a datetime object.
        """

        success = True

        separators = re.findall(r'[^\w\d]', datestamp)
        bad_seps = set(separators) - set('-')

        for separator in bad_seps:
            if not self._add_to_report(111, line_num, table=table,
                                       separator=separator):
                success = False

            datestamp = datestamp.replace(separator, '-')

        tokens = datestamp.split('-')

        if len(tokens) == 1:
            if not self._add_to_report(112, line_num, table=table):
                success = False
        if len(tokens) < 3:
            if not self._add_to_report(113, line_num, table=table):
                success = False
        elif len(tokens) > 3:
            if not self._add_to_report(114, line_num, table=table):
                success = False

        if not success:
            raise ValueError('Parsing errors encountered in #{}.Date'
                             .format(table))

        year = month = day = None

        try:
            year = int(tokens[0])
        except ValueError:
            if not self._add_to_report(302, line_num, table=table,
                                       component='year'):
                success = False
        try:
            month = int(tokens[1])
        except ValueError:
            if not self._add_to_report(302, line_num, table=table,
                                       component='month'):
                success = False
        try:
            day = int(tokens[2])
        except ValueError:
            if not self._add_to_report(302, line_num, table=table,
                                       component='day'):
                success = False

        present_year = datetime.now().year
        if year is not None and year not in range(1924, present_year + 1):
            if not self._add_to_report(303, line_num, table=table,
                                       component='year',
                                       lower='1924', upper='PRESENT'):
                success = False
        if month is not None and month not in range(1, 12 + 1):
            if not self._add_to_report(303, line_num, table=table,
                                       component='month',
                                       lower='01', upper='12'):
                success = False
        if day is not None and day not in range(1, 31 + 1):
            if not self._add_to_report(304, line_num, table=table,
                                       lower='01', upper='31'):
                success = False

        if not success:
            raise ValueError('Parsing errors encountered in #{}.Date'
                             .format(table))
        else:
            return datetime.strptime(datestamp, '%Y-%m-%d').date()

    def parse_utcoffset(self, table, utcoffset, line_num):
        """
        Validates the raw string <utcoffset>, converting it to the expected
        format defined by the regular expression (+|-)\\d\\d:\\d\\d:\\d\\d if
        possible. Returns the converted value or else raises a ValueError.

        The other parameters are used for error reporting.

        :param table: name of table the value was found under.
        :param utcoffset: string value taken from a UTCOffset column.
        :param line_num: line number where the value was found.
        :returns: value converted to expected UTCOffset format.
        """

        success = True

        separators = re.findall(r'[^-\+\w\d]', utcoffset)
        bad_seps = set(separators) - set(':')

        for separator in bad_seps:
            if not self._add_to_report(115, line_num, table=table,
                                       separator=separator):
                success = False
            utcoffset = utcoffset.replace(separator, ':')

        sign = r'(\+|-|\+-)?'
        delim = r'[^-\+\w\d]'
        mandatory_place = r'([\d]{1,2})'
        optional_place = '(' + delim + r'([\d]{0,2}))?'

        template = '^{sign}{mandatory}{optional}{optional}$' \
                   .format(sign=sign, mandatory=mandatory_place,
                           optional=optional_place)
        match = re.findall(template, utcoffset)

        if len(match) == 1:
            sign, hour, _, minute, _, second = match[0]

            if len(hour) < 2:
                if not self._add_to_report(116, line_num, table=table,
                                           component='hour'):
                    success = False
                hour = hour.rjust(2, '0')

            if not minute:
                if not self._add_to_report(117, line_num, table=table,
                                           component='minute'):
                    success = False
                minute = '00'
            elif len(minute) < 2:
                if not self._add_to_report(116, line_num, table=table,
                                           component='minute'):
                    success = False
                minute = minute.rjust(2, '0')

            if not second:
                if not self._add_to_report(117, line_num, table=table,
                                           component='second'):
                    success = False
                second = '00'
            elif len(second) < 2:
                if not self._add_to_report(116, line_num, table=table,
                                           component='second'):
                    success = False
                second = second.rjust(2, '0')

            if all([hour == '00', minute == '00', second == '00']):
                if sign != '+':
                    if not self._add_to_report(119, line_num, table=table,
                                               sign='+'):
                        success = False
                    sign = '+'
            elif not sign:
                if not self._add_to_report(118, line_num, table=table):
                    success = False
                sign = '+'
            elif sign == '+-':
                if not self._add_to_report(119, line_num, table=table,
                                           sign='-'):
                    success = False
                sign = '-'

            if not success:
                raise ValueError('Parsing errors encountered in #{}.UTCOffset'
                                 .format(table))
            try:
                magnitude = time(int(hour), int(minute), int(second))
                return '{}{}'.format(sign, magnitude)
            except (ValueError, TypeError) as err:
                self._add_to_report(305, line_num, table=table)
                raise err

        template = '^{sign}[0]+{delim}?[0]*{delim}?[0]*$' \
                   .format(sign=sign, delim=delim)
        match = re.findall(template, utcoffset)

        if len(match) == 1:
            if not self._add_to_report(120, line_num, table=table):
                raise ValueError('Parsing errors encountered in #{}.UTCOffset'
                                 .format(table))
            else:
                return '+00:00:00'

        self._add_to_report(305, line_num, table=table)
        raise ValueError('Parsing errors encountered in #{}.UTCOffset'
                         .format(table))

    def gen_woudc_filename(self):
        """generate WOUDC filename convention"""

        timestamp = self.extcsv['TIMESTAMP']['Date'].strftime('%Y%m%d')
        instrument_name = self.extcsv['INSTRUMENT']['Name']
        instrument_model = self.extcsv['INSTRUMENT']['Model']

        extcsv_serial = self.extcsv['INSTRUMENT'].get('Number', None)
        instrument_number = extcsv_serial or 'na'

        agency = self.extcsv['DATA_GENERATION']['Agency']

        filename = '{}.{}.{}.{}.{}.csv'.format(timestamp, instrument_name,
                                               instrument_model,
                                               instrument_number, agency)
        if ' ' in filename:
            LOGGER.warning('filename contains spaces: {}'.format(filename))
            file_slug = filename.replace(' ', '-')
            LOGGER.info('filename {} renamed to {}'
                        .format(filename, file_slug))
            filename = file_slug

        return filename

    def collimate_tables(self, tables, schema):
        """
        Convert the lists of raw strings in all the tables in <tables>
        into processed values of an appropriate type.

        Ensure that all one-row tables have their single value reported
        for each field rather than a list containing the one value.

        Assumes that all tables in <tables> are demonstratedly valid.

        :param tables: list of tables in which to process columns.
        :param schema:series of table definitions for the input file.
        :returns: void
        """

        for table_name in tables:
            table_type = table_name.rstrip('0123456789_')
            body = self.extcsv[table_name]

            table_valueline = self.line_num(table_name) + 2

            for field, column in body.items():
                if field != 'comments':
                    converted = [
                        self.typecast_value(table_name, field, val, line)
                        for line, val in enumerate(column, table_valueline)
                    ]
                    if schema[table_type]['rows'] == 1:
                        if not converted:
                            converted.append(None)
                        self.extcsv[table_name][field] = converted[0]
                    else:
                        self.extcsv[table_name][field] = converted

    def check_table_occurrences(self, schema):
        """
        Validate the number of occurrences of each table type in <schema>
        against the expected range of occurrences for that type.
        Returns True iff all tables occur an acceptable number of times.

        :param schema: series of table definitions for the input file.
        :returns: `bool` of whether all tables are within the expected
                  occurrence range.
        """

        success = True

        for table_type in schema.keys():
            if table_type != 'data_table':
                count = self.table_count(table_type)
                occurrence_range = str(schema[table_type]['occurrences'])
                lower, upper = parse_integer_range(occurrence_range)
                if count < lower:
                    line = self.line_num(table_type + '_' + str(count))
                    if not self._add_to_report(213, line, table=table_type,
                                               bound=lower):
                        success = False
                if count > upper:
                    line = self.line_num(table_type + '_' + str(upper + 1))
                    if not self._add_to_report(214, line, table=table_type,
                                               bound=upper):
                        success = False

        return success

    def check_table_height(self, table, definition, num_rows):
        """
        Validate the number of rows in the table named <table> against the
        expected range of rows assuming the table has <num_rows> rows.
        Returns True iff the table has an acceptable number of rows.

        :param table: name of a table in the input file.
        :param definition: schema definition of <table>.
        :param num_rows: number of rows in <table>.
        :returns: `bool` of whether all tables are in the expected
                  height range.
        """

        height_range = str(definition['rows'])
        lower, upper = parse_integer_range(height_range)

        occurrence_range = str(definition['occurrences'])
        is_required, _ = parse_integer_range(occurrence_range)

        headerline = self.line_num(table)
        success = True

        if num_rows == 0 and lower != 0:
            if is_required:
                if not self._add_to_report(208, headerline, table=table):
                    success = False
            elif not self._add_to_report(209, headerline, table=table):
                success = False
        elif num_rows < lower:
            if not self._add_to_report(215, headerline, table=table,
                                       bound=lower):
                success = False
        elif num_rows > upper:
            if not self._add_to_report(216, headerline, table=table,
                                       bound=upper):
                success = False

        return success

    def check_field_validity(self, table, definition):
        """
        Validates the fields of the table named <table>, ensuring that
        all required fields are present, correcting capitalizations,
        creating empty columns for any optional fields that were not
        provided, and removing any unrecognized fields.

        Returns True if the table's fields satisfy its definition.

        :param table: name of a table in the input file.
        :param definition: schema definition for the table.
        :returns: `bool` of whether fields satisfy the table's definition.
        """

        success = True

        required = definition.get('required_fields', ())
        optional = definition.get('optional_fields', ())
        provided = self.extcsv[table].keys()

        required_case_map = {key.lower(): key for key in required}
        optional_case_map = {key.lower(): key for key in optional}
        provided_case_map = {key.lower(): key for key in provided}

        missing_fields = [field for field in required
                          if field not in provided]
        extra_fields = [field for field in provided
                        if field.lower() not in required_case_map]

        fieldline = self.line_num(table) + 1
        valueline = fieldline + 1

        arbitrary_column = next(iter(self.extcsv[table].values()))
        num_rows = len(arbitrary_column)
        null_value = [''] * num_rows

        # Attempt to find a match for all required missing fields.
        for missing in missing_fields:
            match_insensitive = provided_case_map.get(missing.lower(), None)
            if match_insensitive:
                if not self._add_to_report(105, fieldline, table=table,
                                           oldfield=match_insensitive,
                                           newfield=missing):
                    success = False

                self.extcsv[table][missing] = \
                    self.extcsv[table].pop(match_insensitive)
            else:
                if not self._add_to_report(203, fieldline, table=table,
                                           field=missing):
                    success = False
                self.extcsv[table][missing] = null_value

        if len(missing_fields) == 0:
            LOGGER.debug('No missing fields in table {}'.format(table))
        else:
            LOGGER.info('Filled missing fields with null string values')

        # Assess whether non-required fields are optional fields or
        # excess ones that are not part of the table's schema.
        for extra in extra_fields:
            match_insensitive = optional_case_map.get(extra.lower(), None)
            if match_insensitive:
                LOGGER.info('Found optional field #{}.{}'
                            .format(table, extra))

                if extra != match_insensitive:
                    if not self._add_to_report(105, fieldline, table=table,
                                               oldfield=extra,
                                               newfield=match_insensitive):
                        success = False

                    self.extcsv[table][match_insensitive] = \
                        self.extcsv[table].pop(extra)
            elif extra != 'comments':
                if not self._add_to_report(250, fieldline, table=table,
                                           field=extra):
                    success = False
                del self.extcsv[table][extra]

        # Check that no required fields have empty values.
        for field in required:
            column = self.extcsv[table][field]
            if isinstance(column, list) and '' in column:
                line = valueline + column.index('')
                if not self._add_to_report(
                        204, line, table=table, field=field):
                    success = False
        return success

    def number_of_observations(self):
        """
        Returns the total number of unique rows in the Extended CSV's
        data table(s).

        :returns: number of unique data rows in the file.
        """

        if not self._observations_table:
            try:
                self._determine_noncore_schema()
            except (NonStandardDataError, MetadataValidationError) as err:
                LOGGER.warning('Cannot identify data table due to: {}'
                               .format(err))
                return 0

        # Count lines in the file's data table(s)
        data_rows = set()
        for i in range(1, self.table_count(self._observations_table) + 1):
            table_name = self._observations_table + '_' + str(i) \
                if i > 1 else self._observations_table

            columns = OrderedDict()
            for i in self.extcsv[table_name]:
                if i != 'comments':
                    columns[i] = self.extcsv[table_name][i]
            rows = zip(*columns.values())
            data_rows.update(rows)
        return len(data_rows)

    def validate_metadata_tables(self):
        """validate core metadata tables and fields"""

        schema = DOMAINS['Common']
        success = True

        missing_tables = [table for table in schema
                          if table not in self.extcsv.keys()]
        present_tables = [table for table in self.extcsv.keys()
                          if table.rstrip('0123456789_') in schema]

        if len(present_tables) == 0:
            if not self._add_to_report(102):
                raise NonStandardDataError(self.errors)
        elif len(missing_tables) > 0:
            for missing in missing_tables:
                if not self._add_to_report(201, table=missing):
                    success = False
        else:
            LOGGER.debug('No missing metadata tables')

        if not success:
            msg = 'Not an Extended CSV file'
            raise MetadataValidationError(msg, self.errors)

        if not self.check_table_occurrences(schema):
            success = False

        for table in present_tables:
            table_type = table.rstrip('0123456789_')
            definition = schema[table_type]
            body = self.extcsv[table]

            arbitrary_column = list(body.items())[1][1]
            try:
                num_rows = len(arbitrary_column)
            except TypeError:
                LOGGER.info('Extended CSV already collimated')
                return

            if not self.check_field_validity(table, definition) \
               or not self.check_table_height(table, definition, num_rows):
                success = False

            for field in definition.get('optional_fields', []):
                if field not in self.extcsv[table]:
                    self.extcsv[table][field] = [''] * num_rows

        if success:
            self.collimate_tables(present_tables, schema)
            LOGGER.debug('All core tables in file validated.')
        else:
            raise MetadataValidationError('Invalid metadata', self.errors)

    def _determine_noncore_schema(self):
        """
        Sets the table definitions schema and observations data table
        for this Extended CSV instance, based on its CONTENT fields
        and which tables are present.

        :returns: void
        """

        tables = DOMAINS['Datasets']
        curr_dict = tables
        fieldline = self.line_num('CONTENT') + 1
        for field in [('Category', str), ('Level', float), ('Form', int)]:
            field_name, type_converter = field
            try:
                key = str(type_converter(self.extcsv['CONTENT'][field_name]))
            except ValueError:
                key = str(self.extcsv['CONTENT'][field_name])

            if key in curr_dict:
                curr_dict = curr_dict[key]
            else:
                field_name = '#CONTENT.{}'.format(field_name.capitalize())

                self._add_to_report(220, fieldline, field=field_name)
                return False

        if '1' in curr_dict.keys():
            version = self._determine_version(curr_dict)
            LOGGER.info('Identified version as {}'.format(version))
            curr_dict = curr_dict[version]

        self._noncore_table_schema = {k: v for k, v in curr_dict.items()}
        self._observations_table = self._noncore_table_schema.pop('data_table')

    def _determine_version(self, schema):
        """
        Attempt to determine which of multiple possible table definitions
        contained in <schema> fits the instance's Extended CSV file best,
        based on which required or optional tables are present.

        Returns the best-fitting version, or raises an error if there is
        not enough information.

        :param schema: dictionary with nested dictionaries of
                       table definitions as values.
        :returns: version number for the best-fitting table definition.
        """

        versions = set(schema.keys())
        tables = {version: schema[version].keys() for version in versions}
        uniques = {}

        for version in versions:
            u = set(tables[version])
            others = versions - {version}

            for other_version in others:
                u -= set(tables[other_version])

            uniques[version] = u

        candidates = {version: [] for version in versions}
        for table in self.extcsv:
            for version in versions:
                if table in uniques[version]:
                    candidates[version].append(table)

        def rating(version):
            return len(candidates[version]) / len(uniques[version])

        candidate_scores = list(map(rating, versions))
        best_match = max(candidate_scores)
        if best_match == 0:
            self._add_to_report(210)
            raise NonStandardDataError(self.errors)
        else:
            for version in versions:
                if rating(version) == best_match:
                    return version

    def validate_dataset_tables(self):
        """Validate tables and fields beyond the core metadata tables"""

        if not self._noncore_table_schema:
            self._determine_noncore_schema()

        schema = self._noncore_table_schema
        success = True

        if schema is None:
            success = False
            return success

        required_tables = [name for name, body in schema.items()
                           if 'required_fields' in body]
        optional_tables = [name for name, body in schema.items()
                           if 'required_fields' not in body]

        missing_tables = [table for table in required_tables
                          if table not in self.extcsv.keys()]
        present_tables = [table for table in self.extcsv.keys()
                          if table.rstrip('0123456789_') in schema]
        required_tables.extend(DOMAINS['Common'].keys())
        extra_tables = [table for table in self.extcsv.keys()
                        if table.rstrip('0123456789_') not in required_tables]

        dataset = self.extcsv['CONTENT']['Category']
        for missing in missing_tables:
            if not self._add_to_report(201, table=missing):
                success = False

        if not self.check_table_occurrences(schema):
            success = False

        for table in present_tables:
            table_type = table.rstrip('0123456789_')
            definition = schema[table_type]
            body = self.extcsv[table]

            arbitrary_column = list(body.items())[1][1]
            try:
                num_rows = len(arbitrary_column)
            except TypeError:
                LOGGER.info('Extended CSV already collimated')
                return
            if not self.check_field_validity(table, definition) \
               or not self.check_table_height(table, definition, num_rows):
                success = False

            LOGGER.debug('Finished validating table {}'.format(table))
        for table in extra_tables:
            if table.startswith("SAOZ_DATA_V"):
                table_type = table
            else:
                table_type = table.rstrip('0123456789_')
            if table_type not in optional_tables and table_type != 'comments':
                if not self._add_to_report(202, table=table, dataset=dataset):
                    success = False
                del self.extcsv[table]

        for table in optional_tables:
            if table not in self.extcsv:
                LOGGER.warning('Optional table {} is not in file.'.format(
                               table))

        if success:
            self.collimate_tables(present_tables, schema)
            schema['data_table'] = self._observations_table
            return success
        else:
            raise MetadataValidationError('Invalid dataset tables',
                                          self.errors)


class Reader(object):
    """WOUDC Extended CSV reader"""

    def __init__(self, content):
        """
        Parse WOUDC Extended CSV into internal data structure

        :param content: buffer of Extended CSV data
        :returns: `Reader` object
        """

        # Use ExtendedCSV class to facilitate reading
        ecsv = ExtendedCSV(content)
        self.ecsv = ecsv

    @property
    def extcsv(self):
        """
        Get internal data structure

        :returns: object's internal data structure (dictionary)
        """

        return self.ecsv.extcsv

    @property
    def _raw(self):
        """
        Get raw content in the Writer

        :returns: string of raw csv content
        """

        return self.ecsv._raw

    @property
    def noncore_table_schema(self):
        """
        :returns: table schema of the Writer
        """

        return self.ecsv._noncore_table_schema

    @property
    def observations_table(self):
        """
        :returns: name of the Writer's observations table
        """

        return self.ecsv._observations_table

    @property
    def file_comments(self):
        """
        :returns: list of file comments
        """

        return self.ecsv.file_comments

    @property
    def warnings(self):
        """
        :returns: list of file warnings
        """

        return self.ecsv.warnings

    @property
    def errors(self):
        """
        :returns: list of file errors
        """

        return self.ecsv.errors

    def line_num(self, table_name=None, index=1):
        """
        :returns: dictionary of the lines where each table in
        the Writer begins, or line number where <table_name> begins.
        """

        if table_name is not None:
            table_name_n = _table_index(table_name, index)
            try:
                return int(self.ecsv._line_num[table_name_n])
            except KeyError:
                msg = 'no such table: {}'.format(table_name_n)
                LOGGER.error(msg)
        else:
            return self.ecsv._line_num

    def table_count(self, table_name=None):
        """
        :returns: dictionary of the number of occurrences for
        each table in the Writer, or the number of occurrences
        of <table_name> .
        """

        if table_name is not None:
            try:
                return int(self.ecsv._table_count[table_name])
            except KeyError:
                msg = 'no such table: {}'.format(table_name)
                LOGGER.error(msg)
        else:
            return self.ecsv._table_count

    def dataset_validator(self):
        """
        Validate tables and fields beyond the core metadata tables

        :returns: bool for validation result
        """

        return self.ecsv.validate_dataset_tables()

    def metadata_validator(self):
        """
        Validate core metadata tables and fields

        :returns: bool for validation result
        """

        return self.ecsv.validate_metadata_tables()


class Writer(object):
    """WOUDC Extended CSV writer"""

    def __init__(self, ds=None, template=False):
        """
        Initialize a WOUDC Extended CSV writer

        :param ds: OrderedDict of WOUDC tables, fields, values and commons
        :param template: boolean to set default / common Extended CSV tables
        :returns: `Writer` object
        """

        # Use ExtendedCSV class to facilitate reading
        ecsv = ExtendedCSV('')
        self.ecsv = ecsv

        # init object with requested data structure
        if ds is not None:
            for table, fields in ds.items():
                t_toks = table.split('_')
                t = t_toks[0]
                t_index = t_toks[1]
                self.add_table(t)
                for field, values in fields.items():
                    self.add_field(t, field, index=t_index)
                    for value in values:
                        self.add_data(t, value, field, index=t_index)
        if template:  # init object with default metadata tables and fields
            self.add_field('CONTENT', 'Class,Category,Level,Form')
            self.add_field('DATA_GENERATION',
                           'Date,Agency,Version,ScientificAuthority')
            self.add_field('PLATFORM', 'Type,ID,Name,Country,GAW_ID')
            self.add_field('INSTRUMENT', 'Name,Model,Number')
            self.add_field('LOCATION', 'Latitude,Longitude,Height')
            self.add_field('TIMESTAMP', 'UTCOffset,Date,Time')

    @property
    def extcsv(self):
        """
        Get internal data structure

        :returns: object's internal data structure (dictionary)
        """

        return self.ecsv.extcsv

    @property
    def _raw(self):
        """
        Get raw content in the Writer

        :returns: string of raw csv content
        """

        self.ecsv._raw = self.serialize().getvalue()
        return self.ecsv._raw

    @property
    def noncore_table_schema(self):
        """
        :returns: table schema of the Writer
        """

        return self.ecsv._noncore_table_schema

    @property
    def observations_table(self):
        """
        :returns: name of the Writer's observations table
        """

        return self.ecsv._observations_table

    @property
    def file_comments(self):
        """
        :returns: list of file comments
        """

        return self.ecsv.file_comments

    @property
    def warnings(self):
        """
        :returns: list of file warnings
        """

        return self.ecsv.warnings

    @property
    def errors(self):
        """
        :returns: list of file errors
        """

        return self.ecsv.errors

    def line_num(self, table_name=None, index=1):
        """
        :returns: dictionary of the lines where each table in
        the Writer begins, or line number where <table_name> begins.
        """

        if table_name is not None:
            table_name_n = _table_index(table_name, index)
            try:
                return int(self.ecsv._line_num[table_name_n])
            except KeyError:
                msg = 'no such table: {}'.format(table_name_n)
                LOGGER.error(msg)
        else:
            return self.ecsv._line_num

    def table_count(self, table_name=None):
        """
        :returns: dictionary of the number of occurrences for
        each table in the Writer, or the number of occurrences
        of <table_name>.
        """

        if table_name is not None:
            try:
                return self.ecsv._table_count[table_name]
            except KeyError:
                msg = 'no such table: {}'.format(table_name)
                LOGGER.error(msg)
        else:
            return self.ecsv._table_count

    def add_comment(self, comment):
        """
        Add file level comments

        :param comment: file-level comment to be added.
        """

        self.ecsv.add_comment(comment)

    def add_table_comment(self, table, comment, index=1):
        """
        Add table level comments

        :param table: table name
        :param index: table index or grouping
        :param comment: table level comment to be added.
        """

        self.ecsv.add_table_comment(table, comment, index)

    def add_table(self, table, table_comment=None):
        """
        Add table to extcsv

        :param table: table name
        :param table_comment: table comment
        :param index: table index or grouping
        """

        try:
            last_table = max(self.line_num(), key=self.line_num().get)
            line_num = \
                max(len(v) for v in self.extcsv[last_table].values()) + \
                self.line_num()[last_table] + \
                len(self.extcsv[last_table]['comments']) + 2
        except ValueError:
            line_num = 0
        self.ecsv.init_table(table, [], line_num)

        if table_comment is not None:
            index = self.table_count(table)
            self.ecsv.add_table_comment(table, table_comment, index)

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
        if table_n not in self.extcsv.keys():
            self.add_table(table)

        # list input
        if isinstance(field, list):
            self.ecsv.add_field_to_table(table, field, index)

        # string input
        elif delimiter not in field:  # vertical insert
            self.ecsv.add_field_to_table(table, [field], index)

        else:  # horizontal insert
            str_obj = StringIO(field)
            csv_reader = csv.reader(str_obj, delimiter=delimiter)
            fields = next(csv_reader)
            for field in fields:
                self.ecsv.add_field_to_table(table, [field], index)

    def add_data(self, table, data, field=None, index=1, delimiter=',',
                 table_comment=None):
        """
        General-use function for data addition to Extended CSV table field

        :param table: table name
        :data: list of data
        :param field: field name
        :param index: table index or grouping
        """

        table_n = _table_index(table, index)
        # add table if not present
        if table_n not in self.extcsv.keys():
            self.add_table(table)

        # add table comment
        if table_comment is not None:
            self.add_table_comment(table, table_comment, index)

        # add values
        if field is not None:  # fields are specified
            # convert to list
            if not isinstance(field, list):
                field_l = field.split(delimiter)
            else:
                field_l = field
            for f in field_l:  # add field if not present
                if f not in list(self.extcsv[table_n]):
                    self.add_field(table, [f], index, delimiter)
        else:  # field is None: grab all keys from table
            field_l = list(self.extcsv[table_n].keys())[1:]

        # check data
        if not isinstance(data, list):
            str_obj = StringIO(data)
            csv_reader = csv.reader(str_obj, delimiter=delimiter)
            data_l = next(csv_reader)
        else:
            data_l = data
        if len(data_l) != len(field_l) and len(field_l) == 1:
            if len(field_l) == 1:  # vertical insert
                try:
                    last_table = max(self.line_num(), key=self.line_num().get)
                    line_num = \
                        max(
                          len(v) for v in self.extcsv[last_table].values()
                        ) + \
                        self.line_num()[last_table] + \
                        len(self.extcsv[last_table]['comments']) + 2
                except ValueError:
                    line_num = 2
                self.ecsv.add_values_to_table(table_n, data_l, line_num,
                                              fields=field_l, horizontal=False)
            else:
                msg = 'fields / values mismatch: {}:{}; skipping' \
                    .format(field_l, data_l)
                LOGGER.error(msg)
        else:  # horizontal insert
            try:
                try:
                    last_table = max(self.line_num(), key=self.line_num().get)
                    line_num = \
                        max(
                          len(v) for v in self.extcsv[last_table].values()
                        ) + \
                        self.line_num()[last_table] + \
                        len(self.extcsv[last_table]['comments']) + 2
                except ValueError:
                    line_num = 2
                self.ecsv.add_values_to_table(table_n, data_l, line_num,
                                              fields=field_l)
            except Exception as err:
                msg = 'unable to add data {}'.format(err)
                LOGGER.error(msg)

    def remove_table(self, table, index=1):
        """
        Remove table from extcsv

        :param table: table name
        :param index: table index or grouping
        """

        self.ecsv.remove_table(table, index)

    def remove_field(self, table, field, index=1):
        """
        Remove field from extcsv

        :param table: table name
        :param field: field name
        :param index: table index or grouping
        """

        self.ecsv.remove_field(table, field, index)

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

        self.ecsv.remove_values_from_table(table, field, data, index,
                                           d_index, all_occurences)

    def clear_file(self):
        """
        Remove all tables from Extended CSV Writer
        """

        try:
            self.ecsv.clear()
        except Exception as err:
            msg = 'Could not clear Extended CSV: %s' % err
            LOGGER.error(msg)

    def clear_table(self, table, index=1):
        """
        Clear table (all fields except table comments)

        :param table: table name
        :param index: index name
        """

        try:
            self.ecsv.clear_table(table, index)
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

        try:
            self.clear_field(table, field, index)
        except Exception as err:
            msg = 'could not clear field %s' % err
            LOGGER.error(msg)

    def inspect_field(self, table, field, index=1):
        """
        Return the content of a field

        :param table: table name
        :param field: field name
        :param index: table index
        :returns: list of value(s) at extcsv table field
        """

        table_n = _table_index(table, index)
        return self.extcsv[table_n][field]

    def dataset_validator(self):
        """
        Validate tables and fields beyond the core metadata tables

        :returns: bool for validation result
        """

        return self.ecsv.validate_dataset_tables()

    def metadata_validator(self):
        """
        Validate core metadata tables and fields

        :returns: bool for validation result
        """

        return self.ecsv.validate_metadata_tables()

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

        for table, fields in self.extcsv.items():
            try:
                first_num = [t.isdigit() for t in table].index(True)
                mem_file.write('#%s%s' % (table[0: first_num - 1], os.linesep))
            except ValueError:
                mem_file.write('#%s%s' % (table, os.linesep))
            t_comments = fields['comments']
            row = list(fields.keys())[1:]
            csv_writer.writerow(row)
            values = list(fields.values())[1:]
            max_len = 1
            try:
                if isinstance(values[0], list):
                    max_len = len(list(max(values, key=len)))
            except TypeError:
                max_len = 1
            for i in range(0, max_len):
                row = []
                for value in values:
                    try:
                        if isinstance(value, list):
                            row.append(value[i])
                        else:
                            row.append(value)
                    except IndexError:
                        row.append('')
                # clean up row
                for j in range(len(row) - 1, 0, -1):
                    if row[j] != '':
                        break
                    else:
                        row.pop(j)
                csv_writer.writerow(row)
            if t_comments is not None and len(t_comments) > 0:
                for comment in t_comments:
                    mem_file.write('* %s%s' % (comment, os.linesep))
            len1 = list(self.extcsv.keys()).index(table)
            len2 = len(self.extcsv.keys()) - 1
            if len1 != len2:
                mem_file.write('%s' % os.linesep)

        self.ecsv._raw = mem_file.getvalue()
        return mem_file


class NonStandardDataError(Exception):
    """custom exception handler"""

    def __init__(self, errors):
        error_string = '\n' + '\n'.join(map(str, errors))
        super(NonStandardDataError, self).__init__(error_string)

        self.errors = errors


class MetadataValidationError(Exception):
    """custom exception handler"""

    def __init__(self, message, errors):
        """set error list/stack"""
        super(MetadataValidationError, self).__init__(message)
        self.errors = errors
