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

import unicodecsv as csv
import logging
import os
import re
import io
from sets import Set
from StringIO import StringIO
from collections import OrderedDict
from pywoudc import WoudcClient

__version__ = '0.2.1'

LOGGER = logging.getLogger(__name__)

__dirpath = os.path.dirname(os.path.realpath(__file__))

TABLE_CONFIGURATION = os.path.join(__dirpath, 'table_configuration.csv')


class Reader(object):
    """WOUDC Extended CSV reader"""

    def __init__(self, content, parse_tables=False, encoding='utf-8'):
        """
        Parse WOUDC Extended CSV into internal data structure

        :param content: string buffer of content
        :param parse_tables: if True multi-row tables will be parsed into
            a list for each column, otherwise will be left as raw strings
        :param encoding: the encoding scheme with which content is encoded
        """

        meta_fields = [
            'CONTENT',
            'DATA_GENERATION',
            'PLATFORM',
            'INSTRUMENT',
            'LOCATION',
            'TIMESTAMP',
            'MONTHLY',
            'VEHICLE',
            'FLIGHT_SUMMARY',
            'GLOBAL_SUMMARY',
            'DAILY_SUMMARY',
            'GLOBAL_DAILY_SUMMARY',
            'OZONE_SUMMARY',
            'AUXILIARY_DATA'
        ]

        self.sections = {}
        self.metadata_tables = []
        self.data_tables = []
        self.all_tables = []
        self.comments = {}
        self.updated = False
        self.errors = []

        LOGGER.info('processing Extended CSV')
        blocks = self.decompose_extcsv(content)
        self.table_count = {}
        for header, body in blocks:
            # determine delimiter
            if '::' in body:
                body.replace('::', ',')
            if ';' in body:
                body.replace(';', ',')
            if '$' in body:
                body.replace('$', ',')
            if '%' in body:
                body.replace('%', ',')
            try:
                s = StringIO(body)
                c = csv.reader(s, encoding=encoding)
            except Exception as err:
                self.errors.append(_violation_lookup(0))
            if header in meta_fields:  # metadata
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
                self.sections[header]['_raw'] = body
                try:
                    fields = c.next()
                    if len(fields) == 0:
                        self.errors.append(_violation_lookup(10))
                    elif len(fields[0]) > 0 and fields[0][0] == '*':
                        self.errors.append(_violation_lookup(8))
                except StopIteration:
                    msg = 'Extended CSV table %s has no fields' % header
                    LOGGER.info(msg)
                    self.errors.append(_violation_lookup(140, header))
                values = None
                try:
                    values = c.next()
                    if len(values) == 0:
                        self.errors.append(_violation_lookup(10))
                    elif len(values[0]) > 0 and values[0][0] == '*':
                        self.errors.append(_violation_lookup(8))
                except StopIteration:
                    msg = 'Extended CSV table %s has no values' % header
                    LOGGER.info(msg)
                    self.errors.append(_violation_lookup(140, header))
                    continue
                try:
                    anything_more = (c.next()[0]).strip()
                    if all([anything_more is not None, anything_more != '',
                            anything_more != os.linesep,
                            '*' not in anything_more]):
                        self.errors.append(_violation_lookup(140, header))
                except Exception as err:
                    LOGGER.warning(err)
                if len(values) > len(fields):
                    self.errors.append(_violation_lookup(7, header))
                    continue
                i = 0
                for field in fields:
                    field = field.strip()
                    try:
                        self.sections[header][field] = (values[i]).strip()
                        i += 1
                    except (KeyError, IndexError):
                        self.sections[header][field] = ''
                        msg = 'corrupt format section %s skipping' % header
                        LOGGER.debug(msg)
            else:  # payload
                buf = StringIO(None)
                w = csv.writer(buf)
                table = {}
                columns = None
                try:
                    columns = c.next()
                    if len(columns) == 0:
                        self.errors.append(_violation_lookup(10))
                    elif len(columns[0]) > 0 and columns[0][0] == '*':
                        self.errors.append(_violation_lookup(8))
                    if parse_tables:
                        table = {col: [] for col in columns}
                    w.writerow(columns)
                except StopIteration:
                    msg = 'Extended CSV table %s has no fields' % header
                    LOGGER.info(msg)
                    self.errors.append(_violation_lookup(140, header))
                for row in c:
                    if all([row != '', row is not None, row != []]):
                        if '*' not in row[0]:
                            w.writerow(row)
                            # Extend the table dictionary if this row is a
                            # data row.
                            if parse_tables:
                                # Fill in omitted columns with null strings.
                                unlisted_size = len(columns) - len(row)
                                row.extend([''] * unlisted_size)
                                for col, datapoint in zip(columns, row):
                                    table[col].append(datapoint)
                        else:
                            if columns[0].lower() == 'time':
                                self.errors.append(_violation_lookup(21))
                if header not in self.sections:
                    self.all_tables.append(header)
                    self.data_tables.append(header)
                    self.table_count[header] = 1
                else:
                    self.table_count[header] = self.table_count[header] + 1
                    header = '%s%s' % (header, self.table_count[header])
                    self.data_tables.append(header)
                table['_raw'] = buf.getvalue()
                self.sections[header] = table
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

        # check for required table presence
        if 'CONTENT' not in self.metadata_tables:
            self.errors.append(_violation_lookup(1, 'CONTENT'))
        if 'DATA_GENERATION' not in self.metadata_tables:
            self.errors.append(_violation_lookup(1, 'DATA_GENERATION'))
        if 'INSTRUMENT' not in self.metadata_tables:
            self.errors.append(_violation_lookup(1, 'INSTRUMENT'))
        if 'PLATFORM' not in self.metadata_tables:
            self.errors.append(_violation_lookup(1, 'PLATFORM'))
        if 'LOCATION' not in self.metadata_tables:
            self.errors.append(_violation_lookup(1, 'LOCATION'))
        if 'TIMESTAMP' not in self.metadata_tables:
            self.errors.append(_violation_lookup(1, 'TIMESTAMP'))

        if len(self.errors) != 0:
            self.errors = list(set(self.errors))
            error_text = '; '.join(map(str, self.errors))
            msg = 'Unable to parse extended CSV file.'
            raise WOUDCExtCSVReaderError(msg + ' ' + error_text, self.errors)

    def __eq__(self, other):
        """
        equals builtin

        :param other: object to be compared
        :returns: boolean result of comparison
        """

        return self.__dict__ == other.__dict__

    def decompose_extcsv(self, raw):
        """
        Identify tables within ExtCSV file contents, returning an iterable
        of pairs in which the first value is the table's name and the second
        value is the table's body.

        Robust against the divider character # appearing within comments or
        table data.

        :param contents: the untouched contents of an ExtCSV file.
        """

        # Split on # characters, at the start of a line, and any following
        # sequence of capital letters and underscores.
        blocks = re.split(r'(?<![ \w\d])#([A-Z][A-Z0-9_]*)', raw)
        if len(blocks) < 2:
            LOGGER.error('No tables found.')

        # Discard (but check) any comment at the top of the file.
        lead_comment = blocks.pop(0)
        c = StringIO(lead_comment.strip())
        for line in c:
            if all([line.strip() != '', line.strip() != os.linesep,
                    line[0] != '*']):
                self.errors.append(_violation_lookup(9))

        headers = []
        content = []
        for i in range(0, len(blocks), 2):
            headers.append(blocks[i])
            content.append(blocks[i + 1].strip())

        return zip(headers, content)

    def metadata_validator(self):
        """
        Robust validator to check metadata against values
        in WOUDC database. Returns a dictionary of
        errors or warnings depending on severity of violation.

        :returns: Dictionary of the form:
                  {'status' : True if validation passed,
                              False otherwise.
                   'warnings' : a list of warnings.
                   'errors': a list of errors.
                  }
        """
        error_dict = {'status': False, 'warnings': [], 'errors': []}

        client = WoudcClient()

        # Attempt basic validation
        LOGGER.debug('Attempting basic table validation.')
        error, violations = global_validate(self.sections)
        if error:
            error_dict['errors'].append('The following violations were \
found: \n %s' % '\n'.join(violations))
            return error_dict

        LOGGER.info('Basic table validation passed.')

        # Check that Content Category and Level are Valid
        LOGGER.debug('Checking Content Category and Level.')
        category = self.sections['CONTENT']['Category']
        level = self.sections['CONTENT']['Level']
        if category != 'UmkehrN14' and level != '1.0' and level != '1':
            error_dict['errors'].append('Level for category %s must \
be 1.0' % category)
            return error_dict

        if category == 'UmkehrN14' and 'N14_VALUES' in self.sections and level != '1.0' and level != '1': # noqa
            error_dict['errors'].append('Level for category UmkehrN14 \
with table N14_VALUES must be 1.0')
            return error_dict

        if category == 'UmkehrN14' and 'C_PROFILE' in self.sections and level != '2.0' and level != '2': # noqa
            error_dict['errors'].append('Level for category UmkehrN14 \
with table N14_VALUES must be 1.0')
            return error_dict

        LOGGER.info('Content Category and Level are valid.')

        # Attempt further basic validation given category and level
        LOGGER.debug('Attempting specific table validation for category %s and level %s.' % (category, level)) # noqa
        error, violations = global_validate(self.sections, category, '1' if level == '1.0' else '2', '1') # noqa
        if error:
            error_dict['errors'].append('The following violations were \
found: \n %s' % '\n'.join(violations))
            return error_dict

        LOGGER.info('Further table validation passed.')

        # Check agency and platform information
        LOGGER.debug('Resolving Agency and Platform information.')
        f_type = self.sections['PLATFORM']['Type']
        f_ID = self.sections['PLATFORM']['ID']
        f_name = self.sections['PLATFORM']['Name'].encode('utf-8')
        f_country = self.sections['PLATFORM']['Country']
        f_gaw_id = None
        try:
            f_gaw_id = self.sections['PLATFORM']['GAW_ID']
        except Exception:
            error_dict['warnings'].append('GAW_ID field is \
spelled incorrectly.')
        f_agency = self.sections['DATA_GENERATION']['Agency']
        f_lat = float(self.sections['LOCATION']['Latitude'])
        f_lon = float(self.sections['LOCATION']['Longitude'])

        agency_params = {'property_name': 'acronym', 'property_value': f_agency} # noqa
        platform_id_params = {'property_name' : 'platform_id', 'property_value' : f_ID} # noqa
        platform_name_params = {'property_name' : 'platform_name', 'property_value' : f_name} # noqa
        if client.get_data('stations', **agency_params) is None:
            agency_params['property_name'] = 'contributor_name'
            data = client.get_data('stations', **agency_params)
            if data is None:
                acronym_set = Set()
                LOGGER.debug('Resolving Agency through platform ID.')
                data = client.get_data('stations', **platform_id_params)
                if data is not None:
                    for row in data['features']:
                        properties = row['properties']
                        acronym_set.add(properties['acronym'])
                LOGGER.debug('Resolving Agency through platform name.')
                data = client.get_data('stations', **platform_name_params)
                if data is not None:
                    for row in data['features']:
                        properties = row['properties']
                        acronym_set.add(properties['acronym'])

                if acronym_set != Set():
                    LOGGER.info('Possible Agency matches found.')
                    error_dict['errors'].append('The following agencies \
match the given platform name and/or ID: %s' % ','.join(list(acronym_set)))
                    return error_dict

                LOGGER.info('No Agency matches found.')
                error_dict['errors'].append('Agency acronym of %s not \
found in the woudc database. If this is a new agency, \
please notify WOUDC' % f_agency)
                return error_dict
            else:
                LOGGER.info('Agency name used instead of acronym.')
                acronym = data['features'][0]['properties']['acronym']
                error_dict['errors'].append('Please use the \
Agency acronym of %s.' % acronym) # noqa
                return error_dict

        LOGGER.info('Successfully validated Agency.')
        LOGGER.debug('Resolving platform information.')
        data = client.get_data('stations', **platform_id_params)
        flag = False
        a_set = Set()
        if data is not None:
            for row in data['features']:
                properties = row['properties']
                a_set.add(properties['acronym'])
                if properties['acronym'] == f_agency:
                    if properties['platform_type'] != f_type:
                        error_dict['errors'].append('Platform type \
of %s does not match database. Please change it \
to %s' % (f_type, properties['platform_type']))
                        return error_dict
                    if properties['country_code'] != f_country:
                        error_dict['errors'].append('Platform country \
of %s does not match database. Please change it \
to %s' % (f_country, properties['country_code']))
                        return error_dict
                    if properties['platform_name'].encode('utf-8') != f_name:
                        error_dict['errors'].append('Platform name \
of %s does not match database. Please change it \
to %s' % (f_name, properties['platform_name'].encode('utf-8')))
                        return error_dict
                    if abs(float(row['geometry']['coordinates'][0]) - f_lon) >= 1: # noqa
                        error_dict['errors'].append('Location Longitude \
of %s does not match database. Please change it \
to %s.' % (f_lon, row['geometry']['coordinates'][0]))
                        return error_dict
                    if abs(float(row['geometry']['coordinates'][1]) - f_lat) >= 1: # noqa
                        error_dict['errors'].append('Location Latitude \
of %s does not match database. Please change it \
to %s.' % (f_lat, row['geometry']['coordinates'][1]))
                        return error_dict
                    if properties['gaw_id'] != f_gaw_id:
                        error_dict['warnings'].append('Platform GAW_ID \
of %s does not match database. Please change it \
to %s' % (f_gaw_id, properties['gaw_id']))
                    LOGGER.info('Successfully validated platform.')
                    flag = True
        if not flag:
            data = client.get_data('stations', **platform_name_params)
            if data is not None:
                for row in data['features']:
                    properties = row['properties']
                    a_set.add(properties['acronym'])
                    if properties['acronym'] == f_agency:
                        if properties['platform_type'] != f_type:
                            error_dict['errors'].append('Platform type \
of %s does not match database. Please change it \
to %s' % (f_type, properties['platform_type']))
                            return error_dict
                        if properties['country_code'] != f_country:
                            error_dict['errors'].append('Platform country \
of %s does not match database. Please change it \
to %s' % (f_country, properties['country_code']))
                            return error_dict
                        if properties['platform_id'] != f_ID:
                            error_dict['errors'].append('Platform ID \
of %s does not match database. Please change it \
to %s' % (f_ID, properties['platform_id']))
                            return error_dict
                        if abs(float(row['geometry']['coordinates'][0]) - f_lon) >= 1: # noqa
                            error_dict['errors'].append('Location Longitude \
of %s does not match database. Please change it \
to %s.' % (f_lon, row['geometry']['coordinates'][0]))
                            return error_dict
                        if abs(float(row['geometry']['coordinates'][1]) - f_lat) >= 1: # noqa
                            error_dict['errors'].append('Location Latitude \
of %s does not match database. Please change it \
to %s.' % (f_lat, row['geometry']['coordinates'][1]))
                            return error_dict
                        if properties['gaw_id'] != f_gaw_id:
                            error_dict['warnings'].append('Platform GAW_ID \
of %s does not match database. Please change it \
to %s' % (f_gaw_id, properties['gaw_id']))
                        LOGGER.info('Successfully validated platform.')
                        flag = True
            if not flag:
                LOGGER.info('Failed to validate platform.')
                if len(a_set) > 0:
                    error_dict['errors'].append('Agency and Platform \
information do not match. These agencies are valid for this \
platform: %s' % (','.join(list(a_set))))
                    return error_dict
                else:
                    error_dict['errors'].append('Could not find a record \
for either the platform name or ID. If this is a new station, \
please notify WOUDC.')
                    return error_dict

        # Check existence of instrument Name and model
        LOGGER.debug('Resolving Instrument information.')
        inst_name = self.sections['INSTRUMENT']['Name'].lower()
        inst_model = self.sections['INSTRUMENT']['Model']
        inst_model_upper = inst_model.upper()
        inst_name_params = {'property_name': 'instrument_name', 'property_value': inst_name} # noqa
        inst_model_params = {'property_name': 'instrument_model', 'property_value': inst_model} # noqa
        data = client.get_data('instruments', **inst_name_params)
        if data is None:
            LOGGER.info('Failed to located Instrument name.')
            error_dict['errors'].append('Instrument Name is not in database. \
Please verify that it is correct.\nNote: If the instrument name is valid, \
this file will be rejected and then manually processed into the database.')
            return error_dict
        else:
            # Check if a new uri needs to be generated
            found = False
            for row in data['features']:
                properties = row['properties']
                if properties['contributor_id'] == f_agency and properties['data_category'] == category and properties['data_level'] == float(level): # noqa
                    found = True
            if not found:
                error_dict['warnings'].append('This is a new instrument \
class/data_category/data_level for this agency.\nThe \
file will be rejected and then manually processed into the database.')

        data = client.get_data('instruments', **inst_model_params)
        if data is None:
            inst_model_params['property_value'] = inst_model_upper
            data = client.get_data('instruments', **inst_model_params)
            if data is None:
                inst_model_params['property_value'] = inst_model.title()
                data = client.get_data('instruments', **inst_model_params)
                if data is None:
                    LOGGER.info('Failed to located Instrument model.')
                    error_dict['errors'].append('Instrument Model \
is not in database. Please verify that it is correct.\nNote: If \
the instrument model is valid, this file will be rejected and then \
manually processed into the database.')
                    return error_dict

        LOGGER.info('Instrument verification passed.')
        # Check for trailing commas in payload
        LOGGER.debug('Checking payload for trailing commas.')
        payload_tables = []
        for table in self.sections.keys():
            if len(self.sections[table].keys()) == 1:
                payload_tables.append(table)

        for payload_table in payload_tables:
            payload_lines = self.sections[payload_table]['_raw'].split('\n')
            header_len = len(payload_lines[0].strip().strip(',').split(',')) - 1 # noqa
            fewer_commas = False
            for line in payload_lines:
                if line.count(',') > header_len:
                    LOGGER.info('Found trailing commas.')
                    error_dict['errors'].append('This file has extra \
trailing commas. Please remove them before submitting.\nFirst line in \
file with trailing commas:\n%s' % line)
                    return error_dict
                if line.count(',') < header_len and line.strip() != '':
                    fewer_commas = True

        if fewer_commas:
            error_dict['warnings'].append('Some lines in this file have \
fewer commas than there are headers.\nPlease consider adding in extra \
commas for readability.')
        LOGGER.info('No trailing commas found.')
        LOGGER.info('This file passed validation.')
        error_dict['status'] = True
        return error_dict


class WOUDCExtCSVReaderError(Exception):
    """WOUDC extended CSV reader error"""

    def __init__(self, message, errors):
        """provide an error message and error stack"""
        super(WOUDCExtCSVReaderError, self).__init__(message)
        self.errors = errors


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
        0: 'Malformed CSV block detected',
        1: 'Missing required table name: $$$',
        2: 'Table name: $$$ is not from approved list',
        3: 'Missing required field name: $$$',
        4: 'Field name: $$$, is not from approved list',
        5: 'Improper delimiter found (";" or ":" or "$" or "%") '
           'corrected to "," (comma)',
        6: 'Unknown delimiter found.  Delimiter must be "," (comma)',
        7: 'Number of values is greater than number of fields in table: $$$',
        8: 'Remarks - cannot be between TABLE names and Field names nor '
           'between Field names and values of field',
        9: 'Cannot identify data, possibly a remark, '
           'but no asterisk (*) used',
        10: 'Empty rows within a table are not allowed between TABLE names '
            'and Field names nor between Field names and values of field',
        21: 'Improper separator for observation time(s) is used. '
            'Separator for time must be \'-\' (hyphen)',
        140: 'Incorrectly formatted table: $$$. '
             'Table does not contain exactly 3 lines.'
    }
    if rpl_str is not None:
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
            csv_reader = csv.reader(out_file)
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

def load(filename, parse_tables=False):
    """
    Load Extended CSV from file

    :param filename: filename
    :param parse_tables: if True multi-row tables will be parsed into
        a list for each column, otherwise will be left as raw strings
    :returns: Extended CSV data structure
    """

    with io.open(filename, 'rb') as ff:
        content = ff.read()
        try:
            return Reader(content, parse_tables, encoding='utf-8')
        except UnicodeDecodeError:
            LOGGER.info('Unable to read %s with utf8 encoding: '
                        'attempting to read with latin1 encoding.' % filename)
            return Reader(content, parse_tables, encoding='latin1')


def loads(strbuf, parse_tables=False, encoding='utf-8'):
    """
    Load Extended CSV from string

    :param strbuf: string representation of Extended CSV
    :param parse_tables: if True multi-row tables will be parsed into
        a list for each column, otherwise will be left as raw strings
    :param encoding: the encoding scheme with which content is encoded
    :returns: Extended CSV data structure
    """

    return Reader(strbuf, parse_tables=parse_tables, encoding=encoding)


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


class ExtCSVValidatorException(Exception):
    """
    ExtCSV Validator Exception stub.
    """
    pass


def global_validate(dict, category=None, level=None, form=None):
    """
    Validate extcsv for common/metadata tables and fields.
    Duplicate of validator in writer class with some small
    changes to accomodate the reader dict object.

    :returns: list of error codes and violations
    """
    error = False
    violations = []
    if category is not None and level is not None and form is not None:
        rules = table_configuration_lookup(category, level, form)
    else:
        rules = table_configuration_lookup('common')
    for rule in rules:
        table = rule['table'][1:]
        table_required = rule['table_required']
        fields = rule['fields'].split(',')
        fields = [x.lower().strip() for x in fields]
        optional_fields = rule['optional_fields'].split(',')
        optional_fields = [x.lower() for x in optional_fields]
        for f in optional_fields:
            if f in fields:
                fields.remove(f)
        # check required table
        if all([table not in dict.keys(),
                table_required == 'required']):
            if rule['incompatible_table'][1:] not in dict.keys():
                print rule['incompatible_table']
                violations.append(_violation_lookup(1, table))
                error = True
                for field in fields:
                    if field not in optional_fields:
                        error = True
                        violations.append(_violation_lookup(3, field))
        else:
            # check required fields
            if table in dict:
                fields_in = dict[table]
                if category is not None and len(fields_in.keys()) == 1:
                    fields_in = fields_in['_raw'].split('\n')[0].strip().split(',') # noqa
                    fields_in = [x.lower() for x in fields_in]
                else:
                    fields_in = [x.lower() for x in fields_in]
                    fields_in.remove('_raw')
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
                rule['incompatible_table'] in dict.keys()]):
            error = True
            violations.append(_violation_lookup(2, table))

    return [error, violations]
