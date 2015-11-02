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

# Until functions

import os
import csv
import logging
from sets import Set
from StringIO import StringIO

LOGGER = logging.getLogger(__name__)

def validate_extcsv(extcsv):
    """
    validate extcsv for common/metadata tables and fields
    """
    error = False
    violations = []
    rules = table_configuration_lookup('common')
    for rule in rules:
        table = rule['table']
        table = '%s$1' % table[1:]
        table_required = rule['table_required']
        occurance = rule['occurance']
        incompat_table = rule['incompat_table']
        fields = rule['fields'].split(',')
        fields = [x.lower().strip() for x in fields]
        all_field_requird = rule['all_field_requird']
        optional_fields = rule['optional_fields'].split(',')
        optional_fields = [x.lower() for x in optional_fields]
        for f in optional_fields:
            if f in fields:
                fields.remove(f)
        # check required table
        if all([
            table not in extcsv.keys(),
            table_required == 'required'
            ]):
            violations.append(1)
            error = True
            for field in fields:
                if field not in optional_fields:
                    error = True
                    violations.append(3)
                    #update_report(self.report, 3, {'FIELD NAME': field})
        else:
            # check required fields
            fields_in = extcsv[table]
            fields_in = [x.lower() for x in fields_in]
            a = Set(fields_in)
            b = Set(fields)
            c = a ^ b
            while len(c) > 0:
                item = c.pop()
                if all([ # unrecognized field name
                    item not in optional_fields,
                    item in fields_in,
                    item != 'comments'
                    ]): 
                    error = True
                    violations.append(4)
                    #update_report(self.report, 4, {'FIELD NAME': item})
                if item in fields:
                    error = True
                    violations.append(3)
                    #update_report(self.report, 3, {'FIELD NAME': item})
        
        if all([
            rule['incompat_table'] is not None,
            rule['incompat_table'] in extcsv.keys()
            ]):
            error = True
            violations.append(2)
            #update_report(self.report, 2, {'TABLE NAME': table})
            
    return [error, violations]


def serialize_extcsv(extcsv, path=None, to_file=False):
    """
    write extcsv to file
    """
    out_path = None
    mem_file = StringIO()
    extcsv_ds = extcsv.extcsv_ds
    # write to string buffer
    csv_writer = csv.writer(mem_file)
    if len(extcsv.file_comments) != 0:
        for comment in extcsv.file_comments:
            mem_file.write('* %s%s' % (comment, os.linesep))
        mem_file.write('%s' % os.linesep)
    for table, fields in extcsv_ds.iteritems():
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
            for j in range(len(row)-1, 0, -1):
                if row[j] != '':
                    break
                else:
                    row.pop(j)
            csv_writer.writerow(row)
        if len(t_comments) > 0:
            for comment in t_comments:
                mem_file.write('* %s%s' % (comment, os.linesep))
        if extcsv_ds.keys().index(table) != len(extcsv_ds.keys()) - 1:
            mem_file.write('%s' % os.linesep)

    #write to file, if asked
    if to_file:
        if extcsv.filename is not None:
            if path is not None:
                if os.path.isdir(path):
                    out_path = path
                else:
                    msg = 'Invalid path provided: %s' % path
                    LOGGER.error(msg)
            else:
                out_path = extcsv.filename
            LOGGER.info('Path to output file: %s', out_path)
            if out_path is not None:
                try:
                    with open(out_path, 'w') as out_file:
                        out_file.write(mem_file.getvalue())
                except Exception, err:
                    msg = 'Unable to create file at %s to write extended CSV, due to: %s' % (out_path, str(err))
                    LOGGER.error(msg)
        else:
            msg = 'No filename provided for extended CSV. Please set filename: extcsv_obj.filename=<filename>.'
            LOGGER.error(msg)

    return mem_file
    
def setup_logger(logfile, loglevel):
    """Setup logging mechanism"""

    # regular logging format
    msg_format = '[%(asctime)s] [%(levelname)s] file=%(pathname)s \
    line=%(lineno)s module=%(module)s function=%(funcName)s [%(message)s]'
    
    # UAT logging format
    # msg_format = '[%(message)s]'
    
    datetime_format = '%a, %d %b %Y %H:%M:%S'

    loglevels = {
        'CRITICAL': logging.CRITICAL,
        'ERROR': logging.ERROR,
        'WARNING': logging.WARNING,
        'INFO': logging.INFO,
        'DEBUG': logging.DEBUG,
        'NOTSET': logging.NOTSET
    }

    logging.basicConfig(filename=logfile,
                        format=msg_format,
                        datefmt=datetime_format,
                        level=loglevels[loglevel])
                        
                        
def table_configuration_lookup(
    dataset,
    level='n/a',
    form='n/a',
    all_tables=False
    ):
    """
    return table + field presence rules
    for this dataset
    """
    rules = []
    all_tb = []
    rule = {
       'table'          : None,
       'table_required' : None,
       'occurance'      : None,
       'incompat_table' : None,
       'fields'         : None,
       'field_requird'  : None,
       'optional_fields': None
    }
    table_config_dir = '../resources'
    # TODO: host this file/schema somewhere and read via http
    filename = 'table_configuration.csv'
    table_schema_path = os.path.join(table_config_dir, filename)
    try:
        with open(table_schema_path) as out_file:
            csv_reader = csv.reader(out_file, delimiter='|')
            for row in csv_reader:
                rule = {
                   'table'          : None,
                   'table_required' : None,
                   'occurance'      : None,
                   'incompat_table' : None,
                   'fields'         : None,
                   'all_field_requird'  : None,
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
                if all([
                    dataset == ds,
                    level == ll,
                    form == fm
                    ]):
                    rule['table'] = tb
                    rule['table_required'] = req_tb
                    rule['occurance'] = ocr
                    rule['incompat_table'] = inc
                    rule['fields'] = flds
                    rule['all_field_requird'] = req_fld
                    rule['optional_fields'] = opt_flds
                    rules.append(rule)
                if all_tables:
                    all_tb.append(str(tb))
                    if all([
                        inc != '',
                        inc != u'n/a'
                        ]):
                        all_tb.append(str(inc))
    except Exception, err:
        msg = str(err)
        LOGGER.error(msg)
        
    if all_tables:
        return all_tb
    else:
        return rules
