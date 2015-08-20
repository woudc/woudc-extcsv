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

# Build WOUDC Extended CSV

import os
import re
import csv
import logging
import StringIO
from collections import OrderedDict

LOGGER = logging.getLogger(__name__)

class WOUDCextCSVWriter(object):
    """Generic extended CSV object, used for serialization"""
    
    def __init__(self):
        """bootstrap"""
        self._filename = None
        self._file_comments = []
        self._extcsv_ds = OrderedDict()
        
        
    @property
    def filename(self):
        """
        get filename
        """
        return self._filename
        
    @property
    def file_comment(self):
        """
        get file comments
        """
        return self._file_comments
        
    @filename.setter
    def filename(self, value):
        """set filename"""
        self.filename = value
        
    @property
    def extcsv_ds(self):
        """
        get internal data structure
        """
        return self._extcsv_ds

    def add_comment(self, comment):
        """add file level comments"""
        self.fiel_comment.append(comment)
        
    def add_table(self,table, table_comment=None, index=1):
        """
        add table
        """
        table_n = '%s%s' % (table, index)
        if table_n not in self.extcsv_ds.keys():
            self.extcsv_ds[table_n] = OrderedDict()
            self.extcsv_ds[table_n]['comments'] = []
            msg = 'Table: %s at index: %s added.' % (table, index)
            LOGGER.info(msg)
        else:
            msg = 'Table: %s at index: %s already exists' % (table, index)
            LOGGER.error(msg)
            
        if table_comment is not None:
            self.extcsv_ds[table_n]['comments'].append(table_comment)


    def add_field(self, table, field, index=1, delim=None):
        """
        add field
        """
        table = '%s%s' % (table, index)
        if delim is None: # vertical insert
            if field not in self.extcsv_ds[table_n].keys():
                self.extcsv_ds[table_n][field] = []
                msg = 'Field: %s was added to Table: %s at index: %s' % (field, table, index)
                LOGGER.info(msg)
            else:
                msg = 'Field: %s alraedy persent in Table: %s at index: %s' % (field, table, index)
                LOGGER.error(msg)
        else: # horizontal insert
            str_ob = StringIO.StringIO(field)
            csv_reader = csv.reader(str_obj, delimiter=delim)
            fields = csv_reader.next()
            for field in fields:
                if field not in self.extcsv_ds[table_n].keys():
                    self.extcsv_ds[table_n][field] = []
                    msg = 'Field: %s was added to Table: %s at index: %s' % (field, table, index)
                    LOGGER.info(msg)
                else:
                    msg = 'Field: %s alraedy persent in Table: %s at index: %s' % (field, table, index)
                    LOGGER.error(msg)
                
            


    def add_data(self, table, field, data, index=1, delim=None):
        """
        add data
        """
        table_n = '%s%s' % (table, index)
        if delim is None: # vertical insert
            if not isinstance(data, list):
                self.extcsv_ds[table_n][field].append(str(data))
                msg = 'Data added to Field: %s of Table: %s at index: %s' % (field, table, index)
                LOGGER.info(msg)
            else            
                self.extcsv_ds[table_n][field] + data
                msg = 'Data added to Field: %s of Table: %s at index: %s' % (field, table, index)
                LOGGER.info(msg)
        else: # horizontal insert
            str_ob = StringIO.StringIO(data)
            csv_reader = csv.reader(str_obj, delimiter=delim)
            data_l = csv_reader.next()
            for data in data_l:
                data_index = data_l.index(data)
                field = self.extcsv_ds[table_n].keys()[data_index]
                self.extcsv_ds[table_n][field].append(str(data))
            
            
            
    def remove_table(self, table, index=1):
        """
        remove table
        """
        table_n = '%s%s' % (table, index)
        try:
            del self.extcsv_ds[talbe_n]
            msg = 'Removed Table: %s at index: %s' % (table, index)
            LOGGER.info(msg)
        except Exception, err:
            msg = 'Unable to delete Table: %s at index: %s, due to: %s' (table, index, str(err))
            LOGGER.error(msg)
        
        
    def remove_field(self, table, field, index=1):
        """
        remove field from table, including it's data
        """
        table_n = '%s%s' % (table, index)
        try:
            del self.extcsv_ds[table_n][field]
            msg = 'Removed Field: %s of Table: %s at index: %s' % (field, table, index)
            LOGGER.info(msg)
        except Exception, err:
            msg = 'Unable to remove Field: %s of Table: %s at index: %s, due to: %s' % (field, table, index, str(err))
            LOGGER.error(msg)
            
            
    def remove_data(self, table, field, data=None, index=1, d_index=None, all=False):
        """
        remove data
        """
        table_n = '%s%s' % (table, index)
        if all([
            not all,
            d_index is None,
            data is not None
            ]): # remove first occurance
            self.extcsv_ds[table_n][field].remove(data)
            msg = 'First occurance of data: %s from Field: %s of Table: %s at index: %s was removeed' %\
                (data, field, table, index)
            LOGGER.info(msg)
        if d_index is not None: # remove by index
            self.extcsv_ds[table_n][field].pop(d_index)
            msg = 'Data at index: %s from Field: %s of Table: %s at index: %s was removed' %\
                (d_index, field, table, index)
            LOGGER.info(msg)
        if all([
            data is not None,
            all
            ]): # remove all
            self.extcsv_ds[table_n][field] = filter(lambda a: a != data, self.extcsv_ds[table_n][field])
            msg = 'All occurance of data: %s from Field: %s of table: %s at index: %s removed.' %\
                (data, field, table, index)
            LOGGER.info(msg)
            
                
    def view_table(self, table, index=1):
        """
        return table
        """
        table_n = '%s%s' % (table, index)
        return self.extcsv_ds[table_n]


    def view_field(self, table, field, index=1):
        """
        return field
        """
        table_n = '%s%s' % (table, index)
        return self.extcsv_ds[table_n][field]
        
    def get_ds():
        """
        for testing only
        """
        print self.extcsv_ds