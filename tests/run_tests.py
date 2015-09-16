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
import logging
from woudc.extcsv import WOUDCextCSVWriter
from extcsv.util import setup_logger
import unittest


def msg(test_id, test_description):
    """convenience function to print out test id and desc"""
    return '%s: %s' % (test_id, test_description)
    
def get_data(extcsv, table, field, index=1):
    """convenience function, gets data from extcsv object""" 
    sep = '$'
    table_t = '%s%s%s' % (table, sep, index)
    return extcsv.extcsv_ds[table_t][field]


class extcsv_writer(unittest.TestCase):
    """Test suite for package woudc.extcsv.WOUDCextCSVWriter"""
    def setUp(self):
        """setup test fixtures, etc."""
        print msg(self.id(), self.shortDescription())

    def tearDown(self):
        """return to pristine state"""
        pass

    def test_smoke(self):
        """Simple Smoke Test"""
        # test assertions go here
        self.assertEquals(1, 1, 'Expected equality')
        
        
    def test_add_table_1(self):
        """
        Test adding new table
        """
        # new extcsv object
        extcsv = WOUDCextCSVWriter()
        extcsv.add_table('CONTENT', 'This table stores basic metadata, stored at index 1')
        self.assertTrue('CONTENT$1' in extcsv.extcsv_ds.keys(), 'Table CONTENT got added.')
        self.assertEquals('This table stores basic metadata, stored at index 1', get_data(extcsv,'CONTENT', 'comments')[0])

        
    def test_add_table_2(self):
        """
        Test adding new table at index 2
        """
        # new extcsv object
        extcsv = WOUDCextCSVWriter()
        extcsv.add_table('CONTENT', 'This table stores basic metadata, stored at index 2', index=2)
        self.assertTrue('CONTENT$2' in extcsv.extcsv_ds.keys(), 'Table CONTENT at index 2 got added.')
        
    def test_add_table_3(self):
        """
        Test order of tables to see if order of insert is preserved
        """
        # new extcsv object
        extcsv = WOUDCextCSVWriter()
        extcsv.add_table('CONTENT', 'This table stores basic metadata, stored at index 1 (default)')
        extcsv.add_table('CONTENT', 'This table stores basic metadata, stored at index 2', index=2)
        extcsv.add_table('CONTENT', 'This table stores basic metadata, stored at index 3', index=3)
        extcsv.add_table('CONTENT', 'This table stores basic metadata, stored at index 4', index=4)
        self.assertTrue('CONTENT$1' in extcsv.extcsv_ds.keys(), 'Table CONTENT at index 1 got added.')
        self.assertTrue('CONTENT$2' in extcsv.extcsv_ds.keys(), 'Table CONTENT at index 2 got added.')
        self.assertTrue('CONTENT$3' in extcsv.extcsv_ds.keys(), 'Table CONTENT at index 3 got added.')
        self.assertTrue('CONTENT$4' in extcsv.extcsv_ds.keys(), 'Table CONTENT at index 4 got added.')
        self.assertEquals(0, extcsv.extcsv_ds.keys().index('CONTENT$1'))
        self.assertEquals(1, extcsv.extcsv_ds.keys().index('CONTENT$2'))
        self.assertEquals(2, extcsv.extcsv_ds.keys().index('CONTENT$3'))
        self.assertEquals(3, extcsv.extcsv_ds.keys().index('CONTENT$4'))


    def test_add_field_1(self):
        """
        Test adding new field to table
        """
        # new extcsv object
        extcsv = WOUDCextCSVWriter()
        extcsv.add_field('CONTENT', 'Class')
        self.assertTrue('CONTENT$1' in extcsv.extcsv_ds.keys(), 'Table CONTENT got added.')
        self.assertTrue('Class' in extcsv.extcsv_ds['CONTENT$1'].keys(), 'Field CONTENT.Class got added.')

        
    def test_add_field_2(self):
        """
        Test adding multiple new fields to table
        """
        # new extcsv object
        extcsv = WOUDCextCSVWriter()
        extcsv.add_field('CONTENT', 'Class,Category,Level')
        self.assertTrue('CONTENT$1' in extcsv.extcsv_ds.keys(), 'Table CONTENT got added.')
        self.assertTrue('Class' in extcsv.extcsv_ds['CONTENT$1'].keys(), 'Field CONTENT.Class got added.')
        self.assertTrue('Category' in extcsv.extcsv_ds['CONTENT$1'].keys(), 'Field CONTENT.Class got added.')
        self.assertTrue('Level' in extcsv.extcsv_ds['CONTENT$1'].keys(), 'Field CONTENT.Class got added.')
        
    def test_add_field_3(self):
        """
        Test order of insert of fields
        """
        # new extcsv object
        extcsv = WOUDCextCSVWriter()
        extcsv.add_field('CONTENT', 'Class,Category,Level')
        self.assertEquals(0,extcsv.extcsv_ds['CONTENT$1'].keys().index('comments'))
        self.assertEquals(1,extcsv.extcsv_ds['CONTENT$1'].keys().index('Class'))
        self.assertEquals(2,extcsv.extcsv_ds['CONTENT$1'].keys().index('Category'))
        self.assertEquals(3,extcsv.extcsv_ds['CONTENT$1'].keys().index('Level'))


    def test_add_value_1(self):
        """
        Test adding new value to existing table.field, veritically
        """
        # new extcsv object
        extcsv = WOUDCextCSVWriter()
        extcsv.add_data('CONTENT', 'WOUDC', field='Class')
        self.assertTrue('CONTENT$1' in extcsv.extcsv_ds.keys(), 'Table CONTENT got added.')
        self.assertTrue('Class' in extcsv.extcsv_ds['CONTENT$1'].keys(), 'Field CONTENT.Class got added.')
        self.assertTrue('WOUDC' in get_data(extcsv,'CONTENT', 'Class'), 'Value added to CONTENT.Class.')
        
    def test_add_value_2(self):
        """
        Test adding new value to existing table.field, horizontally
        """
        # new extcsv object
        extcsv = WOUDCextCSVWriter()
        extcsv.add_field('CONTENT', 'Class,Category,Level')
        extcsv.add_data('CONTENT', 'a,b,c')
        self.assertTrue('CONTENT$1' in extcsv.extcsv_ds.keys(), 'Table CONTENT got added.')
        self.assertTrue('Class' in extcsv.extcsv_ds['CONTENT$1'].keys(), 'Field CONTENT.Class got added.')
        self.assertTrue('Category' in extcsv.extcsv_ds['CONTENT$1'].keys(), 'Field CONTENT.Class got added.')
        self.assertTrue('Level' in extcsv.extcsv_ds['CONTENT$1'].keys(), 'Field CONTENT.Class got added.')
        self.assertTrue('a' in get_data(extcsv,'CONTENT', 'Class'), 'Value added to CONTENT.Class.')
        self.assertTrue('b' in get_data(extcsv,'CONTENT', 'Category'), 'Value added to CONTENT.Class.')
        self.assertTrue('c' in get_data(extcsv,'CONTENT', 'Level'), 'Value added to CONTENT.Class.')
        
    def test_add_value_3(self):
        """
        Test adding value to table, when multiple tables by the same name are present
        """
        # new extcsv object
        extcsv = WOUDCextCSVWriter()
        extcsv.add_field('GLOBAL', 'Wavelength,S-Irradiance,Time')
        extcsv.add_field('GLOBAL', 'Wavelength,S-Irradiance,Time', index=2)
        extcsv.add_field('GLOBAL', 'Wavelength,S-Irradiance,Time', index=3)
        extcsv.add_data('GLOBAL', '290.0', index=2, field='Wavelength')
        extcsv.add_data('GLOBAL', '07:28:49', index=3, field='Time')
        extcsv.add_data('GLOBAL', '1.700E-06', field='S-Irradiance')
        self.assertEquals(['290.0'], get_data(extcsv,'GLOBAL', 'Wavelength', index=2))
        self.assertEquals(['07:28:49'], get_data(extcsv,'GLOBAL', 'Time', index=3))
        self.assertEquals(['1.700E-06'], get_data(extcsv,'GLOBAL', 'S-Irradiance'))
        
        
    def test_add_value_4(self):
        """
        Test adding multiple values to a field, order of insert is perserved
        """
        # new extcsv object
        extcsv = WOUDCextCSVWriter()
        extcsv.add_data('GLOBAL','290.0,1.700E-06', field='Wavelength,S-Irradiance,Time')
        extcsv.add_data('GLOBAL','290.5,8.000E-07', field='Wavelength,S-Irradiance,Time')
        extcsv.add_data('GLOBAL','291.0,0.000E+00', field='Wavelength,S-Irradiance,Time')
        extcsv.add_data('GLOBAL','291.5,8.000E-07', field='Wavelength,S-Irradiance,Time')
        self.assertEquals(['290.0', '290.5', '291.0', '291.5'], get_data(extcsv,'GLOBAL', 'Wavelength'))
        self.assertEquals(['1.700E-06', '8.000E-07', '0.000E+00', '8.000E-07'], get_data(extcsv,'GLOBAL', 'S-Irradiance'))
        self.assertEquals([], get_data(extcsv,'GLOBAL', 'Time'))
        
        
    def test_add_value_5(self):
        """
        Test adding list of vlaues
        """
        # new extcsv object
        extcsv = WOUDCextCSVWriter()
        extcsv.add_data('CONTENT', 'WOUDC', field='Class')
        extcsv.add_data('CONTENT', ['v1,v2',7,8,9,10], field='Class')
        self.assertEquals(['WOUDC', 'v1,v2',7,8,9,10], get_data(extcsv, 'CONTENT', 'Class'))
        
        
    def test_remove_table(self):
        """
        Test removing table
        """
        # new extcsv object
        extcsv = WOUDCextCSVWriter()
        extcsv.add_data('CONTENT', 'WOUDC', field='Class')
        extcsv.add_data('PLATFORM', 'ID', field='001')
        extcsv.add_data('TIMESTAMP', 'Time', field='00:00:00')
        extcsv.add_data('TIMESTAMP', 'Time', field='01:00:00', index=2)
        extcsv.remove_table('CONTENT')
        extcsv.remove_table('TIMESTAMP', index=2)
        self.assertTrue('CONTENT$1' not in extcsv.extcsv_ds.keys())
        self.assertTrue('TIMESTAMP$2' not in extcsv.extcsv_ds.keys())
        self.assertTrue('TIMESTAMP$1' in extcsv.extcsv_ds.keys())
        
        
    def test_remove_field(self):
        """
        Test removing field
        """
        # new extcsv object
        extcsv = WOUDCextCSVWriter()
        extcsv.add_data('TABLE', 'v1,v2', field='Field1,Field2,Field3')
        extcsv.add_data('TABLE', 'v1,v2', field='Field1,Field2,Field3', index=2)
        extcsv.remove_field('TABLE', 'Field2', index=2)
        extcsv.remove_field('TABLE', 'Field1')
        self.assertTrue('Fields2' not in extcsv.extcsv_ds['TABLE$2'].keys())
        self.assertTrue('Fields1' not in extcsv.extcsv_ds['TABLE$1'].keys())
        self.assertEquals(['comments','Field2','Field3'], extcsv.extcsv_ds['TABLE$1'].keys())
        self.assertEquals(['comments','Field1','Field3'], extcsv.extcsv_ds['TABLE$2'].keys())
        
        
    def test_remove_value_1(self):
        """
        Test remove first occurance value
        """
        # new extcsv object
        extcsv = WOUDCextCSVWriter()
        extcsv.add_data('TABLE', ['v1','v2','v1','v1','v2','v1'], field='Field1')
        extcsv.add_data('TABLE', ['v1','v2','v3','v2'], field='Field1', index=2)
        extcsv.remove_data('TABLE', 'Field1', 'v1')
        self.assertEquals('v2', get_data(extcsv, 'TABLE', 'Field1')[0])
        self.assertEquals(['v2','v1','v1','v2','v1'], get_data(extcsv, 'TABLE', 'Field1'))
        extcsv.remove_data('TABLE', 'Field1', 'v2', index=2)
        self.assertEquals('v1', get_data(extcsv, 'TABLE', 'Field1', index=2)[0])
        
        
    def test_remove_value_2(self):
        """
        Test remove value by index
        """
        # new extcsv object
        extcsv = WOUDCextCSVWriter()
        extcsv.add_data('TABLE', ['v1','v2','v1','v1','v2','v1'], field='Field1')
        extcsv.add_data('TABLE', ['v1','v2','v3','v2'], field='Field1', index=2)
        extcsv.remove_data('TABLE', 'Field1', 'v1', d_index=3)
        self.assertEquals('v2', get_data(extcsv, 'TABLE', 'Field1')[3])
        self.assertEquals(['v1', 'v2', 'v1', 'v2', 'v1'], get_data(extcsv, 'TABLE', 'Field1'))
        extcsv.remove_data('TABLE', 'Field1', 'v2', index=2, d_index=3)
        self.assertEquals('v3', get_data(extcsv, 'TABLE', 'Field1', index=2)[2])



    def test_remove_value_3(self):
        """
        Test remove all occurance of value
        """
        # new extcsv object
        extcsv = WOUDCextCSVWriter()
        extcsv.add_data('TABLE', ['v1','v2','v1','v1','v2','v1'], field='Field1')
        extcsv.add_data('TABLE', ['v1','v2','v3','v2'], field='Field1', index=2)
        extcsv.remove_data('TABLE', 'Field1', 'v1', all_occurances=True)
        self.assertTrue('v1' not in get_data(extcsv, 'TABLE', 'Field1'))
        extcsv.remove_data('TABLE', 'Field1', 'v2', index=2, all_occurances=True)
        self.assertTrue('v2' not in get_data(extcsv, 'TABLE', 'Field1', index=2))

        
if __name__ == '__main__':
    unittest.main()
