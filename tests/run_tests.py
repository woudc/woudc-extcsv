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

import io
import unittest
from woudc_extcsv import (dump, dumps, load, loads, kw_restrict,
                          Reader, WOUDCExtCSVReaderError, Writer)

try:
    from StringIO import StringIO
    import unicodecsv as csv
except ImportError:
    from io import StringIO
    import csv


def msg(test_id, test_description):
    """helper function to print out test id and desc"""

    return '%s: %s' % (test_id, test_description)


def load_test_data_file(filename):
    """helper function to open test file regardless of invocation"""

    try:
        return load(filename)
    except IOError:
        return load('tests/{}'.format(filename))


def get_data(extcsv, table, field, index=1):
    """helper function, gets data from extcsv object"""

    sep = '$'
    table_t = '%s%s%s' % (table, sep, index)
    return extcsv.extcsv_ds[table_t][field]


def get_file_string(file_path):
    """helper function, to open test file and return
       unicode string of file contents
    """

    try:
        kwargs = kw_restrict('open', encoding='utf-8')
        with io.open(file_path, **kwargs) as ff:
            return ff.read()
    except UnicodeError:
        kwargs = kw_restrict('open', encoding='latin1')
        with io.open(file_path, **kwargs) as ff:
            return ff.read()


class WriterTest(unittest.TestCase):
    """Test suite for Writer"""

    def setUp(self):
        """setup test fixtures, etc."""

        print(msg(self.id(), self.shortDescription()))

    def tearDown(self):
        """return to pristine state"""

        pass

    def test_dump_file(self):
        """Test file dumping"""

        with self.assertRaises(TypeError):
            dump()

    def test_add_table_1(self):
        """Test adding new table"""

        extcsv = Writer()
        extcsv.add_table('CONTENT', 'basic metadata, index 1')
        self.assertTrue('CONTENT$1' in extcsv.extcsv_ds.keys(),
                        'table not found')
        self.assertEqual('basic metadata, index 1',
                         get_data(extcsv, 'CONTENT', 'comments')[0],
                         'invalid table comment')

    def test_add_table_2(self):
        """Test adding new table at a specific index"""

        extcsv = Writer()
        extcsv.add_table('CONTENT', 'basic metadata, index 2', index=2)
        self.assertTrue('CONTENT$2' in extcsv.extcsv_ds.keys(),
                        'table not found')

    def test_add_table_3(self):
        """Test order of tables to see if order of insert is preserved"""

        extcsv = Writer()
        extcsv.add_table('CONTENT', 'basic metadata, index 1')
        extcsv.add_table('CONTENT', 'basic metadata, index 2', index=2)
        extcsv.add_table('CONTENT', 'basic metadata, index 3', index=3)
        extcsv.add_table('CONTENT', 'basic metadata, index 4', index=4)

        keys = list(extcsv.extcsv_ds.keys())
        self.assertTrue('CONTENT$1' in keys, 'table 1 not found')
        self.assertTrue('CONTENT$2' in keys, 'table 2 not found')
        self.assertTrue('CONTENT$3' in keys, 'table 3 not found')
        self.assertTrue('CONTENT$4' in keys, 'table 4 not found')
        self.assertEqual(0, keys.index('CONTENT$1'), 'table 1 index mismatch')
        self.assertEqual(1, keys.index('CONTENT$2'), 'table 2 index mismatch')
        self.assertEqual(2, keys.index('CONTENT$3'), 'table 3 index mismatch')
        self.assertEqual(3, keys.index('CONTENT$4'), 'table 4 index mismatch')

    def test_add_field_1(self):
        """Test adding new field to table"""

        extcsv = Writer()
        extcsv.add_field('CONTENT', 'Class')
        self.assertTrue('CONTENT$1' in extcsv.extcsv_ds.keys(),
                        'table not found')
        self.assertTrue('Class' in extcsv.extcsv_ds['CONTENT$1'].keys(),
                        'field not found')

    def test_add_field_2(self):
        """Test adding multiple new fields to table"""

        extcsv = Writer()
        extcsv.add_field('CONTENT', 'Class,Category,Level')
        self.assertTrue('CONTENT$1' in extcsv.extcsv_ds.keys(),
                        'table CONTENT not found')
        self.assertTrue('Class' in extcsv.extcsv_ds['CONTENT$1'].keys(),
                        'field CONTENT.Class not found')
        self.assertTrue('Category' in extcsv.extcsv_ds['CONTENT$1'].keys(),
                        'field CONTENT.Class not found')
        self.assertTrue('Level' in extcsv.extcsv_ds['CONTENT$1'].keys(),
                        'field CONTENT.Class not found')

    def test_add_field_3(self):
        """Test order of insert of fields"""

        extcsv = Writer()
        extcsv.add_field('CONTENT', 'Class,Category,Level')
        keys = list(extcsv.extcsv_ds['CONTENT$1'].keys())
        self.assertEqual(0, keys.index('comments'), 'index 0 mismatch')
        self.assertEqual(1, keys.index('Class'), 'index 1 mismatch')
        self.assertEqual(2, keys.index('Category'), 'index 2 mismatch')
        self.assertEqual(3, keys.index('Level'), 'index 3 mismatch')

    def test_add_value_1(self):
        """Test adding new value to existing table.field, veritically"""

        extcsv = Writer()
        extcsv.add_data('CONTENT', 'WOUDC', field='Class')
        self.assertTrue('CONTENT$1' in extcsv.extcsv_ds.keys(),
                        'table CONTENT not found')
        self.assertTrue('Class' in extcsv.extcsv_ds['CONTENT$1'].keys(),
                        'field CONTENT.Class not found')
        self.assertTrue('WOUDC' in get_data(extcsv, 'CONTENT', 'Class'),
                        'value CONTENT.CLASS WOUDC not found')

    def test_add_value_2(self):
        """Test adding new value to existing table.field, horizontally"""

        extcsv = Writer()
        extcsv.add_field('CONTENT', 'Class,Category,Level')
        extcsv.add_data('CONTENT', 'a,b,c')
        self.assertTrue('CONTENT$1' in extcsv.extcsv_ds.keys(),
                        'table CONTENT not found')
        self.assertTrue('Class' in extcsv.extcsv_ds['CONTENT$1'].keys(),
                        'field CONTENT.Class not found')
        self.assertTrue('Category' in extcsv.extcsv_ds['CONTENT$1'].keys(),
                        'field CONTENT.Class not found')
        self.assertTrue('Level' in extcsv.extcsv_ds['CONTENT$1'].keys(),
                        'field CONTENT.Class not found')
        self.assertTrue('a' in get_data(extcsv, 'CONTENT', 'Class'),
                        'value CONTENT.Class not found')
        self.assertTrue('b' in get_data(extcsv, 'CONTENT', 'Category'),
                        'value CONTENT.Class not found')
        self.assertTrue('c' in get_data(extcsv, 'CONTENT', 'Level'),
                        'value CONTENT.Class not found')

    def test_add_value_3(self):
        """Test adding value to table given identical table names"""

        extcsv = Writer()
        extcsv.add_field('GLOBAL', 'Wavelength,S-Irradiance,Time')
        extcsv.add_field('GLOBAL', 'Wavelength,S-Irradiance,Time', index=2)
        extcsv.add_field('GLOBAL', 'Wavelength,S-Irradiance,Time', index=3)
        extcsv.add_data('GLOBAL', '290.0', index=2, field='Wavelength')
        extcsv.add_data('GLOBAL', '07:28:49', index=3, field='Time')
        extcsv.add_data('GLOBAL', '1.700E-06', field='S-Irradiance')
        self.assertEqual(['290.0'],
                         get_data(extcsv, 'GLOBAL', 'Wavelength', index=2),
                         'expected specific value')
        self.assertEqual(['07:28:49'],
                         get_data(extcsv, 'GLOBAL', 'Time', index=3),
                         'expected specific value')
        self.assertEqual(['1.700E-06'],
                         get_data(extcsv, 'GLOBAL', 'S-Irradiance'),
                         'expected specific value')

    def test_add_value_4(self):
        """Test insert order when adding multiple values to a field"""

        extcsv = Writer()
        field_val = 'Wavelength,S-Irradiance,Time'

        extcsv.add_data('GLOBAL', '290.0,1.700E-06', field=field_val)
        extcsv.add_data('GLOBAL', '290.5,8.000E-07', field=field_val)
        extcsv.add_data('GLOBAL', '291.0,0.000E+00', field=field_val)
        extcsv.add_data('GLOBAL', '291.5,8.000E-07', field=field_val)
        self.assertEqual(['290.0', '290.5', '291.0', '291.5'],
                         get_data(extcsv, 'GLOBAL', 'Wavelength'),
                         'expected specific value')
        self.assertEqual(['1.700E-06', '8.000E-07', '0.000E+00', '8.000E-07'],
                         get_data(extcsv, 'GLOBAL', 'S-Irradiance'),
                         'expected specific value')
        self.assertEqual([], get_data(extcsv, 'GLOBAL', 'Time'),
                         'expected specific value')

    def test_add_value_5(self):
        """Test adding list of values"""
        extcsv = Writer()
        extcsv.add_data('CONTENT', 'WOUDC', field='Class')
        extcsv.add_data('CONTENT', ['v1,v2', 7, 8, 9, 10], field='Class')
        self.assertEqual(['WOUDC', 'v1,v2', 7, 8, 9, 10],
                         get_data(extcsv, 'CONTENT', 'Class'),
                         'expected specific value')

    def test_remove_table(self):
        """Test removing table"""
        # new extcsv object
        extcsv = Writer()
        extcsv.add_data('CONTENT', 'WOUDC', field='Class')
        extcsv.add_data('PLATFORM', 'ID', field='001')
        extcsv.add_data('TIMESTAMP', 'Time', field='00:00:00')
        extcsv.add_data('TIMESTAMP', 'Time', field='01:00:00', index=2)
        extcsv.remove_table('CONTENT')
        extcsv.remove_table('TIMESTAMP', index=2)
        self.assertTrue('CONTENT$1' not in extcsv.extcsv_ds.keys(),
                        'unexpected table found')
        self.assertTrue('TIMESTAMP$2' not in extcsv.extcsv_ds.keys(),
                        'unexpected table found')
        self.assertTrue('TIMESTAMP$1' in extcsv.extcsv_ds.keys(),
                        'unexpected table found')

    def test_remove_field(self):
        """Test removing field"""
        extcsv = Writer()
        extcsv.add_data('TABLE', 'v1,v2', field='Field1,Field2,Field3')
        extcsv.add_data('TABLE', 'v1,v2', field='Field1,Field2,Field3',
                        index=2)
        extcsv.remove_field('TABLE', 'Field2', index=2)
        extcsv.remove_field('TABLE', 'Field1')
        self.assertTrue('Fields2' not in extcsv.extcsv_ds['TABLE$2'].keys(),
                        'unexpected field found')
        self.assertTrue('Fields1' not in extcsv.extcsv_ds['TABLE$1'].keys(),
                        'unexpected field found')
        self.assertEqual(['comments', 'Field2', 'Field3'],
                         list(extcsv.extcsv_ds['TABLE$1'].keys()),
                         'expected specific value')
        self.assertEqual(['comments', 'Field1', 'Field3'],
                         list(extcsv.extcsv_ds['TABLE$2'].keys()),
                         'expected specific value')

    def test_remove_value_first(self):
        """Test remove first occurence value"""
        extcsv = Writer()
        extcsv.add_data('TABLE', ['v1', 'v2', 'v1', 'v1', 'v2', 'v1'],
                        field='Field1')
        extcsv.add_data('TABLE', ['v1', 'v2', 'v3', 'v2'], field='Field1',
                        index=2)
        extcsv.remove_data('TABLE', 'Field1', 'v1')
        self.assertEqual('v2', get_data(extcsv, 'TABLE', 'Field1')[0],
                         'expected specific value')
        self.assertEqual(['v2', 'v1', 'v1', 'v2', 'v1'],
                         get_data(extcsv, 'TABLE', 'Field1'),
                         'expected specific value')
        extcsv.remove_data('TABLE', 'Field1', 'v2', index=2)
        self.assertEqual('v1', get_data(extcsv, 'TABLE',
                                        'Field1', index=2)[0],
                         'expected specific value')

    def test_remove_value_by_index(self):
        """Test remove value by index"""
        extcsv = Writer()
        extcsv.add_data('TABLE', ['v1', 'v2', 'v1', 'v1', 'v2', 'v1'],
                        field='Field1')
        extcsv.add_data('TABLE', ['v1', 'v2', 'v3', 'v2'], field='Field1',
                        index=2)
        extcsv.remove_data('TABLE', 'Field1', 'v1', d_index=3)
        self.assertEqual('v2', get_data(extcsv, 'TABLE', 'Field1')[3],
                         'expected specific value')
        self.assertEqual(['v1', 'v2', 'v1', 'v2', 'v1'],
                         get_data(extcsv, 'TABLE', 'Field1'),
                         'expected specific value')
        extcsv.remove_data('TABLE', 'Field1', 'v2', index=2, d_index=3)
        self.assertEqual('v3', get_data(extcsv, 'TABLE', 'Field1',
                                        index=2)[2],
                         'expected specific value')

    def test_remove_value_all_occurences(self):
        """Test remove all occurences of value"""
        extcsv = Writer()
        extcsv.add_data('TABLE', ['v1', 'v2', 'v1', 'v1', 'v2', 'v1'],
                        field='Field1')
        extcsv.add_data('TABLE', ['v1', 'v2', 'v3', 'v2'],
                        field='Field1', index=2)
        extcsv.remove_data('TABLE', 'Field1', 'v1', all_occurences=True)
        self.assertTrue('v1' not in get_data(extcsv, 'TABLE', 'Field1'),
                        'unexpected value found')
        extcsv.remove_data('TABLE', 'Field1', 'v2', index=2,
                           all_occurences=True)
        self.assertFalse('v2' in get_data(extcsv, 'TABLE', 'Field1', index=2),
                         'unexpected value found')


class ReaderTest(unittest.TestCase):
    """Test suite for Reader"""

    def setUp(self):
        """setup test fixtures, etc."""

        print(msg(self.id(), self.shortDescription()))

    def tearDown(self):
        """return to pristine state"""

        pass

    def test_load_file(self):
        """Test file loading"""

        with self.assertRaises(TypeError):
            load()

    def test_table_presence(self):
        """Test if table exists"""

        extcsv_to =\
            load_test_data_file('data/20061201.brewer.mkiv.153.imd.csv')
        self.assertTrue('CONTENT' in extcsv_to.sections,
                        'check totalozone table presence')
        self.assertTrue('DATA_GENERATION' in extcsv_to.sections,
                        'check totalozone table presence')
        self.assertTrue('DAILY' in extcsv_to.sections,
                        'check totalozone table presence')
        self.assertTrue('CONTENT' in extcsv_to.sections,
                        'check totalozone table presence')
        self.assertTrue('TIMESTAMP2' in extcsv_to.sections,
                        'check totalozone table presence')
        self.assertTrue('TIMESTAMP3' not in extcsv_to.sections,
                        'check totalozone table not presence')

        extcsv_sp =\
            load_test_data_file('data/20040109.brewer.mkiv.144.epa_uga.csv')
        self.assertTrue('DATA_GENERATION' in extcsv_sp.sections,
                        'check spectral table presence')
        self.assertTrue('GLOBAL_SUMMARY' in extcsv_sp.sections,
                        'check spectral table presence')
        self.assertTrue('GLOBAL_SUMMARY23' in extcsv_sp.sections,
                        'check spectral table presence')
        self.assertTrue('GLOBAL_SUMMARY25' not in extcsv_sp.sections,
                        'check spectral table presence')

    def test_field_presence(self):
        """Test if field exist"""

        extcsv_oz =\
            load_test_data_file('data/20151021.ecc.6a.6a28340.smna.csv')
        self.assertTrue('Category' in extcsv_oz.sections['CONTENT'],
                        'check ozonesonde field presence')
        self.assertTrue('Version' in extcsv_oz.sections['DATA_GENERATION'],
                        'check ozonesonde field presence')
        self.assertTrue('ScientificAuthority'
                        in extcsv_oz.sections['DATA_GENERATION'],
                        'check ozonesonde field presence')
        self.assertTrue('Date' in extcsv_oz.sections['TIMESTAMP'],
                        'check ozonesonde field presence')
        self.assertTrue('SondeTotalO3' in extcsv_oz.sections['FLIGHT_SUMMARY'],
                        'check ozonesonde field presence')
        self.assertTrue('WLCode' in extcsv_oz.sections['FLIGHT_SUMMARY'],
                        'check ozonesonde field presence')
        self.assertTrue('ib2' in extcsv_oz.sections['AUXILIARY_DATA'],
                        'check ozonesonde field presence')
        self.assertTrue('BackgroundCorr'
                        in extcsv_oz.sections['AUXILIARY_DATA'],
                        'check ozonesonde field presence')
        self.assertTrue('_raw' in extcsv_oz.sections['PROFILE'],
                        'check ozonesonde field presence')
        self.assertTrue('O3PartialPressure' not in
                        extcsv_oz.sections['PROFILE'],
                        'check ozonesonde field presence')
        oz_profile = StringIO(extcsv_oz.sections['PROFILE']['_raw'])
        pr_rows = csv.reader(oz_profile)
        pr_header = next(pr_rows)
        self.assertTrue('O3PartialPressure' in pr_header,
                        'check ozonesonde profile field presence')
        self.assertEqual(0, pr_header.index('Pressure'),
                         'check ozonesonde profile field order')
        self.assertEqual(3, pr_header.index('WindSpeed'),
                         'check ozonesonde profile field order')
        self.assertEqual(len(pr_header) - 1,
                         pr_header.index('SampleTemperature'),
                         'check ozonesonde profile field order')

    def test_value(self):
        """Test values"""

        extcsv_to =\
            load_test_data_file('data/20061201.brewer.mkiv.153.imd.csv')
        self.assertEqual('WOUDC', extcsv_to.sections['CONTENT']['Class'],
                         'check totalozone value')
        self.assertEqual('', extcsv_to.sections['PLATFORM']['GAW_ID'],
                         'check totalozone value')
        self.assertEqual('11.45', extcsv_to.sections['LOCATION']['Longitude'],
                         'check totalozone value')
        self.assertEqual('+00:00:00',
                         extcsv_to.sections['TIMESTAMP']['UTCOffset'],
                         'check totalozone value')
        self.assertEqual('2006-12-01',
                         extcsv_to.sections['TIMESTAMP']['Date'],
                         'check totalozone value')
        self.assertEqual('2006-12-31',
                         extcsv_to.sections['TIMESTAMP2']['Date'],
                         'check totalozone value')
        self.assertEqual('21.4', extcsv_to.sections['MONTHLY']['StdDevO3'],
                         'check totalozone value')
        to_daily = StringIO(extcsv_to.sections['DAILY']['_raw'])
        daily_rows = csv.reader(to_daily)
        daily_header = next(daily_rows)
        daily_row = None
        # seek
        for i in range(1, 11):
            daily_row = next(daily_rows)
        self.assertEqual('219', daily_row[daily_header.index('ColumnO3')],
                         'check totalozone daily value')
        self.assertEqual('0', daily_row[daily_header.index('ObsCode')],
                         'check totalozone daily value')
        self.assertEqual('', daily_row[daily_header.index('UTC_Begin')],
                         'check totalozone daily value')
        # seek
        for i in range(1, 11):
            daily_row = next(daily_rows)
        self.assertEqual('259', daily_row[daily_header.index('ColumnO3')],
                         'check totalozone daily value')

        extcsv_sp =\
            load_test_data_file('data/20040109.brewer.mkiv.144.epa_uga.csv')
        self.assertEqual('2.291E+00',
                         extcsv_sp.sections['GLOBAL_SUMMARY']['IntCIE'],
                         'check spectral value')
        self.assertEqual('000000',
                         extcsv_sp.sections['GLOBAL_SUMMARY']['Flag'],
                         'check spectral value')
        sp_global = StringIO(extcsv_sp.sections['GLOBAL9']['_raw'])
        global_rows = csv.reader(sp_global)
        global_header = next(global_rows)
        global_row = None
        # seek
        for i in range(1, 53):
            global_row = next(global_rows)
        self.assertEqual('2.026E-01',
                         global_row[global_header.index('S-Irradiance')],
                         'check spectral global value')
        self.assertEqual('315.5',
                         global_row[global_header.index('Wavelength')],
                         'check spectral global value')
        self.assertEqual('000020',
                         extcsv_sp.sections['GLOBAL_SUMMARY24']['Flag'],
                         'check spectral value')
        self.assertEqual('000020',
                         extcsv_sp.sections['GLOBAL_SUMMARY24']['Flag'],
                         'check spectral value')

        sp_daily_tot =\
            StringIO(extcsv_sp.sections['GLOBAL_DAILY_TOTALS']['_raw'])
        daily_tot_rows = csv.reader(sp_daily_tot)
        daily_tot_header = next(daily_tot_rows)
        daily_tot_row = None
        # seek
        for i in range(1, 145):
            daily_tot_row = next(daily_tot_rows)
        self.assertEqual('361.5',
                         daily_tot_row[daily_tot_header.index('Wavelength')],
                         'check spectral global daily total value')
        self.assertEqual('1.055E+01',
                         daily_tot_row[
                             daily_tot_header.index('S-Irradiance')],
                         'check spectral global daily total value')
        self.assertEqual('6.440E+02',
                         extcsv_sp.
                         sections['GLOBAL_DAILY_SUMMARY']['IntACGIH'],
                         'check spectral global daily summary value')

        extcsv_oz =\
            load_test_data_file('data/20151021.ecc.6a.6a28340.smna.csv')
        self.assertEqual('6a',
                         extcsv_oz.sections['INSTRUMENT']['Model'],
                         'check ozonesonde value')
        self.assertEqual('323.75',
                         extcsv_oz.sections['FLIGHT_SUMMARY']['SondeTotalO3'],
                         'check ozonesonde value')
        self.assertEqual('-0.99',
                         extcsv_oz.
                         sections['FLIGHT_SUMMARY']['CorrectionFactor'],
                         'check ozonesonde value')
        self.assertEqual('131',
                         extcsv_oz.sections['FLIGHT_SUMMARY']['Number'],
                         'check ozonesonde value')
        oz_profile = StringIO(extcsv_oz.sections['PROFILE']['_raw'])
        profile_rows = csv.reader(oz_profile)
        profile_header = next(profile_rows)
        profile_row = None
        # seek
        for i in range(1, 598):
            profile_row = next(profile_rows)
        self.assertEqual('85.4',
                         profile_row[profile_header.index('Pressure')],
                         'check ozonesonde profile value')
        self.assertEqual('255',
                         profile_row[profile_header.index('WindDirection')],
                         'check ozonesonde profile value')

    def test_writer_reader(self):
        """
        Produce woudc_extcsv.Writer object,
        use woudc_extcsv.Reader to check values
        """

        # produce extcsv object
        extcsv = Writer(template=True)

        # add data here
        extcsv.add_comment('This file was generated by\
        WODC_TO_CSX v1.0 using WODC 80-column formatted data.')
        extcsv.add_comment('\'na\' is used where Instrument\
        Model or Number are not available.')
        extcsv.add_data('CONTENT', 'WOUDC,TotalOzone,1.0,1')
        extcsv.add_data('DATA_GENERATION', '2014-08-28,NOAA-CMDL,0.0')
        extcsv.add_data('PLATFORM', 'STN,031,MAUNA LOA,USA')
        extcsv.add_data('INSTRUMENT', 'Dobson,Beck,076')
        extcsv.add_data('LOCATION', '19.533,-155.574,3405')
        extcsv.add_data('TIMESTAMP', '+00:00:00,2014-04-01')
        extcsv.add_data('DAILY', '2014-04-01,0,2,283,,,,18',
                        field='Date,WLCode,ObsCode,ColumnO3,StdDevO3,\
UTC_Begin,UTC_End,UTC_Mean,nObs,mMu,ColumnSO2')
        extcsv.add_data('DAILY', '2014-04-08,0,0,288,,,,23')
        extcsv.add_data('DAILY', '2014-04-09,0,0,279,,,,23')
        extcsv.add_data('DAILY', '2014-04-10,0,0,273,,,,24')
        extcsv.add_data('DAILY', '2014-04-11,0,0,274,,,,21')
        extcsv.add_data('DAILY', '2014-04-12,0,2,271,,,,18')
        extcsv.add_data('DAILY', '2014-04-13,0,2,274,,,,18')
        extcsv.add_data('DAILY', '2014-04-14,0,0,283,,,,23')
        extcsv.add_data('DAILY', '2014-04-15,0,0,285,,,,23')
        extcsv.add_data('DAILY', '2014-04-16,0,0,284,,,,23')
        extcsv.add_data('DAILY', '2014-04-17,0,0,280,,,,22')
        extcsv.add_data('DAILY', '2014-04-18,0,2,268,,,,18')
        extcsv.add_data('DAILY', '2014-04-19,0,2,271,,,,18')
        extcsv.add_data('DAILY', '2014-04-20,0,2,264,,,,18')
        extcsv.add_data('DAILY', '2014-04-21,0,0,278,,,,23')
        extcsv.add_data('DAILY', '2014-04-22,0,0,276,,,,21')
        extcsv.add_data('DAILY', '2014-04-23,0,0,280,,,,23')
        extcsv.add_data('DAILY', '2014-04-24,0,0,269,,,,22')
        extcsv.add_data('DAILY', '2014-04-25,0,0,275,,,,21')
        extcsv.add_data('DAILY', '2014-04-26,0,2,278,,,,18')
        extcsv.add_data('DAILY', '2014-04-28,0,0,296,,,,21')
        extcsv.add_data('DAILY', '2014-04-29,0,0,291,,,,23')
        extcsv.add_data('DAILY', '2014-04-30,0,0,294,,,,21',
                        table_comment='    1992 Coefficients in use')
        extcsv.add_data('TIMESTAMP', '+00:00:00,2014-04-30',
                        field='UTCOffset,Date,Time', index=2)
        extcsv.add_data('MONTHLY', '2014-04-01,279,8.3,23',
                        field='Date,ColumnO3,StdDevO3,Npts')

        extcsv_s = dumps(extcsv)

        # load my extcsv into Reader
        my_extcsv_to = loads(extcsv_s)
        # check tables
        self.assertTrue('DAILY' in my_extcsv_to.sections,
                        'check totalozone table in my extcsv')
        self.assertTrue('PLATFORM' in my_extcsv_to.sections,
                        'check totalozone table in my extcsv')
        self.assertTrue('LOCATION' in my_extcsv_to.sections,
                        'check totalozone table in my extcsv')
        self.assertTrue('TIMESTAMP' in my_extcsv_to.sections,
                        'check totalozone table in my extcsv')
        self.assertTrue('DATA_GENERATION' in my_extcsv_to.sections,
                        'check totalozone table in my extcsv')
        self.assertTrue('TIMESTAMP2' in my_extcsv_to.sections,
                        'check totalozone table in my extcsv')
        self.assertTrue('MONTHLY' in my_extcsv_to.sections,
                        'check totalozone in my extcsv')

        # check fields
        self.assertTrue('Level' in my_extcsv_to.sections['CONTENT'],
                        'check totalozone field in my extcsv')
        self.assertTrue('UTCOffset' in my_extcsv_to.sections['TIMESTAMP'],
                        'check totalozone field in my extcsv')
        self.assertTrue('ScientificAuthority'
                        in my_extcsv_to.sections['DATA_GENERATION'],
                        'check totalozone field in my extcsv')
        self.assertTrue('Time' in my_extcsv_to.sections['TIMESTAMP2'],
                        'check totalozone field in my extcsv')
        self.assertTrue('ColumnO3' in my_extcsv_to.sections['MONTHLY'],
                        'check totalozone  field in my extcsv')

        # check values
        self.assertEqual('19.533',
                         my_extcsv_to.sections['LOCATION']['Latitude'],
                         'check totalozone value in my extcsv')
        self.assertEqual('NOAA-CMDL',
                         my_extcsv_to.sections['DATA_GENERATION']['Agency'],
                         'check totalozone value in my extcsv')
        self.assertEqual('1', my_extcsv_to.sections['CONTENT']['Form'],
                         'check totalozone value in my extcsv')
        self.assertEqual('23', my_extcsv_to.sections['MONTHLY']['Npts'],
                         'check totalozone value in my extcsv')
        my_to_daily = StringIO(my_extcsv_to.sections['DAILY']['_raw'])
        my_daily_rows = csv.reader(my_to_daily)
        my_daily_header = next(my_daily_rows)
        self.assertTrue('WLCode' in my_daily_header,
                        'check totalozone daily field in my extcsv')
        self.assertTrue('nObs' in my_daily_header,
                        'check totalozone daily field in my extcsv')
        self.assertTrue('ColumnO3' in my_daily_header,
                        'check totalozone daily field in my extcsv')
        self.assertTrue('ColumnSO2' in my_daily_header,
                        'check totalozone daily field in my extcsv')
        self.assertEqual(0, my_daily_header.index('Date'),
                         'check totalozone daily field order in my extcsv')
        self.assertEqual(len(my_daily_header) - 1,
                         my_daily_header.index('ColumnSO2'),
                         'check totalozone daily field order in my extcsv')
        my_daily_row = None
        # seek
        for i in range(1, 6):
            my_daily_row = next(my_daily_rows)
        self.assertEqual('274',
                         my_daily_row[my_daily_header.index('ColumnO3')],
                         'check totalozone daily value in my extcsv')
        self.assertEqual('',
                         my_daily_row[my_daily_header.index('StdDevO3')],
                         'check totalozone daily value in my extcsv')
        self.assertEqual('',
                         my_daily_row[my_daily_header.index('UTC_Begin')],
                         'check totalozone daily value in my extcsv')
        self.assertEqual('21',
                         my_daily_row[my_daily_header.index('UTC_Mean')],
                         'check totalozone daily value in my extcsv')
        for i in range(1, 18):
            my_daily_row = next(my_daily_rows)
        self.assertEqual('291',
                         my_daily_row[my_daily_header.index('ColumnO3')],
                         'check totalozone daily value in my extcsv')
        self.assertEqual('23',
                         my_daily_row[my_daily_header.index('UTC_Mean')],
                         'check totalozone daily value in my extcsv')

        with self.assertRaises(TypeError):
            extcsv_s = dump(extcsv)

    def test_reader_error(self):
        """Test if reader error is thrown
        for malformed extcsv
        """

        with self.assertRaises(WOUDCExtCSVReaderError):
            extcsv_to = load_test_data_file(
                'data/20061201.brewer.mkiv.153.imd-bad.csv'
            )
            self.assertIsInstance(extcsv_to, Reader,
                                  'validate instance type')


class ValidatorTest(unittest.TestCase):
    """Test suite for Writer"""

    def setUp(self):
        """setup test fixtures, etc."""

        print(msg(self.id(), self.shortDescription()))

    def tearDown(self):
        """return to pristine state"""

        pass

    def test_bad_platform_name(self):
        """Test that bad platform names are resolved using platform ID"""

        contents = get_file_string('tests/data/UV617FEB-bad-platform.woudc')
        reader = loads(contents)
        dict = reader.metadata_validator()
        self.assertEqual(''.join(dict['errors']), 'Platform name of Sapporo does not \
match database. Please change it to Lauder')

    def test_bad_platform_id(self):
        """Test that bad platform IDs are resolved using platform name"""

        contents = get_file_string('tests/data/UV617FEB-bad-platform-id.woudc')
        reader = loads(contents)
        dict = reader.metadata_validator()
        self.assertEqual(''.join(dict['errors']), 'Platform ID of 024 does not match \
database. Please change it to 256')

    def test_bad_platform_country(self):
        """Test that country names are resolved to country codes"""

        contents =\
            get_file_string('tests/data/UV617FEB-bad-platform-country.woudc')
        reader = loads(contents)
        dict = reader.metadata_validator()
        self.assertEqual(''.join(dict['errors']), 'Platform country of New Zealand \
does not match database. Please change it to NZL')

    def test_bad_agency(self):
        """Test that platform info is used to resolve bad agencies"""

        contents = get_file_string('tests/data/UV617FEB-bad-agency.woudc')
        reader = loads(contents)
        dict = reader.metadata_validator()
        self.assertTrue('The following agencies match the given \
platform name and/or ID:' in ''.join(dict['errors']))

    def test_different_agency(self):
        """Test that a valid (but wrong) agency is distinct
           from a bad agency
        """

        contents =\
            get_file_string('tests/data/UV617FEB-different-agency.woudc')
        reader = loads(contents)
        dict = reader.metadata_validator()
        self.assertTrue('Agency and Platform information do not \
match. These agencies are valid for this \
platform:' in ''.join(dict['errors']))

    def test_agency_name(self):
        """Test that agency names are resolved to acronyms"""

        contents = get_file_string('tests/data/UV617FEB-agency-name.woudc')
        reader = loads(contents)
        dict = reader.metadata_validator()
        self.assertEqual(''.join(dict['errors']), 'Please use the Agency \
acronym of NIWA-LAU.')

    def test_bad_location(self):
        """Test that locations off by >= 1 degree are caught"""

        contents = get_file_string('tests/data/UV617FEB-bad-location.woudc')
        reader = loads(contents)
        dict = reader.metadata_validator()
        self.assertEqual(''.join(dict['errors']), 'Location Latitude of -46.038 does \
not match database. Please change it to -45.0379981995.')

    def test_bad_instrument_name(self):
        """Test that unknown instrument name produces
           tentative new instrument error
        """

        contents =\
            get_file_string('tests/data/UV617FEB-bad-instrument-name.woudc')
        reader = loads(contents)
        dict = reader.metadata_validator()
        self.assertTrue('Instrument Name is not in database. \
Please verify that it is correct.' ''.join(dict['errors']))

    def test_bad_instrument_model(self):
        """Test that unknown instrument model produces
           tentative new model error
        """

        contents =\
            get_file_string('tests/data/UV617FEB-bad-instrument-model.woudc')
        reader = loads(contents)
        dict = reader.metadata_validator()
        self.assertTrue('Instrument Model is not in database. \
Please verify that it is correct.' in ''.join(dict['errors']))

    def test_no_agency_matches(self):
        """Test that bad agency and platform information produces
           tentative new agency error
        """

        contents = get_file_string('tests/data/UV617FEB-no-match.woudc')
        reader = loads(contents)
        dict = reader.metadata_validator()
        self.assertEqual(''.join(dict['errors']), 'Agency acronym of ZZZZZZ not \
found in the woudc database. If this is a new agency, \
please notify WOUDC')

    def test_no_matches(self):
        """Test that a bad platform name and ID produces no matches"""

        contents =\
            get_file_string('tests/data/UV617FEB-no-platform-match.woudc')
        reader = loads(contents)
        dict = reader.metadata_validator()
        self.assertEqual(''.join(dict['errors']), 'Could not find a record for \
either the platform name or ID. If this is a new \
station, please notify WOUDC.')

    def test_bad_content_level(self):
        """Test that content level has to be 1.0 or 2.0"""

        contents =\
            get_file_string('tests/data/UV617FEB-bad-content-level.woudc')
        reader = loads(contents)
        dict = reader.metadata_validator()
        self.assertEqual(''.join(dict['errors']), 'Level for category Spectral \
must be 1.0')

    def test_trailing_commas(self):
        """Test that trailing commas are detected"""

        contents =\
            get_file_string('tests/data/UV617FEB-trailing-commas.woudc')
        reader = loads(contents)
        dict = reader.metadata_validator()
        self.assertTrue('This file has extra trailing commas. \
Please remove them before submitting.' in ''.join(dict['errors']))

    def test_good_file(self):
        """Test that a good file passes validation"""

        contents = get_file_string('tests/data/UV617FEB.woudc')
        reader = loads(contents)
        dict = reader.metadata_validator()
        self.assertTrue(dict['status'])

    def test_non_ascii_file(self):
        """Test that a non_ascii file passes validation"""

        contents = get_file_string('tests/data/test-non-ascii.TO1')
        reader = loads(contents, encoding='latin1')
        dict = reader.metadata_validator()
        self.assertTrue(dict['status'])


# main
if __name__ == '__main__':
    unittest.main()
