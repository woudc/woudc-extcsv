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

import io
import logging
import unittest

from datetime import date, time
from woudc_extcsv import (ExtendedCSV, Reader, Writer, _table_index,
                          DOMAINS, dumps, load,
                          NonStandardDataError, MetadataValidationError)

LOGGER = logging.getLogger(__name__)


def get_data(extcsv, table, field, index=1):
    """helper function, gets data from extcsv object"""

    table_t = _table_index(table, index)
    return extcsv.extcsv[table_t][field]


def get_file_string(file_path):
    """helper function, to open test file and return
       unicode string of file contents
    """

    try:
        with io.open(file_path, encoding='utf-8') as ff:
            return ff.read()
    except UnicodeError:
        with io.open(file_path, encoding='latin1') as ff:
            return ff.read()


def load_test_data_file(filename, reader=True):
    """helper function to open test file regardless of invocation"""
    try:
        return load(filename, reader=reader)
    except IOError:
        return load('tests/{}'.format(filename), reader=reader)


def msg(test_id, test_description):
    """helper function to print out test id and desc"""

    return '%s: %s' % (test_id, test_description)


class ParserTest(unittest.TestCase):
    """Test suite for ExtendedCSV Parser and Parser helpers"""

    def setUp(self):
        """setup test fixtures, etc."""

        print(msg(self.id(), self.shortDescription()))

    def tearDown(self):
        """return to pristine state"""

        pass

    def test_get_value_type(self):
        """test value typing"""

        # Create empty dummy ExtendedCSV object for testing
        dummy = ExtendedCSV('')

        self.assertIsNone(dummy.typecast_value('Dummy', 'TEst', '', 0))
        self.assertIsInstance(
            dummy.typecast_value('Dummy', 'TEST', 'foo', 0), str)
        self.assertIsInstance(
            dummy.typecast_value('Dummy', 'test', '1', 0), int)
        self.assertIsInstance(
            dummy.typecast_value('Dummy', 'test', '022', 0), str)
        self.assertIsInstance(
            dummy.typecast_value('Dummy', 'test', '1.0', 0), float)
        self.assertIsInstance(
            dummy.typecast_value('Dummy', 'test', '1.0-1', 0), str)
        self.assertIsInstance(
            dummy.typecast_value('Dummy', 'date', '2011-11-11', 0), date)
        self.assertIsInstance(
            dummy.typecast_value('Dummy', 'time', '11:11:11', 0), time)
        self.assertIsInstance(
            dummy.typecast_value('Dummy', 'utcoffset', '+00:00:00', 0), str)

        bad_input = 'a generic string'
        self.assertEqual(
            dummy.typecast_value('Dummy', 'date', bad_input, 0), bad_input)
        self.assertEqual(
            dummy.typecast_value('Dummy', 'time', bad_input, 0), bad_input)
        self.assertEqual(
            dummy.typecast_value('Dum', 'utcoffset', bad_input, 0), bad_input)

    def test_field_capitalization(self):
        """Test that field names with incorrect capitalizations are fixed"""

        ecsv = load_test_data_file(
            'data/general/ecsv-field-capitalization.csv',
            reader=False
        )

        ecsv.validate_metadata_tables()

        content_fields = DOMAINS['Common']['CONTENT']['required_fields']
        content_values = ['WOUDC', 'TotalOzone', 1.0, 1]
        for field, value in zip(content_fields, content_values):
            self.assertIn(field, ecsv.extcsv['CONTENT'])
            self.assertEqual(ecsv.extcsv['CONTENT'][field], value)

        platform_fields = DOMAINS['Common']['PLATFORM']['required_fields'] \
            + DOMAINS['Common']['PLATFORM']['optional_fields']
        platform_values = ['STN', '002', 'Tamanrasset', 'DZA', None]
        for field, value in zip(platform_fields, platform_values):
            self.assertIn(field, ecsv.extcsv['PLATFORM'])
            self.assertEqual(ecsv.extcsv['PLATFORM'][field], value)

        ecsv.validate_dataset_tables()

        definition = DOMAINS['Datasets']['TotalOzone']['1.0']['1']
        daily_fields = definition['DAILY']['required_fields'] \
            + definition['DAILY'].get('optional_fields', [])
        for field in daily_fields:
            self.assertIn(field, ecsv.extcsv['DAILY'])
            self.assertEqual(len(ecsv.extcsv['DAILY'][field]), 30)

    def test_non_extcsv(self):
        """Test that various non-extcsv text files fail to parse"""

        # Text file is not in Extended CSV format
        with self.assertRaises(NonStandardDataError):
            ecsv = load_test_data_file(
                    'data/general/not-an-ecsv.dat', reader=False)
            ecsv.validate_metadata_tables()

        # Text file not in Extended CSV format, featuring non-ASCII characters
        with self.assertRaises(NonStandardDataError):
            ecsv = load_test_data_file(
                    'data/general/euc-jp.dat', reader=False)
            ecsv.validate_metadata_tables()

    def test_missing_required_table(self):
        """Test that files with missing required tables fail to parse"""

        ecsv = load_test_data_file(
            'data/general/ecsv-missing-location-table.csv',
            reader=False)

        self.assertIsInstance(ecsv, ExtendedCSV)

        with self.assertRaises(MetadataValidationError):
            ecsv.validate_metadata_tables()

    def test_missing_required_value(self):
        """Test that files with missing required values fail to parse"""

        # File contains empty/null value for required field
        ecsv = load_test_data_file(
                'data/general/ecsv-missing-location-latitude.csv',
                reader=False)
        self.assertIsInstance(ecsv, ExtendedCSV)

        with self.assertRaises(MetadataValidationError):
            ecsv.validate_metadata_tables()

        # Required column is entirely missing in the table
        with self.assertRaises(MetadataValidationError):
            ecsv = load_test_data_file(
                    'data/general/ecsv-missing-instrument-name.csv',
                    reader=False)
            ecsv.validate_metadata_tables()

    def test_missing_optional_table(self):
        """Test that files with missing optional tables parse successfully"""

        ecsv = load_test_data_file(
                'data/general/ecsv-missing-monthly-table.csv',
                reader=False)
        ecsv.validate_metadata_tables()

        ecsv.validate_dataset_tables()
        self.assertNotIn('MONTHLY', ecsv.extcsv)
        self.assertTrue(set(DOMAINS['Common']).issubset(
                        set(ecsv.extcsv.keys())))

    def test_missing_optional_value(self):
        """Test that files with missing optional values parse successfully"""

        # File contains empty/null value for optional LOCATION.Height
        ecsv = load_test_data_file(
                'data/general/ecsv-missing-location-height.csv',
                reader=False)
        ecsv.validate_metadata_tables()

        self.assertIsNone(ecsv.extcsv['LOCATION']['Height'])

        # File missing whole optional column - PLATFORM.GAW_ID
        ecsv = load_test_data_file(
                'data/general/ecsv-missing-platform-gawid.csv',
                reader=False)
        ecsv.validate_metadata_tables()

        self.assertIn('GAW_ID', ecsv.extcsv['PLATFORM'])
        self.assertIsNone(ecsv.extcsv['PLATFORM']['GAW_ID'])

    def test_empty_tables(self):
        """Test that files fail to parse if a table has no rows of values"""

        with self.assertRaises(NonStandardDataError):
            ecsv = load_test_data_file(
                    'data/general/ecsv-empty-timestamp2-table.csv',
                    reader=False)
            ecsv.validate_metadata_tables()

    def test_table_height(self):
        """Test that files fail to parse if a table has too many rows"""

        with self.assertRaises(MetadataValidationError):
            ecsv = load_test_data_file(
                    'data/general/ecsv-excess-timestamp-table-rows.csv',
                    reader=False)
            ecsv.validate_metadata_tables()

    def test_table_occurrences(self):
        """Test that files fail to parse if a table appears too many times"""

        with self.assertRaises(MetadataValidationError):
            ecsv = load_test_data_file(
                    'data/general/ecsv-excess-location-table.csv',
                    reader=False)
            ecsv.validate_metadata_tables()

    def test_line_spacing(self):
        """Test that files can parse no matter the space between tables"""

        ecsv = load_test_data_file(
                'data/general/ecsv-no-spaced.csv',
                reader=False)
        ecsv.validate_metadata_tables()
        self.assertTrue(set(DOMAINS['Common']).issubset(set(ecsv.extcsv)))

        ecsv = load_test_data_file(
                'data/general/ecsv-double-spaced.csv',
                reader=False)
        ecsv.validate_metadata_tables()
        self.assertTrue(set(DOMAINS['Common']).issubset(set(ecsv.extcsv)))

    def test_comments(self):
        """Test that comments in files are ignored while parsing"""

        ecsv = load_test_data_file(
                'data/general/ecsv-comments.csv',
                reader=False)
        ecsv.validate_metadata_tables()
        self.assertTrue(set(DOMAINS['Common']).issubset(set(ecsv.extcsv)))


class TimestampParsingTest(unittest.TestCase):
    """Test suite for parser.ExtendedCSV._parse_timestamp"""

    def setUp(self):
        # Only need a dummy parser since no input is coming from files.
        self.parser = ExtendedCSV('')

    def _parse_timestamp(self, raw_string):
        return self.parser.parse_timestamp('Dummy', raw_string, 0)

    def test_success(self):
        """Test parsing valid timestamps"""

        self.assertEqual(self._parse_timestamp('00:00:00'), time(hour=0))
        self.assertEqual(self._parse_timestamp('12:00:00'), time(hour=12))

        self.assertEqual(self._parse_timestamp('21:30:00'),
                         time(hour=21, minute=30))
        self.assertEqual(self._parse_timestamp('16:00:45'),
                         time(hour=16, second=45))
        self.assertEqual(self._parse_timestamp('11:10:30'),
                         time(hour=11, minute=10, second=30))

        self.assertEqual(self._parse_timestamp('0:30:00'),
                         time(hour=0, minute=30))
        self.assertEqual(self._parse_timestamp('9:15:00'),
                         time(hour=9, minute=15))

    def test_invalid_parts(self):
        """Test parsing timestamps fails with non-numeric characters"""

        with self.assertRaises(ValueError):
            self._parse_timestamp('0a:00:00')
        with self.assertRaises(ValueError):
            self._parse_timestamp('z:00:00')
        with self.assertRaises(ValueError):
            self._parse_timestamp('12:a2:00')
        with self.assertRaises(ValueError):
            self._parse_timestamp('12:20:kb')

        with self.assertRaises(ValueError):
            self._parse_timestamp('A generic string')

    def test_out_of_range(self):
        """
        Test parsing timestamps where components have invalid
        numeric values
        """

        self.assertEqual(self._parse_timestamp('08:15:60'),
                         time(hour=8, minute=16))
        self.assertEqual(self._parse_timestamp('08:15:120'),
                         time(hour=8, minute=17))
        self.assertEqual(self._parse_timestamp('08:15:90'),
                         time(hour=8, minute=16, second=30))

        self.assertEqual(self._parse_timestamp('08:60:00'), time(hour=9))
        self.assertEqual(self._parse_timestamp('08:75:00'),
                         time(hour=9, minute=15))
        self.assertEqual(self._parse_timestamp('08:120:00'), time(hour=10))

        self.assertEqual(self._parse_timestamp('08:84:60'),
                         time(hour=9, minute=25))
        self.assertEqual(self._parse_timestamp('08:84:150'),
                         time(hour=9, minute=26, second=30))
        self.assertEqual(self._parse_timestamp('08:85:36001'),
                         time(hour=19, minute=25, second=1))

    def test_bad_separators(self):
        """Test parsing timestamps with separators other than ':'"""

        self.assertEqual(self._parse_timestamp('01-30-00'),
                         time(hour=1, minute=30))
        self.assertEqual(self._parse_timestamp('01/30/00'),
                         time(hour=1, minute=30))

        self.assertEqual(self._parse_timestamp('01:30-00'),
                         time(hour=1, minute=30))
        self.assertEqual(self._parse_timestamp('01-30:00'),
                         time(hour=1, minute=30))

    def test_12_hour_clock(self):
        """Test parsing timestamps which use am/pm format"""

        self.assertEqual(self._parse_timestamp('01:00:00 am'), time(hour=1))
        self.assertEqual(self._parse_timestamp('01:00:00 pm'), time(hour=13))

        self.assertEqual(self._parse_timestamp('05:30:00 am'),
                         time(hour=5, minute=30))
        self.assertEqual(self._parse_timestamp('05:30:00 pm'),
                         time(hour=17, minute=30))

        self.assertEqual(self._parse_timestamp('12:00:00 am'), time(hour=0))
        self.assertEqual(self._parse_timestamp('12:00:00 pm'), time(hour=12))


class DatestampParsingTest(unittest.TestCase):
    """Test suite for parser.ExtendedCSV._parse_datestamp"""

    def setUp(self):
        # Only need a dummy parser since no input is coming from files.
        self.parser = ExtendedCSV('')

    def _parse_datestamp(self, raw_string):
        return self.parser.parse_datestamp('Dummy', raw_string, 0)

    def test_success(self):
        """Test parsing valid dates"""

        self.assertEqual(self._parse_datestamp('2013-05-01'),
                         date(year=2013, month=5, day=1))
        self.assertEqual(self._parse_datestamp('1968-12-31'),
                         date(year=1968, month=12, day=31))

        self.assertEqual(self._parse_datestamp('1940-01-01'),
                         date(year=1940, month=1, day=1))
        self.assertEqual(self._parse_datestamp('2000-02-28'),
                         date(year=2000, month=2, day=28))

        present = date.today()
        self.assertEqual(self._parse_datestamp(present.strftime('%Y-%m-%d')),
                         present)

    def test_invalid_parts(self):
        """Test parsing dates fails with non-numeric characters"""

        with self.assertRaises(ValueError):
            self._parse_datestamp('2019AD-10-31')
        with self.assertRaises(ValueError):
            self._parse_datestamp('z-02-14')
        with self.assertRaises(ValueError):
            self._parse_datestamp('2016-1a-00')
        with self.assertRaises(ValueError):
            self._parse_datestamp('1994-0k-gb')

        with self.assertRaises(ValueError):
            self._parse_datestamp('A generic string')

    def test_out_of_range(self):
        """Test parsing dates where components have invalid numeric values"""

        with self.assertRaises(ValueError):
            self._parse_datestamp('2001-04-35')
        with self.assertRaises(ValueError):
            self._parse_datestamp('2014-06-00')
        with self.assertRaises(ValueError):
            self._parse_datestamp('1971-02-30')

        with self.assertRaises(ValueError):
            self._parse_datestamp('1996-31-12')
        with self.assertRaises(ValueError):
            self._parse_datestamp('2003-00-01')

    def test_bad_separators(self):
        """Test parsing dates with separators other than '-'"""

        self.assertEqual(self._parse_datestamp('2019/01/24'),
                         date(year=2019, month=1, day=24))
        self.assertEqual(self._parse_datestamp('2019:01:24'),
                         date(year=2019, month=1, day=24))

        self.assertEqual(self._parse_datestamp('2019:01/24'),
                         date(year=2019, month=1, day=24))
        self.assertEqual(self._parse_datestamp('2019-01/24'),
                         date(year=2019, month=1, day=24))
        self.assertEqual(self._parse_datestamp('2019:01-24'),
                         date(year=2019, month=1, day=24))

    def test_number_of_parts(self):
        """Test parsing dates with incorrect numbers of components"""

        with self.assertRaises(ValueError):
            self._parse_datestamp('20190124')
        with self.assertRaises(ValueError):
            self._parse_datestamp('2019-0124')
        with self.assertRaises(ValueError):
            self._parse_datestamp('201901-24')
        with self.assertRaises(ValueError):
            self._parse_datestamp('2019')

        with self.assertRaises(ValueError):
            self._parse_datestamp('2019-01-24-12-30')
        with self.assertRaises(ValueError):
            self._parse_datestamp('2019-06-30:16')


class UTCOffsetParsingTest(unittest.TestCase):
    """Test suite for parser.ExtendedCSV._parse_utcoffset"""

    def setUp(self):
        # Only need a dummy parser since no input is coming from files.
        self.parser = ExtendedCSV('')

    def _parse_offset(self, raw_string):
        return self.parser.parse_utcoffset('Dummy', raw_string, 0)

    def test_success(self):
        """Test parsing valid UTC offsets"""

        candidates = [
            '+09:00:00',
            '-04:00:00',
            '+01:30:00',
            '+11:15:30',
            '-03:00:45'
        ]

        for candidate in candidates:
            self.assertEqual(self._parse_offset(candidate), candidate)

    def test_sign_variation(self):
        """Test parsing UTC offsets with various signs (or lacks thereof)"""
        self.assertEqual(self._parse_offset('+05:30:00'), '+05:30:00')
        self.assertEqual(self._parse_offset('05:30:00'), '+05:30:00')

        self.assertEqual(self._parse_offset('-08:00:00'), '-08:00:00')
        # The case below occasionally shows up during backfilling.
        self.assertEqual(self._parse_offset('+-08:00:00'), '-08:00:00')

        self.assertEqual(self._parse_offset('+00:00:00'), '+00:00:00')
        self.assertEqual(self._parse_offset('-00:00:00'), '+00:00:00')
        self.assertEqual(self._parse_offset('00:00:00'), '+00:00:00')

    def test_missing_parts(self):
        """Test parsing UTC offsets where not all parts are provided."""

        self.assertEqual(self._parse_offset('+13:00:'), '+13:00:00')
        self.assertEqual(self._parse_offset('+13::'), '+13:00:00')
        self.assertEqual(self._parse_offset('+13::00'), '+13:00:00')

        self.assertEqual(self._parse_offset('-02:30:'), '-02:30:00')
        self.assertEqual(self._parse_offset('-02::56'), '-02:00:56')

        with self.assertRaises(ValueError):
            self._parse_offset(':00:00')
        with self.assertRaises(ValueError):
            self._parse_offset('+:00:00')
        with self.assertRaises(ValueError):
            self._parse_offset('::')

    def test_part_lengths(self):
        """Test parsing UTC offsets where some parts are not 2 digits long"""

        self.assertEqual(self._parse_offset('+0:00:00'), '+00:00:00')
        self.assertEqual(self._parse_offset('+8:00:00'), '+08:00:00')
        self.assertEqual(self._parse_offset('-6:00:00'), '-06:00:00')

        self.assertEqual(self._parse_offset('+11:3:'), '+11:03:00')
        self.assertEqual(self._parse_offset('-5:5:'), '-05:05:00')
        self.assertEqual(self._parse_offset('-6:12:4'), '-06:12:04')

        with self.assertRaises(ValueError):
            self._parse_offset('+001:00:00')
        with self.assertRaises(ValueError):
            self._parse_offset('+00:350:00')
        with self.assertRaises(ValueError):
            self._parse_offset('-09:00:314159')

    def test_out_of_range(self):
        """Test that UTC offsets with invalid numeric parts fail to parse"""

        with self.assertRaises(ValueError):
            self._parse_offset('+60:00:00')
        with self.assertRaises(ValueError):
            self._parse_offset('-03:700:00')
        with self.assertRaises(ValueError):
            self._parse_offset('-00:00:10800')

    def test_missing_separators(self):
        """Test parsing UTC offsets where there are fewer than 3 parts"""

        self.assertEqual(self._parse_offset('-03:30'), '-03:30:00')
        self.assertEqual(self._parse_offset('06:15'), '+06:15:00')

        self.assertEqual(self._parse_offset('-10'), '-10:00:00')
        self.assertEqual(self._parse_offset('04'), '+04:00:00')

        self.assertEqual(self._parse_offset('+0'), '+00:00:00')
        self.assertEqual(self._parse_offset('0'), '+00:00:00')
        self.assertEqual(self._parse_offset('000000'), '+00:00:00')
        self.assertEqual(self._parse_offset('-000000'), '+00:00:00')

    def test_bad_separators(self):
        """Test parsing dates with separators other than '-'"""

        self.assertEqual(self._parse_offset('+02|45|00'), '+02:45:00')
        self.assertEqual(self._parse_offset('+02/45/00'), '+02:45:00')

        self.assertEqual(self._parse_offset('+02:45/00'), '+02:45:00')
        self.assertEqual(self._parse_offset('+02|45:00'), '+02:45:00')
        self.assertEqual(self._parse_offset('+02/45|00'), '+02:45:00')

    def test_invalid_parts(self):
        """Test parsing UTC offsets which contain non-numeric characters"""

        with self.assertRaises(ValueError):
            self._parse_offset('-08:00:4A')
        with self.assertRaises(ValueError):
            self._parse_offset('-08:z1:00')
        with self.assertRaises(ValueError):
            self._parse_offset('-b4:10:4A')

        with self.assertRaises(ValueError):
            self._parse_offset('a generic string')


class DatasetValidationTest(unittest.TestCase):
    """Test suite for dataset-specific validation checks and corrections"""

    def test_totalozone_checks(self):
        """Test that TotalOzone checks produce expected warnings/errors"""

        # Test a file with bad delimiter
        ecsv = load_test_data_file(
                'data/totalozone/totalozone-bad-separator.csv',
                reader=False)
        ecsv.validate_metadata_tables()
        ecsv.validate_dataset_tables()
        self.assertEqual(len(ecsv.warnings), 1)
        # self.assertEqual(ecsv.warnings[0],
        #    "Improper delimiter used '{separator}' corrected to ',' (comma)")
        self.assertEqual(len(ecsv.errors), 0)

        # Test a file with empty lines between table name and fields
        ecsv = load_test_data_file(
                'data/totalozone/totalozone-empty-lines.csv',
                reader=False)
        self.assertEqual(None, ecsv.validate_metadata_tables())
        self.assertEqual(True, ecsv.validate_dataset_tables())
        self.assertEqual(len(ecsv.warnings), 9)

        # Test file missing #DAILY.ColumnsSO2
        ecsv = load_test_data_file(
                'data/totalozone/totalozone-missing-location.csv',
                reader=False)
        with self.assertRaises(MetadataValidationError):
            ecsv.validate_metadata_tables()
            ecsv.validate_dataset_tables()
        self.assertEqual(len(ecsv.warnings), 0)
        self.assertEqual(len(ecsv.errors), 2)
        self.assertEqual(ecsv.errors[0],
                         'Required field #LOCATION.Latitude is null or empty')

        # Test file where CONTENT.Form is not included
        ecsv = load_test_data_file(
                'data/totalozone/totalozone-missing-form.csv', reader=False)
        with self.assertRaises(MetadataValidationError):
            ecsv.validate_metadata_tables()
            ecsv.validate_dataset_tables()
        self.assertEqual(len(ecsv.warnings), 0)
        self.assertEqual(len(ecsv.errors), 1)
        self.assertEqual(ecsv.errors[0],
                         'Missing required field #CONTENT.Form')

        # Test a file with no issues
        ecsv = load_test_data_file(
                'data/totalozone/totalozone-correct.csv',
                reader=False)
        self.assertEqual(None, ecsv.validate_metadata_tables())
        self.assertEqual(True, ecsv.validate_dataset_tables())
        self.assertEqual(len(ecsv.warnings), 0)
        self.assertEqual(len(ecsv.errors), 0)

    def test_totalozoneobs_checks(self):
        """Test that TotalOzoneObs checks produce expected warnings/errors"""

        # Test that incorrect capitalization is detected
        ecsv = load_test_data_file(
                'data/totalozoneobs/totalozoneobs-capitalizations.csv',
                reader=False)
        ecsv.validate_metadata_tables()
        ecsv.validate_dataset_tables()
        self.assertEqual(len(ecsv.warnings), 2)
        self.assertEqual(ecsv.warnings[0],
                         '#OBSERVATIONS field WLcode capitalization should '
                         'be WLCode')
        self.assertEqual(len(ecsv.errors), 0)

        # Test that extra field is detected
        ecsv = load_test_data_file(
                'data/totalozoneobs/totalozoneobs-extra-field.csv',
                reader=False)
        ecsv.validate_metadata_tables()
        ecsv.validate_dataset_tables()
        self.assertEqual(len(ecsv.warnings), 1)
        self.assertEqual(len(ecsv.errors), 0)

        # Test that no warnings/errors show up for a correct file
        ecsv = load_test_data_file(
                'data/totalozoneobs/totalozoneobs-correct.csv',
                reader=False)
        ecsv.validate_metadata_tables()
        ecsv.validate_dataset_tables()
        self.assertEqual(len(ecsv.warnings), 0)
        self.assertEqual(len(ecsv.errors), 0)

    def test_spectral_checks(self):
        """Test that Spectral checks produce warnings/errors when expected"""

        # Test file with an excess profile table
        ecsv = load_test_data_file(
                'data/spectral/spectral-extra-profile.csv',
                reader=False)
        self.assertEqual(None, ecsv.validate_metadata_tables())
        self.assertTrue(ecsv.validate_dataset_tables())
        self.assertEqual(len(ecsv.warnings), 0)
        self.assertEqual(len(ecsv.errors), 0)

        # Test another file with extra tables
        ecsv = load_test_data_file(
            'data/spectral/spectral-extra-timestamp.csv',
            reader=False)
        self.assertEqual(None, ecsv.validate_metadata_tables())
        self.assertTrue(ecsv.validate_dataset_tables())
        self.assertEqual(len(ecsv.warnings), 0)
        self.assertEqual(len(ecsv.errors), 0)

        # Refresh and test again with a good file
        ecsv = load_test_data_file(
                'data/spectral/spectral-correct.csv',
                reader=False)
        self.assertEqual(None, ecsv.validate_metadata_tables())
        self.assertTrue(ecsv.validate_dataset_tables())
        self.assertEqual(len(ecsv.warnings), 0)
        self.assertEqual(len(ecsv.errors), 0)

    def test_lidar_checks(self):
        """Test that Lidar checks produce warnings/errors when expected"""

        # Test file with an excess profile table
        ecsv = load_test_data_file(
                'data/lidar/lidar-extra-profile.csv',
                reader=False)
        self.assertEqual(None, ecsv.validate_metadata_tables())
        self.assertTrue(ecsv.validate_dataset_tables())
        self.assertEqual(len(ecsv.warnings), 0)
        self.assertEqual(len(ecsv.errors), 0)

        # Test with a different file with an extra table
        ecsv = load_test_data_file(
                'data/lidar/lidar-extra-summary.csv',
                reader=False)
        self.assertEqual(None, ecsv.validate_metadata_tables())
        self.assertTrue(ecsv.validate_dataset_tables())
        self.assertEqual(len(ecsv.warnings), 0)
        self.assertEqual(len(ecsv.errors), 0)

        # Refresh and test again with a third correct file
        ecsv = load_test_data_file(
                'data/lidar/lidar-correct.csv',
                reader=False)
        self.assertEqual(None, ecsv.validate_metadata_tables())
        self.assertTrue(ecsv.validate_dataset_tables())
        self.assertEqual(len(ecsv.warnings), 0)
        self.assertEqual(len(ecsv.errors), 0)

    def _helper_test_umkehr(self, level):
        """
        Test that Umkehr fils checks produce warnings/errors when expected,
        in an Umkehr Level <level> file.
        """

        if float(level) == 1.0:
            prefix = 'umkehr1'
        elif float(level) == 2.0:
            prefix = 'umkehr2'

        # Test a file with unique, out-of-order dates
        ecsv = load_test_data_file(
            'data/umkehr/{}-disordered.csv'.format(prefix),
            reader=False)
        self.assertEqual(None, ecsv.validate_metadata_tables())
        self.assertTrue(ecsv.validate_dataset_tables())
        self.assertEqual(len(ecsv.errors), 0)

        # Test a file with non-unique (and out-of-order) dates
        ecsv = load_test_data_file(
            'data/umkehr/{}-duplicated.csv'.format(prefix),
            reader=False)
        self.assertEqual(None, ecsv.validate_metadata_tables())
        self.assertTrue(ecsv.validate_dataset_tables())
        self.assertEqual(len(ecsv.errors), 0)

        # Test file where each TIMESTAMP.Date disagrees with the data table
        ecsv = load_test_data_file(
            'data/umkehr/{}-mismatch-timestamp-date.csv'.format(prefix),
            reader=False)
        self.assertEqual(None, ecsv.validate_metadata_tables())
        self.assertTrue(ecsv.validate_dataset_tables())
        self.assertEqual(len(ecsv.errors), 0)

        # Test file where TIMESTAMP.Times do not match between tables
        ecsv = load_test_data_file(
            'data/umkehr/{}-mismatch-timestamp-time.csv'.format(prefix),
            reader=False)
        self.assertEqual(None, ecsv.validate_metadata_tables())
        self.assertTrue(ecsv.validate_dataset_tables())
        self.assertEqual(len(ecsv.errors), 0)

        # Test that missing second TIMESTAMP table is detected/filled in
        ecsv = load_test_data_file(
            'data/umkehr/{}-missing-timestamp.csv'.format(prefix),
            reader=False)
        self.assertEqual(None, ecsv.validate_metadata_tables())
        self.assertTrue(ecsv.validate_dataset_tables())
        self.assertEqual(len(ecsv.errors), 0)

        # Test a file with no issues
        ecsv = load_test_data_file(
            'data/umkehr/{}-correct.csv'.format(prefix),
            reader=False)
        self.assertEqual(None, ecsv.validate_metadata_tables())
        self.assertTrue(ecsv.validate_dataset_tables())
        self.assertEqual(len(ecsv.errors), 0)

    def test_umkehr_level1_checks(self):
        """Test that expected warnings/errors are found in Umkehr Level 1"""

        self._helper_test_umkehr(1.0)

    def test_umkehr_level2_checks(self):
        """Test that expected warnings/errors are found in Umkehr Level 2"""

        self._helper_test_umkehr(2.0)


class ExtendedCSVTest(unittest.TestCase):
    """Test suite for ExtendedCSV"""

    def setUp(self):
        """setup test fixtures, etc."""

        print(msg(self.id(), self.shortDescription()))

    def tearDown(self):
        """return to pristine state"""

        pass

    def test_load_file(self):
        """Test loading an ExtendedCSV object"""

        with self.assertRaises(TypeError):
            load()
            load('data/totalozone/20061201.brewer.mkiv.153.imd.csv',
                 reader=False)

    def test_table_presence(self):
        """Test if tables exist"""
        extcsv_to =\
            load_test_data_file(
                    'data/totalozone/20061201.brewer.mkiv.153.imd.csv',
                    reader=False)
        self.assertTrue('CONTENT' in extcsv_to.extcsv,
                        'check totalozone table presence')
        self.assertTrue('DATA_GENERATION' in extcsv_to.extcsv,
                        'check totalozone table presence')
        self.assertTrue('DAILY' in extcsv_to.extcsv,
                        'check totalozone table presence')
        self.assertTrue('CONTENT' in extcsv_to.extcsv,
                        'check totalozone table presence')
        self.assertTrue('TIMESTAMP' in extcsv_to.extcsv,
                        'check totalozone table presence')

        extcsv_sp =\
            load_test_data_file(
                    'data/spectral/20040109.brewer.mkiv.144.epa_uga.csv',
                    reader=False)
        self.assertTrue('DATA_GENERATION' in extcsv_sp.extcsv,
                        'check spectral table presence')
        self.assertTrue('GLOBAL' in extcsv_sp.extcsv,
                        'check spectral table presence')
        self.assertTrue('GLOBAL_SUMMARY' in extcsv_sp.extcsv,
                        'check spectral table presence')

    def test_field_presence(self):
        """Test if fields exist"""

        extcsv_oz =\
            load_test_data_file(
                    'data/ozonesonde/20151021.ecc.6a.6a28340.smna.csv',
                    reader=False)
        self.assertTrue('Category' in extcsv_oz.extcsv['CONTENT'],
                        'check ozonesonde field presence')
        self.assertTrue('Version' in extcsv_oz.extcsv['DATA_GENERATION'],
                        'check ozonesonde field presence')
        self.assertTrue('ScientificAuthority'
                        in extcsv_oz.extcsv['DATA_GENERATION'],
                        'check ozonesonde field presence')
        self.assertTrue('Date' in extcsv_oz.extcsv['TIMESTAMP'],
                        'check ozonesonde field presence')
        self.assertTrue('SondeTotalO3' in extcsv_oz.extcsv['FLIGHT_SUMMARY'],
                        'check ozonesonde field presence')
        self.assertTrue('WLCode' in extcsv_oz.extcsv['FLIGHT_SUMMARY'],
                        'check ozonesonde field presence')
        self.assertTrue('ib2' in extcsv_oz.extcsv['AUXILIARY_DATA'],
                        'check ozonesonde field presence')
        self.assertTrue('BackgroundCorr'
                        in extcsv_oz.extcsv['AUXILIARY_DATA'],
                        'check ozonesonde field presence')
        self.assertTrue('O3PartialPressure' in
                        extcsv_oz.extcsv['PROFILE'],
                        'check ozonesonde field presence')
        self.assertTrue('O3PartialPressure' in
                        extcsv_oz.extcsv['PROFILE'],
                        'check ozonesonde field presence')
        self.assertTrue('Pressure' in
                        extcsv_oz.extcsv['PROFILE'],
                        'check ozonesonde field presence')
        self.assertTrue('WindSpeed' in
                        extcsv_oz.extcsv['PROFILE'],
                        'check ozonesonde field presence')
        self.assertTrue('SampleTemperature' in
                        extcsv_oz.extcsv['PROFILE'],
                        'check ozonesonde field presence')

    def test_value(self):
        """Test values"""

        extcsv_to =\
            load_test_data_file(
                    'data/totalozone/20061201.brewer.mkiv.153.imd.csv',
                    reader=False)
        self.assertEqual('WOUDC', extcsv_to.extcsv['CONTENT']['Class'][0],
                         'check totalozone value')
        self.assertEqual('', extcsv_to.extcsv['PLATFORM']['GAW_ID'][0],
                         'check totalozone value')
        self.assertEqual('11.45', extcsv_to.extcsv['LOCATION']['Longitude'][0],
                         'check totalozone value')
        self.assertEqual('+00:00:00',
                         extcsv_to.extcsv['TIMESTAMP']['UTCOffset'][0],
                         'check totalozone value')
        self.assertEqual('2006-12-01',
                         extcsv_to.extcsv['TIMESTAMP']['Date'][0],
                         'check totalozone value')
        self.assertEqual('21.4', extcsv_to.extcsv['MONTHLY']['StdDevO3'][0],
                         'check totalozone value')

        to_daily = extcsv_to.extcsv['DAILY']
        self.assertEqual('218', to_daily['ColumnO3'][10],
                         'check totalozone daily value')
        self.assertEqual('0', to_daily['ObsCode'][10],
                         'check totalozone daily value')
        self.assertEqual('', to_daily['UTC_Begin'][10],
                         'check totalozone daily value')

        extcsv_sp =\
            load_test_data_file(
                    'data/spectral/20040109.brewer.mkiv.144.epa_uga.csv',
                    reader=False)
        self.assertEqual('2.291E+00',
                         extcsv_sp.extcsv['GLOBAL_SUMMARY']['IntCIE'][0],
                         'check spectral value')
        self.assertEqual('000000',
                         extcsv_sp.extcsv['GLOBAL_SUMMARY']['Flag'][0],
                         'check spectral value')
        sp_global = extcsv_sp.extcsv['GLOBAL_9']
        self.assertEqual('2.225E-01',
                         sp_global['S-Irradiance'][53],
                         'check spectral global value')
        self.assertEqual('316.5',
                         sp_global['Wavelength'][53],
                         'check spectral global value')
        self.assertEqual('000020',
                         extcsv_sp.extcsv['GLOBAL_SUMMARY_24']['Flag'][0],
                         'check spectral value')

        sp_daily_tot = extcsv_sp.extcsv['GLOBAL_DAILY_TOTALS']
        self.assertEqual('362.5',
                         sp_daily_tot['Wavelength'][145],
                         'check spectral global daily total value')
        self.assertEqual('1.257E+01',
                         sp_daily_tot['S-Irradiance'][145],
                         'check spectral global daily total value')
        self.assertEqual('6.440E+02',
                         extcsv_sp.
                         extcsv['GLOBAL_DAILY_SUMMARY']['IntACGIH'][0],
                         'check spectral global daily summary value')

        extcsv_oz =\
            load_test_data_file(
                    'data/ozonesonde/20151021.ecc.6a.6a28340.smna.csv',
                    reader=False)
        self.assertEqual('6a',
                         extcsv_oz.extcsv['INSTRUMENT']['Model'][0],
                         'check ozonesonde value')
        self.assertEqual('323.75',
                         extcsv_oz.extcsv['FLIGHT_SUMMARY']['SondeTotalO3'][0],
                         'check ozonesonde value')
        self.assertEqual('-0.99',
                         extcsv_oz.
                         extcsv['FLIGHT_SUMMARY']['CorrectionFactor'][0],
                         'check ozonesonde value')
        self.assertEqual('131',
                         extcsv_oz.extcsv['FLIGHT_SUMMARY']['Number'][0],
                         'check ozonesonde value')
        oz_profile = extcsv_oz.extcsv['PROFILE']
        # seek
        self.assertEqual('84.9', oz_profile['Pressure'][598],
                         'check ozonesonde profile value')
        self.assertEqual('254', oz_profile['WindDirection'][598],
                         'check ozonesonde profile value')

    def test_determine_version_broadband(self):
        """Test assigning a table definition version with multiple options"""

        ecsv = load_test_data_file(
                'data/broad-band/'
                '20080101.Kipp_Zonen.UV-S-E-T.000560.PMOD-WRC.csv',
                reader=False)
        ecsv.validate_metadata_tables()

        schema = DOMAINS['Datasets']['Broad-band']['1.0']['1']
        version = ecsv._determine_version(schema)

        self.assertEqual(version, '2')
        for param in schema[version]:
            if param != 'data_table':
                self.assertIn(param, ecsv.extcsv)

        ecsv = load_test_data_file(
                'data/broad-band/'
                '20100109.Kipp_Zonen.UV-S-B-C.020579.ASM-ARG.csv',
                reader=False)
        ecsv.validate_metadata_tables()

        schema = DOMAINS['Datasets']['Broad-band']['1.0']['1']
        version = ecsv._determine_version(schema)

        self.assertEqual(version, '1')
        for param in schema[version]:
            if param != 'data_table':
                self.assertIn(param, ecsv.extcsv)

    def test_number_of_observations(self):
        """Test counting of observation rows in a generic file"""

        # Test throughout various datasets with different data table names.
        ecsv = load_test_data_file(
                'data/totalozone/20111101.Brewer.MKIII.201.RMDA.csv',
                reader=False)
        ecsv.validate_metadata_tables()

        self.assertEqual(ecsv.number_of_observations(), 30)

        # Umkehr
        ecsv = load_test_data_file(
                'data/umkehr/umkehr2-correct.csv',
                reader=False)
        ecsv.validate_metadata_tables()

        self.assertEqual(ecsv.number_of_observations(), 13)

        # Broad-band (table has been partially deleted)
        ecsv = load_test_data_file(
                'data/broad-band/'
                '20080101.Kipp_Zonen.UV-S-E-T.000560.PMOD-WRC.csv',
                reader=False)
        ecsv.validate_metadata_tables()

        self.assertEqual(ecsv.number_of_observations(), 5)

    def test_number_of_observations_duplicates(self):
        """Test counting of observation rows in a file with duplicate rows"""

        ecsv = load_test_data_file(
            'data/umkehr/umkehr1-duplicated.csv',
            reader=False)

        ecsv.validate_metadata_tables()

        self.assertLessEqual(ecsv.number_of_observations(), 14)

    def test_number_of_observations_multiple_tables(self):
        """
        Test counting of observation rows in a file with multiple data tables
        """

        # Lidar
        ecsv = load_test_data_file(
            'data/lidar/lidar-correct.csv',
            reader=False)
        ecsv.validate_metadata_tables()
        self.assertEqual(ecsv.number_of_observations(), 15)

        # Spectral
        ecsv = load_test_data_file(
            'data/spectral/spectral-extra-profile.csv',
            reader=False)
        ecsv.validate_metadata_tables()
        self.assertEqual(ecsv.number_of_observations(), 15)

    def test_row_filling(self):
        """Test that omitted columns in a row are filled in with nulls"""

        ecsv = ExtendedCSV('')
        ecsv.init_table('TIMESTAMP', ['UTCOffset', 'Date', 'Time'], 1)
        ecsv.add_values_to_table('TIMESTAMP', ['+00:00:00', '2019-04-30'], 3)

        self.assertIsInstance(ecsv.extcsv['TIMESTAMP']['Time'], list)
        self.assertEqual(len(ecsv.extcsv['TIMESTAMP']['Time']), 1)
        self.assertEqual(ecsv.extcsv['TIMESTAMP']['Time'][0], '')

        # Test the all-too-common case where all table rows have 10 commas
        instrument_fields = ['Name', 'Model', 'Number']
        ten_commas = ['ECC', '6A', '2174', '', '', '', '', '', '', '', '']

        ecsv.init_table('INSTRUMENT', instrument_fields, 12)
        ecsv.add_values_to_table('INSTRUMENT', ten_commas, 14)

        self.assertEqual(len(list(ecsv.extcsv['INSTRUMENT'].items())), 4)
        for field in instrument_fields:
            self.assertEqual(len(ecsv.extcsv['INSTRUMENT']['Name']), 1)
            self.assertNotEqual(ecsv.extcsv['INSTRUMENT'][field][0], '')

    def test_column_conversion(self):
        """Test that single-row tables are recognized and collimated"""

        content_fields = ['Class', 'Category', 'Level', 'Form']
        content_values = ['WODUC', 'Broad-band', '1.0', '1']

        global_fields = ['Time', 'Irradiance']
        global_values = [
            ['00:00:00', '0.1'],
            ['00:00:02', '0.2'],
            ['00:00:04', '0.3'],
            ['00:00:06', '0.4'],
            ['00:00:08', '0.5'],
            ['00:00:10', '0.6'],
        ]

        ecsv = ExtendedCSV('')
        self.assertEqual(ecsv.init_table('CONTENT', content_fields, 1),
                         'CONTENT')
        ecsv.add_values_to_table('CONTENT', content_values, 3)
        self.assertEqual(ecsv.init_table('GLOBAL', global_fields, 4),
                         'GLOBAL')
        for line_num, row in enumerate(global_values, 5):
            ecsv.add_values_to_table('GLOBAL', row, line_num)

        for field in content_fields:
            self.assertIsInstance(ecsv.extcsv['CONTENT'][field], list)
            self.assertEqual(len(ecsv.extcsv['CONTENT'][field]), 1)
            for value in ecsv.extcsv['CONTENT'][field]:
                self.assertIsInstance(value, str)
        for field in global_fields:
            self.assertIsInstance(ecsv.extcsv['GLOBAL'][field], list)
            self.assertEqual(len(ecsv.extcsv['GLOBAL'][field]), 6)
            for value in ecsv.extcsv['GLOBAL'][field]:
                self.assertIsInstance(value, str)

        ecsv.collimate_tables(['CONTENT'], DOMAINS['Common'])
        self.assertIsInstance(ecsv.extcsv['CONTENT']['Class'], str)
        self.assertIsInstance(ecsv.extcsv['CONTENT']['Category'], str)
        self.assertIsInstance(ecsv.extcsv['CONTENT']['Level'], float)
        self.assertIsInstance(ecsv.extcsv['CONTENT']['Form'], int)

        ecsv.collimate_tables(['GLOBAL'],
                              DOMAINS['Datasets']
                              ['Broad-band']['1.0']['1']['2'])
        self.assertIsInstance(ecsv.extcsv['GLOBAL']['Time'], list)
        self.assertIsInstance(ecsv.extcsv['GLOBAL']['Irradiance'], list)

        for value in ecsv.extcsv['GLOBAL']['Time']:
            self.assertIsInstance(value, time)
        for value in ecsv.extcsv['GLOBAL']['Irradiance']:
            self.assertIsInstance(value, float)

    def test_submissions(self):
        """Test parsing of previously submitted Extended CSV files"""

        # Error-free file
        ecsv = load_test_data_file(
                'data/broad-band/'
                '20080101.Kipp_Zonen.UV-S-E-T.000560.PMOD-WRC.csv',
                reader=False)

        ecsv.validate_metadata_tables()

        self.assertEqual('20080101.Kipp_Zonen.UV-S-E-T.000560.PMOD-WRC.csv',
                         ecsv.gen_woudc_filename())

        # Error-free file with a space in its instrument name
        ecsv = load_test_data_file(
                'data/general/ecsv-space-in-instrument-name.csv',
                reader=False)
        ecsv.validate_metadata_tables()

        self.assertEqual('20111101.Brewer-foo.MKIII.201.RMDA.csv',
                         ecsv.gen_woudc_filename())
        self.assertTrue(set(DOMAINS['Common'].keys()).issubset(
                        set(ecsv.extcsv.keys())))

        # Error-free file with special, non-ASCII characters
        ecsv = load_test_data_file(
            'data/totalozone/Brewer229_Daily_SEP2016.493',
            reader=False)
        ecsv.validate_metadata_tables()

        self.assertTrue(set(DOMAINS['Common'].keys()).issubset(
                        set(ecsv.extcsv.keys())))
        self.assertEqual(ecsv.extcsv['PLATFORM']['Name'], 'Río Gallegos')

    def test_build_table(self):
        """Test table management methods directly"""

        ecsv = ExtendedCSV('')
        fields = ['Class', 'Category', 'Level', 'Form']
        values = ['WOUDC', 'Spectral', '1.0', '1']

        self.assertEqual(ecsv.init_table('CONTENT', fields, 40), 'CONTENT')
        self.assertEqual(ecsv.table_count('CONTENT'), 1)
        self.assertEqual(ecsv.line_num('CONTENT'), 40)

        self.assertIn('CONTENT', ecsv.extcsv)
        for field in fields:
            self.assertIn(field, ecsv.extcsv['CONTENT'])
            self.assertEqual(ecsv.extcsv['CONTENT'][field], [])

        ecsv.add_values_to_table('CONTENT', values, 41)

        self.assertEqual(ecsv.line_num('CONTENT'), 40)
        for field, value in zip(fields, values):
            self.assertEqual(ecsv.extcsv['CONTENT'][field], [value])

        self.assertEqual(ecsv.init_table('CONTENT', fields, 44), 'CONTENT_2')
        self.assertEqual(ecsv.table_count('CONTENT'), 2)
        self.assertEqual(ecsv.line_num('CONTENT'), 40)
        self.assertEqual(ecsv.line_num('CONTENT_2'), 44)

        self.assertIn('CONTENT', ecsv.extcsv)
        self.assertIn('CONTENT_2', ecsv.extcsv)
        for field, value in zip(fields, values):
            self.assertIn(field, ecsv.extcsv['CONTENT'])
            self.assertIn(field, ecsv.extcsv['CONTENT_2'])
            self.assertEqual(ecsv.extcsv['CONTENT'][field], [value])
            self.assertEqual(ecsv.extcsv['CONTENT_2'][field], [])

        ecsv.add_values_to_table('CONTENT_2', values, 41)

        for field, value in zip(fields, values):
            self.assertEqual(ecsv.extcsv['CONTENT'][field], [value])
            self.assertEqual(ecsv.extcsv['CONTENT_2'][field], [value])

        ecsv.remove_table('CONTENT', index=2)
        self.assertIn('CONTENT', ecsv.extcsv)
        self.assertEqual(ecsv.table_count('CONTENT'), 1)
        self.assertEqual(ecsv.line_num('CONTENT'), 40)

        ecsv.remove_table('CONTENT')
        self.assertEqual(ecsv.table_count('CONTENT'), 0)
        self.assertIsNone(ecsv.line_num('CONTENT'))
        self.assertEqual(ecsv.extcsv, {})

    def test_build_table_2(self):
        """
        Manually produce woudc_extcsv.ExtendedCSV object,
        check values and table formats
        """

        # produce extcsv object
        extcsv = ExtendedCSV('')

        # add data here
        extcsv.init_table('CONTENT',
                          ['Class', 'Category', 'Level', 'Form'],
                          1)
        extcsv.add_values_to_table('CONTENT',
                                   ['WOUDC', 'TotalOzone', '1.0', '1'],
                                   2)
        extcsv.init_table('DATA_GENERATION',
                          ['Date', 'Agency', 'Version', 'ScientificAuthority'],
                          5)

        with self.assertRaises(MetadataValidationError):
            extcsv.validate_metadata_tables()

        extcsv.add_values_to_table('DATA_GENERATION',
                                   ['2014-08-28', 'NOAA-CMDL', '0.0', '@'],
                                   6)
        extcsv.init_table('PLATFORM',
                          ['Type', 'ID', 'Name', 'Country', 'GAW_ID'],
                          8)
        extcsv.add_values_to_table('PLATFORM',
                                   ['STN', '031', 'MAUNA LOA', 'USA', '4952'],
                                   9)
        extcsv.init_table('INSTRUMENT', ['Name', 'Model', 'Number'], 11)
        extcsv.add_values_to_table('INSTRUMENT', ['Dobson', 'Beck', '076'], 12)
        extcsv.init_table('LOCATION', ['Latitude', 'Longitude', 'Height'], 14)
        extcsv.add_values_to_table('LOCATION',
                                   ['19.533', '-155.574', '3405'],
                                   15)
        extcsv.init_table('TIMESTAMP', ['UTCOffset', 'Date', 'Time'], 17)
        extcsv.add_values_to_table('TIMESTAMP',
                                   ['+00:00:00', '2014-04-01', '00:00:00'],
                                   18)
        extcsv.init_table('DAILY',
                          ['Date', 'WLCode', 'ObsCode', 'ColumnO3',
                           'StdDevO3', 'UTC_Begin', 'UTC_End',
                           'UTC_Mean', 'nObs', 'mMu', 'ColumnSO2'],
                          20)
        extcsv.add_values_to_table('DAILY',
                                   ['2014-04-01', '0', '2', '283',
                                       '', '', '', '18'],
                                   21)
        extcsv.add_values_to_table('DAILY',
                                   ['2014-04-08', '0', '0', '288',
                                       '', '', '', '23'],
                                   22)
        extcsv.add_values_to_table('DAILY',
                                   ['2014-04-09', '0', '0', '279',
                                       '', '', '', '23'],
                                   23)
        extcsv.add_values_to_table('DAILY',
                                   ['2014-04-10', '0', '0', '273',
                                       '', '', '', '24'],
                                   24)
        extcsv.add_values_to_table('DAILY',
                                   ['2014-04-11', '0', '0', '274',
                                       '', '', '', '21'],
                                   25)
        extcsv.add_values_to_table('DAILY',
                                   ['2014-04-12', '0', '2', '271',
                                       '', '', '', '18'],
                                   26)
        extcsv.add_values_to_table('DAILY',
                                   ['2014-04-13', '0', '2', '274',
                                       '', '', '', '18'],
                                   27)
        extcsv.add_values_to_table('DAILY',
                                   ['2014-04-14', '0', '0', '283',
                                       '', '', '', '23'],
                                   28)
        extcsv.add_values_to_table('DAILY',
                                   ['2014-04-15', '0', '0', '285',
                                       '', '', '', '23'],
                                   29)
        extcsv.add_values_to_table('DAILY',
                                   ['2014-04-16', '0', '0', '284',
                                       '', '', '', '23'],
                                   30)
        extcsv.add_values_to_table('DAILY',
                                   ['2014-04-17', '0', '0', '280',
                                       '', '', '', '22'],
                                   31)
        extcsv.add_values_to_table('DAILY',
                                   ['2014-04-18', '0', '2', '268',
                                       '', '', '', '18'],
                                   32)
        extcsv.add_values_to_table('DAILY',
                                   ['2014-04-19', '0', '2', '271',
                                       '', '', '', '18'], 33)
        extcsv.add_values_to_table('DAILY',
                                   ['2014-04-20', '0', '2', '264',
                                       '', '', '', '18'],
                                   34)
        extcsv.add_values_to_table('DAILY',
                                   ['2014-04-21', '0', '0', '278',
                                       '', '', '', '23'],
                                   35)
        extcsv.add_values_to_table('DAILY',
                                   ['2014-04-22', '0', '0', '276',
                                       '', '', '', '21'],
                                   36)
        extcsv.add_values_to_table('DAILY',
                                   ['2014-04-23', '0', '0', '280',
                                       '', '', '', '23'],
                                   37)
        extcsv.add_values_to_table('DAILY',
                                   ['2014-04-24', '0', '0', '269',
                                       '', '', '', '22'],
                                   38)
        extcsv.add_values_to_table('DAILY',
                                   ['2014-04-25', '0', '0', '275',
                                       '', '', '', '21'],
                                   39)
        extcsv.add_values_to_table('DAILY',
                                   ['2014-04-26', '0', '2', '278',
                                       '', '', '', '18'],
                                   40)
        extcsv.add_values_to_table('DAILY',
                                   ['2014-04-28', '0', '0', '296',
                                       '', '', '', '21'],
                                   41)
        extcsv.add_values_to_table('DAILY',
                                   ['2014-04-29', '0', '0', '291',
                                       '', '', '', '23'],
                                   42)
        extcsv.add_values_to_table('DAILY',
                                   ['2014-04-30', '0', '0', '294',
                                       '', '', '', '21'],
                                   43)
        extcsv.init_table('TIMESTAMP_2', ['UTCOffset', 'Date', 'Time'], 45)
        extcsv.add_values_to_table('TIMESTAMP_2',
                                   ['+00:00:00', '2014-04-30', ''],
                                   46)
        extcsv.init_table('MONTHLY',
                          ['Date', 'ColumnO3', 'StdDevO3', 'Npts'],
                          48)
        extcsv.add_values_to_table('MONTHLY',
                                   ['2014-04-01', '279', '8.3', '23'],
                                   49)

        # validate metadata and collimate data tables
        self.assertEqual(None, extcsv.validate_metadata_tables(),
                         'validate totalozone metadata tables')
        self.assertTrue(extcsv.validate_dataset_tables(),
                        'validate totalozone data tables')

        # check tables
        self.assertTrue('DAILY' in extcsv.extcsv,
                        'check totalozone table in my extcsv')
        self.assertTrue('PLATFORM' in extcsv.extcsv,
                        'check totalozone table in my extcsv')
        self.assertTrue('LOCATION' in extcsv.extcsv,
                        'check totalozone table in my extcsv')
        self.assertTrue('TIMESTAMP' in extcsv.extcsv,
                        'check totalozone table in my extcsv')
        self.assertTrue('DATA_GENERATION' in extcsv.extcsv,
                        'check totalozone table in my extcsv')
        self.assertTrue('MONTHLY' in extcsv.extcsv,
                        'check totalozone in my extcsv')

        # check fields
        self.assertTrue('Level' in extcsv.extcsv['CONTENT'],
                        'check totalozone field in my extcsv')
        self.assertTrue('UTCOffset' in extcsv.extcsv['TIMESTAMP'],
                        'check totalozone field in my extcsv')
        self.assertTrue('ScientificAuthority'
                        in extcsv.extcsv['DATA_GENERATION'],
                        'check totalozone field in my extcsv')
        self.assertTrue('Time' in extcsv.extcsv['TIMESTAMP_2'],
                        'check totalozone field in my extcsv')
        self.assertTrue('ColumnO3' in extcsv.extcsv['MONTHLY'],
                        'check totalozone  field in my extcsv')

        # check values
        self.assertEqual(19.533,
                         extcsv.extcsv['LOCATION']['Latitude'],
                         'check totalozone value in my extcsv')
        self.assertEqual('NOAA-CMDL',
                         extcsv.extcsv['DATA_GENERATION']['Agency'],
                         'check totalozone value in my extcsv')
        self.assertEqual(1, extcsv.extcsv['CONTENT']['Form'],
                         'check totalozone value in my extcsv')
        self.assertEqual(date(2014, 4, 1),
                         extcsv.extcsv['TIMESTAMP']['Date'],
                         'check totalozone value in my extcsv')
        self.assertEqual(23, extcsv.extcsv['MONTHLY']['Npts'],
                         'check totalozone value in my extcsv')

        self.assertTrue('WLCode' in extcsv.extcsv['DAILY'],
                        'check totalozone daily field in my extcsv')
        self.assertTrue('nObs' in extcsv.extcsv['DAILY'],
                        'check totalozone daily field in my extcsv')
        self.assertTrue('ColumnO3' in extcsv.extcsv['DAILY'],
                        'check totalozone daily field in my extcsv')
        self.assertTrue('ColumnSO2' in extcsv.extcsv['DAILY'],
                        'check totalozone daily field in my extcsv')

        daily_items = list(extcsv.extcsv['DAILY'].items())
        self.assertEqual('Date', daily_items[1][0],
                         'check totalozone daily field order in my extcsv')
        self.assertEqual('ColumnSO2',
                         daily_items[len(daily_items)-1][0],
                         'check totalozone daily field order in my extcsv')
        # seek
        self.assertEqual(271,
                         extcsv.extcsv['DAILY']['ColumnO3'][5],
                         'check totalozone daily value in my extcsv')
        self.assertEqual(None,
                         extcsv.extcsv['DAILY']['StdDevO3'][5],
                         'check totalozone daily value in my extcsv')
        self.assertEqual(None,
                         extcsv.extcsv['DAILY']['UTC_Begin'][5],
                         'check totalozone daily value in my extcsv')
        self.assertEqual(18,
                         extcsv.extcsv['DAILY']['UTC_Mean'][5],
                         'check totalozone daily value in my extcsv')
        self.assertEqual(275,
                         extcsv.extcsv['DAILY']['ColumnO3'][18],
                         'check totalozone daily value in my extcsv')
        self.assertEqual(21,
                         extcsv.extcsv['DAILY']['UTC_Mean'][18],
                         'check totalozone daily value in my extcsv')

    def test_validators(self):
        """
        Test dataset and metadata validation checkers
        """

        # load totalozone file ExtendedCSV object
        extcsv_to = load_test_data_file(
            'data/totalozone/20061201.brewer.mkiv.153.imd.csv',
            reader=False
        )

        # check field values
        self.assertEqual('1.0',
                         extcsv_to.extcsv['CONTENT']['Level'][0],
                         'check totalozone value in my extcsv')
        self.assertEqual('400',
                         extcsv_to.extcsv['PLATFORM']['ID'][0],
                         'check totalozone value in my extcsv')
        self.assertEqual('MKIV', extcsv_to.extcsv['INSTRUMENT']['Model'][0],
                         'check totalozone value in my extcsv')
        self.assertEqual('+00:00:00',
                         extcsv_to.extcsv['TIMESTAMP']['UTCOffset'][0],
                         'check totalozone value in my extcsv')
        self.assertEqual('218',
                         extcsv_to.extcsv['DAILY']['ColumnO3'][11],
                         'check totalozone value in my extcsv')

        # validate metadata tables, collimate
        self.assertEqual(None, extcsv_to.validate_metadata_tables(),
                         'validate totalozone metadata tables')
        self.assertTrue(extcsv_to.validate_dataset_tables(),
                        'validate totalozone data tables')
        # check collimation
        self.assertEqual(1.0,
                         extcsv_to.extcsv['CONTENT']['Level'],
                         'check totalozone value in my extcsv')
        self.assertEqual(400,
                         extcsv_to.extcsv['PLATFORM']['ID'],
                         'check totalozone value in my extcsv')
        self.assertEqual('MKIV', extcsv_to.extcsv['INSTRUMENT']['Model'],
                         'check totalozone value in my extcsv')
        self.assertEqual('+00:00:00',
                         extcsv_to.extcsv['TIMESTAMP']['UTCOffset'],
                         'check totalozone value in my extcsv')
        self.assertEqual(218,
                         extcsv_to.extcsv['DAILY']['ColumnO3'][11],
                         'check totalozone value in my extcsv')

    def test_reader_error(self):
        """
        Test if reader error is thrown for malformed extcsv
        """

        try:
            extcsv_to = load_test_data_file(
                'data/totalozone/20061201.brewer.mkiv.153.imd.csv',
                reader=False
            )
            self.assertIsInstance(extcsv_to, ExtendedCSV,
                                  'validate instance type')
        except Exception:
            self.assertRaises(NonStandardDataError)


class ReaderTest(unittest.TestCase):
    """Test suite for ExtendedCSV"""

    def setUp(self):
        """setup test fixtures, etc."""

        print(msg(self.id(), self.shortDescription()))

    def tearDown(self):
        """return to pristine state"""

        pass

    def test_table_presence(self):
        """Test if table exists"""

        extcsv_to =\
            load_test_data_file(
                    'data/totalozone/20061201.brewer.mkiv.153.imd.csv',
                    reader=True)
        self.assertTrue('CONTENT' in extcsv_to.extcsv,
                        'check totalozone table presence')
        self.assertTrue('DATA_GENERATION' in extcsv_to.extcsv,
                        'check totalozone table presence')
        self.assertTrue('DAILY' in extcsv_to.extcsv,
                        'check totalozone table presence')
        self.assertTrue('CONTENT' in extcsv_to.extcsv,
                        'check totalozone table presence')
        self.assertTrue('TIMESTAMP_2' in extcsv_to.extcsv,
                        'check totalozone table presence')
        self.assertTrue('TIMESTAMP_3' not in extcsv_to.extcsv,
                        'check totalozone table not presence')

        extcsv_sp =\
            load_test_data_file(
                    'data/spectral/20040109.brewer.mkiv.144.epa_uga.csv')
        self.assertTrue('DATA_GENERATION' in extcsv_sp.extcsv,
                        'check spectral table presence')
        self.assertTrue('GLOBAL_SUMMARY' in extcsv_sp.extcsv,
                        'check spectral table presence')
        self.assertTrue('GLOBAL_SUMMARY_23' in extcsv_sp.extcsv,
                        'check spectral table presence')
        self.assertTrue('GLOBAL_SUMMARY_25' not in extcsv_sp.extcsv,
                        'check spectral table presence')

    def test_field_presence(self):
        """Test if field exist"""

        extcsv_oz =\
            load_test_data_file(
                    'data/ozonesonde/20151021.ecc.6a.6a28340.smna.csv',
                    reader=True)
        self.assertTrue('Category' in extcsv_oz.extcsv['CONTENT'],
                        'check ozonesonde field presence')
        self.assertTrue('Version' in extcsv_oz.extcsv['DATA_GENERATION'],
                        'check ozonesonde field presence')
        self.assertTrue('ScientificAuthority'
                        in extcsv_oz.extcsv['DATA_GENERATION'],
                        'check ozonesonde field presence')
        self.assertTrue('Date' in extcsv_oz.extcsv['TIMESTAMP'],
                        'check ozonesonde field presence')
        self.assertTrue('SondeTotalO3' in extcsv_oz.extcsv['FLIGHT_SUMMARY'],
                        'check ozonesonde field presence')
        self.assertTrue('WLCode' in extcsv_oz.extcsv['FLIGHT_SUMMARY'],
                        'check ozonesonde field presence')
        self.assertTrue('ib2' in extcsv_oz.extcsv['AUXILIARY_DATA'],
                        'check ozonesonde field presence')
        self.assertTrue('BackgroundCorr'
                        in extcsv_oz.extcsv['AUXILIARY_DATA'],
                        'check ozonesonde field presence')
        self.assertTrue('O3PartialPressure' in
                        extcsv_oz.extcsv['PROFILE'],
                        'check ozonesonde field presence')

        oz_profile = extcsv_oz.extcsv['PROFILE']
        self.assertTrue('O3PartialPressure' in oz_profile,
                        'check ozonesonde profile field presence')
        self.assertTrue('Pressure' in oz_profile,
                        'check ozonesonde profile field presence')

    def test_value(self):
        """Test values"""

        extcsv_to =\
            load_test_data_file(
                    'data/totalozone/20061201.brewer.mkiv.153.imd.csv',
                    reader=True)
        self.assertEqual(['WOUDC'], extcsv_to.extcsv['CONTENT']['Class'],
                         'check totalozone value')
        self.assertEqual([''], extcsv_to.extcsv['PLATFORM']['GAW_ID'],
                         'check totalozone value')
        self.assertEqual(['11.45'], extcsv_to.extcsv['LOCATION']['Longitude'],
                         'check totalozone value')
        self.assertEqual(['+00:00:00'],
                         extcsv_to.extcsv['TIMESTAMP']['UTCOffset'],
                         'check totalozone value')
        self.assertEqual(['2006-12-01'],
                         extcsv_to.extcsv['TIMESTAMP']['Date'],
                         'check totalozone value')
        self.assertEqual(['2006-12-31'],
                         extcsv_to.extcsv['TIMESTAMP_2']['Date'],
                         'check totalozone value')
        self.assertEqual(['21.4'], extcsv_to.extcsv['MONTHLY']['StdDevO3'],
                         'check totalozone value')

        to_daily = extcsv_to.extcsv['DAILY']
        self.assertEqual('219', to_daily['ColumnO3'][9],
                         'check totalozone daily value')
        self.assertEqual('0', to_daily['ObsCode'][9],
                         'check totalozone daily value')
        self.assertEqual('', to_daily['UTC_Begin'][9],
                         'check totalozone daily value')
        self.assertEqual('219', to_daily['ColumnO3'][9],
                         'check totalozone daily value')

        extcsv_sp =\
            load_test_data_file(
                    'data/spectral/20040109.brewer.mkiv.144.epa_uga.csv',
                    reader=True)
        self.assertEqual(['2.291E+00'],
                         extcsv_sp.extcsv['GLOBAL_SUMMARY']['IntCIE'],
                         'check spectral value')
        self.assertEqual(['000000'],
                         extcsv_sp.extcsv['GLOBAL_SUMMARY']['Flag'],
                         'check spectral value')

        sp_global = extcsv_sp.extcsv['GLOBAL_9']
        self.assertEqual('2.026E-01',
                         sp_global['S-Irradiance'][51],
                         'check spectral global value')
        self.assertEqual('315.5',
                         sp_global['Wavelength'][51],
                         'check spectral global value')
        self.assertEqual(['000020'],
                         extcsv_sp.extcsv['GLOBAL_SUMMARY_24']['Flag'],
                         'check spectral value')
        self.assertEqual(['000020'],
                         extcsv_sp.extcsv['GLOBAL_SUMMARY_24']['Flag'],
                         'check spectral value')

        sp_daily_tot = extcsv_sp.extcsv['GLOBAL_DAILY_TOTALS']
        self.assertEqual('361.5',
                         sp_daily_tot['Wavelength'][143],
                         'check spectral global daily total value')
        self.assertEqual('1.055E+01',
                         sp_daily_tot['S-Irradiance'][143],
                         'check spectral global daily total value')
        self.assertEqual(['6.440E+02'],
                         extcsv_sp.
                         extcsv['GLOBAL_DAILY_SUMMARY']['IntACGIH'],
                         'check spectral global daily summary value')

        extcsv_oz =\
            load_test_data_file(
                    'data/ozonesonde/20151021.ecc.6a.6a28340.smna.csv',
                    reader=True)
        self.assertEqual(['6a'],
                         extcsv_oz.extcsv['INSTRUMENT']['Model'],
                         'check ozonesonde value')
        self.assertEqual(['323.75'],
                         extcsv_oz.extcsv['FLIGHT_SUMMARY']['SondeTotalO3'],
                         'check ozonesonde value')
        self.assertEqual(['-0.99'],
                         extcsv_oz.
                         extcsv['FLIGHT_SUMMARY']['CorrectionFactor'],
                         'check ozonesonde value')
        self.assertEqual(['131'],
                         extcsv_oz.extcsv['FLIGHT_SUMMARY']['Number'],
                         'check ozonesonde value')

        oz_profile = extcsv_oz.extcsv['PROFILE']
        self.assertEqual('85.4',
                         oz_profile['Pressure'][596],
                         'check ozonesonde profile value')
        self.assertEqual('255',
                         oz_profile['WindDirection'][596],
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
        # dump to string
        extcsv_s = dumps(extcsv)

        # load my extcsv into Reader
        my_extcsv_to = Reader(extcsv_s)

        # check tables
        self.assertTrue('DAILY' in my_extcsv_to.extcsv,
                        'check totalozone table in my extcsv')
        self.assertTrue('PLATFORM' in my_extcsv_to.extcsv,
                        'check totalozone table in my extcsv')
        self.assertTrue('LOCATION' in my_extcsv_to.extcsv,
                        'check totalozone table in my extcsv')
        self.assertTrue('TIMESTAMP' in my_extcsv_to.extcsv,
                        'check totalozone table in my extcsv')
        self.assertTrue('DATA_GENERATION' in my_extcsv_to.extcsv,
                        'check totalozone table in my extcsv')
        self.assertTrue('TIMESTAMP_2' in my_extcsv_to.extcsv,
                        'check totalozone table in my extcsv')
        self.assertTrue('MONTHLY' in my_extcsv_to.extcsv,
                        'check totalozone in my extcsv')

        # check fields
        self.assertTrue('Level' in my_extcsv_to.extcsv['CONTENT'],
                        'check totalozone field in my extcsv')
        self.assertTrue('UTCOffset' in my_extcsv_to.extcsv['TIMESTAMP'],
                        'check totalozone field in my extcsv')
        self.assertTrue('ScientificAuthority'
                        in my_extcsv_to.extcsv['DATA_GENERATION'],
                        'check totalozone field in my extcsv')
        self.assertTrue('Time' in my_extcsv_to.extcsv['TIMESTAMP_2'],
                        'check totalozone field in my extcsv')
        self.assertTrue('ColumnO3' in my_extcsv_to.extcsv['MONTHLY'],
                        'check totalozone  field in my extcsv')

        # check values
        self.assertEqual(['19.533'],
                         my_extcsv_to.extcsv['LOCATION']['Latitude'],
                         'check totalozone value in my extcsv')
        self.assertEqual(['NOAA-CMDL'],
                         my_extcsv_to.extcsv['DATA_GENERATION']['Agency'],
                         'check totalozone value in my extcsv')
        self.assertEqual(['1'], my_extcsv_to.extcsv['CONTENT']['Form'],
                         'check totalozone value in my extcsv')
        self.assertEqual(['23'], my_extcsv_to.extcsv['MONTHLY']['Npts'],
                         'check totalozone value in my extcsv')

        my_to_daily = my_extcsv_to.extcsv['DAILY']
        my_daily_header = list(my_to_daily.keys())
        self.assertTrue('WLCode' in my_daily_header,
                        'check totalozone daily field in my extcsv')
        self.assertTrue('nObs' in my_daily_header,
                        'check totalozone daily field in my extcsv')
        self.assertTrue('ColumnO3' in my_daily_header,
                        'check totalozone daily field in my extcsv')
        self.assertTrue('ColumnSO2' in my_daily_header,
                        'check totalozone daily field in my extcsv')
        self.assertEqual(1, my_daily_header.index('Date'),
                         'check totalozone daily field order in my extcsv')
        self.assertEqual(len(my_daily_header) - 1,
                         my_daily_header.index('ColumnSO2'),
                         'check totalozone daily field order in my extcsv')

        self.assertEqual('274',
                         my_to_daily['ColumnO3'][4],
                         'check totalozone daily value in my extcsv')
        self.assertEqual('',
                         my_to_daily['StdDevO3'][4],
                         'check totalozone daily value in my extcsv')
        self.assertEqual('',
                         my_to_daily['UTC_Begin'][4],
                         'check totalozone daily value in my extcsv')
        self.assertEqual('21',
                         my_to_daily['UTC_Mean'][4],
                         'check totalozone daily value in my extcsv')

        self.assertEqual('275',
                         my_to_daily['ColumnO3'][18],
                         'check totalozone daily value in my extcsv')
        self.assertEqual('21',
                         my_to_daily['UTC_Mean'][18],
                         'check totalozone daily value in my extcsv')


class WriterTest(unittest.TestCase):
    """Test suite for ExtendedCSV"""

    def setUp(self):
        """setup test fixtures, etc."""

        print(msg(self.id(), self.shortDescription()))

    def tearDown(self):
        """return to pristine state"""

        pass

    def test_add_table_1(self):
        """Test adding new table"""

        extcsv = Writer()
        extcsv.add_table('CONTENT', 'basic metadata, index 1')
        self.assertTrue('CONTENT' in extcsv.extcsv.keys(),
                        'table not found')
        self.assertEqual('basic metadata, index 1',
                         get_data(extcsv, 'CONTENT', 'comments')[0],
                         'invalid table comment')

    def test_add_table_2(self):
        """
            Test adding new table at a specific index. Checking that
            the tables are created in ascending order
        """

        extcsv = Writer()
        extcsv.add_table('CONTENT', 'basic metadata, index 1')
        self.assertTrue('CONTENT' in extcsv.extcsv.keys(),
                        'table not found')
        extcsv.add_table('CONTENT', 'basic metadata, index 2')
        self.assertTrue('CONTENT_2' in extcsv.extcsv.keys(),
                        'table not found')

    def test_add_table_3(self):
        """Test order of tables to see if order of insert is preserved"""

        extcsv = Writer()
        extcsv.add_table('CONTENT', 'basic metadata, index 1')
        extcsv.add_table('CONTENT', 'basic metadata, index 2')
        extcsv.add_table('CONTENT', 'basic metadata, index 3')
        extcsv.add_table('CONTENT', 'basic metadata, index 4')

        keys = list(extcsv.extcsv.keys())
        self.assertTrue('CONTENT' in keys, 'table 1 not found')
        self.assertTrue('CONTENT_2' in keys, 'table 2 not found')
        self.assertTrue('CONTENT_3' in keys, 'table 3 not found')
        self.assertTrue('CONTENT_4' in keys, 'table 4 not found')
        self.assertEqual(0, keys.index('CONTENT'), 'table 1 index mismatch')
        self.assertEqual(1, keys.index('CONTENT_2'), 'table 2 index mismatch')
        self.assertEqual(2, keys.index('CONTENT_3'), 'table 3 index mismatch')
        self.assertEqual(3, keys.index('CONTENT_4'), 'table 4 index mismatch')

    def test_add_field_1(self):
        """Test adding new field to table"""

        extcsv = Writer()
        extcsv.add_field('CONTENT', 'Class')
        self.assertTrue('CONTENT' in extcsv.extcsv.keys(),
                        'table not found')
        self.assertTrue('Class' in extcsv.extcsv['CONTENT'].keys(),
                        'field not found')

        extcsv.add_field('DATA_GENERATION', ['Date'])
        self.assertTrue('DATA_GENERATION' in extcsv.extcsv.keys(),
                        'table not found')
        self.assertTrue('Date' in extcsv.extcsv['DATA_GENERATION'].keys(),
                        'field not found')

    def test_add_field_2(self):
        """Test adding multiple new fields to table"""

        extcsv = Writer()
        extcsv.add_field('CONTENT', 'Class,Category,Level')
        self.assertTrue('CONTENT' in extcsv.extcsv.keys(),
                        'table CONTENT not found')
        self.assertTrue('Class' in extcsv.extcsv['CONTENT'].keys(),
                        'field CONTENT.Class not found')
        self.assertTrue('Category' in extcsv.extcsv['CONTENT'].keys(),
                        'field CONTENT.Category not found')
        self.assertTrue('Level' in extcsv.extcsv['CONTENT'].keys(),
                        'field CONTENT.Level not found')

        extcsv.add_field('DATA_GENERATION',
                         ['Date', 'Agency', 'Version'])
        self.assertTrue('DATA_GENERATION' in extcsv.extcsv.keys(),
                        'table DATA_GENERATION not found')
        self.assertTrue('Date' in extcsv.extcsv['DATA_GENERATION'].keys(),
                        'field DATA_GENERATION.Date not found')
        self.assertTrue('Agency' in extcsv.extcsv['DATA_GENERATION'].keys(),
                        'field DATA_GENERATION.Agency not found')
        self.assertTrue('Version' in extcsv.extcsv['DATA_GENERATION'].keys(),
                        'field DATA_GENERATION.Version not found')

    def test_add_field_3(self):
        """Test order of insert of fields"""

        extcsv = Writer()
        extcsv.add_field('CONTENT', 'Class,Category,Level')
        keys = list(extcsv.extcsv['CONTENT'].keys())
        self.assertEqual(0, keys.index('comments'), 'index 0 mismatch')
        self.assertEqual(1, keys.index('Class'), 'index 1 mismatch')
        self.assertEqual(2, keys.index('Category'), 'index 2 mismatch')
        self.assertEqual(3, keys.index('Level'), 'index 3 mismatch')

        extcsv.add_field('DATA_GENERATION', ['Date', 'Agency', 'Version'])
        keys = list(extcsv.extcsv['DATA_GENERATION'].keys())
        self.assertEqual(0, keys.index('comments'), 'index 0 mismatch')
        self.assertEqual(1, keys.index('Date'), 'index 1 mismatch')
        self.assertEqual(2, keys.index('Agency'), 'index 2 mismatch')
        self.assertEqual(3, keys.index('Version'), 'index 3 mismatch')

    def test_add_value_1(self):
        """Test adding new value to existing table.field, veritically"""

        extcsv = Writer()
        extcsv.add_data('CONTENT', 'WOUDC', field='Class')
        extcsv.add_data('CONTENT', 'TotalOzone', field='Category')
        self.assertTrue('CONTENT' in extcsv.extcsv.keys(),
                        'table CONTENT not found')
        self.assertTrue('Class' in extcsv.extcsv['CONTENT'].keys(),
                        'field CONTENT.Class not found')
        self.assertTrue('Category' in extcsv.extcsv['CONTENT'].keys(),
                        'field CONTENT.Category not found')
        self.assertTrue('WOUDC' in get_data(extcsv, 'CONTENT', 'Class'),
                        'value CONTENT.Class WOUDC not found')
        self.assertTrue('TotalOzone' in get_data(
                            extcsv, 'CONTENT', 'Category'
                        ),
                        'value CONTENT.Category TotalOzone not found')

    def test_add_value_2(self):
        """Test adding new value to existing table.field, horizontally"""

        extcsv = Writer()
        extcsv.add_field('CONTENT', 'Class,Category,Level')
        extcsv.add_data('CONTENT', 'a,b,c')
        self.assertTrue('CONTENT' in extcsv.extcsv.keys(),
                        'table CONTENT not found')
        self.assertTrue('Class' in extcsv.extcsv['CONTENT'].keys(),
                        'field CONTENT.Class not found')
        self.assertTrue('Category' in extcsv.extcsv['CONTENT'].keys(),
                        'field CONTENT.Category not found')
        self.assertTrue('Level' in extcsv.extcsv['CONTENT'].keys(),
                        'field CONTENT.Level not found')
        self.assertTrue('a' in get_data(extcsv, 'CONTENT', 'Class'),
                        'value CONTENT.Class not found')
        self.assertTrue('b' in get_data(extcsv, 'CONTENT', 'Category'),
                        'value CONTENT.Category not found')
        self.assertTrue('c' in get_data(extcsv, 'CONTENT', 'Level'),
                        'value CONTENT.Level not found')

        extcsv.add_field('DATA_GENERATION', ['Date', 'Agency', 'Version'])
        extcsv.add_data('DATA_GENERATION', ['d', 'e', 'f'])
        self.assertTrue('DATA_GENERATION' in extcsv.extcsv.keys(),
                        'table DATA_GENERATION not found')
        self.assertTrue('Date' in extcsv.extcsv['DATA_GENERATION'].keys(),
                        'field DATA_GENERATION.Date not found')
        self.assertTrue('Agency' in extcsv.extcsv['DATA_GENERATION'].keys(),
                        'field DATA_GENERATION.Agency not found')
        self.assertTrue('Version' in extcsv.extcsv['DATA_GENERATION'].keys(),
                        'field DATA_GENERATION.Version not found')
        self.assertTrue('d' in get_data(extcsv, 'DATA_GENERATION', 'Date'),
                        'value DATA_GENERATION.Date not found')
        self.assertTrue('e' in get_data(extcsv, 'DATA_GENERATION', 'Agency'),
                        'value DATA_GENERATION.Agency not found')
        self.assertTrue('f' in get_data(extcsv, 'DATA_GENERATION', 'Version'),
                        'value DATA_GENERATION.Version not found')

    def test_add_value_3(self):
        """Test adding value to table given identical table names"""

        extcsv = Writer()
        extcsv.add_field('GLOBAL', 'Wavelength,S-Irradiance,Time')
        extcsv.add_field('GLOBAL', 'Wavelength,S-Irradiance,Time', index=2)
        extcsv.add_field('GLOBAL', 'Wavelength,S-Irradiance,Time', index=3)
        extcsv.add_data('GLOBAL', '290.0', index=2, field='Wavelength')
        extcsv.add_data('GLOBAL', '07:28:49,07:29:03', index=3, field='Time')
        extcsv.add_data('GLOBAL', '1.700E-06', field='S-Irradiance')
        self.assertEqual(['290.0'],
                         get_data(extcsv, 'GLOBAL', 'Wavelength', index=2),
                         'expected specific value')
        self.assertEqual(['07:28:49', '07:29:03'],
                         get_data(extcsv, 'GLOBAL', 'Time', index=3),
                         'expected specific value')
        self.assertEqual(['1.700E-06'],
                         get_data(extcsv, 'GLOBAL', 'S-Irradiance'),
                         'expected specific value')

        extcsv.add_data('GLOBAL', ['290.5', '291.0'], index=2,
                        field='Wavelength')
        extcsv.add_data('GLOBAL', ['8.000E-07'], field='S-Irradiance')
        self.assertEqual(['290.0', '290.5', '291.0'],
                         get_data(extcsv, 'GLOBAL', 'Wavelength', index=2),
                         'expected specific value')
        self.assertEqual(['1.700E-06', '8.000E-07'],
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
        self.assertTrue('CONTENT' not in extcsv.extcsv.keys(),
                        'unexpected table found')
        self.assertTrue('TIMESTAMP_2' not in extcsv.extcsv.keys(),
                        'unexpected table found')
        self.assertTrue('TIMESTAMP' in extcsv.extcsv.keys(),
                        'unexpected table found')

    def test_remove_field(self):
        """Test removing field"""
        extcsv = Writer()
        extcsv.add_data('TABLE', 'v1,v2', field='Field1,Field2,Field3')
        extcsv.add_data('TABLE', 'v1,v2', field='Field1,Field2,Field3',
                        index=2)
        extcsv.remove_field('TABLE', 'Field2', index=2)
        extcsv.remove_field('TABLE', 'Field1')
        self.assertTrue('Fields2' not in extcsv.extcsv['TABLE_2'].keys(),
                        'unexpected field found')
        self.assertTrue('Fields1' not in extcsv.extcsv['TABLE'].keys(),
                        'unexpected field found')
        self.assertEqual(['comments', 'Field2', 'Field3'],
                         list(extcsv.extcsv['TABLE'].keys()),
                         'expected specific value')
        self.assertEqual(['comments', 'Field1', 'Field3'],
                         list(extcsv.extcsv['TABLE_2'].keys()),
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


# main
if __name__ == '__main__':
    unittest.main()
