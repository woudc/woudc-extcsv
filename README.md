[![Build Status](https://github.com/woudc/woudc-extcsv/workflows/build%20%E2%9A%99%EF%B8%8F/badge.svg)](https://github.com/woudc/woudc-extcsv/actions)

# WOUDC Extended CSV library

Python package providing read/write support for files of the
[WOUDC](https://woudc.org) Extended CSV format.

## Installation

### pip

Install latest stable version from [PyPI](https://pypi.org/project/woudc-extcsv).

```bash
pip3 install woudc-extcsv
```

### From source
Install latest development version.

```bash
python3 -m venv woudc-extcsv
cd woudc-extcsv
. bin/activate
git clone https://github.com/woudc/woudc-extcsv.git
cd woudc-extcsv
pip3 install .
```

## Usage

### Reader Objects

```python
from woudc_extcsv import Reader
# read from file
with open('file.csv') as ff:
    ecsv = Reader(ff.read())
# read from string
ecsv = Reader(my_ecsv_string)
```

### Writer Objects

```python
import woudc_extcsv
# create new writer object with common/metadata tables and fields available
ecsv = woudc_extcsv.Writer(template=True)

# Add file comments
ecsv.add_comment('This is a file level comment.')
ecsv.add_comment('This is another file level comment.')

# Add metadata
ecsv.add_data('CONTENT', 
              'WOUDC,Spectral,1.0,1')
ecsv.add_data('DATA_GENERATION',
              '2002-05-29,JMA,1.0')
ecsv.add_data('PLATFORM',
              'STN,7,Kagoshima,JPN,47827')
ecsv.add_data('INSTRUMENT',
              'Brewer,MKII,059')
ecsv.add_data('LOCATION',
              '31.63,130.6,283')

# Add new table
ecsv.add_table('TIMESTAMP')
# Add fields
ecsv.add_field('TIMESTAMP', 'UTCOffset,Date,Time')
# Add data
ecsv.add_data('TIMESTAMP', '+08:38:47,1991-01-01,06:38:47')

# Add new table, fields, and data at the same time
ecsv.add_data('GLOBAL_SUMMARY',
              '06:38:47,7.117E-04,8.980E-03,94.12,99.99,114.64,001000,999',
              field='Time,IntACGIH,IntCIE,ZenAngle,MuValue,AzimAngle,Flag,TempC')
ecsv.add_data('GLOBAL',
              '290.0,0.000E+00',
              field='Wavelength,S-Irradiance,Time')
ecsv.add_data('GLOBAL',
              '290.5,0.000E+00')
ecsv.add_data('GLOBAL',
              '291.0,0.000E+00')
ecsv.add_table_comment('GLOBAL', 'This is a table level comment', index=1)
# Add table for new groupings
ecsv.add_data('TIMESTAMP',
              '+08:38:46,1991-01-01,07:38:46',
              field='UTCOffset,Date,Time',
              index=2)

ecsv.add_data('GLOBAL_SUMMARY',
              '07:38:46,2.376E-02,3.984E-01,82.92,6.75,122.69,100000,999',
              field='Time,IntACGIH,IntCIE,ZenAngle,MuValue,AzimAngle,Flag,TempC',
              index=2, table_comment='This is a table level comment.')
ecsv.add_table_comment('GLOBAL_SUMMARY', 'This is another table level comment', index=2)
# Write to string
ecsvs = woudc_extcsv.dumps(ecsv)
                
# Write to file
# validate (check if all common tables and their fields are present), if so dump to file
# if not, print violations
woudc_extcsv.dump(ecsv, 'spectral-sample.csv')
```

### Convenience Functions

```python
import woudc_extcsv
# load from file into Reader object
ecsv = woudc_extcsv.load('file.csv')
# load from string into Reader object
ecsv = woudc_extcsv.loads(my_ecsv_string)
# dump to file from Writer object
ecsv = woudc_extcsv.dump(ecsv_writer, 'file.csv')
# dump to string from Writer object
ecsv = woudc_extcsv.dumps(ecsv_writer)
```

### ExtendedCSV Objects
The ExtendedCSV class is a parser class used in the Reader and Writer classes, and can be used to both read and write to an Extended CSV object.

```python
from woudc_extcsv import ExtendedCSV
# read from file
with open('file.csv') as ff:
    ecsv = ExtendedCSV(ff.read())
# read from string
ecsv = Extended(my_ecsv_string)

# Add a file-level comment
ecsv.add_comment('This is a file level comment')
# Add new table to object
ecsv.init_table('GLOBAL', ['Wavelength', 'S-Irradiance'], line_num)
# Add another field to the new table
ecsv.add_field_to_table('GLOBAL', ['Time'])
# Add data to table
ecsv.add_values_to_table('GLOBAL', ['290.5', '0.000E+00', ''], line_num)
# Add a table comment
ecsv.add_table_comment('GLOBAL', 'This is a table level comment')
# Remove a table
ecsv.remove_table('GLOBAL')
# Validate Extended CSV and collimate the tables
# Check metadata tables
ecsv.validate_metadata_tables()
# Check dataset-specific tables
ecsv.validate_dataset_tables()
```

### Error Handling

```python
from woudc_extcsv import ExtendedCSV, NonStandardDataError, MetadataValidationError

try:
    ecsv = ExtendedCSV('bad content!')
except (NonStandardDataError, MetadataValidationError) as err:
    print(err.message)
    for error in err.errors:
         print(error)
```

## Development

```bash
python3 -m venv woudc-extcsv
cd woudc-extcsv
source bin/activate
git clone https://github.com/woudc/woudc-extcsv.git
cd woudc-extcsv
pip3 install .
pip3 install ".[dev]"
```

### Running Tests

```bash
python3 run_tests.py
```

## Releasing

```bash
# create release (x.y.z is the release version)
vi pyproject.toml  # update [project]/version
git commit -am 'update release version x.y.z'
git push origin master
git tag -a x.y.z -m 'tagging release version x.y.z'
git push --tags

# upload to PyPI
rm -fr build dist *.egg-info
python3 -m build
twine upload dist/*

# publish release on GitHub (https://github.com/woudc/woudc-extcsv/releases/new)

# bump version back to dev
vi pyproject.toml  # update [project]/version
git commit -am 'back to dev'
git push origin master
```

### Code Conventions

woudc-extcsv code conventions are as per
[PEP8](https://www.python.org/dev/peps/pep-0008).

```bash
# code should always pass the following
find -type f -name "*.py" | xargs flake8
```

## Issues

All bugs, enhancements and issues are managed on
[GitHub](https://github.com/woudc/woudc-extcsv/issues).

## History

The roots of woudc-extcsv originate within the WOUDC backend processing system
in support of processing data submissions.  woudc-extcsv was refactored
into a standalone library providing read/write support of the data centre's
core ingest format.

In 2015 woudc-extcsv was made publically available in support of the Treasury
Board [Policy on Acceptable Network and Device Use]
(http://www.tbs-sct.gc.ca/pol/doc-eng.aspx?id=27122).

## Contact

* [Tom Kralidis](https://github.com/tomkralidis)
* [Thinesh Sornalingam](https://github.com/thineshsornalingam)
