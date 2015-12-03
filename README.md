[![Build Status](https://travis-ci.org/woudc/woudc-extcsv.png?branch=master)](https://travis-ci.org/woudc/woudc-extcsv) [![Build status](https://ci.appveyor.com/api/projects/status/02koln2pe4ap5kvd/branch/master?svg=true)](https://ci.appveyor.com/project/tomkralidis/woudc-extcsv/branch/master)

# WOUDC Extended CSV library

Python package providing read/write support of the
[WOUDC](http://woudc.org) Extended CSV format.

## Installation

### Requirements

woudc-extcsv requires Python 2.7.

### Dependencies

None.

### Installing the Package

```bash
# via pip
pip install woudc-extcsv
# via easy_install
easy_install woudc-extcsv
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
ecsv.add_data('TIMESTAMP', '+08:38:47', field='UTCOffset')
# Add more data
ecsv.add_data('TIMESTAMP', '1991-01-01', field='Date')
ecsv.add_data('TIMESTAMP', '06:38:47', field='Time')

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
# Add table for new groupings
ecsv.add_data('TIMESTAMP',
                '+08:38:46,1991-01-01,07:38:46',
                field='UTCOffset,Date,Time',
                index=2)

ecsv.add_data('GLOBAL_SUMMARY',
                '07:38:46,2.376E-02,3.984E-01,82.92,6.75,122.69,100000,999',
                field='Time,IntACGIH,IntCIE,ZenAngle,MuValue,AzimAngle,Flag,TempC',
                index=2, table_comment='This is a table level comment.')

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
ecsv = woudc_extcsv.load(my_ecsv_string)
# dump to file from Writer object
ecsv = woudc_extcsv.dump('file.csv')
# dump to string from Writer object
ecsv = woudc_extcsv.dumps(my_ecsv_string)
```

## Examples

See the `examples/` directory for sample scripts.

## Development

For development environments, install
in a Python [virtualenv](http://virtualenv.org):

```bash
virtualenv foo
cd foo
. bin/activate
# fork master
# fork https://github.com/woudc/woudc-extcsv on GitHub
# clone your fork to create a branch
git clone https://github.com/{your GitHub username}/woudc-extcsv.git
cd woudc-extcsv
# install dev packages
pip install -r requirements-dev.txt
# create upstream remote
git remote add upstream https://github.com/woudc/woudc-extcsv.git
git pull upstream master
git branch my-cool-feature
git checkout my-cool-feature
# start dev
git commit -m 'implement cool feature'
# push to your fork
git push origin my-cool-feature
# issue Pull Request on GitHub
git checkout master
# cleanup/update once your branch is merged on GitHub
# remove branch
git branch -D my-cool-feature
# update your fork
git pull upstream master
git push origin master
```

### Running Tests

```bash
# via distutils
python setup.py test
# manually
python run_tests.py
# report test coverage
coverage run --source woudc_extcsv setup.py test
coverage report -m
```

### Code Conventions

woudc_extcsv code conventions are as per
[PEP8](https://www.python.org/dev/peps/pep-0008).

```bash
# code should always pass the following
find -type f -name "*.py" | xargs pep8 --ignore=E402
find -type f -name "*.py" | xargs pyflakes
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

* [Tom Kralidis](http://geds20-sage20.ssc-spc.gc.ca/en/GEDS20/?pgid=015&dn=CN%3Dtom.kralidis%40canada.ca%2COU%3DDAT-GES%2COU%3DMON-STR%2COU%3DMON-DIR%2COU%3DMSCB-DGSMC%2COU%3DDMO-CSM%2COU%3DEC-EC%2CO%3Dgc%2CC%3Dca)
* [Thinesh Sornalingam](http://geds20-sage20.ssc-spc.gc.ca/en/GEDS20/?pgid=015&dn=CN%3Dthinesh.sornalingam%40canada.ca%2COU%3DDAT-GES%2COU%3DMON-STR%2COU%3DMON-DIR%2COU%3DMSCB-DGSMC%2COU%3DDMO-CSM%2COU%3DEC-EC%2CO%3DGC%2CC%3DCA)
