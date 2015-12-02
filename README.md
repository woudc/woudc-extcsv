# WOUDC Extended CSV library

Python package providing read/write support of the
[WOUDC](http://woudc.org) Extended CSV format.

## Installation

### Requirements

woudc-extcsv requires Python 2.6 or greater. woudc-extcsv works with Python 3.

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
TODO
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
