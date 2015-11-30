# WMO WOUDC extended CSV library

Python package providing read/write support of the WOUDC Extended CSV format.

## Installation


## Requirements

* Python 2.6 and above.

## Dependencies


## Installing the Package

```bash
git clone http://gitlab.ssc.etg.gc.ca/woudc/woudc-extcsv.git && cd woudc-extcsv
python setup.py build
python setup.py install
```

## Development

For development environments, install
within a Python virtualenv (http://virtualenv.org):


```bash
virtualenv foo
cd foo
. bin/activate
# Fork master master
# Fork http://gitlab.ssc.etg.gc.ca/woudc/woudc-extcsv.git
# Clone your fork to create a branch
# git clone http://gitlab.ssc.etg.gc.ca/{your gitlab username}/woudc-extcsv.git && cd woudc-extcsv
# install steps as per above
# start dev
```

### Running tests

```bash
# via distutils
python setup.py test
# manually
python run_tests.py
```

### Code Conventions

woudc_extcsv code conventions are as per
[PEP8](https://www.python.org/dev/peps/pep-0008)

```bash
# code should always pass the following
find -type f -name "*.py" | xargs pep8 --ignore=E402
find -type f -name "*.py" | xargs pyflakes
```

## Issues

All bugs, enhancements and issues are managed on
[GitHub](https://github.com/woudc/woudc_extcsv/issues)
