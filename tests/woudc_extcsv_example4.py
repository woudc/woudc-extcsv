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

# Create extcsv object
# Example 4: Create object from a supplied OrderedDict and write to file

import os
import logging
from collections import OrderedDict
from woudc.extcsv import WOUDCextCSVWriter
from extcsv.util import setup_logger

# setup logging
setup_logger('../etc/extcsv4.log', 'DEBUG')


# new extcsv object
in_ds = OrderedDict([('CONTENT$1', OrderedDict([('comments', []), ('Class', ['WOUDC']), ('Category', ['Spectral']), ('Level', ['1.0']), ('Form', ['1'])])), ('DATA_GENERATION$1', OrderedDict([('comments', []), ('Date', ['2005-04-30']), ('Agency', ['EPA_UGA']), ('Version', ['2.00']), ('ScientificAuthority', [])])), ('PLATFORM$1', OrderedDict([('comments', []), ('Type', ['STN']), ('ID', ['388']), ('Name', ['Shenandoah']), ('Country', ['USA']), ('GAW_ID', [])])), ('INSTRUMENT$1', OrderedDict([('comments', []), ('Name', ['Brewer']), ('Model', ['MKIV']), ('Number', ['137'])])), ('LOCATION$1', OrderedDict([('comments', ['Time reported is Solar Time.  Subtract UTCOffset for UTC.', '"Reformatted by the WOUDC"', 'This is a table comment.']), ('Latitude', ['38.52']), ('Longitude', ['-78.44']), ('Height', ['1073'])])), ('TIMESTAMP$1', OrderedDict([('comments', []), ('UTCOffset', ['-05:27:17']), ('Date', ['2004-01-31']), ('Time', ['07:28:49'])])), ('GLOBAL_SUMMARY$1', OrderedDict([('comments', []), ('Time', ['07:28:49']), ('IntACGIH', ['9.891E-02']), ('IntCIE', ['1.745E+00']), ('ZenAngle', ['84.36']), ('MuValue', ['7.79']), ('AzimAngle', ['117.27']), ('Flag', ['000030']), ('TempC', ['12'])])), ('GLOBAL$1', OrderedDict([('comments', []), ('Wavelength', ['290.0', '290.5', '291.0', '291.5']), ('S-Irradiance', ['1.700E-06', '8.000E-07', '0.000E+00', '8.000E-07']), ('Time', [])])), ('TIMESTAMP$2', OrderedDict([('comments', []), ('UTCOffset', ['-05:27:18']), ('Date', ['2004-01-31']), ('Time', ['07:58:48'])])), ('GLOBAL_SUMMARY$2', OrderedDict([('comments', []), ('Time', ['07:58:48']), ('IntACGIH', ['3.617E-01']), ('IntCIE', ['5.720E+00']), ('ZenAngle', ['79.26']), ('MuValue', ['4.92']), ('AzimAngle', ['122.36']), ('Flag', ['000030']), ('TempC', ['12'])])), ('GLOBAL$2', OrderedDict([('comments', []), ('Wavelength', ['290.0', '290.5', '291.0', '291.5']), ('S-Irradiance', ['0.000E+00', '9.000E-07', '0.000E+00', '4.100E-06']), ('Time', [])])), ('GLOBAL_DAILY_SUMMARY$1', OrderedDict([('comments', []), ('IntACGIH', ['1.794E+02']), ('IntCIE', ['1.387E+03'])])), ('GLOBAL_DAILY_SUMMsfsfARY$1', OrderedDict([('comments', []), ('IntACGIH', []), ('IntCIE', [])]))])
extcsv = WOUDCextCSVWriter(ds=in_ds)
extcsv.filename = 'extcsv4.csv'

#extcsv.get_ds()
extcsv.serialize()