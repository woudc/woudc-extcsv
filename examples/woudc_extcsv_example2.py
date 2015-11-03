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
# Example 2: Create a spectral object and write it to file

import os
import logging
from woudc.extcsv import WOUDCextCSVWriter
from woudc.extcsv.util import setup_logger

# setup logging
setup_logger('../etc/extcsv2.log', 'DEBUG')


# new extcsv object
extcsv = WOUDCextCSVWriter()
extcsv.filename = 'extcsv2.csv'

# add data here
extcsv.add_data('CONTENT','WOUDC,Spectral,1.0,1', field='Class,Category,Level,Form')
extcsv.add_data('DATA_GENERATION','2005-04-30,EPA_UGA,2.00', field='Date,Agency,Version,ScientificAuthority')
extcsv.add_data('PLATFORM','STN,388,Shenandoah,USA', field='Type,ID,Name,Country,GAW_ID')
extcsv.add_data('INSTRUMENT','Brewer,MKIV,137', field='Name,Model,Number')
extcsv.add_data('LOCATION','38.52,-78.44,1073', field='Latitude,Longitude,Height')
extcsv.add_data('LOCATION',['Time reported is Solar Time.  Subtract UTCOffset for UTC.', '"Reformatted by the WOUDC"', 'This is a table comment.'], field='comments')
extcsv.add_data('TIMESTAMP','-05:27:17,2004-01-31,07:28:49', field='UTCOffset,Date,Time')
extcsv.add_data('GLOBAL_SUMMARY','07:28:49,9.891E-02,1.745E+00,84.36,7.79,117.27,000030,12', field='Time,IntACGIH,IntCIE,ZenAngle,MuValue,AzimAngle,Flag,TempC')
extcsv.add_data('GLOBAL','290.0,1.700E-06', field='Wavelength,S-Irradiance,Time')
extcsv.add_data('GLOBAL','290.5,8.000E-07', field='Wavelength,S-Irradiance,Time')
extcsv.add_data('GLOBAL','291.0,0.000E+00', field='Wavelength,S-Irradiance,Time')
extcsv.add_data('GLOBAL','291.5,8.000E-07', field='Wavelength,S-Irradiance,Time')
extcsv.add_data('TIMESTAMP','-05:27:18,2004-01-31,07:58:48', field='UTCOffset,Date,Time', index=2)
extcsv.add_data('GLOBAL_SUMMARY','07:58:48,3.617E-01,5.720E+00,79.26,4.92,122.36,000030,12', field='Time,IntACGIH,IntCIE,ZenAngle,MuValue,AzimAngle,Flag,TempC', index=2)
extcsv.add_data('GLOBAL','290.0,0.000E+00', field='Wavelength,S-Irradiance,Time', index=2)
extcsv.add_data('GLOBAL','290.5,9.000E-07', field='Wavelength,S-Irradiance,Time', index=2)
extcsv.add_data('GLOBAL','291.0,0.000E+00', field='Wavelength,S-Irradiance,Time', index=2)
extcsv.add_data('GLOBAL','291.5,4.100E-06', field='Wavelength,S-Irradiance,Time', index=2)
extcsv.add_data('GLOBAL_DAILY_SUMMARY','1.794E+02,1.387E+03', field='IntACGIH,IntCIE')

extcsv.add_field('GLOBAL_DAILY_SUMMsfsfARY', 'IntACGIH,IntCIE')

extcsv.add_comment('This is a spectral file')
extcsv.add_comment('This is a file comment.')
#extcsv.get_ds()

# write out file to disk
mem_file = extcsv.serialize()
try:
    with open(extcsv.filename, 'w') as out_file:
        out_file.write(mem_file.getvalue())
except Exception, err:
    msg = 'Unable to write file: %s to write extended CSV, due to: %s' % (extcsv.filename, str(err))
    LOGGER.error(msg)
