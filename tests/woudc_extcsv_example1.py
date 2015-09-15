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
# Example 1: Create object using various writer methods

import os
import logging
from woudc.extcsv import WOUDCextCSVWriter
from extcsv.util import setup_logger

# setup logging
setup_logger('../etc/extcsv.log', 'DEBUG')


# new extcsv object
extcsv = WOUDCextCSVWriter()

# add table
extcsv.add_table('CONTENT', 'This table stores basic metadata')
extcsv.add_table('CONTENT', 'This table stores basic metadata')
extcsv.add_table('CONTENT', 'This table stores basic metadata', index=2)

# add field, vertical/single
extcsv.add_field('CONTENT', 'Class')
# add filed, horizontal/multiple
extcsv.add_field('CONTENT', 'Class,Category,Level')

# add new table through add field
extcsv.add_field('PLATFORM', 'Class,Category,Level')
extcsv.add_field('PLATFORM', 'Class,Category,Level', index=2)

# add data, vertical/single
extcsv.add_data('CONTENT', 'WOUDC', field='Class')
extcsv.add_data('CONTENT', 'Value1', field='Class')

# add data, horizontal/multiple
extcsv.add_data('CONTENT', 'a,b,c')

extcsv.add_data('CONTENT','d,e,f')

extcsv.add_field('CONTENT', 'Class,Category,Level', index=2)
extcsv.add_data('CONTENT','d,e,f', index=2)
extcsv.add_data('CONTENT','g,h,i', index=2, field='Category')
extcsv.add_data('CONTENT',[1,2,3,4,5], index=2, field='Category')
extcsv.add_data('CONTENT',['sfsf,sdfsf',7,8,9,10], index=2, field='Category')
extcsv.add_data('CONTENT',['sfsf,sdfsf',7,8,9,10, 7, 8, 8], index=3, field='Category')
extcsv.add_data('CONTENT','a,b', index=4, field='Category,field2,field3')
extcsv.add_data('PLATFORM','value', index=1, field='Class')

# remove data
extcsv.remove_table('CONTENT')
extcsv.remove_field('CONTENT', 'Category', index=2)
extcsv.remove_data('CONTENT', 'Class', data='d', index=2)
extcsv.remove_data('CONTENT', 'Class', data='e', index=2)
extcsv.remove_data('CONTENT', 'Class', d_index=1, index=2)
extcsv.remove_data('CONTENT', 'Category', d_index=0, index=3)
extcsv.remove_data('CONTENT', 'Category', data=7, index=3, all_occurances=True)
extcsv.remove_data('CONTENT', 'Category', data=23424, index=3, all_occurances=True)
extcsv.remove_data('CONTENT', 'Category', data=8, index=3, all_occurances=True)
extcsv.remove_data('CONTENT', 'Category', data=10, index=3, all_occurances=True)


#clear
#extcsv.clear_file()
extcsv.clear_table('CONTENT', index=3)
extcsv.add_data('CONTENT','new_value', index=3, field='Category')
extcsv.clear_field('CONTENT', index=3, field='Category')

extcsv.inspect_table('CONTENT', index=3)


extcsv.add_comment('Comment1')
extcsv.add_comment('Comment2')
extcsv.add_comment('Comment3')
extcsv.filename = 'extcsv1.csv'
#extcsv.get_ds()

extcsv.serialize()


