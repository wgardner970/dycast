#! /usr/bin/env python
#$Id: dbf_print.py,v 1.3 2008/04/08 15:56:21 alan Exp alan $
# Simple script for printing out the content of a DBF.
# Meant only for quick debugging

import sys
import os
import optparse

# Workaround to load dbf library:
if sys.platform == 'win32':
    lib_dir = "C:\\DYCAST\\application\\libs"
else:
    lib_dir = "/Users/alan/Documents/DYCAST/py_bin/libs"

sys.path.append(lib_dir)            # the hard-coded library above
sys.path.append("libs")             # a library relative to current folder

sys.path.append(lib_dir+os.sep+"dbfpy") # the hard-coded library above
sys.path.append("libs"+os.sep+"dbfpy")  # a library relative to current folder

try:
    import dbf
except ImportError:
    print "couldn't import dbf library in path:", sys.path
    sys.exit()
# end workaround


usage = "usage: %prog [options] file.dbf"
p = optparse.OptionParser(usage)
options, arguments = p.parse_args()

for filename in arguments:
    dbfn = dbf.Dbf(filename, readOnly = 1)
    for fldName in dbfn.fieldNames:
        print '%s\t' % fldName,
    print
    for recnum in range(0,len(dbfn)):
        rec = dbfn[recnum]
        for fldName in dbfn.fieldNames:
            print '%s\t' % rec[fldName],
        print
    dbfn.close()
        
