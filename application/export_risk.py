#! /usr/bin/env python
#$Id: export_risk.py,v 1.3 2008/02/01 23:04:45 alan Exp alan $

import sys
import dycast
import datetime
import optparse
import logging

from services import logging_service


logging_service.init_logging()

usage = "usage: %prog [options] YYYY-MM-DD"
p = optparse.OptionParser(usage)
p.add_option('--startdate', 
            default="today", 
            )
p.add_option('--enddate')
p.add_option('--dbf', '-d')
p.add_option('--txt', '-t')
#p.add_option('--outfile', '-o')
p.add_option('--config', '-c', 
            default="./dycast.config", 
            help="load config file FILE", 
            metavar="FILE")

options, arguments = p.parse_args()

config_file = options.config
dycast.read_config(config_file)

dycast.init_db()


startdate_string = options.startdate
if startdate_string == "today" or not startdate_string:
    startdate = datetime.date.today()
else:
    try:
        # This very simple parsing will work fine if date is YYYY-MM-DD
        (y, m, d) = startdate_string.split('-')
        startdate = datetime.date(int(y), int(m), int(d))
    except Exception, inst:
        print "couldn't parse", startdate_string
        print inst
        sys.exit()


enddate_string = options.enddate
if enddate_string == "today":
    enddate = datetime.date.today()
elif not enddate_string:
    enddate = startdate
else:
    try:
        # This very simple parsing will work fine if date is YYYY-MM-DD
        (y, m, d) = enddate_string.split('-')
        enddate = datetime.date(int(y), int(m), int(d))
    except Exception, inst:
        print "couldn't parse", enddate_string
        print inst
        sys.exit()

if options.txt:
    dycast.export_risk(startdate, enddate, "txt")
else:
    dycast.export_risk(startdate, enddate, "dbf")

