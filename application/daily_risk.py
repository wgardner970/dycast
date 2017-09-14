#! /usr/bin/env python
#$Id: daily_risk.py,v 1.9 2008/04/08 15:56:57 alan Exp alan $

import sys
import dycast
import datetime
import optparse
import logging

from services import logging_service


logging_service.init_logging()

usage = "usage: %prog [options] YYYY-MM-DD"
required = "srid".split()

p = optparse.OptionParser(usage)
p.add_option('--startdate', 
            default="today", 
            )
p.add_option('--enddate')
p.add_option('--srid')
p.add_option('--extent_min_x')
p.add_option('--extent_min_y')
p.add_option('--extent_max_x')
p.add_option('--extent_max_y')

p.add_option('--config', '-c', 
            default="./dycast.config", 
            help="load config file FILE", 
            metavar="FILE"
            )

options, arguments = p.parse_args()

for r in required:
    if options.__dict__[r] is None:
        parser.error("parameter %s required"%r)
        sys.exit(1)

config_file = options.config

try:
    dycast.read_config(config_file)
except:
    print "could not read config file:", config_file
    sys.exit()

user_coordinate_system = options.srid
extent_min_x = float(options.extent_min_x)
extent_min_y = float(options.extent_min_y)
extent_max_x = float(options.extent_max_x)
extent_max_y = float(options.extent_max_y)

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

dycast.daily_risk(startdate, enddate, user_coordinate_system, extent_min_x, extent_min_y, extent_max_x, extent_max_y)

