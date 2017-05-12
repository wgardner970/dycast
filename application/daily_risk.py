#! /usr/bin/env python
#$Id: daily_risk.py,v 1.9 2008/04/08 15:56:57 alan Exp alan $

import sys
import dycast
import datetime
import optparse

usage = "usage: %prog [options] YYYY-MM-DD"
required = "srid".split()

p = optparse.OptionParser(usage)
p.add_option('--date', 
            default="today", 
            )
p.add_option('--srid')
p.add_option('--extent_min_x')
p.add_option('--extent_min_y')
p.add_option('--extent_max_x')
p.add_option('--extent_max_y')

p.add_option('--startpoly', '-s')
p.add_option('--endpoly', '-e')
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

riskdate = options.date
user_coordinate_system = options.srid
extent_min_x = float(options.extent_min_x)
extent_min_y = float(options.extent_min_y)
extent_max_x = float(options.extent_max_x)
extent_max_y = float(options.extent_max_y)

dycast.init_logging()
dycast.init_db()


if riskdate == "today" or not riskdate:
    riskdate = datetime.date.today()
else:
    try:
        # This very simple parsing will work fine if date is YYYY-MM-DD
        (y, m, d) = riskdate.split('-')
        riskdate = datetime.date(int(y), int(m), int(d))
    except Exception, inst:
        print "couldn't parse", riskdate
        print inst
        sys.exit()

if options.endpoly and options.startpoly:
    dycast.daily_risk(riskdate, options.startpoly, options.endpoly)
elif options.endpoly and not options.startpoly:
    print "ERROR: the endpoly option is only supported along with startpoly"
elif options.startpoly:
    dycast.daily_risk(riskdate, options.startpoly)
else:
    dycast.daily_risk(riskdate, user_coordinate_system, extent_min_x, extent_min_y, extent_max_x, extent_max_y)

