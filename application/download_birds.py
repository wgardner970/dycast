#! /usr/bin/env python
#$Id: download_birds.py,v 1.3 2008/02/01 23:04:45 alan Exp alan $
#
# Downlaod a dead bird .tsv file from an FTP site
#
# Meant to be run daily as a scheduled task
#
# On Fridays, this script will create a backup of the current file before 
# overwriting it with the newly downloaded file
#
# The connection parameters for the FTP site are given in a config file
#
# See load_birds.py for more information about the dead bird file format

import sys
import dycast
import optparse
from ftplib import FTP

usage = "usage: %prog [options]"
p = optparse.OptionParser(usage)
p.add_option('--config', '-c', 
            default="./dycast.config", 
            help="load config file FILE", 
            metavar="FILE"
            )
options, arguments = p.parse_args()

config_file = options.config

try:
    dycast.read_config(config_file)
except:
    print "could not read config file:", config_file
    sys.exit()

dycast.init_logging()

dycast.info("downloading birds")
dycast.download_birds()

