#! /usr/bin/env python
#
# Read a file containing dead birds and load them into the database
#
# The file(s) can be provided as the arguments like so:
#
# load_birds.py bird_file.tsv bird_file2.tsv
#
# Or as a pipe like this, using "-" as the argument:
#
# cat bird_file.tsv | load_birds.py -
#
# The first line (field names) will be discarded
# remaining lines must be tab-separated, in the following order:
# id
# date (MM/DD/YYYY)
# long (decimal degrees)
# lat (decimal degrees)
# species

import optparse
import os
import inspect
import dycast

usage = "usage: %prog [options] datafile.tsv"
current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
default_config = os.path.join(current_dir, 'dycast.config')

p = optparse.OptionParser(usage)
p.add_option('--config', '-c', 
            default=default_config, 
            help="load config file FILE", 
            metavar="FILE")
options, arguments = p.parse_args()

config_file = options.config

dycast.read_config(config_file)

dycast.init_logging()

dycast.init_db()

# If arguments includes multiple filenames, fileinput will handle them all

for file in arguments:
    (lines_read, lines_processed, lines_loaded, lines_skipped) = dycast.load_bird_file(file)

