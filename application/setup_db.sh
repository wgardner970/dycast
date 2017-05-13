#!/bin/sh
#$Id: setup_db.sh,v 1.5 2008/03/31 18:41:52 alan Exp alan $

DBNAME=dycast
USERNAME=postgres
DYCAST_PATH=/Users/alan/Documents/DYCAST/
DYCAST_APP_PATH=$DYCAST_PATH/application/
DYCAST_INIT_PATH=$DYCAST_PATH/init/
#PG_BIN_PATH=/usr/local/pgsql/bin...?
PG_SHARE_PATH=/usr/local/pgsql/share/

dropdb -U $USERNAME $DBNAME # if necessary
createdb -U $USERNAME --encoding=UTF8 $DBNAME
createlang -U $USERNAME plpgsql $DBNAME
psql -U $USERNAME -d $DBNAME -f $PG_SHARE_PATH/lwpostgis.sql
psql -U $USERNAME -d $DBNAME -f $PG_SHARE_PATH/spatial_ref_sys.sql
psql -U $USERNAME -d $DBNAME -f $DYCAST_APP_PATH/postgres_init.sql

psql -U $USERNAME -d $DBNAME -f $DYCAST_INIT_PATH/dumped_dist_margs.sql
psql -U $USERNAME -d $DBNAME -f $DYCAST_INIT_PATH/dumped_county_codes.sql
psql -U $USERNAME -d $DBNAME -f $DYCAST_INIT_PATH/dumped_effects_polys.sql
psql -U $USERNAME -d $DBNAME -f $DYCAST_INIT_PATH/dumped_effects_poly_centers.sql

# create inbox, outbox (as maildir) but test for existence first
mkdir $DYCAST_PATH/inbox
mkdir $DYCAST_PATH/outbox
mkdir $DYCAST_PATH/outbox/tmp
mkdir $DYCAST_PATH/outbox/cur
mkdir $DYCAST_PATH/outbox/new
