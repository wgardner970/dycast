#!/bin/bash

PGDBNAME=${PGDBNAME}
PGUSER=${PGUSER:-postgres}

ZIKAST_INBOX=${ZIKAST_INBOX:-$ZIKAST_PATH/inbox}
ZIKAST_INBOX_COMPLETED=${ZIKAST_INBOX}/completed
ZIKAST_OUTBOX=${ZIKAST_OUTBOX:-$ZIKAST_PATH/outbox}
ZIKAST_INIT_PATH=${ZIKAST_APP_PATH}/init

PG_SHARE_PATH_2_0=/usr/share/postgresql/9.6/contrib/postgis-2.3

init_zikast() {
	if [[ "${FORCE_DB_INIT}" == "True" ]]; then
		echo ""
		echo "*** Warning: FORCE_DB_INIT = True ***"
		echo "" 
		init_db
	elif db_exists; then
		echo "Database ${PGDBNAME} already exists, skipping initialization."
	else
		echo "Database ${PGDBNAME} does not exists."
		init_db
	fi
	
	init_directories
}


db_exists() {
	psql -lqt -h ${PGHOST} -U ${PGUSER} | cut -d \| -f 1 | grep -qw ${PGDBNAME}
}


init_db() {
	echo "Initializing database..."

	echo "Dropping existing database ${PGDBNAME}"
	dropdb -h ${PGHOST} -U ${PGUSER} ${PGDBNAME} # if necessary
	echo "" 

	echo "Creating database ${PGDBNAME}"
	createdb -h ${PGHOST} -U ${PGUSER} --encoding=UTF8 ${PGDBNAME} --template template0
	echo "" 
	
	### Using the new 9.1+ extension method:
	echo "Creating extension 'postgis'"
    psql -h ${PGHOST} -U ${PGUSER} -d ${PGDBNAME} -c "CREATE EXTENSION postgis;" 
	echo "" 

    ### And we need legacy functions (currently)
    # psql -h ${PGHOST} -U ${PGUSER} -d ${PGDBNAME} -f ${PG_SHARE_PATH_2_0}/legacy.sql
    # psql -h ${PGHOST} -U ${PGUSER} -d ${PGDBNAME} -f ${PG_SHARE_PATH_2_0}/postgis.sql
    # psql -h ${PGHOST} -U ${PGUSER} -d ${PGDBNAME} -f ${PG_SHARE_PATH_2_0}/spatial_ref_sys.sql

	echo "Running ${ZIKAST_INIT_PATH}/postgres_init.sql"
	psql -h ${PGHOST} -U ${PGUSER} -d ${PGDBNAME} -f ${ZIKAST_INIT_PATH}/postgres_init.sql
	echo "" 

	echo "Running ${ZIKAST_INIT_PATH}/dengue_brazil/effects_poly_centers_projected_71824_brazil.sql"
	psql -h ${PGHOST} -U ${PGUSER} -d ${PGDBNAME} -f ${ZIKAST_INIT_PATH}/dengue_brazil/effects_poly_centers_projected_71824_brazil.sql
	echo "" 

	echo "Running ${ZIKAST_INIT_PATH}/dumped_dist_margs.sql"
	psql -h ${PGHOST} -U ${PGUSER} -d ${PGDBNAME} -f ${ZIKAST_INIT_PATH}/dumped_dist_margs.sql
	echo "" 
}


init_directories() {
	if [[ ! -d ${ZIKAST_INBOX_COMPLETED} ]]; then
		mkdir -p ${ZIKAST_INBOX_COMPLETED}
	fi
	
	if [[ ! -d ${ZIKAST_OUTBOX}/tmp ]]; then
		mkdir -p ${ZIKAST_OUTBOX}/{tmp,cur,new}
	fi
}


listen_for_input() {
	echo ""
	echo "*** Zikast is now listening for new .tsv files in ${ZIKAST_INBOX}... ***"
	echo "" 
	while true; do
		# echo "Running daily_tasks.py..."
		# python ${ZIKAST_APP_PATH}/daily_tasks.py
		for file in ${ZIKAST_INBOX}/*.tsv; do
			if [[ -f ${file} ]]; then
			
				echo "Loading input file: ${file}..."
				python ${ZIKAST_APP_PATH}/load_birds.py "${file}"
				
				echo "Completed loading input file, moving it to ${ZIKAST_INBOX_COMPLETED}"
				filename=$(basename "$file")
				mv "${file}" "${ZIKAST_INBOX_COMPLETED}/${filename}_completed"
				
				echo "Generating risk..."
				python ${ZIKAST_APP_PATH}/daily_risk.py --date 1998-01-04
				
				echo "Exporting risk..."
				python ${ZIKAST_APP_PATH}/export_risk.py 1998-01-04

				echo "Done."
			fi
		done
		
		sleep 5
	done
}

init_zikast
listen_for_input
