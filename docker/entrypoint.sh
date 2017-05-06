#!/bin/bash

PGDBNAME=${PGDBNAME}
PGUSER=${PGUSER:-postgres}

ZIKAST_INBOX=${ZIKAST_INBOX:-$ZIKAST_PATH/inbox}
ZIKAST_INBOX_COMPLETED=${ZIKAST_INBOX}/completed
ZIKAST_OUTBOX=${ZIKAST_OUTBOX:-$ZIKAST_PATH/outbox}
ZIKAST_INIT_PATH=${ZIKAST_APP_PATH}/init

PG_SHARE_PATH_2_0=/usr/share/postgresql/9.6/contrib/postgis-2.3

START_DATE=$(date -I -d "${START_DATE}") || exit -1
END_DATE=$(date -I -d "${END_DATE}") || exit -1


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
				

				echo "" 
				echo "Generating risk..."
				echo ""
				current_day="${START_DATE}"
				srid = "29193"
				extent_min_x = 197457.283284349
				extent_max_x = 224257.283284349
				extent_min_y = 7639474.3256114
				extent_max_y = 7666274.3256114
				while [[ ! "${current_day}" > "${END_DATE}" ]]; do 
					echo "Generating risk for ${current_day}..."
					python ${ZIKAST_APP_PATH}/daily_risk.py --date ${current_day} --extent_min_x ${extent_min_x} --extent_max_x ${extent_max_x} --extent_min_y ${extent_min_y} --extent_max_y ${extent_max_y}
					current_day=$(date -I -d "${current_day} + 1 day")
				done
				
				
				echo "" 
				echo "Exporting risk..."
				echo "" 
				current_day="${START_DATE}"
				while [[ ! "${current_day}" > "${END_DATE}" ]]; do 
					echo "Exporting risk for ${current_day}..."
					python ${ZIKAST_APP_PATH}/export_risk.py ${current_day}
					current_day=$(date -I -d "${current_day} + 1 day")
				done
				

				echo "Done."
			fi
		done
		
		sleep 5
	done
}

init_zikast
listen_for_input
