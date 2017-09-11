#!/bin/bash

PGDBNAME=${PGDBNAME}
PGUSER=${PGUSER:-postgres}

DYCAST_INBOX=${DYCAST_INBOX:-$DYCAST_PATH/inbox}
DYCAST_INBOX_COMPLETED=${DYCAST_INBOX}/completed
DYCAST_OUTBOX=${DYCAST_OUTBOX:-$DYCAST_PATH/outbox}
DYCAST_INIT_PATH=${DYCAST_APP_PATH}/init

PG_SHARE_PATH_2_0=/usr/share/postgresql/9.6/contrib/postgis-2.3

START_DATE=$(date -I -d "${START_DATE}") || exit -1
END_DATE=$(date -I -d "${END_DATE}") || exit -1

USER_COORDINATE_SYSTEM="${USER_COORDINATE_SYSTEM}"
EXTENT_MIN_X=${EXTENT_MIN_X}
EXTENT_MIN_Y=${EXTENT_MIN_Y}
EXTENT_MAX_X=${EXTENT_MAX_X}
EXTENT_MAX_Y=${EXTENT_MAX_Y}

init_dycast() {
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
	echo "" && echo ""
	echo "*** Initializing database ***"
	echo ""
	echo "*** Any existing Dycast database will be deleted ***"
	echo "" && echo ""

	echo "5" && sleep 1 && echo "4" && sleep 1 && echo "3" && sleep 1 && echo "2" && sleep 1 &&	echo "1" &&	sleep 1 && echo "0" && echo ""

	echo "Dropping existing database ${PGDBNAME}"
	dropdb -h ${PGHOST} -U ${PGUSER} ${PGDBNAME} # if necessary
	echo ""

	echo "Creating database ${PGDBNAME}"
	createdb -h ${PGHOST} -U ${PGUSER} --encoding=UTF8 ${PGDBNAME}
	echo ""

	### Using the new 9.1+ extension method:
	echo "Creating extension 'postgis'"
	psql -h ${PGHOST} -U ${PGUSER} -d ${PGDBNAME} -c "CREATE EXTENSION postgis;" 
	echo ""

	echo "Running ${DYCAST_INIT_PATH}/postgres_init.sql"
	psql -h ${PGHOST} -U ${PGUSER} -d ${PGDBNAME} -f ${DYCAST_INIT_PATH}/postgres_init.sql
	echo ""

	echo "Importing Monte Carlo data"
	psql -h ${PGHOST} -U ${PGUSER} -d ${PGDBNAME} -c "\COPY dist_margs FROM '${DYCAST_INIT_PATH}/Dengue_min_75.csv' delimiter ',';"
	echo "" 
}


init_directories() {
	if [[ ! -d ${DYCAST_INBOX_COMPLETED} ]]; then
		mkdir -p ${DYCAST_INBOX_COMPLETED}
	fi

	if [[ ! -d ${DYCAST_OUTBOX}/tmp ]]; then
		mkdir -p ${DYCAST_OUTBOX}/{tmp,cur,new}
	fi
}

run_tests() {

	echo "Running unit tests..."
	nosetests -vv --exe tests

	exit_code=$?
	if [[ ! "${exit_code}" == "0" ]]; then
		echo "Unit test(s) failed, exiting..."
		exit ${exit_code}
	fi

}

listen_for_input() {
	echo ""
	echo "*** Dycast is now listening for new .tsv files in ${DYCAST_INBOX}... ***"
	echo ""

	while true; do
		for file in ${DYCAST_INBOX}/*.tsv; do
			if [[ -f ${file} ]]; then

				echo "Loading input file: ${file}..."
				python ${DYCAST_APP_PATH}/load_cases.py --srid ${USER_COORDINATE_SYSTEM} "${file}"

				exit_code=$?
				if [[ ! "${exit_code}" == "0" ]]; then
					echo "load_cases failed, exiting..."
					exit ${exit_code}
				fi

				echo "Completed loading input file, moving it to ${DYCAST_INBOX_COMPLETED}"
				filename=$(basename "$file")
				mv "${file}" "${DYCAST_INBOX_COMPLETED}/${filename}_completed"


				echo ""
				echo "Generating risk..."
				echo ""
				python ${DYCAST_APP_PATH}/daily_risk.py --startdate ${START_DATE} --enddate ${END_DATE} --srid ${USER_COORDINATE_SYSTEM} --extent_min_x ${EXTENT_MIN_X} --extent_min_y ${EXTENT_MIN_Y} --extent_max_x ${EXTENT_MAX_X} --extent_max_y ${EXTENT_MAX_Y}


				echo ""
				echo "Exporting risk..."
				echo ""
				python ${DYCAST_APP_PATH}/export_risk.py --startdate ${START_DATE} --enddate ${END_DATE} --txt true

				echo "Done."
			fi
		done
		sleep 5
	done
}

check_variable() {
	local VARIABLE_VALUE=$1
	local VARIABLE_NAME=$2
	if [[ -z ${VARIABLE_VALUE} ]] || [[ "${VARIABLE_VALUE}" == "" ]]; then
		echo "ERROR! Environment variable ${VARIABLE_NAME} not specified. Exiting..."
		exit 1
	fi
}

check_all_variables() {
	check_variable "${START_DATE}" START_DATE
	check_variable "${END_DATE}" END_DATE
	check_variable "${USER_COORDINATE_SYSTEM}" USER_COORDINATE_SYSTEM
	check_variable "${EXTENT_MIN_X}" EXTENT_MIN_X
	check_variable "${EXTENT_MIN_Y}" EXTENT_MIN_Y
	check_variable "${EXTENT_MAX_X}" EXTENT_MAX_X
	check_variable "${EXTENT_MAX_Y}" EXTENT_MAX_Y
	check_variable "${PGPASSWORD}" PGPASSWORD
	check_variable "${PGDBNAME}" PGDBNAME
	check_variable "${PGHOST}" PGHOST
	check_variable "${PGPORT}" PGPORT
}

check_all_variables
init_zikast
run_tests
listen_for_input
