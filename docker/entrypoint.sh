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



HELP_TEXT="

Arguments:  
	run_dycast: Default. Loads any .tsv files in ${DYCAST_INBOX}, generates risk and exports it to ${DYCAST_OUTBOX}  
	setup_dycast: Initializes Dycast: creates the database if it does not exist and sets up necessary folders  
	setup_db: Delete any existing Dycast database and set up a fresh one  
	load_cases: Load cases from specified (absolute) file path on the container, e.g. 'load_cases /dycast/inbox/cases.tsv'  
	generate_risk: Generates risk from the cases currently in the database  
	export_risk: Exports risk currently in the database  
	run_tests: Run unit tests  
	-h or help: Display help text  
"

display_help() {
	echo "${HELP_TEXT}"
}



init_dycast() {
	if db_exists; then
		echo "Database ${PGDBNAME} already exists, skipping initialization."
	else
		echo "Database ${PGDBNAME} does not exists."
		init_db
	fi

	init_directories
}


wait_for_db() {
	echo "Testing database connection..."
	tries=0
	while true
	do
		psql -h ${PGHOST} -p ${PGPORT} -U postgres -c "select 1" >&/dev/null
		return_code=$?
		((tries++))
		if [[ ${return_code} == 0 ]]; then
	        break
		elif [[ ${tries} == 6 ]]; then
		    echo "Database server cannot be reached, exiting..."
			exit 1
		fi
		sleep 1
	done
	echo "Connected."
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

				load_cases "${file}"
				move_case_file "${filePath}"

				generate_risk

				export_risk

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

check_database_variables() {
	check_variable "${PGPASSWORD}" PGPASSWORD
	check_variable "${PGDBNAME}" PGDBNAME
	check_variable "${PGHOST}" PGHOST
	check_variable "${PGPORT}" PGPORT
}


move_case_file() {
	local file="$1"
	echo "Completed loading input file, moving it to ${DYCAST_INBOX_COMPLETED}"
	filename=$(basename "$file")
	mv "${file}" "${DYCAST_INBOX_COMPLETED}/${filename}_completed"
}


### Commands

load_cases() {
	local filePath="$1"
	echo "Loading input file: ${filePath}..."
	python ${DYCAST_APP_PATH}/load_cases.py --srid ${USER_COORDINATE_SYSTEM} "${filePath}"

	exit_code=$?
	if [[ ! "${exit_code}" == "0" ]]; then
		echo "Command 'load_cases' failed, exiting..."
		exit ${exit_code}
	else 
		echo "Done loading cases"
	fi
}


generate_risk() {
	echo ""
	echo "Generating risk..."
	echo ""
	python ${DYCAST_APP_PATH}/daily_risk.py --startdate ${START_DATE} --enddate ${END_DATE} --srid ${USER_COORDINATE_SYSTEM} --extent_min_x ${EXTENT_MIN_X} --extent_min_y ${EXTENT_MIN_Y} --extent_max_x ${EXTENT_MAX_X} --extent_max_y ${EXTENT_MAX_Y}
}


export_risk() {
	echo ""
	echo "Exporting risk for ${START_DATE} to ${END_DATE}..."
	echo ""
	python ${DYCAST_APP_PATH}/export_risk.py --startdate ${START_DATE} --enddate ${END_DATE} --txt true	
}

### Starting point ###


# Use -gt 1 to consume two arguments per pass in the loop (e.g. each
# argument has a corresponding value to go with it).
# Use -gt 0 to consume one or more arguments per pass in the loop (e.g.
# some arguments don't have a corresponding value to go with it, such as --help ).

# If no arguments are supplied, assume the server needs to be run
if [[ $#  -eq 0 ]]; then
	run_dycast
fi

# Else, process arguments
echo "Full command: $@"
while [[ $# -gt 0 ]]
do
	key="$1"
	echo "Command: ${key}"

	case ${key} in
		run_dycast)
			wait_for_db
			check_all_variables
			init_dycast
			run_tests
			listen_for_input
		;;
		setup_dycast)
			wait_for_db
			init_dycast
		;;
		setup_db)
			wait_for_db
			init_db
		;;
		load_cases)
			filePath="$2"
			if [[ -z ${filePath} ]] || [[ "${filePath}" == "" ]]; then
				echo "Specify a file path after the 'load_cases' command."
				display_help
				exit 1
			fi
			check_database_variables
			wait_for_db
			load_cases "${filePath}"
			shift # next argument
		;;
		generate_risk)
			check_all_variables
			wait_for_db
			generate_risk
		;;
		export_risk)
			check_database_variables
			wait_for_db
			export_risk
		;;
		run_tests)
			check_all_variables
			wait_for_db
			run_tests
		;;
		help|-h)
			display_help
		;;
		*)
			exec "$@"
		;;
	esac
	shift # next argument or value
done