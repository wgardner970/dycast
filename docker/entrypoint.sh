#!/bin/bash

set -e

export DBUSER=${DBUSER:-postgres}
# psql uses PGPASSWORD environment variable to login
export PGPASSWORD=${DBPASSWORD}
export TEST_DBNAME=${TEST_DBNAME:-test_$DBNAME}

DYCAST_INBOX=${DYCAST_INBOX:-$DYCAST_PATH/inbox}
DYCAST_INBOX_COMPLETED=${DYCAST_INBOX}/completed
DYCAST_OUTBOX=${DYCAST_OUTBOX:-$DYCAST_PATH/outbox}
DYCAST_INIT_PATH=${DYCAST_APP_PATH}/init

PG_SHARE_PATH_2_0=/usr/share/postgresql/9.6/contrib/postgis-2.3

START_DATE=$(date -I -d "${START_DATE}") || exit -1
END_DATE=$(date -I -d "${END_DATE}") || exit -1
SRID_CASES="${SRID_CASES}"

SRID_EXTENT="${SRID_EXTENT}"
EXTENT_MIN_X=${EXTENT_MIN_X}
EXTENT_MIN_Y=${EXTENT_MIN_Y}
EXTENT_MAX_X=${EXTENT_MAX_X}
EXTENT_MAX_Y=${EXTENT_MAX_Y}



HELP_TEXT="

============== Dycast help ==============


Dycast supports any of the following commands:

$(python dycast.py --help)



**** To get more help, use 'docker run cvast/cvast-dycast command --help' ****




	Additional commands provided by Docker container:

	[setup_dycast] 		Initializes Dycast: creates the database if it does not exist and 
				sets up necessary folders  

	[setup_db] 		Deletes any existing Dycast database and sets up a fresh one  

	[run_tests] 		Run unit tests  

	[help] or [-h]		Display help text  
"

display_help() {
	echo "${HELP_TEXT}"
}



check_init_dycast() {
	if ! $(db_exists); then
		echo "Dycast database is not initialized yet. Please run the 'setup-dycast' command"
	fi
}


init_test_db() {
	if ! $(test_db_exists); then
		echo "Dycast test database is not initialized yet. Initializing test database..."
		init_db --test --monte-carlo-file Dengue_max_100_40000.csv
	else
		echo "Dycast test database exists: ${TEST_DBNAME}"
	fi
}


db_exists() {
	$(psql -lqt -h ${DBHOST} -U ${DBUSER} | cut -d \| -f 1 | grep -qw ${DBNAME})
}


test_db_exists() {
	$(psql -lqt -h ${DBHOST} -U ${DBUSER} | cut -d \| -f 1 | grep -qw ${TEST_DBNAME})
}


wait_for_db() {
	echo "Testing database connection: ${DBHOST}:${DBPORT}..."
	set +e
	tries=0
	connected="False"
	while [[ ${connected} == "False" ]]
	do
		psql -h ${DBHOST} -p ${DBPORT} -U postgres -c "select 1" >&/dev/null
		return_code=$?
		((tries++))
		if [[ ${return_code} == 0 ]]; then
	        connected="True"
		elif [[ ${tries} == 45 ]]; then
		    echo "Database server cannot be reached, exiting..."
			exit 1
		fi
		sleep 1
	done
	set -e
	echo "Connected."
}


init_directories() {
	if [[ ! -d ${DYCAST_INBOX_COMPLETED} ]]; then
		mkdir -p ${DYCAST_INBOX_COMPLETED}
	fi

	if [[ ! -d ${DYCAST_OUTBOX}/tmp ]]; then
		mkdir -p ${DYCAST_OUTBOX}/{tmp,cur,new}
	fi
}


init_test_variables() {
	export DYCAST_INBOX=${TEST_DYCAST_INBOX:-$DYCAST_PATH/tests/inbox}
	export DYCAST_INBOX_COMPLETED=${DYCAST_INBOX}/completed
	export DYCAST_OUTBOX=${TEST_DYCAST_INBOX:-$DYCAST_PATH/tests/outbox}
}


prepare_launch() {
	if [[ ! ${help} == true ]]; then
		wait_for_db
		check_init_dycast
		init_directories
	fi	
}

prepare_launch_test() {
	if [[ ! ${help} == true ]]; then
		wait_for_db
		init_test_db
		init_test_variables
		init_directories
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


check_database_variables() {
	check_variable "${DBPASSWORD}" DBPASSWORD
	check_variable "${DBNAME}" DBNAME
	check_variable "${DBHOST}" DBHOST
	check_variable "${DBPORT}" DBPORT
}


move_case_file() {
	local file="$1"
	echo "Completed loading input file, moving it to ${DYCAST_INBOX_COMPLETED}"
	filename=$(basename "$file")
	mv "${file}" "${DYCAST_INBOX_COMPLETED}/${filename}_completed"
}



### Commands

run_dycast() {
	local arguments="$@"
	echo "Running Dycast using arguments: ${arguments}..."
	python ${DYCAST_APP_PATH}/dycast.py run_dycast ${arguments}

	exit_code=$?
	if [[ ! "${exit_code}" == "0" ]]; then
		echo "Command 'run_dycast' failed, exiting..."
		exit ${exit_code}
	else 
		echo "Finished running the full Dycast procedure"
	fi
}


load_cases() {
	local arguments="$@"
	echo "Loading cases using arguments: ${arguments}..."
	python ${DYCAST_APP_PATH}/dycast.py load_cases ${arguments}

	exit_code=$?
	if [[ ! "${exit_code}" == "0" ]]; then
		echo "Command 'load_cases' failed, exiting..."
		exit ${exit_code}
	else 
		echo "Done loading cases"
	fi
}


generate_risk() {
	local arguments="$@"
	echo ""
	echo "Generating risk using arguments: ${arguments}..."
	echo ""
	python ${DYCAST_APP_PATH}/dycast.py generate_risk ${arguments}

	exit_code=$?
	if [[ ! "${exit_code}" == "0" ]]; then
		echo "Command 'generate_risk' failed, exiting..."
		exit ${exit_code}
	else 
		echo "Done generating risk"
	fi
}


export_risk() {
	local arguments="$@"
	echo ""
	echo "Exporting risk using arguments: ${arguments}..."
	echo ""
	python ${DYCAST_APP_PATH}/dycast.py export_risk ${arguments}	

	exit_code=$?
	if [[ ! "${exit_code}" == "0" ]]; then
		echo "Command 'export_risk' failed, exiting..."
		exit ${exit_code}
	else 
		echo "Done exporting risk"
	fi
}


init_db() {
	local arguments="$@"
	echo "Initializing database using arguments: ${arguments}..."
	python ${DYCAST_APP_PATH}/dycast.py init_db ${arguments}

	exit_code=$?
	if [[ ! "${exit_code}" == "0" ]]; then
		echo "Command 'init_db' failed, exiting..."
		exit ${exit_code}
	else 
		echo "Done initializing database"
	fi
}


run_tests() {
	local arguments="$@"
	
	echo "Running unit tests..."
	if [[ -z ${arguments} ]]; then
		nosetests -vv --exe tests
	else
		nosetests -v --exe ${arguments}
	fi

	exit_code=$?
	if [[ ! "${exit_code}" == "0" ]]; then
		echo "Unit test(s) failed, exiting..."
		exit ${exit_code}
	fi
}

### Starting point ###


# Use -gt 1 to consume two arguments per pass in the loop (e.g. each
# argument has a corresponding value to go with it).
# Use -gt 0 to consume one or more arguments per pass in the loop (e.g.
# some arguments don't have a corresponding value to go with it, such as --help ).

# If no arguments are supplied, assume the server needs to be run
if [[ $#  -eq 0 ]]; then
	display_help
fi

# Else, process arguments
full_command="$@"
arguments="${@:2}"
command="$1"
echo "Command: ${command}"

if [[ ! "${command}" == "help" ]] && 
	[[ ! "${command}" == "-h" ]] && 
	[[ ! "${command}" == "--help" ]] && 
	[[ ! "${arguments}" == "-h" ]] && 
	[[ ! "${arguments}" == "--help" ]]; then
		check_database_variables
else 
	help=true
fi

if [[ ! -z ${arguments} ]]; then
	echo "Arguments: ${arguments}"
fi

case ${command} in
	### First list all commands built into Dycast, not into this Docker entrypoint
	run_dycast)
		prepare_launch
		run_dycast ${arguments}
	;;
	load_cases)
		prepare_launch
		load_cases ${arguments}
	;;
	generate_risk)
		prepare_launch
		generate_risk ${arguments}
	;;
	export_risk)
		prepare_launch
		export_risk ${arguments}
	;;
	init_db)
		prepare_launch
		init_db ${arguments}
	;;
	### From here list only commands that are specific for this Docker entrypoint
	run_tests)
		prepare_launch_test
		run_tests ${arguments}
	;;	
	help|-h|--help)
		display_help
	;;
	*)
		exec "$@"
	;;
esac
