import configargparse
import dycast
from services import logging_service
from services import config_service

logging_service.init_logging()


def create_parser():
    # Common arguments for all subcommands
    common_parser = configargparse.ArgParser(add_help=False)

    common_parser.add('--config', '-c',
                      default="./dycast.config",
                      help="Override default config file")

    # Main parser that keeps all subcommand separate, while still using common arguments from the common_parser
    main_parser = configargparse.ArgParser(add_help=True)
    subparsers = main_parser.add_subparsers(
        title='Commands', help='Choose from the following commands:')

    # Load cases
    load_cases_parser = subparsers.add_parser('load_cases',
                                              parents=[common_parser],
                                              help='Loads cases into Dycast from a specified file',
                                              argument_default=configargparse.SUPPRESS)
    load_cases_parser.set_defaults(func=dycast.import_cases)

    # Generate risk
    generate_risk_parser = subparsers.add_parser('generate_risk',
                                                 parents=[common_parser],
                                                 help='Generates risk from the cases currently in the database',
                                                 argument_default=configargparse.SUPPRESS)
    generate_risk_parser.set_defaults(func=dycast.generate_risk)

    # Export cases
    export_risk_parser = subparsers.add_parser('export_risk',
                                               parents=[common_parser],
                                               help='Exports risk currently in the database to --export_directory',
                                               argument_default=configargparse.SUPPRESS)
    export_risk_parser.set_defaults(func=dycast.export_risk)

    # Run Dycast (run all steps)
    run_dycast_parser = subparsers.add_parser('run_dycast',
                                              parents=[common_parser],
                                              help='Loads any .tsv files in (--files | -f), generates risk and exports it to --export_directory',
                                              argument_default=configargparse.SUPPRESS)
    run_dycast_parser.set_defaults(func=dycast.run_dycast)

    # Common arguments: load_cases & run_dycast
    for subparser in [load_cases_parser, run_dycast_parser]:
        subparser.add('--files', '-f',
                      nargs='+',
                      env_var='FILES',
                      help='File path or paths (space separated) to load cases from. Supported format: .TSV')
        subparser.add('--import-directory',
                      env_var='IMPORT_DIRECTORY',
                      help='Override default set in dycast.config. Path to load cases from. If no (--files | -f) is specified, Dycast will look in this directory for .tsv files to load into the database')
        subparser.add('--srid-cases',
                      env_var='SRID_CASES',
                      help='The SRID (projection) of the cases you are loading. Only required if your cases are in lat/long, not when they are in PostGIS geometry format')

    # Common arguments: generate_risk & run_dycast
    for subparser in [generate_risk_parser, run_dycast_parser]:
        subparser.add('--startdate', '-s',
                      env_var='START_DATE',
                      help='The start date from which to generate risk')
        subparser.add('--enddate', '-e',
                      env_var='END_DATE',
                      help='The end date to which to generate risk')
        subparser.add('--extent-min-x',
                      env_var='EXTENT_MIN_X',
                      help='The minimum point on the X axis (north-west)')
        subparser.add('--extent-min-y',
                      env_var='EXTENT_MIN_Y',
                      help='The minimum point on the Y axis (north-west)')
        subparser.add('--extent-max-x',
                      env_var='EXTENT_MAX_X',
                      help='The maximum point on the X axis (south-east)')
        subparser.add('--extent-max-y',
                      env_var='EXTENT_MAX_Y',
                      help='The maximum point on the Y axis (south-east)')
        subparser.add('--srid-extent',
                      env_var='SRID_EXTENT',
                      help='The SRID (projection) of the specified extent.')
        subparser.add('--spatial-domain', env_var='SPATIAL_DOMAIN',
                      help='Spatial domain used in Dycast risk generation and statistical analysis')
        subparser.add('--temporal-domain', env_var='TEMPORAL_DOMAIN',
                      help='Temporal domain used in Dycast risk generation and statistical analysis')
        subparser.add('--close-in-space', env_var='CLOSE_SPACE',
                      help='The amount of meters between two cases that is considered "close in space" in Dycast risk generation and statistical analysis')
        subparser.add('--close-in-time', env_var='CLOSE_TIME',
                      help='The amount of days between two cases that is considered "close in time" in Dycast risk generation and statistical analysis')
        subparser.add('--case-threshold', env_var='CASE_THRESHOLD',
                      help='Spatial domain used in Dycast risk generation and statistical analysis')

    # Common arguments: export_risk & run_dycast
    for subparser in [export_risk_parser, run_dycast_parser]:
        subparser.add('--export-directory', env_var='EXPORT_DIRECTORY',
                      help='Optional. Default is defined in dycast.config')
        subparser.add('--export-format',
                      env_var='EXPORT_FORMAT', default='txt')
        subparser.add('--export-prefix',
                      env_var='EXPORT_PREFIX', help='Set a prefix for the output file so that it is easy to recognize')

    return main_parser


def main():
    parser = create_parser()
    args = parser.parse_args()

    dictionary_args = vars(args)

    if args.func:
        args.func(**dictionary_args)


if __name__ == '__main__':
    main()
