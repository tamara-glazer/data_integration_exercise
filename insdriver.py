import rest_inspection
from hdrh import histogram
import random
import sys, traceback
import argparse
import logging
from datetime import datetime
from datetime import timedelta

# logging
logger = logging.getLogger('sfinspect')


def run_inspect(args):
    logger.info("Starting Inspection Driver")
    if args.debug:
        logger.setLevel(logging.DEBUG)
        logger.debug("Debug Mode")
    client = rest_inspection.client()
    try:
        client.open_connection()
        if not args.skipbuild:
            client.build_tables()
        else:
            logger.info("Skipping Build")
        if not args.skipload:
            client.load_inspection(args.limit_load, args.inspect_file)
        else:
            logger.info("Skipping load")
        if not args.skipindex:
            client.build_indexes()
        else:
            logger.info("Skipping indexing")
        if not args.skipclean:
            client.clean_dirty_inspection()
        else:
            logger.info("Skipping cleaning")
        if not args.skipjoin:
            client.join_tables()
        else:
            logger.info("Skipping joining")
        if not args.skipcheck:
            client.check_tables([])
        else:
            logger.info("Skipping check")
    except:
        logger.error("Error: %s" % sys.exc_info()[0])
        traceback.print_exc()
    finally:
        client.close_connection()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run a load and clean of dirty inspection data. Phases in order are  Build, Load, Index, Clean, Join')

    parser.add_argument('--limit_load', help='Limit the number of records to load (use only for testing)', type=int, dest='limit_load', default=None)
    parser.add_argument('--skipbuild', action='store_true', help='Skip the building tables phase')
    parser.add_argument('--skipload', action='store_true', help='Skip the loading phase')
    parser.add_argument('--skipindex', action='store_true', help='Skip the index phase')
    parser.add_argument('--skipclean', action='store_true', help='Skip the clean phase')
    parser.add_argument('--skipjoin', action='store_true', help='Skip the join phase')
    parser.add_argument('--skipcheck', action='store_true', help='Skip the check phase')
    parser.add_argument('--debug', action='store_true', help='Run in debug mode')
    parser.add_argument('--override', action='store_true', help='Override Client db connection parameters (for grading only)')
    parser.add_argument('--inspect_file', help='The tsv file to load from')
    parser.add_argument('--min_year', help='Min Year for Loaded Data', type=int, dest='min_year', default=2018)
    parser.add_argument('--max_year', help='Max Year for Loaded Data', type=int, dest='max_year', default=2018)

    args = parser.parse_args()
    run_inspect(args)
