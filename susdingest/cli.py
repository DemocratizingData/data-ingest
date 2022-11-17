#!/usr/bin/env python3

import logging
import logging.config
import sys
import os
import yaml
from susdingest import S3Loader, JSONLoader, DatamodelLoader, SUSDDatabase
from argparse import ArgumentParser
from urllib.parse import urlparse
from pathlib import Path
from sqlalchemy.engine.url import make_url as db_url


def connstring_with_db(conn, db):
    url = db_url(conn)
    return url.set(database=db)


def parse_s3(bucket):
    parsed = urlparse(bucket)
    if not parsed.netloc:
        raise ValueError(f'could not get bucket from s3 url: {bucket}, expecting s3://{{bucket}}/{{prefix}}')
    return parsed.netloc, parsed.path[1:]


def parse_args(argv):
    ap = ArgumentParser()
    ap.add_argument('--bucket', default=os.getenv('SUSD_BUCKET'))
    ap.add_argument('--connection-string', default=os.getenv('SUSD_CONNECTION_STRING'))
    ap.add_argument('--staging-db', default=os.getenv('SUSD_STAGING_DB'))
    ap.add_argument('--final-db', default=os.getenv('SUSD_FINAL_DB'))
    ap.add_argument('--mirror', default=os.getenv('SUSD_MIRROR_DIR'))
    ap.add_argument('--log-config', default=os.getenv('SUSD_LOG_CONFIG'))
    return ap.parse_args(argv)


def require_args(args, keys):
    for key in keys:
        if key not in args or not getattr(args, key):
            raise ValueError(f'Missing required argument {key}')


def ingest_latest(args):
    require_args(args, ['bucket', 'mirror', 'connection_string', 'staging_db', 'final_db'])
    bucket, prefix = parse_s3(args.bucket)
    try:
        updated = S3Loader(bucket, prefix, args.mirror).load_s3()
    except Exception as e:
        logging.getLogger('notify').error(f'failed to load data from s3 {bucket} to {args.mirror}: {e}')
        raise e
    logging.info(f'loaded from S3, found updated datasets: {updated}')
    force_reload = [i.relative_to(args.mirror).parent.parts for i in Path(args.mirror).glob('*/*/.force_reload')]
    logging.info(f'datasets requested to force_load: {force_reload}')
    staging_db = connstring_with_db(args.connection_string, args.staging_db)
    final_db = connstring_with_db(args.connection_string, args.final_db)
    for agency, version in set(updated).union(set(force_reload)):
        try:
            if (agency, version) in force_reload:
                force = True
                Path(args.mirror).joinpath(agency, version, '.force_reload').unlink()
            else:
                force = False
            JSONLoader.from_path(args.mirror, agency, version) \
                      .with_validation().with_db(staging_db).load_db(force=force)
        except Exception as e:
            logging.getLogger('notify').error(f'failed to load data from JSON to staging for {agency}, {version}: {e}')
            continue
        try:
            DatamodelLoader(SUSDDatabase.from_url(final_db), args.staging_db).copy_from_staging(agency, version)
            logging.getLogger('notify').info(f'completed load of {agency}, {version} to final table!')
        except Exception as e:
            logging.getLogger('notify').error(
                f'failed to load from staging to final table for {agency}, {version}: {e}')
            continue


def main_with_args(argv):
    args = parse_args(argv)
    if args.log_config:
        with open(args.log_config, 'r') as f:
            conf = yaml.load(f, yaml.SafeLoader)
            conf['disable_existing_loggers'] = False
            logging.config.dictConfig(conf)
    ingest_latest(args)


def main():
    logging.basicConfig(format='%(asctime)-15s %(levelname)s %(message)s', level=logging.INFO)
    main_with_args(sys.argv[1:])


if __name__ == '__main__':
    main()
