import boto3
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path, PosixPath


logger = logging.getLogger(__name__)


def ensure_trailing_slash(s):
    return s if s.endswith('/') else f'{s}/'


class S3Loader:

    def __init__(self, bucket, source_prefix, dest_prefix, aws_id=None, aws_key=None, profile_name=None,
                 credfile=None):
        self.bucket = bucket
        self.source_prefix = ensure_trailing_slash(source_prefix)
        self.dest_prefix = dest_prefix
        self.max_concurrent = 5
        self._creds = {}
        if aws_id or aws_key:
            if not aws_id or not aws_key:
                raise Exception('if aws_id or aws_key specified, both must be!')
            self._creds = {
                'aws_access_key_id': aws_id,
                'aws_secret_access_key': aws_key,
            }
        elif profile_name:
            self._creds = {
                'profile_name': profile_name
            }
        elif credfile:
            logger.debug(f'reading credentials from {credfile}')
            with open(credfile) as f:
                self._creds = json.load(f)

    def _download_object(self, s3object, dest_file):
        logger.info(f'Downloading object {s3object.key} to {dest_file}')
        s3object.download_file(str(dest_file))

    def load_s3(self, overwrite=False):
        sess = boto3.Session(**self._creds)
        s3 = sess.resource('s3')
        bucket = s3.Bucket(self.bucket)
        obj_count, dl_count, skip_count, timing = 0, 0, 0, time.time()
        updated = set()
        logger.info(f'listing and fetching objects from s3://{self.bucket}/{self.source_prefix} to {self.dest_prefix}')
        with ThreadPoolExecutor(max_workers=self.max_concurrent) as executor:
            dlfutures = []
            try:
                for s3_object in bucket.objects.filter(Prefix=self.source_prefix).all():
                    s3_file = PosixPath(s3_object.key)
                    s3_stem = s3_file.relative_to(self.source_prefix)
                    dest_file = Path(self.dest_prefix, s3_stem)
                    if dest_file.exists() and not overwrite:
                        skip_count += 1
                        continue
                    if len(s3_stem.parts) > 2:
                        updated.add(s3_stem.parts[:2])
                    dest_file.parent.mkdir(parents=True, exist_ok=True)
                    dlfutures.append(executor.submit(self._download_object, s3_object.Object(), dest_file))
                    obj_count += 1
                for dlfuture in as_completed(dlfutures):
                    dlfuture.result()
                    dl_count += 1
            except KeyboardInterrupt:
                logger.warning('canceling downloads!')
            except Exception as e:
                logger.error('caught exception in downloading files')
                raise e
            finally:
                executor.shutdown(wait=False)
                for future in dlfutures:
                    future.cancel()

        timing = time.time() - timing
        logger.info(f'total objects: {obj_count} downloaded: {dl_count}, files skipped: {skip_count} in {timing:0.2f}s')
        return updated
