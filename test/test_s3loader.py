import json
from unittest import TestCase
from unittest.mock import MagicMock, mock_open, patch
from susdingest import s3loader


fakecreds = {'aws_access_key_id': 'id', 'aws_secret_access_key': 'key'}
fakecredsfile = json.dumps(fakecreds)


class TestS3Loader(TestCase):

    def setUp(self):
        self.pathmock = MagicMock()
        self.pathmock.return_value.exists.return_value = False
        self.s3objmock = MagicMock()
        self.s3objmock.key = '/agency/version/to/file'
        self.s3objlist = [self.s3objmock for i in range(20)]
        self.botomock = MagicMock()
        self.botomock.Session() \
                     .resource() \
                     .Bucket() \
                     .objects \
                     .filter() \
                     .all \
                     .return_value = self.s3objlist
        s3loader.boto3 = self.botomock
        s3loader.Path = self.pathmock

    def test_reads_default_credentials_file_when_given(self):
        with patch('susdingest.s3loader.open', mock_open(read_data=fakecredsfile)):
            ldr = s3loader.S3Loader('', '', '', credfile='fakecreds')
            assert(ldr._creds == fakecreds)

    def test_profile_overrides_credentials_file(self):
        ldr = s3loader.S3Loader('', '', '', profile_name='test_profile')
        ldr.load_s3()
        self.botomock.Session.assert_called_with(profile_name='test_profile')

    def test_id_and_key_overrides_profile_and_file(self):
        ldr = s3loader.S3Loader('', '', '', aws_id='test_id', aws_key='test_key', profile_name='unused')
        ldr.load_s3()
        self.botomock.Session.assert_called_with(aws_access_key_id='test_id', aws_secret_access_key='test_key')

    def test_download_when_no_files_exist(self):
        ldr = s3loader.S3Loader('', '', '', profile_name='profile')
        updated = ldr.load_s3()
        assert(self.s3objmock.Object().download_file.call_count == len(self.s3objlist))
        assert(updated == {('agency', 'version')})

    def test_default_skip_download_when_files_exist(self):
        self.pathmock.return_value.exists.return_value = True
        ldr = s3loader.S3Loader('', '', '', profile_name='profile')
        updated = ldr.load_s3()
        assert(self.s3objmock.Object().download_file.call_count == 0)
        assert(updated == set())

    def test_download_when_files_exist_with_overwrite(self):
        self.pathmock.return_value.exists.return_value = True
        ldr = s3loader.S3Loader('', '', '', profile_name='profile')
        updated = ldr.load_s3(overwrite=True)
        assert(self.s3objmock.Object().download_file.call_count == len(self.s3objlist))
        assert(updated == {('agency', 'version')})

    def test_runtime_exception_during_download_fails_and_cancels(self):
        self.s3objmock.Object().download_file.side_effect = Exception
        ldr = s3loader.S3Loader('', '', '', profile_name='profile')
        ldr.max_concurrent = 1
        with self.assertRaises(Exception):
            ldr.load_s3()
        print(self.s3objmock.Object().download_file.call_count)
        # this is not the greatest test as nondeterministic...
        assert(self.s3objmock.Object().download_file.call_count < len(self.s3objlist))
