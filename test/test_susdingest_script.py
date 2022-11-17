import logging
import susdingest.cli
from unittest import TestCase
from susdingest.cli import parse_s3, connstring_with_db, ingest_latest, parse_args, main_with_args
from unittest.mock import MagicMock
from pathlib import Path


class TestIngestScript(TestCase):
    def setUp(self):
        self.mirror_dir = Path(__file__).with_name('example_data')
        self.connstring = 'proto://user:password@host:1111/database?charset=utf8'
        self.test_argv = (
            f'--bucket s3://bucket/prefix --mirror {self.mirror_dir} --connection-string {self.connstring} '
            '--staging-db sdb --final-db fdb').split()

    def setup_mock_actions(self):
        susdingest.cli.S3Loader = MagicMock()
        susdingest.cli.JSONLoader = MagicMock()
        susdingest.cli.DatamodelLoader = MagicMock()
        susdingest.cli.SUSDDatabase = MagicMock()

    def test_connstring_add_db(self):
        cs = 'proto://user:password@host:1111/database?charset=utf8'
        csdb = connstring_with_db(cs, 'override_db')
        assert(str(csdb) == 'proto://user:password@host:1111/override_db?charset=utf8')

    def test_parse_s3(self):
        s3 = 's3://bucket/path/prefix'
        assert(parse_s3(s3) == ('bucket', 'path/prefix'))

    def test_parse_s3_requries_url(self):
        with self.assertRaises(ValueError):
            parse_s3('bucket/prefix')

    def test_ingest(self):
        self.setup_mock_actions()
        susdingest.cli.S3Loader.return_value.load_s3.return_value = [('agency', 'version')]
        main_with_args(self.test_argv)
        assert(len(susdingest.cli.JSONLoader.mock_calls) > 0)
        susdingest.cli.JSONLoader.from_path.assert_called_once_with(str(self.mirror_dir), 'agency', 'version')
        assert(len(susdingest.cli.DatamodelLoader.mock_calls) > 0)

    def test_ingest_force_reload_flag(self):
        self.setup_mock_actions()
        susdingest.cli.S3Loader.return_value.load_s3.return_value = []
        flag_file = self.mirror_dir.joinpath('agency', 'version', '.force_reload')
        flag_file.touch()
        main_with_args(self.test_argv)
        assert(len(susdingest.cli.JSONLoader.mock_calls) > 0)
        susdingest.cli.JSONLoader.from_path.assert_called_once_with(str(self.mirror_dir), 'agency', 'version')
        assert(len(susdingest.cli.DatamodelLoader.mock_calls) > 0)
        assert(not flag_file.exists())

    def test_ingest_no_data_skips_loading(self):
        self.setup_mock_actions()
        susdingest.cli.S3Loader.return_value.load_s3.return_value = []
        main_with_args(self.test_argv)
        assert(len(susdingest.cli.JSONLoader.mock_calls) == 0)
        assert(len(susdingest.cli.DatamodelLoader.mock_calls) == 0)

    def test_ingest_s3_data_plus_force_reload(self):
        self.setup_mock_actions()
        susdingest.cli.S3Loader.return_value.load_s3.return_value = [('agency_2', 'version')]
        flag_file = self.mirror_dir.joinpath('agency', 'version', '.force_reload')
        flag_file.touch()
        main_with_args(self.test_argv)
        assert(len(susdingest.cli.JSONLoader.mock_calls) > 0)
        susdingest.cli.JSONLoader.from_path.assert_any_call(str(self.mirror_dir), 'agency_2', 'version')
        susdingest.cli.JSONLoader.from_path.assert_any_call(str(self.mirror_dir), 'agency', 'version')
        assert(len(susdingest.cli.DatamodelLoader.mock_calls) > 0)
        assert(not flag_file.exists())

    def test_ingest_missing_required_args_fails(self):
        with self.assertRaises(ValueError):
            ingest_latest(parse_args(''))

    def test_use_logging_config_loads_from_file(self):
        ex = Path(__file__).parent.joinpath('example_data', 'log-config-ex.yaml')
        argv = f'--log-config {ex}'.split()
        logging.getLogger().setLevel('WARNING')
        assert(logging.getLogger().level == logging.WARNING)
        assert(logging.getLogger('notify').level == logging.NOTSET)
        # we expect it to fail, only need to inspect loggigng setup
        try:
            main_with_args(argv)
        except:
            pass
        assert(logging.getLogger().level == logging.ERROR)
        assert(logging.getLogger('notify').level == logging.ERROR)
        # make sure the logging for modules is not disabled, by testing one
        assert(not logging.getLogger('susdingest.s3loader').disabled)
