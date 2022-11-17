import pandas as pd
import sqlalchemy
from pathlib import Path
from unittest import TestCase
from unittest.mock import MagicMock
from sqlalchemy.engine import Connectable
from susdingest.jsonloader import JSONLoader


sqlalchemy.inspect = MagicMock()


class TestJsonLoader(TestCase):

    def setUp(self):
        examples = Path(__file__).with_name('example_data').joinpath('agency', 'version')
        self.json_files = examples.joinpath('json', 'publications').glob('publication*.json.gz')
        self.meta_file = examples.joinpath('stat', 'export_metadata.json')
        sqlalchemy.inspect = MagicMock()

    def _setupDb(self, loader):
        loader.sqlengine = MagicMock()
        self.sqlcon = MagicMock()
        self.sqlcon.__class__ = Connectable
        loader.sqlengine.connect.return_value.__enter__.return_value = self.sqlcon

    def test_instantiate_from_files_without_exception(self):
        JSONLoader(self.json_files)

    def test_all_table_transformations_produce_dataframes(self):
        jl = JSONLoader(self.json_files)
        for kind in JSONLoader.export_kinds:
            assert(type(getattr(jl, kind)) == pd.DataFrame)

    def test_metadata_validation(self):
        jl = JSONLoader(self.json_files, metadata_file=self.meta_file)
        assert(len(jl.publications) > 0)
        jl.validate()

    def test_metadata_validation_excepts_when_has_missing(self):
        jl = JSONLoader(self.json_files, metadata_file=self.meta_file)
        # inject a bogus record in metadata
        jl.metadata['stats']['overall']['unique_documents_per_year'].append({'publication_year': 3000, 'documents': 1})
        assert(len(jl.publications) > 0)
        with self.assertRaises(Exception):
            jl.validate()

    def test_metadata_validation_excepts_when_has_more(self):
        jl = JSONLoader(self.json_files, metadata_file=self.meta_file)
        # inject a bogus record in publications
        jl.data = pd.concat([jl.data, pd.DataFrame({'eid': ['x'], 'publication_year': [3000]}).set_index('eid')])
        print(jl.publications[['publication_year']])
        assert(len(jl.publications) > 0)
        with self.assertRaises(Exception):
            jl.validate()

    def test_metadata_validation_ignores_zero_documents(self):
        jl = JSONLoader(self.json_files, metadata_file=self.meta_file)
        # inject a bogus record in metadata
        jl.metadata['stats']['overall']['unique_documents_per_year'].append({'publication_year': 3000, 'documents': 0})
        assert(len(jl.publications) > 0)
        jl.validate()

    def test_load_to_db_general(self):
        jl = JSONLoader(self.json_files, agency='agency', run_version='version')
        self._setupDb(jl)
        sqlalchemy.inspect.return_value.has_table.return_value = False
        jl.load_db()
        assert(len(self.sqlcon.mock_calls) > 0)

    def test_load_to_db_no_overwrite(self):
        jl = JSONLoader(self.json_files, agency='agency', run_version='version')
        self._setupDb(jl)
        sqlalchemy.inspect.return_value.has_table.return_value = True
        with self.assertRaises(ValueError):
            jl.load_db()

    def test_load_to_db_overwrites_with_force_option(self):
        jl = JSONLoader(self.json_files, agency='agency', run_version='version', force=True)
        self._setupDb(jl)
        sqlalchemy.inspect.return_value.has_table.return_value = True
        # the logic here is a bit strange, but the assumption is that the ValueError from the no overwrite test is due
        # to table existing, whereas we should have moved passed it in this test (but the mocking is getting too deep to
        # ensure we have the tables it is trying to drop)
        with self.assertRaises(Exception) as exp:
            jl.load_db()
        assert(not isinstance(exp, ValueError))

    def test_load_db_fails_fast_without_conn_string(self):
        jl = JSONLoader(self.json_files, agency='agency', run_version='version')
        assert(not jl.sqlengine)
        with self.assertRaises(ValueError):
            jl.load_db()

    def test_with_db_method_sets_connstring_and_returns_self(self):
        jl = JSONLoader(self.json_files, agency='agency', run_version='version')
        assert(jl.with_db('connstring').conn_str == 'connstring')

    def test_with_validation_method_validates_and_returns_self(self):
        jl = JSONLoader(self.json_files, metadata_file=self.meta_file)
        assert(jl.with_validation() == jl)

    def test_loader_from_agency_version_path_reads_dir_structure(self):
        p = Path(__file__).with_name('example_data').joinpath('agency', 'version')
        jl = JSONLoader.from_path(p)
        assert(jl.agency == 'agency')
        assert(jl.run_version == 'version')
        assert(jl.metadata is not None)
        jl.validate()

    def test_loader_from_base_path_with_agency_version_arguments(self):
        p = Path(__file__).with_name('example_data')
        jl = JSONLoader.from_path(p, 'agency', 'version')
        assert(jl.agency == 'agency')
        assert(jl.run_version == 'version')
        assert(jl.metadata is not None)
        jl.validate()

    def test_datasets_table_from_metadata(self):
        p = Path(__file__).with_name('example_data')
        jl = JSONLoader.from_path(p, 'agency', 'version')
        assert(len(jl.datasets) > 0)
        assert(type(jl.datasets) == pd.DataFrame)
