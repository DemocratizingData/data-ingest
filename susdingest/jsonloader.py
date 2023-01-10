import pandas as pd
import sqlalchemy
import json
import logging
from argparse import ArgumentParser
from pathlib import Path

logger = logging.getLogger(__name__)


class JSONLoader:
    export_kinds = ['publications', 'dyads', 'authors', 'affiliations', 'topics', 'topicclusters', 'asjcs', 'ufcs',
                    'datasets']
    sqlengine = None

    @classmethod
    def from_path(cls, path, agency=None, run_version=None):
        if not isinstance(path, Path):
            path = Path(path)
        if agency or run_version:
            if not agency and run_version:
                raise ValueError('either both or none of agency version must be sepcified')
            path = path.joinpath(agency, run_version)
        else:
            agency = path.resolve().parent.name
            run_version = path.resolve().name
        json_files = list(path.glob('json/publications/*.json.gz'))
        if not json_files:
            raise RuntimeError('no publication data found under path {path}')
        metadata_file = path.joinpath('stat/export_metadata.json')
        if not metadata_file.exists():
            raise RuntimeError('no export metadata found under path {path}')
        return cls(json_files, metadata_file=metadata_file, agency=agency, run_version=run_version)

    def __init__(self, json_files, metadata_file=None, agency=None, run_version=None, conn_str=None, force=None):
        self.agency = agency
        self.run_version = run_version
        self.conn_str = conn_str
        self.force = force
        self.json_files = json_files
        self.metadata = None
        if metadata_file:
            with open(metadata_file, 'r') as f:
                self.metadata = json.load(f)
        self.load_json()

    def load_json(self):
        self.data = pd.concat([pd.read_json(i, lines=True).assign(src_file=str(i)) for i in self.json_files]) \
                      .set_index('eid')
        logger.debug(f'loaded {len(self.data)} records from json files')

    def normalize_col(self, column, explode=True):
        if explode:
            col = self.data.explode(column)[column].dropna()
        else:
            col = self.data[column].dropna()
        return pd.json_normalize(col).set_index(col.index).convert_dtypes()

    def with_nullable_as_required(self, df, nullable, required):
        for col in nullable:
            if col not in df:  # this means no values specified in the entire dataset
                logger.warning(f'optional column {col} missing from data, adding as nulls')
                df[col] = pd.NA
        return df[nullable + required]

    @property
    def publications(self):
        required_cols = ['doi', 'publication_title']
        optional_cols = ['publication_type', 'publication_year', 'publication_month', 'citation_count',
                         'field_weighted_citation_impact', 'journal_publishername', 'joiurnal_title',
                         'journal_scopus_source_id', 'journal_issn_isbn']
        publications = self.with_nullable_as_required(self.data.copy(), optional_cols + ['expressions'], required_cols)
        publications['journal_issn_isbn'] = publications['journal_issn_isbn'] \
            .apply(lambda x: '|'.join(str(i) for i in x) if x == x else x)
        publications['tested_expressions'] = publications['expressions'] \
            .apply(lambda x: json.dumps(x) if x == x else x)
        publications = publications.drop(columns=['expressions'])
        if 'journal_citescore' in self.data:
            citescore = self.normalize_col('journal_citescore', explode=False) \
                            .rename(columns=lambda x: f'journal_{x}')
            publications = publications.join(citescore)
        return self.with_nullable_as_required(
            publications, required_cols, optional_cols + ['journal_citescore_year', 'journal_citescore_value']) \
            .convert_dtypes()

    @property
    def dyads(self):
        required_cols = ['snippet', 'model', 'alias']
        optional_cols = ['fuzzy_score', 'is_fuzzy', 'fuzzy_score', 'score']
        columns_map = {
            'snippets': 'snippet',
            'linked_alias.fuzzy_score': 'fuzzy_score',
            'linked_alias.is_fuzzy': 'is_fuzzy',
            'linked_alias.alias_id': 'alias_id',
            'identified_dataset_name': 'alias',
        }
        dyads = self.normalize_col('identified_datasets') \
                    .explode('snippets') \
                    .explode('models') \
                    .assign(model=lambda x: x.models.apply(lambda y: y.get('model')),
                            score=lambda x: x.models.apply(lambda y: y.get('score'))) \
                    .rename(columns=columns_map)
        dyads['snippet'] = dyads['snippet'].replace(['', 'Not Available'], pd.NA)
        dyads = self.with_nullable_as_required(dyads, optional_cols, required_cols)
        return dyads.drop_duplicates().convert_dtypes()

    @property
    def authors(self):
        optional_cols = ['given_name', 'family_name', 'pn_given_name', 'pn_family_name', 'author_id', 'author_position',
                         'affiliation_sequences']
        authors = self.with_nullable_as_required(self.normalize_col('authors'), optional_cols, [])
        authors['affiliation_sequences'] = authors['affiliation_sequences'].apply(
            lambda x: json.dumps(x) if x == x else x)
        return authors.dropna(how='all')

    @property
    def affiliations(self):
        required_cols = ['affiliation_normalized']
        optional_cols = ['affiliation_organization', 'affiliation_address_part', 'affiliation_postal_code',
                         'affiliation_city', 'affiliation_city_group', 'affiliation_state', 'country_code',
                         'affiliation_ids']
        affils = self.normalize_col('affiliations')
        rename = {i: i.replace('affiliation_text.', '') for i in affils.columns if i.startswith('affiliation_text.')}
        affils = affils.rename(columns=rename)
        affils = self.with_nullable_as_required(affils, optional_cols, required_cols)
        affils['affiliation_organization'] = affils['affiliation_organization'].apply(
            lambda x: ', '.join(x) if x == x else x)
        affils['affiliation_id'] = affils['affiliation_ids'].apply(
            lambda x: x[0] if x == x else x).astype('Int64')
        affils['affiliation_ids'] = affils['affiliation_ids'].apply(
            lambda x: '|'.join(str(i) for i in x) if x == x else x)
        affils['affiliation_normalized'] = affils['affiliation_normalized'].apply(
            lambda x: json.dumps(x) if x == x else x)
        return affils

    @property
    def asjcs(self):
        required_cols = ['asjc', 'label']
        return self.with_nullable_as_required(self.normalize_col('asjcs'), [], required_cols)

    @property
    def topics(self):
        optional_cols = ['topic_id', 'keywords', 'prominence']
        topics = self.normalize_col('topic', explode=False)
        topics['keywords'] = topics['keywords'].apply(lambda x: '|'.join(x))
        return self.with_nullable_as_required(topics, optional_cols, [])

    @property
    def topicclusters(self):
        optional_cols = ['topic_cluster_id', 'keywords', 'prominence']
        topicclusters = self.normalize_col('topic_cluster', explode=False)
        topicclusters['keywords'] = topicclusters['keywords'].apply(lambda x: '|'.join(x))
        return self.with_nullable_as_required(topicclusters, optional_cols, [])

    @property
    def ufcs(self):
        optional_cols = ['concept_id', 'concept_name', 'rank', 'a_freq']
        return self.with_nullable_as_required(self.normalize_col('unified_fingerprint_concepts'), optional_cols, [])

    @property
    def datasets(self):
        if not self.metadata:
            return pd.DataFrame([])
        return pd.DataFrame(
            self.metadata['stats']['documents_per_alias']
        )[['alias', 'alias_id', 'parent_alias_id', 'alias_type']].convert_dtypes()

    def dump_csv(self):
        for kind in self.export_kinds:
            getattr(self, kind).to_csv(f'{kind}.csv')

    def start_sql(self):
        if not self.sqlengine:
            if not self.conn_str:
                raise ValueError('need database connection string')
            logger.debug('creating sql engine')
            self.sqlengine = sqlalchemy.create_engine(self.conn_str)

    def validate(self, raise_exception=True):
        if not self.metadata:
            raise ValueError('need metadata to validate!')
        uniq_docs = self.metadata['stats']['overall']['unique_documents_per_year']
        year_counts_expected = pd.DataFrame(uniq_docs).convert_dtypes().set_index('publication_year')
        year_counts_observed = self.publications.reset_index() \
                                                .groupby('publication_year') \
                                                .nunique()[['eid']].rename(columns={'eid': 'documents'})
        year_counts_valid = year_counts_expected.join(year_counts_observed, rsuffix='_o', how='outer') \
            .fillna(0).assign(diff=lambda x: x['documents'] - x['documents_o'])['diff'] \
                      .apply(lambda x: x == 0).all()
        logger.info(f'input validation of publications by year: {year_counts_valid}')
        eid_no_duplicates = self.data.assign(c=1).groupby('eid').count().c.max() == 1
        logger.info(f'input validation Eid uniqueness: {eid_no_duplicates}')
        valid = year_counts_valid and eid_no_duplicates
        if not valid and raise_exception:
            raise AssertionError(f'input validation failed for {self.agency}_{self.run_version}!')
        return valid

    def with_validation(self):
        self.validate()
        return self

    def with_db(self, conn_str):
        self.conn_str = conn_str
        return self

    def load_db(self, force=None):
        if not (self.agency and self.run_version):
            raise ValueError('need agency and version info')
        force = self.force if force is None else force
        prefix = f'{self.agency}_{self.run_version}'
        self.start_sql()
        with self.sqlengine.connect() as connection:
            for kind in self.export_kinds:
                table_name = f'{prefix}_{kind}'
                logger.info(f'loading table {kind} to database with table name {table_name}')
                getattr(self, kind).reset_index().to_sql(
                    table_name, index=False, con=connection, if_exists='replace' if force else 'fail')


if __name__ == '__main__':
    ap = ArgumentParser()
    ap.add_argument('-d', '--dump')
    ap.add_argument('-f', '--filter')
    ap.add_argument('-c', '--connection-string')
    ap.add_argument('-a', '--agency')
    ap.add_argument('-r', '--run-version')
    ap.add_argument('--to-db', action='store_true')
    ap.add_argument('--force', action='store_true')
    ap.add_argument('--to-csv', action='store_true')
    ap.add_argument('json_files', nargs='+')
    args = ap.parse_args()

    jl = JSONLoader(
        args.json_files, agency=args.agency, run_version=args.run_version, conn_str=args.connection_string,
        force=args.force)
    if args.dump:
        table = getattr(jl, args.dump)
        if args.filter:
            print(table[args.filter.split(',')])
        else:
            print(table)
            print('columns:')
            print(table.dtypes)
    elif args.to_csv:
        jl.dump_csv()
    elif args.to_db:
        jl.load_db()
