import logging
import networkx as nx
import os
import pandas
import sqlalchemy as sqla

MSSQL = 'MSSQL'
PSQL = 'PSQL'


class SUSDDatabase:
    # wraps a Microsoft SQL Server database by default
    # kind can also be PSQL, but not all methods are implemented for PSQL (yet)
    #
    # TBD do we want to support CasJobs iso/as well as sqlalchemy?
    #

    @classmethod
    def from_url(cls, url):
        db_info = sqla.engine.url.make_url(url)
        kind = MSSQL if 'mssql' in db_info.drivername else PSQL if 'postgresql' in db_info.drivername else None
        return cls({'host': db_info.host,
                    'database': db_info.database,
                    'user': db_info.username,
                    'pwd': db_info.password}, kind=kind)

    def __init__(self, AUTH, DATABASE=None, kind=MSSQL, schema='dbo'):
        # AUTH should be a dict with (username, password, host,database)
        # database can be overridden with the DATABASE parameter
        # TBD should we use a connection string instead of all of this?
        self.AUTH = AUTH  # should check all required parameters are there
        self.SERVER = AUTH['host']
        if kind not in [MSSQL, PSQL]:
            raise Exception(f"only support {MSSQL} and {PSQL} database kinds")
        self.KIND = kind

        if DATABASE is None:
            self.DATABASE = AUTH['database']
        else:
            self.DATABASE = DATABASE
        self.SCHEMA = schema
        self.ENGINE = self.__create_engine()
        self.file_path = os.path.abspath(os.path.dirname(__file__))

    def execute_query(self, sql):
        with self.ENGINE.connect() as conn:
            return pandas.read_sql(sql, conn)

    def execute_update(self, statement):
        with self.ENGINE.connect() as connection:
            with connection.begin():
                connection.execute(sqla.text(statement))

    def __create_engine(self):
        if self.KIND == MSSQL:
            return sqla.create_engine(
                (f"mssql+pymssql://{self.AUTH['user']}:{self.AUTH['pwd']}@{self.SERVER}:1433/{self.DATABASE}"
                 "?charset=utf8"))
        elif self.KIND == PSQL:
            return sqla.create_engine(
                f"postgresql://{self.AUTH['user']}:{self.AUTH['pwd']}@{self.SERVER}:5432/{self.DATABASE}")

    def create_schema(self):
        try:
            self.ENGINE.execute(sqla.schema.CreateSchema(self.SCHEMA))
        except Exception as e:
            # TBD better logging. should not be an error but a warning in case schema already exists
            #   so must inspect the error message probably
            logging.error("SCHEMA may exist already, ignore this error then\n"+str(e))
            pass

    def get_run_id(self, AGENCY, VERSION):
        sql = f"select id from {self.SCHEMA}.agency_run where agency=%s and version=%s"
        with self.ENGINE.connect() as con:
            RUN = pandas.read_sql(sql, con, params=(AGENCY, VERSION))
            if len(RUN) == 1:
                return RUN.id[0]
            else:
                return None

    def create_tables(self):
        # create script for creating the tables for the data model and execute it
        #
        # this function uses a pre-defined initialization script, only needs replacing {SCHEMA} with actual schema.
        ddl_file = f'{self.file_path}/sql/initialize-dm-database.sql'
        with open(ddl_file, "r") as f:
            ddl = f.read()
        ddl = ddl.replace("{SCHEMA}", self.SCHEMA)
        try:
            self.execute_update(ddl)
            logging.debug("executed SQL script\n:"+ddl)
        except Exception as e:
            logging.error(str(e)+"\n"+ddl)

    def drop_all_tables(self, keep_tables=[]):
        # create and return script for dropping all tables for the data model
        # if write_sql_only==False, actually execute the script in the database as well
        # drops tables in right order to not get problems with foreign key realtionships
        tables = reversed(list(self.sorted_tables().keys()))
        drop_all = "\n".join([f'drop table {self.SCHEMA}.{t}' for t in tables if t not in keep_tables])
        try:
            self.execute_update(drop_all)
        except Exception as e:
            logging.error("ERROR in drop all tables SQL script:\n"+str(e)+"\n"+drop_all)

    def delete_from_all_tables(self, keep_tables=[]):
        # create and return script for emptying all tables for the data model
        # if write_sql_only==False, actually execute the script in the database as well
        # deletes from tables in right order to not get problems with foreign key realtionships
        tables = reversed(list(self.sorted_tables().keys()))
        delete_all = "\n".join([f'delete from {self.SCHEMA}.{t}' for t in tables if t not in keep_tables])
        try:
            self.execute_update(delete_all)
            logging.debug("deleted all tables with SQL script :\n"+delete_all)
        except Exception as e:
            logging.error("ERROR executing delete from all tables SQL script:\n"+str(e)+"\n"+delete_all)

    def sorted_tables(self):
        # sort tables in the specified schema according to FK relationships
        # returns tables ordered such that if table t1 refers to table t2,
        # t2 will be earlier in the result than t1 TEST if true
        # MS SQL only for now as it uses MS SQL specific tables/views to get the foreign key metadata
        if self.KIND != MSSQL:
            raise Exception("sorted_tables not supported for postgres database")

        with self.ENGINE.connect() as conn:
            sql = """
                SELECT c.table_name
                ,      string_agg(c.column_name,',') within group(order by c.ordinal_position) as columns
                ,      sum(case when c.column_name='run_id' then 1 else 0 end) as has_run_id
                ,      max(case when
                           COLUMNPROPERTY(object_id(c.TABLE_SCHEMA+'.'+c.TABLE_NAME)
                                        , c.COLUMN_NAME, 'IsIdentity') = 1
                            then c.column_name else null end) as identity_column
                  FROM information_Schema.tables t
                  JOIN information_schema.columns c
                    ON c.table_schema=t.table_schema
                   AND c.table_name=t.table_name
                 WHERE t.table_schema = %s and t.table_type='BASE TABLE'
                 GROUP BY c.table_name
                """
            tables = pandas.read_sql(sql, conn, params=(self.SCHEMA,))
            tables = {t.table_name: {
                'columns': t.columns,
                'identity_column': t.identity_column,
                'has_run_id': t.has_run_id,
                'FKs': set()} for t in tables.itertuples()}

            sql = """
                SELECT OBJECT_NAME(fk.parent_object_id) as from_table
                ,      OBJECT_NAME(fk.referenced_object_id) to_table
                  FROM sys.foreign_keys fk
                 INNER JOIN sys.foreign_key_columns fkc
                    ON fkc.constraint_object_id = fk.object_id
                 INNER JOIN sys.columns c1
                    ON fkc.parent_column_id = c1.column_id
                   AND fkc.parent_object_id = c1.object_id
                 INNER JOIN sys.columns c2
                    ON fkc.referenced_column_id = c2.column_id
                   AND fkc.referenced_object_id = c2.object_id
                 INNER join sys.schemas s
                    ON s.schema_id=fk.schema_id
                 WHERE s.name=%s
                """
            FKs = pandas.read_sql(sql, conn, params=(self.SCHEMA,))
            for f in FKs.itertuples():
                tables[f.from_table]['FKs'].add(f.to_table)

        graph = nx.DiGraph()
        for t, _ in tables.items():
            graph.add_node(t)
        edges = [(t, td) for t, i in tables.items() for td in i['FKs']]
        graph.add_edges_from(edges)
        ordered = reversed(list(nx.topological_sort(graph)))
        return {t: {'columns': tables[t]['columns'],
                    'identity_column': tables[t]['identity_column'],
                    'has_run_id': True if tables[t]['has_run_id'] == 1 else False,
                    'FKs': tables[t]['FKs'] if len(tables[t]['FKs']) > 0 else None} for t in ordered}
