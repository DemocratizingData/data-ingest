import os
import logging

from .susddatabase import SUSDDatabase


class DatamodelLoader:
    def __init__(self, DM_database: SUSDDatabase, JSON_database: str, JSON_schema: str = 'dbo'):
        # DM_database is the database containing the data model
        # JSON_database is name of database containing the elsevier data, JSON_schema is the corresponding schem
        self.DM_database = DM_database
        self.DM_SCHEMA = DM_database.SCHEMA
        self.ELSEVIER_SCHEMA = f"{JSON_database}.{JSON_schema}"
        self.file_path = os.path.abspath(os.path.dirname(__file__))

    def json_prefix(self, AGENCY, VERSION):
        return f"{AGENCY.strip().replace(' ','_')}_{VERSION}_"

    def create_run(self, AGENCY, VERSION):
        # create an entry in the agency_run table and return its id
        # TODO add more metadata fields here?
        # TBD should this entry be loaded by jsonloader,
        #   or should jsonloader load metadata in its own tables to be retrieved here?
        sql = f"""
        IF NOT EXISTS (SELECT * FROM  {self.DM_SCHEMA}.agency_run
                           WHERE agency='{AGENCY}' and version='{VERSION}')
           BEGIN
              INSERT INTO {self.DM_SCHEMA}.agency_run(agency,version)
              VALUES( '{AGENCY}','{VERSION}')
           END
        """
        # TODO add try/catch with logging in case something goes wrong
        self.DM_database.execute_update(sql)
        return self.DM_database.get_run_id(AGENCY, VERSION)

    def copy_from_staging(self, AGENCY, VERSION, if_exists='replace', write_sql_log=False):
        if if_exists not in ['fail', 'replace']:
            raise Exception("if_exists must have value in ('fail', 'replace')")
        RUN_ID = self.DM_database.get_run_id(AGENCY, VERSION)
        if RUN_ID is not None:
            if if_exists == 'fail':
                raise Exception(f"run exists for {AGENCY} / {VERSION}")
        else:
            RUN_ID = self.create_run(AGENCY, VERSION)

        copy_file = f'{self.file_path}/sql/insert_elsevier.sql'
        with open(copy_file, "r") as f:
            copy_command = f.read()
        copy_command = copy_command.replace("{DATABASE}", self.DM_database.DATABASE)
        copy_command = copy_command.replace("{SCHEMA}", self.DM_SCHEMA)
        copy_command = copy_command.replace("{ELSEVIER_SCHEMA}", self.ELSEVIER_SCHEMA)
        copy_command = copy_command.replace("{ELSEVIER_PREFIX}", self.json_prefix(AGENCY, VERSION))
        copy_command = copy_command.replace("{ORG}", AGENCY)
        copy_command = copy_command.replace("{VERSION}", VERSION)
        copy_command = copy_command.replace("{RUN_ID}", str(RUN_ID))
        logging.debug(f'submitting following SQL script\n{copy_command}')
        self.DM_database.execute_update(copy_command)

    def delete_agency_run(self, AGENCY, VERSION, write_sql_log=False):
        # delete all data for an agency run
        # may be used before reloading it
        RUN_ID = self.DM_database.get_run_id(AGENCY, VERSION)
        delete_file = f'{self.file_path}/sql/delete_agency_run.sql'
        with open(delete_file, "r") as f:
            command = f.read()
        command = command.replace("{DATABASE}", self.DM_database.DATABASE)
        command = command.replace("{SCHEMA}", self.DM_SCHEMA)
        command = command.replace("{RUN_ID}", str(RUN_ID))
        logging.debug(f'submitting following SQL script\n{command}')
        self.DM_database.execute_update(command)
