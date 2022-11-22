create table {SCHEMA}.model(
  id BIGINT IDENTITY(1,1) NOT NULL,
  name varchar(32) not null,  --
  github_commit_url varchar(1024) null,
  description nvarchar(max),
  last_updated_date datetime NOT NULL default getdate(),
  CONSTRAINT pk_model_name PRIMARY KEY (id),
  CONSTRAINT unq_model_name UNIQUE(name)
);
insert into {SCHEMA}.model(name)
values('string_matching'),('model1'),('model2'),('model3')

create table {SCHEMA}.agency_run(
  id BIGINT IDENTITY(1,1) NOT NULL,
  agency varchar(32) not null,  -- decided against a separate table for agency for now
  [version] varchar(32) not null,
  run_date date,
  last_updated_date datetime NOT NULL default getdate(),
  CONSTRAINT pk_agency_run_id PRIMARY KEY (id),
  CONSTRAINT uq_agency_run_agency_version UNIQUE(agency,[version])
);

CREATE TABLE {SCHEMA}.dataset_alias (
    id BIGINT IDENTITY(1,1) NOT NULL,
    run_id BIGINT not null,
    alias_id BIGINT not null, -- externally decided fr now
    parent_alias_id BIGINT NULL,
    alias varchar(160) ,
    alias_type varchar(50) ,
    url varchar(2048) ,
    last_updated_date datetime DEFAULT getdate() NOT NULL,
    CONSTRAINT pk_dataset_alias_id PRIMARY KEY (id),
    CONSTRAINT fk_dataset_alias_run_id FOREIGN KEY (run_id) REFERENCES {SCHEMA}.agency_run(id)
);
create index ix_dataset_alias_run_id_alias_id on {SCHEMA}.dataset_alias(run_id,alias_id);


CREATE TABLE {SCHEMA}.author (
    id BIGINT IDENTITY(1,1) NOT NULL,
    run_id BIGINT not null,
    external_id varchar(128) ,  -- TBD scopus_id?? or keep open for alternative external repositories? Could this be a URL?
    given_name nvarchar(150) ,  -- the preferred given namme
    family_name nvarchar(150) , -- the preferred family name
    last_updated_date datetime DEFAULT getdate() NOT NULL,
    CONSTRAINT pk_author_id PRIMARY KEY (id),
    CONSTRAINT fk_author_run_id FOREIGN KEY (run_id) REFERENCES {SCHEMA}.agency_run(id)
);

CREATE TABLE {SCHEMA}.asjc (
    id BIGINT IDENTITY(1,1) NOT NULL,
    run_id BIGINT not null,
    code bigint not null ,  -- TBD correct datatype?
    label nvarchar(max) ,
    last_updated_date datetime DEFAULT getdate() NOT NULL,
    CONSTRAINT pk_asjc_id PRIMARY KEY (id),
    CONSTRAINT fk_asjc_run_id FOREIGN KEY (run_id) REFERENCES {SCHEMA}.agency_run(id)
);

CREATE TABLE {SCHEMA}.publisher (
    id BIGINT IDENTITY(1,1) NOT NULL,
    run_id BIGINT not null,
    external_id nvarchar(128) ,
    name nvarchar(120),
    last_updated_date datetime DEFAULT getdate() NOT NULL,
    CONSTRAINT pk_publisher_id PRIMARY KEY (id),
    CONSTRAINT fk_publisher_run_id FOREIGN KEY (run_id) REFERENCES {SCHEMA}.agency_run(id),
    CONSTRAINT uq_publisher_name UNIQUE(run_id,name)
);

CREATE TABLE {SCHEMA}.journal (
    id BIGINT IDENTITY(1,1) NOT NULL,
    run_id BIGINT not null,
    publisher_id BIGINT NULL, -- NB reflects the publisher of the journal at the time of the model_run
    external_id varchar(128) NULL,
    title varchar(1028) NOT NULL,  -- Q: unique? unique in combination with publisher_id?
    cite_score decimal(9,2) NULL,
    last_updated_date datetime DEFAULT getdate() NOT NULL,
    CONSTRAINT pk_journal_id PRIMARY KEY (id),
    CONSTRAINT fk_journal_run_id FOREIGN KEY (run_id) REFERENCES {SCHEMA}.agency_run(id),
    CONSTRAINT fk_journal_publisher FOREIGN KEY (publisher_id) REFERENCES {SCHEMA}.publisher(id)
);

CREATE TABLE {SCHEMA}.publication (
    id BIGINT IDENTITY(1,1) NOT NULL,
    run_id BIGINT not null,
    journal_id BIGINT NULL,
    external_id varchar(128) ,
    title varchar(400) ,
    doi varchar(80) ,
    [year] integer NULL,
    [month] integer NULL,
    pub_type varchar(30) ,
    citation_count integer NULL,
    fw_citation_impact float NULL,
    tested_expressions varchar(max) NULL,
    last_updated_date datetime DEFAULT getdate() NOT NULL,
    CONSTRAINT pk_publication_id PRIMARY KEY (id),
    CONSTRAINT fk_publication_run_id FOREIGN KEY (run_id) REFERENCES {SCHEMA}.agency_run(id),
    CONSTRAINT fk_publication_journal FOREIGN KEY (journal_id) REFERENCES {SCHEMA}.journal(id)
);
 CREATE INDEX ix_publication_external_id ON {SCHEMA}.publication (  external_id );

CREATE TABLE {SCHEMA}.publication_author (
    id BIGINT IDENTITY(1,1) NOT NULL,
    run_id BIGINT NOT NULL,
    publication_id BIGINT NOT NULL,
    author_id BIGINT NULL,      -- may be null if a scopus author cannot be identified
    given_name nvarchar(150) ,  -- the given namme as used in the publication
    family_name nvarchar(150) , -- the family name as used in the publication
    author_position integer,
    last_updated_date datetime DEFAULT getdate() NOT NULL,
    CONSTRAINT pk_publication_author_id PRIMARY KEY (id),
    CONSTRAINT fk_publication_author_run_id FOREIGN KEY (run_id) REFERENCES {SCHEMA}.agency_run(id),
    CONSTRAINT fk_publication_author_publication_id FOREIGN KEY (publication_id) REFERENCES {SCHEMA}.publication(id),
    CONSTRAINT fk_publication_author_aithor_id FOREIGN KEY (author_id) REFERENCES {SCHEMA}.author(id)
);

CREATE TABLE {SCHEMA}.publication_affiliation (
    id BIGINT IDENTITY(1,1) NOT NULL,
    run_id BIGINT not null,
    publication_id bigint not null,
    sequence_number smallint,
    external_id varchar(128),  -- scopus id for elsevier, may need to remain more general-named than that
    institution_name nvarchar(750),
    [address] nvarchar(750),
    city nvarchar(128),
    state nvarchar(128),
    country_code nvarchar(10),
    postal_code nvarchar(32),
    last_updated_date datetime DEFAULT getdate() NOT NULL,
    CONSTRAINT pk_publication_affiliation_id PRIMARY KEY (id),
    CONSTRAINT fk_publication_affiliation_run_id FOREIGN KEY (run_id) REFERENCES {SCHEMA}.agency_run(id),
    CONSTRAINT fk_publication_affiliation_publication_id FOREIGN KEY (publication_id) REFERENCES {SCHEMA}.publication(id)
)

CREATE TABLE {SCHEMA}.publication_asjc (
    id BIGINT IDENTITY(1,1) NOT NULL,
    run_id BIGINT NOT NULL,
    publication_id BIGINT NOT NULL,
    asjc_id BIGINT NOT NULL,
    last_updated_date datetime DEFAULT getdate() NOT NULL,
    CONSTRAINT pk_publication_asjc_id PRIMARY KEY (id),
    CONSTRAINT fk_publication_asjc_run_id FOREIGN KEY (run_id) REFERENCES {SCHEMA}.agency_run(id),
    CONSTRAINT fk_publication_asjc_publication_id FOREIGN KEY (publication_id) REFERENCES {SCHEMA}.publication(id),
    CONSTRAINT fk_publication_asjc_asjc_id FOREIGN KEY (asjc_id) REFERENCES {SCHEMA}.asjc(id)
);

CREATE TABLE {SCHEMA}.dyad (
    id BIGINT IDENTITY(1,1) NOT NULL,
    run_id BIGINT NOT NULL,
    publication_id BIGINT NOT NULL,
    dataset_alias_id BIGINT NULL,
    alias_id BIGINT NULL,
    mention_candidate varchar(1028) not null,
    snippet varchar(max),
    last_updated_date datetime DEFAULT getdate() NOT NULL,
    is_fuzzy bit NULL default 0,
    fuzzy_score real NULL default 0,
    CONSTRAINT pk_dyad_id PRIMARY KEY (id),
    CONSTRAINT fk_dyad_run_id FOREIGN KEY (run_id) REFERENCES {SCHEMA}.agency_run(id),
    CONSTRAINT fk_dyad_publication_id FOREIGN KEY (publication_id) REFERENCES {SCHEMA}.publication(id),
    CONSTRAINT fk_dyad_dataset_alias_id FOREIGN KEY (dataset_alias_id) REFERENCES {SCHEMA}.dataset_alias(id)
);

CREATE TABLE {SCHEMA}.dyad_model (
    id BIGINT IDENTITY(1,1) NOT NULL,
    run_id BIGINT NOT NULL,
    dyad_id BIGINT NOT NULL,
    model_id bigint not null,
    score real,
    last_updated_date datetime DEFAULT getdate() NOT NULL,
    CONSTRAINT pk_dyada_model_id PRIMARY KEY (id),
    CONSTRAINT fk_dyad_model_run FOREIGN KEY (run_id) REFERENCES {SCHEMA}.agency_run(id),
    CONSTRAINT fk_dyad_model_pda FOREIGN KEY (dyad_id) REFERENCES {SCHEMA}.dyad(id),
    CONSTRAINT fk_pda_model_model FOREIGN KEY (model_id) REFERENCES {SCHEMA}.model(id)
);

CREATE TABLE {SCHEMA}.topic (
    id BIGINT IDENTITY(1,1) NOT NULL,
    run_id BIGINT not null,
    keywords varchar(1028), -- |-separated list of keywords, alternative to normalized model
    external_topic_id varchar(128) ,
    prominence real,
    last_updated_date datetime DEFAULT getdate() NOT NULL,
    CONSTRAINT topic_PK PRIMARY KEY (id),
    CONSTRAINT fk_topic_run_id FOREIGN KEY (run_id) REFERENCES {SCHEMA}.agency_run(id)
);

CREATE TABLE {SCHEMA}.publication_topic (
    id BIGINT IDENTITY(1,1) NOT NULL,
    run_id BIGINT NOT NULL,
    publication_id BIGINT NOT NULL,
    topic_id BIGINT NOT NULL,
    score real NULL,
    last_updated_date datetime DEFAULT getdate() NOT NULL,
    CONSTRAINT publication_topic_PK_3 PRIMARY KEY (id),
    CONSTRAINT fk_publication_topic_run_id FOREIGN KEY (run_id) REFERENCES {SCHEMA}.agency_run(id),
    CONSTRAINT fk_publication_topic_publication_id FOREIGN KEY (publication_id) REFERENCES {SCHEMA}.publication(id),
    CONSTRAINT fk_publication_topic_topic_id FOREIGN KEY (topic_id) REFERENCES {SCHEMA}.topic(id)
);
CREATE INDEX ix_publication_topic_topic_id ON {SCHEMA}.publication_topic (  topic_id ASC  );
CREATE INDEX ix_publication_topic_publication_id ON {SCHEMA}.publication_topic(publication_id);

CREATE TABLE {SCHEMA}.author_affiliation (
    id BIGINT IDENTITY(1,1) NOT NULL,
    run_id BIGINT NOT NULL,
    publication_author_id BIGINT NULL,
    publication_affiliation_id BIGINT NULL,
    last_updated_date datetime default getdate() NOT NULL,
    CONSTRAINT pk_author_affiliation_id PRIMARY KEY (id),
    CONSTRAINT fk_author_affiliation_run_id FOREIGN KEY (run_id) REFERENCES {SCHEMA}.agency_run(id),
    CONSTRAINT fk_author_affiliation_publication_author_id FOREIGN KEY (publication_author_id) REFERENCES {SCHEMA}.publication_author(id),
    CONSTRAINT fk_author_affiliation_publication_affiliation_id FOREIGN KEY (publication_affiliation_id) REFERENCES {SCHEMA}.publication_affiliation(id)
);

-- DROP TABLE {SCHEMA}.issn;
CREATE TABLE {SCHEMA}.issn (
    id BIGINT IDENTITY(1,1) NOT NULL,
    run_id BIGINT NOT NULL,
    journal_id BIGINT NULL,
    ISSN varchar(13) ,
    last_updated_date datetime NOT NULL default getdate(),
    CONSTRAINT pk_issn_id PRIMARY KEY (id),
    CONSTRAINT fk_issn_run_id FOREIGN KEY (run_id) REFERENCES {SCHEMA}.agency_run(id)
);

--==============================
-- tables related to validation
CREATE TABLE {SCHEMA}.[susd_user] (
    id BIGINT IDENTITY(1,1) NOT NULL,
    first_name varchar(100),
    last_name varchar(100),
    email varchar(100),
    [password] varchar(100) , -- correct collations?
                              -- TBD whether we want to keep this table here
    last_updated_date datetime DEFAULT getdate() NOT NULL,
    CONSTRAINT pk_susd_user_id PRIMARY KEY (id)
);
alter table {SCHEMA}.susd_user add constraint unq_susd_user_email unique(email)

CREATE TABLE {SCHEMA}.[reviewer] (
    id BIGINT IDENTITY(1,1) NOT NULL,
    susd_user_id BIGINT not null,
    run_id BIGINT not null,
    roles varchar(max) NULL,
    last_updated_date datetime DEFAULT getdate() NOT NULL,
    CONSTRAINT pk_reviewer_id PRIMARY KEY (id),
    CONSTRAINT fk_reviewer_run_id FOREIGN KEY (run_id) REFERENCES {SCHEMA}.agency_run(id),
    CONSTRAINT fk_reviewer_susd_user_id FOREIGN KEY (susd_user_id) REFERENCES {SCHEMA}.susd_user(id)
);

CREATE TABLE {SCHEMA}.snippet_validation (
    id BIGINT IDENTITY(1,1) NOT NULL,
    run_id BIGINT not null,
    reviewer_id BIGINT not null,
    dyad_id BIGINT not null,
    is_dataset_reference smallint ,
    agency_dataset_identified smallint NULL,
    notes nvarchar(max) ,
    last_updated_date datetime DEFAULT getdate() NOT NULL,
    CONSTRAINT pk_reviewer_snippet_id PRIMARY KEY (id),
    CONSTRAINT fk_reviewer_snippet_run_id FOREIGN KEY (run_id) REFERENCES {SCHEMA}.agency_run(id),
    CONSTRAINT fk_reviewer_snippet_dyad_id FOREIGN KEY (dyad_id) REFERENCES {SCHEMA}.dyad(id),
    CONSTRAINT fk_reviewer_snippet_reviewer_id FOREIGN KEY (reviewer_id) REFERENCES {SCHEMA}.reviewer(id)
);
 CREATE INDEX ix_snippet_validation_reviewer_id ON {SCHEMA}.snippet_validation (  reviewer_id );
 create unique index uix_snippet_validation_dyad_reviewer_run on {SCHEMA}.snippet_validation(run_id,reviewer_id,dyad_id);

-- GEO coding
CREATE TABLE {SCHEMA}.[affiliation_geocoding](
    [id] [bigint] IDENTITY(1,1) NOT NULL,
    source varchar(10),   -- GOOGLE or OSM for now
    [geo_location] [geography] NULL,
    [geo_boundingbox] [geography] NULL,
    [boundingbox] [varchar](max) NULL,
    [lat] [float] NULL,
    [lon] [float] NULL,
    city nvarchar(4000) NULL,
    state nvarchar(4000) NULL,
    state_short nvarchar(4000) NULL,
    country nvarchar(4000) NULL,
    country_short nvarchar(10) NULL,
    [display_name] [nvarchar](max) NULL,
    [q] [nvarchar](max) NOT NULL,
    [q_final] [varchar](max) NULL,
    [nattempt] [smallint] NULL,
    [importance] [float] NULL,
 CONSTRAINT [pk_affiliation_geocoding] PRIMARY KEY CLUSTERED 
(
    [id] ASC
))

CREATE TABLE {SCHEMA}.[publication_affiliation_geocoding](
    [run_id] [bigint] NOT NULL,
    [publication_affiliation_id] [bigint] NOT NULL,
    [affiliation_geocoding_id] [bigint] NOT NULL
)

ALTER TABLE {SCHEMA}.[publication_affiliation_geocoding] ADD  CONSTRAINT [fk_publication_affiliation_agcid] FOREIGN KEY([affiliation_geocoding_id])
REFERENCES {SCHEMA}.[affiliation_geocoding] ([id])

ALTER TABLE {SCHEMA}.[publication_affiliation_geocoding] ADD  CONSTRAINT [fk_publication_affiliation_arcid] FOREIGN KEY([run_id])
REFERENCES {SCHEMA}.[agency_run] ([id])

ALTER TABLE {SCHEMA}.[publication_affiliation_geocoding]  WITH CHECK ADD  CONSTRAINT [fk_publication_affiliation_pagcid] FOREIGN KEY([publication_affiliation_id]) 
REFERENCES {SCHEMA}.[publication_affiliation] ([id])

