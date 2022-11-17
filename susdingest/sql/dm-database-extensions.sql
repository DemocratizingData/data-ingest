-- ========== extra ==========

-- full GEO table with a unique ID
CREATE TABLE [dbo].[affiliation_geo](
  id bigint identity(1,1) not null,
  [q] [nvarchar](max) NOT NULL,
  [address] [nvarchar](750) NULL,
  [city] [nvarchar](128) NULL,
  [state] [nvarchar](128) NULL,
  [country_code] [nvarchar](10) NULL,
  [boundingbox] [varchar](max) NULL,
  [lat] [float] NULL,
  [lon] [float] NULL,
  [q_final] [varchar](max) NULL,
  [nattempt] [smallint] NULL,
  [display_name] [nvarchar](max) NULL,
  [importance] [float] NULL
) 
ALTER TABLE affiliation_geo add constraint pk_affiliation_geo_id primary key(id)

CREATE TABLE [dbo].[publication_affiliation_geo](
  run_id bigint not null,
  [publication_affiliation_id] [bigint] NOT NULL,
  affiliation_geo_id bigint not null
) 
ALTER TABLE [dbo].[publication_affiliation_geo]  WITH CHECK ADD  CONSTRAINT [fk_publication_affiliation_arid] FOREIGN KEY([run_id])
REFERENCES [dbo].[agency_run] ([id])
ALTER TABLE [dbo].[publication_affiliation_geo]  WITH CHECK ADD  CONSTRAINT [fk_publication_affiliation_agid] FOREIGN KEY([affiliation_geo_id])
REFERENCES [dbo].[affiliation_geo] ([id])
ALTER TABLE [dbo].[publication_affiliation_geo]  WITH CHECK ADD  CONSTRAINT [fk_publication_affiliation_pagid] FOREIGN KEY([publication_affiliation_id])
REFERENCES [dbo].[publication_affiliation] ([id])
GO

/*
CREATE TABLE [dbo].[affiliation_geo_staging](
  [q] [nvarchar](max) NOT NULL,
  [address] [nvarchar](750) NULL,
  [city] [nvarchar](128) NULL,
  [state] [nvarchar](128) NULL,
  [country_code] [nvarchar](10) NULL,
  [boundingbox] [varchar](max) NULL,
  [lat] [float] NULL,
  [lon] [float] NULL,
  [q_final] [varchar](max) NULL,
  [nattempt] [smallint] NULL,
  [display_name] [nvarchar](max) NULL,
  [importance] [float] NULL
) 

-- insert
insert into affiliation_geo(q,address,city,state,country_code,boundingbox,lat,lon,q_final,nattempt,display_name,importance)
select q,address,city,state,country_code,boundingbox,lat,lon,q_final,nattempt,display_name,importance
from affiliation_geo_staging

-- populate publication_affiliation_geo
with pa as (
select run_id,id, concat_ws(',',replace(pa.address,'|',','),pa.city,pa.state,cc.country_name) as q
,      pa.address, pa.city, pa.state,pa.country_code
  from publication_affiliation pa
  left outer join country_code cc on cc.country_code=pa.country_code
)
insert into publication_affiliation_geo
select pa.run_id,pa.id, ag.id 
  from pa
  inner join affiliation_geo ag 
     on isnull(ag.address,'')=isnull(pa.address,'')
    and isnull(ag.city,'')=isnull(pa.city,'')
    and isnull(ag.state,'')=isnull(pa.state,'')
    and isnull(ag.country_code,'')=isnull(pa.country_code,'')
    and ag.q_final is not null
 where not exists (
    select *
      from publication_affiliation_geo
     where publication_affiliation_id=pa.id and affiliation_geo_id=ag.id
)
*/




-- VIEW DyadsValidated joins dataset and validation information to publication_dataset_alias entires
create view DyadsValidated as (
select ar.agency,ar.version
,      ds.id as dataset_id, ds.alias as dataset
,      pda.publication_id
,      sum(case when sv.agency_dataset_identified=1 then 1 else 0 end) as num_identified
,      sum(case when sv.agency_dataset_identified=-1 then 1 else 0 end) as num_rejected
,      sum(case when sv.agency_dataset_identified=0 then 1 else 0 end) as num_uncertain
,      sum(case when sv.agency_dataset_identified is null then 1 else 0 end) as num_tbd
,      count(distinct da.id) as num_distinct_alias
,      count(distinct pda.mention_candidate) as num_distinct_mention
  from agency_run ar
  join dataset_alias ds on ds.run_id=ar.id and ds.parent_alias_id=ds.alias_id
  join dataset_alias da on da.run_id=ar.id and da.parent_alias_id=ds.alias_id
  join dyad pda on pda.run_id=ar.id and pda.dataset_alias_id=da.id
  join snippet_validation sv on sv.run_id=ar.id and sv.publication_dataset_alias_id=pda.id
 group by ar.agency,ar.version,ds.id,ds.alias,pda.publication_id
 )

 GO
