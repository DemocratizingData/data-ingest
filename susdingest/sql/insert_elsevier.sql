INSERT INTO {SCHEMA}.dataset_alias (run_id, alias_id,
  parent_alias_id,alias,alias_type)
SELECT {RUN_ID},alias_id,parent_alias_id,alias,alias_type
  FROM {ELSEVIER_SCHEMA}.{ELSEVIER_PREFIX}datasets
;

INSERT INTO {SCHEMA}.publisher (run_id,external_id, name)
SELECT DISTINCT {RUN_ID},NULL, journal_publishername
FROM {ELSEVIER_SCHEMA}.{ELSEVIER_PREFIX}publications
;

INSERT INTO {SCHEMA}.journal(run_id,publisher_id,title,external_id,cite_score)
SELECT DISTINCT {RUN_ID},p.id as publisher_id, journal_title
,     journal_scopus_source_id, journal_citescore_value
  FROM {ELSEVIER_SCHEMA}.{ELSEVIER_PREFIX}publications pm
  left outer join {SCHEMA}.publisher p
    on p.name=pm.journal_publishername
   and p.run_id={RUN_ID}
;

INSERT INTO {SCHEMA}.issn (run_id,journal_id, issn)
SELECT DISTINCT {RUN_ID},j.id, f.value as issn
FROM {ELSEVIER_SCHEMA}.{ELSEVIER_PREFIX}publications epm
inner JOIN {SCHEMA}.journal j
   ON j.external_id = epm.journal_scopus_source_id
  and j.title=epm.journal_title
  and j.run_id={RUN_ID}
CROSS APPLY string_split(epm.journal_issn_isbn, '|') as f
WHERE f.value IS NOT NULL and rtrim(f.value) != ''
;

with journal_publisher as (
select distinct j.*,p.name as publisher
  from {SCHEMA}.journal j
  left outer join {SCHEMA}.publisher p on p.id=j.publisher_id
 where j.run_id={RUN_ID}
)
INSERT INTO {SCHEMA}.publication (run_id,journal_id, external_id, title, doi,
    [year], [month], pub_type, citation_count, fw_citation_impact,tested_expressions)
    SELECT DISTINCT {RUN_ID},jp.id
,      epm.Eid AS external_id, epm.publication_title, epm.doi, epm.publication_year
,      epm.publication_month, epm.publication_type, epm.citation_count, epm.field_weighted_citation_impact,epm.tested_expressions
    FROM {ELSEVIER_SCHEMA}.{ELSEVIER_PREFIX}publications epm
    join journal_publisher jp
    on epm.journal_title=jp.title
   and epm.journal_scopus_source_id=jp.external_id
   and (epm.journal_publishername=jp.publisher or (epm.journal_publishername is null and jp.publisher is null))


INSERT INTO {SCHEMA}.dyad (
  run_id, publication_id, dataset_alias_id, alias_id, fuzzy_score, is_fuzzy,mention_candidate, snippet)
SELECT DISTINCT {RUN_ID} as run_id, p.id as publication_id,da.id,pda.alias_id as alias_id
,      pda.fuzzy_score
,      pda.is_fuzzy
,      pda.alias, pda.snippet
    FROM {ELSEVIER_SCHEMA}.{ELSEVIER_PREFIX}dyads pda
    INNER JOIN {SCHEMA}.publication p ON p.external_id = pda.eid and p.run_id={RUN_ID}
    LEFT OUTER JOIN {SCHEMA}.dataset_alias da ON da.alias_id = pda.alias_id and da.run_id={RUN_ID}

INSERT INTO {SCHEMA}.dyad_model ( run_id, dyad_id, model_id, score)
SELECT distinct {RUN_ID} as run_id,pda.id ,m.id,d.score
    FROM {ELSEVIER_SCHEMA}.{ELSEVIER_PREFIX}dyads d
    INNER JOIN {SCHEMA}.publication p
        ON p.external_id = d.eid and p.run_id={RUN_ID}
    INNER JOIN {SCHEMA}.dyad pda
        ON pda.publication_id=p.id
       and isnull(pda.alias_id,-1)=isnull(d.alias_id,-1)
       and pda.run_id={RUN_ID}
       and pda.mention_candidate=d.alias
       and isnull(pda.snippet,'')=isnull(d.snippet,'')
    INNER JOIN {SCHEMA}.model m on m.name=d.model
;

INSERT INTO {SCHEMA}.topic(run_id,keywords, external_topic_id,prominence)
SELECT DISTINCT {RUN_ID}, etk.keywords,etk.Topic_Id,prominence
from {ELSEVIER_SCHEMA}.{ELSEVIER_PREFIX}topics etk
;

INSERT INTO {SCHEMA}.publication_topic(run_id, publication_id, topic_id, score)
SELECT DISTINCT {RUN_ID},p.id, t.id , NULL as score
from {SCHEMA}.publication p
inner join {ELSEVIER_SCHEMA}.{ELSEVIER_PREFIX}topics et on et.eid=p.external_id
inner join {SCHEMA}.topic t on t.external_topic_id = et.topic_id and t.run_id={RUN_ID}
where p.run_id={RUN_ID}
;

insert into {SCHEMA}.publication_ufc(run_id,publication_id,concept_id,concept_name,rank,a_freq)
select distinct {RUN_ID}, p.id, u.concept_id, u.concept_name, u.rank, u.a_freq
  from {ELSEVIER_SCHEMA}.{ELSEVIER_PREFIX}ufcs u
  join {SCHEMA}.publication p
    on p.run_id={RUN_ID} and p.external_id=u.eid
;

INSERT INTO {SCHEMA}.asjc (run_id,code,label)
SELECT DISTINCT {RUN_ID},ASJC,Label
  FROM {ELSEVIER_SCHEMA}.{ELSEVIER_PREFIX}asjcs a

INSERT INTO {SCHEMA}.publication_asjc(run_id,publication_id,asjc_id)
SELECT {RUN_ID}, p.id, a.id
FROM {ELSEVIER_SCHEMA}.{ELSEVIER_PREFIX}asjcs epm
INNER JOIN {SCHEMA}.publication p ON p.external_id = epm.EID and p.run_id={RUN_ID}
join {SCHEMA}.asjc a on cast(a.code as varchar(20))=epm.asjc and a.run_id={RUN_ID}
;

WITH epa as (
SELECT author_id, pn_given_name, pn_family_name
,      row_number() over (partition by author_id order by len(pn_given_name+pn_family_name) desc) as rank
  FROM {ELSEVIER_SCHEMA}.{ELSEVIER_PREFIX}authors epa
 WHERE not(author_id is null and pn_given_name is null and pn_family_name is null)
)
INSERT INTO {SCHEMA}.author (run_id,external_id, given_name, family_name)
SELECT DISTINCT {RUN_ID},cast(author_id as varchar(128)) as external_id, pn_given_name, pn_family_name
FROM epa
WHERE rank=1

INSERT INTO {SCHEMA}.publication_author (run_id,publication_id, author_id,given_name,family_name,author_position)
SELECT DISTINCT {RUN_ID}, p.id, a.id,epa.given_name,epa.family_name,epa.author_position
FROM {ELSEVIER_SCHEMA}.{ELSEVIER_PREFIX}authors epa
INNER JOIN {SCHEMA}.publication p ON p.external_id = epa.Eid and p.run_id={RUN_ID}
LEFT OUTER JOIN {SCHEMA}.author a
     ON a.external_id = cast(epa.author_id as varchar(128))
    and a.run_id={RUN_ID}

SET ANSI_WARNINGS OFF
INSERT INTO {SCHEMA}.publication_affiliation (
   run_id, publication_id,sequence_number,external_id, institution_name,
   address,country_code,state,city,postal_code)
SELECT  distinct {RUN_ID}
,      p.id
,      affiliation_sequence,  affiliation_ids
,      affiliation_organization
,      affiliation_address_part, country_code, affiliation_state, affiliation_city , affiliation_postal_code
 FROM {ELSEVIER_SCHEMA}.{ELSEVIER_PREFIX}affiliations a
 join {SCHEMA}.publication p on p.run_id={RUN_ID} and p.external_id=a.eid
SET ANSI_WARNINGS ON


INSERT INTO {SCHEMA}.author_affiliation (run_id, publication_author_id, publication_affiliation_id)
SELECT DISTINCT {RUN_ID}, pa.id, af.id
FROM {ELSEVIER_SCHEMA}.{ELSEVIER_PREFIX}authors epa
INNER JOIN {SCHEMA}.publication p ON p.external_id = epa.Eid and p.run_id={RUN_ID}
INNER JOIN {SCHEMA}.author a
     on a.external_id=cast(epa.author_id as varchar(128)) and a.run_id={RUN_ID}
    AND isnull(a.given_name,'') = isnull(epa.pn_given_name,'')
    AND isnull(a.family_name,'') = isnull(epa.pn_family_name,'')
INNER JOIN {SCHEMA}.publication_author pa
    ON a.id = pa.author_id
    AND isnull(pa.given_name,'') = isnull(epa.given_name,'')
    AND isnull(pa.family_name,'') = isnull(epa.family_name,'')
    and pa.publication_id=p.id
    and pa.run_id={RUN_ID}
cross apply openjson(epa.affiliation_sequences,'$') as af_s
INNER JOIN {SCHEMA}.publication_affiliation af
    ON af.publication_id=p.id
    and af.sequence_number=cast(af_s.Value as integer)
    and af.run_id={RUN_ID}
