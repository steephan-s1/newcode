CREATE PACKAGE DBOWNER.PACK_STATS_PUB_CAMPAIGN_JRBI
  CREATE OR REPLACE EDITIONABLE PACKAGE "DBOWNER"."PACK_STATS_PUB_CAMPAIGN_JRBI" 
AS

PROCEDURE processCAPRIAcceptedData(pOverrideCheckingTodaysData IN VARCHAR2 DEFAULT 'FALSE');
PROCEDURE processCAPRIRejectedData(pOverrideCheckingTodaysData IN VARCHAR2 DEFAULT 'FALSE');
PROCEDURE processCAPRIPublishedData(pOverrideCheckingTodaysData IN VARCHAR2 DEFAULT 'FALSE');
PROCEDURE processCAPRIMendeleyStatsData(pOverrideCheckingTodaysData IN VARCHAR2 DEFAULT 'FALSE');
PROCEDURE processCAPRIWebpublishedData(pOverrideCheckingTodaysData IN VARCHAR2 DEFAULT 'FALSE');
PROCEDURE processCAPRIDatainBriefData(pOverrideCheckingTodaysData IN VARCHAR2 DEFAULT 'FALSE');


PROCEDURE processMasterData;

PROCEDURE processLog (pProcess IN VARCHAR2, pLogType IN VARCHAR2, pMessage IN VARCHAR2);
FUNCTION getLoadID RETURN NUMBER;
FUNCTION checkOriginForToday (pOrigin in VARCHAR2) RETURN NUMBER;
FUNCTION deleteOrigin (pOrigin in VARCHAR2, pDate DATE) RETURN VARCHAR2;

PROCEDURE runDataChecks (vPercentage IN NUMBER);

END PACK_STATS_PUB_CAMPAIGN_JRBI;
/
CREATE OR REPLACE EDITIONABLE PACKAGE BODY "DBOWNER"."PACK_STATS_PUB_CAMPAIGN_JRBI" 
AS

    vErrorMessage        VARCHAR2(4000);
    vDataLoadID          NUMBER := 0;
    vDataRecords         NUMBER := 0;
    origin_loaded_today  EXCEPTION;


PROCEDURE processCAPRIAcceptedData(pOverrideCheckingTodaysData IN VARCHAR2 DEFAULT 'FALSE')
IS

BEGIN

processLog('processCAPRIAcceptedData','Information','Started Procedure'); 

IF upper(pOverrideCheckingTodaysData) <> 'TRUE' THEN
    vDataRecords := checkOriginForToday('Accepted_CAPRIData');
    IF vDataRecords > 0 THEN
         RAISE origin_loaded_today;
    END IF; 
END IF;    


BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE tbl_temp_jrbi_accepted_capri purge';
    EXCEPTION WHEN OTHERS THEN NULL;
END;

/* MARKAU-7310 check PIT codes are FLA, REV, SCO */

EXECUTE IMMEDIATE '
create table tbl_temp_jrbi_accepted_capri as 
select distinct
		  a.party_id,
          a.dedupe_id,
          a.orig_party_ref,
          ''JRBI'' AS user_group,
          a.title,
          a.firstname,
          a.lastname,
          a.email,
          i.orig_item_ref    AS article_id,
          i.item_milestone   AS article_status,
          i.issue_date 		 AS article_acceptance_date,
		  CASE
            WHEN trunc(i.issue_date) BETWEEN trunc(sysdate) - 7 and trunc(sysdate)
            THEN ''Y''
            ELSE ''N''
          END AS date_flag,
          ''Y'' AS article_accepted ,
		  i.class       AS publication_item,
          i.code AS article_doi,
		  CASE
            WHEN SUBSTR(i.identifier,1,1) = ''S''
            THEN SUBSTR(regexp_replace(i.identifier,''[()-]''),1)
            ELSE regexp_replace(i.identifier,''[()-]'')
          END                                       AS article_pii,
		  i.name             AS article_name,
		  substr(j.identifier,1,4)||''-''||substr(j.identifier,5,8) as journal_issn  ,
		  j.name             AS journal_name,
          ''STMJ''           AS OWNER,
          a.ISO_COUNTRY_CODE AS ISO_COUNTRY_CODE,	
		  j.pmg_code as pmg,
		  j.pmg_descr,
		  a.CMX_ID as CMX_ID,
		  j.show_status as Journal_BU,
          i.publisher as Item_URL,
		  ''Elsevier BV''    AS LEGAL_ENTITY
from
dbowner.tbl_parties a 
join dbowner.tbl_items i
on a.orig_party_ref = i.authors
and a.orig_system = ''JRBI'' and i.orig_system = ''JRBI'' and i.item_type = ''ART''
and a.email is not null
and lower(i.item_milestone) = ''accept''
AND (i.init_pub_date  is null
OR   i.last_pub_date  is null) 
and trunc(i.issue_date) between trunc(sysdate) - 7 and trunc(sysdate)
and lower(i.class) in ( ''full length article'',''full-length article'',''review article'',''short communication'')
join dbowner.tbl_items j 
on i.parent_ref = j.orig_item_ref
and j.item_type = ''JOU'' and j.orig_system = ''JRBI''  '
;


EXECUTE IMMEDIATE '
insert into tbl_temp_jrbi_accepted_capri 
select distinct
		  a.party_id,
          a.dedupe_id,
          a.orig_party_ref,
          ''JRBI'' AS user_group,
          a.title,
          a.firstname,
          a.lastname,
          a.email,
          i.orig_item_ref    AS article_id,
          i.item_milestone   AS article_status,
          i.issue_date AS article_acceptance_date,
		  CASE
            WHEN trunc(i.issue_date) BETWEEN trunc(sysdate) - 7 and trunc(sysdate)
            THEN ''Y''
            ELSE ''N''
          END AS date_flag,
          ''Y'' AS article_accepted ,
		  i.class       AS publication_item,
          i.code        AS article_doi,
		  CASE
            WHEN SUBSTR(i.identifier,1,1) = ''S''
            THEN SUBSTR(regexp_replace(i.identifier,''[()-]''),1)
            ELSE regexp_replace(i.identifier,''[()-]'')
          END                                       AS article_pii,
		  i.name             AS article_name,
		  substr(j.identifier,1,4)||''-''||substr(j.identifier,5,8) as journal_issn  ,
		  j.name             AS journal_name,
          ''STMJ''          AS OWNER,
          a.ISO_COUNTRY_CODE AS ISO_COUNTRY_CODE,	
		  j.pmg_code as pmg,
		  j.pmg_descr,
		  a.CMX_ID as CMX_ID,
		  j.show_status as Journal_BU,
          i.publisher as Item_URL,
		  ''Elsevier BV''    AS LEGAL_ENTITY
from
dbowner.tbl_parties a
join dbowner.tbl_interests u
on a.orig_party_ref = u.party_ref 
and a.orig_system = ''JRBI'' and u.orig_system = ''JRBI'' and u.interest_type = ''ART''
and a.email is not null
and a.email not in (select email from tbl_temp_jrbi_accepted_capri where user_group = ''JRBI'')
join dbowner.tbl_items i
on u.interest_value = i.orig_item_ref
and a.orig_system = ''JRBI'' and i.orig_system = ''JRBI'' and i.item_type = ''ART''
and lower(i.item_milestone) = ''accept''
and trunc(i.issue_date) between trunc(sysdate) - 7 and trunc(sysdate)
and lower(i.class) in ( ''full length article'',''full-length article'',''review article'',''short communication'')
join dbowner.tbl_items j 
on i.parent_ref = j.orig_item_ref
and j.item_type = ''JOU'' and j.orig_system = ''JRBI''  ' ; 

commit; 


vDataLoadID := getLoadID;

EXECUTE IMMEDIATE '
    INSERT INTO TBL_MDT_JRBI_MASTER_DATA
    SELECT  DISTINCT
            '||vDataLoadID||',
              ''Accepted_CAPRIData'',
              SYSDATE,
              NULL,
              ''N'',
              NULL,
              NULL,       
              TITLE,
              FIRSTNAME,
              LASTNAME,
              EMAIL,
              CAST (NULL AS VARCHAR2(20)) AS ROLE ,
              CAST (NULL AS VARCHAR2(20)) AS JOB_TITLE,
              CAST (NULL AS VARCHAR2(20)) AS ORGANIZATION,
              CAST (NULL AS VARCHAR2(20)) AS ORG_TYPE,
              ISO_COUNTRY_CODE            AS COUNTRY_CODE,
              OWNER,
              ORIG_PARTY_REF                                                      AS CAPRI_ID,
              CAST (NULL AS VARCHAR2(20))                                         AS SIS_ID,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_BUSINESS_DIVISION,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_SALES_DIVISION,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_ACCOUNT_ORG_TYPE,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_ACCOUNT_NAME,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_CONTACT_TYPE,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_CONTACT_TITLE,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_CONTACT_FIRSTNAME,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_CONTACT_LASTNAME,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_CONTACT_EMAIL,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_CONTACT_PHONE,
              CAST (NULL AS VARCHAR2(20))                                         AS LANGUAGE,
              USER_GROUP                                                          AS FLEX1,
              ''Accepted''                                                        AS FLEX2,
              JOURNAL_ISSN                                                        AS FLEX3,
              ARTICLE_PII                                                         AS FLEX4,
              ARTICLE_ID                                                          AS FLEX5,
              ARTICLE_DOI                                                         AS FLEX6,
              REPLACE(REPLACE(REPLACE(ARTICLE_NAME,''"'',''""''),''”'',''""''), ''“'',''""'') AS FLEX7,
              JOURNAL_NAME                                                        AS FLEX8,
              PMG                                                                 AS FLEX9,
              JOURNAL_BU                                         				  AS FLEX10,
              Item_URL                                                            AS FLEX11,
              article_acceptance_date                                             AS FLEX12,
              CAST (NULL AS VARCHAR2(20))                                         AS FLEX13,
              CAST (NULL AS VARCHAR2(20))                                         AS FLEX14,
              CAST (NULL AS VARCHAR2(20))                                         AS FLEX15,
              CAST (NULL AS VARCHAR2(20))                                         AS LL_FIRSTNAME,
              CAST (NULL AS VARCHAR2(20))                                         AS LL_LASTNAME,
              CAST (NULL AS VARCHAR2(20))                                         AS LL_ORGANISATION,
              CAST (null as varchar2(20)) 					      				  AS CMX_ID,
			  LEGAL_ENTITY 
            FROM tbl_temp_jrbi_accepted_capri';
            COMMIT;

processLog('processCAPRIAcceptedData','Information','Completed Procedure');    

EXCEPTION WHEN origin_loaded_today THEN
    processLog('processCAPRIAcceptedData','Error','Data already exists for today');
          WHEN OTHERS THEN 
    vErrorMessage := SQLERRM;
    processLog('processCAPRIAcceptedData','Error',vErrorMessage);
END processCAPRIAcceptedData;


PROCEDURE processCAPRIRejectedData(pOverrideCheckingTodaysData IN VARCHAR2 DEFAULT 'FALSE')
IS

BEGIN

processLog('processCAPRIRejectedData','Information','Started Procedure'); 

IF upper(pOverrideCheckingTodaysData) <> 'TRUE' THEN
    vDataRecords := checkOriginForToday('Rejected_CAPRIData');
    IF vDataRecords > 0 THEN
         RAISE origin_loaded_today;
    END IF;    
END IF;    

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE tbl_temp_jrbi_rejected_capri purge';
EXCEPTION WHEN OTHERS THEN NULL;
END;

 EXECUTE IMMEDIATE '
CREATE TABLE tbl_temp_jrbi_rejected_capri AS
select distinct
		  a.party_id,
          a.dedupe_id,
          a.orig_party_ref,
          ''JRBI'' AS user_group,
          a.title,
          a.firstname,
          a.lastname, 
		  a.title || a.firstname || a.lastname as fullname,
          a.email,
          i.orig_item_ref    AS article_id,
          i.item_milestone   AS article_status,
          i.added_date AS article_rejected_date,
          i.imprint AS doc_ms_no,
		  CASE
            WHEN i.added_date between trunc(sysdate) - 7 and trunc(sysdate)
            THEN ''Y''
            ELSE ''N''
          END AS date_flag,
          ''Y'' AS article_rejected ,
		  i.class       AS publication_item,
          i.code AS article_doi,
		  CASE
            WHEN SUBSTR(i.identifier,1,1) = ''S''
            THEN SUBSTR(regexp_replace(i.identifier,''[()-]''),1)
            ELSE regexp_replace(i.identifier,''[()-]'')
          END                                       AS article_pii,
		  i.name             AS article_name,
		  substr(j.identifier,1,4)||''-''||substr(j.identifier,5,8) as journal_issn  ,
		  j.name             AS journal_name,
          ''STMJ''          AS OWNER,
          a.ISO_COUNTRY_CODE AS ISO_COUNTRY_CODE,	
		  j.pmg_code as pmg,
		  j.pmg_descr,
		  a.CMX_ID as CMX_ID,
		  j.show_status as Journal_BU,
		  j.parent_ref as parent_ref,
		  ''Elsevier BV''    AS LEGAL_ENTITY
from
dbowner.tbl_parties a 
join dbowner.tbl_items i
on a.orig_party_ref = i.authors
and a.orig_system = ''JRBI'' and i.orig_system = ''JRBI'' and i.item_type = ''ART''
and a.email is not null
and lower(i.item_milestone) = ''reject''
and (i.init_pub_date            IS  NULL
and  i.last_pub_date            IS  NULL) 
and trunc(i.added_date) between trunc(sysdate) - 7 and trunc(sysdate)
join dbowner.tbl_items j 
on i.parent_ref = j.orig_item_ref
and j.item_type = ''JOU'' and j.orig_system = ''JRBI'' ';

/* MARKAU-7310 Include co-authors in script, PIT code3s should be: TBC */

EXECUTE IMMEDIATE '
insert into tbl_temp_jrbi_rejected_capri 
select distinct
		  a.party_id,
          a.dedupe_id,
          a.orig_party_ref,
          ''JRBI'' AS user_group,
          a.title,
          a.firstname,
          a.lastname, 
		  a.title || a.firstname || a.lastname as fullname,
          a.email,
          i.orig_item_ref    AS article_id,
          i.item_milestone   AS article_status,
          i.added_date AS article_rejected_date,
          i.imprint AS doc_ms_no,
		  CASE
            WHEN i.added_date between trunc(sysdate) - 7 and trunc(sysdate)
            THEN ''Y''
            ELSE ''N''
          END AS date_flag,
          ''Y'' AS article_rejected ,
		  i.class       AS publication_item,
          i.code AS article_doi,
		  CASE
            WHEN SUBSTR(i.identifier,1,1) = ''S''
            THEN SUBSTR(regexp_replace(i.identifier,''[()-]''),1)
            ELSE regexp_replace(i.identifier,''[()-]'')
          END                                       AS article_pii,
		  i.name             AS article_name,
		  substr(j.identifier,1,4)||''-''||substr(j.identifier,5,8) as journal_issn  ,
		  j.name             AS journal_name,
          ''STMJ''          AS OWNER,
          a.ISO_COUNTRY_CODE AS ISO_COUNTRY_CODE,	
		  j.pmg_code as pmg,
		  j.pmg_descr,
		  a.CMX_ID as CMX_ID,
		  j.show_status as Journal_BU,
		  j.parent_ref as parent_ref,
		  ''Elsevier BV''    AS LEGAL_ENTITY
from
dbowner.tbl_parties a
join dbowner.tbl_interests u
on a.orig_party_ref = u.party_ref 
and a.orig_system = ''JRBI'' and u.orig_system = ''JRBI'' and u.interest_type = ''ART''
and a.email is not null
and a.email not in (select email from tbl_temp_jrbi_rejected_capri where user_group = ''JRBI'')
join dbowner.tbl_items i
on u.interest_value = i.orig_item_ref
and a.orig_system = ''JRBI'' and i.orig_system = ''JRBI'' and i.item_type = ''ART''
and lower(i.item_milestone) = ''reject''
and trunc(i.issue_date) between trunc(sysdate) - 7 and trunc(sysdate)
join dbowner.tbl_items j 
on i.parent_ref = j.orig_item_ref
and j.item_type = ''JOU'' and j.orig_system = ''JRBI'' ';

vDataLoadID := getLoadID;

EXECUTE IMMEDIATE '
INSERT INTO TBL_MDT_JRBI_MASTER_DATA
    SELECT  DISTINCT
            '||vDataLoadID||',
              ''Rejected_CAPRIData'',
              SYSDATE,
              NULL,
              ''N'',
              NULL,
              NULL,       
              TITLE,
              FIRSTNAME,
              LASTNAME,
              EMAIL,
              CAST (NULL AS VARCHAR2(20)) AS ROLE ,
              CAST (NULL AS VARCHAR2(20)) AS JOB_TITLE,
              CAST (NULL AS VARCHAR2(20)) AS ORGANIZATION,
              CAST (NULL AS VARCHAR2(20)) AS ORG_TYPE,
              ISO_COUNTRY_CODE            AS COUNTRY_CODE,
              OWNER,
              ORIG_PARTY_REF                                                      AS CAPRI_ID,
              CAST (NULL AS VARCHAR2(20))                                         AS SIS_ID,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_BUSINESS_DIVISION,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_SALES_DIVISION,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_ACCOUNT_ORG_TYPE,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_ACCOUNT_NAME,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_CONTACT_TYPE,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_CONTACT_TITLE,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_CONTACT_FIRSTNAME,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_CONTACT_LASTNAME,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_CONTACT_EMAIL,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_CONTACT_PHONE,
              CAST (NULL AS VARCHAR2(20))                                         AS LANGUAGE,
              USER_GROUP                                                          AS FLEX1,
              ''Rejected''                                                        AS FLEX2,
              JOURNAL_ISSN                                                        AS FLEX3,
              ARTICLE_PII                                                         AS FLEX4,
              ARTICLE_ID                                                          AS FLEX5,
              ARTICLE_DOI                                                         AS FLEX6,
              REPLACE(REPLACE(REPLACE(ARTICLE_NAME,''"'',''""''),''”'',''""''), ''“'',''""'') AS FLEX7,
              JOURNAL_NAME                                                        AS FLEX8,
              PMG                                                                 AS FLEX9,
              JOURNAL_BU                                         				  AS FLEX10,
              parent_ref                                         				  AS FLEX11,
              article_rejected_date                                               AS FLEX12,
              doc_ms_no                                                           AS FLEX13,
              CAST (NULL AS VARCHAR2(20))                                         AS FLEX14,
              CAST (NULL AS VARCHAR2(20))                                         AS FLEX15,
              CAST (NULL AS VARCHAR2(20))                                         AS LL_FIRSTNAME,
              CAST (NULL AS VARCHAR2(20))                                         AS LL_LASTNAME,
              CAST (NULL AS VARCHAR2(20))                                         AS LL_ORGANISATION,
              CAST (null as varchar2(20)) 					      				  AS CMX_ID,
			  LEGAL_ENTITY
            FROM tbl_temp_jrbi_rejected_capri' ; 

commit;

processLog('processCAPRIRejectedData','Information','Completed Procedure');    

EXCEPTION WHEN origin_loaded_today THEN
    processLog('processCAPRIRejectedData','Error','Data already exists for today');
          WHEN OTHERS THEN 
    vErrorMessage := SQLERRM;
    processLog('processCAPRIRejectedData','Error',vErrorMessage);
END processCAPRIRejectedData;


PROCEDURE processCAPRIPublishedData(pOverrideCheckingTodaysData IN VARCHAR2 DEFAULT 'FALSE')
IS

BEGIN

processLog('processCAPRIPublishedData','Information','Started Procedure'); 


IF upper(pOverrideCheckingTodaysData) <> 'TRUE' THEN
    vDataRecords := checkOriginForToday('Published_CAPRIData');
    IF vDataRecords > 0 THEN
         RAISE origin_loaded_today;
    END IF;    
END IF;


BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE tbl_temp_jrbi_published_capri purge';
EXCEPTION WHEN OTHERS THEN NULL;
END;

/* MARKAU-7310  check PIT codes are FLA, REV, SCO */

 EXECUTE IMMEDIATE '
CREATE TABLE tbl_temp_jrbi_published_capri AS
select distinct
a.party_id,
          a.dedupe_id,
          a.orig_party_ref,
          ''JRBI'' AS user_group,
          a.title,
          a.firstname,
          a.lastname,
          a.email,
          i.orig_item_ref    AS article_id,
        i.item_milestone  AS item_milestone,
		i.issue_milestone AS issue_milestone,
		i.last_pub_date   AS last_pub_date,
		i.init_pub_date   AS init_pub_date,
		  CASE
            WHEN i.last_pub_date between trunc(sysdate) - 7 and trunc(sysdate)
            THEN ''Y''
            ELSE ''N''
          END AS date_flag,
          ''Y'' AS article_published ,
		  i.class       AS publication_item,
          i.code AS article_doi,
		  CASE
            WHEN SUBSTR(i.identifier,1,1) = ''S''
            THEN SUBSTR(regexp_replace(i.identifier,''[()-]''),1)
            ELSE regexp_replace(i.identifier,''[()-]'')
          END                                       AS article_pii,
		  i.name             AS article_name,
		  substr(j.identifier,1,4)||''-''||substr(j.identifier,5,8) as journal_issn  ,
		  j.name             AS journal_name,
          ''STMJ''          AS OWNER,
          a.ISO_COUNTRY_CODE AS ISO_COUNTRY_CODE,	
		  j.pmg_code as pmg,
		  j.pmg_descr,
		  a.CMX_ID as CMX_ID,
		  j.show_status as Journal_BU,
		  i.publisher as Item_URL,
		  ''Elsevier BV''    AS LEGAL_ENTITY
from
dbowner.tbl_parties a 
join dbowner.tbl_items i
on a.orig_party_ref = i.authors
and a.orig_system = ''JRBI'' and i.orig_system = ''JRBI'' and i.item_type = ''ART''
and a.email is not null
and (i.init_pub_date            IS NOT NULL
and i.last_pub_date             IS NOT NULL) 
and lower(i.class) in (''full length article'',''full-length article'',''review article'',''short communication'')
and trunc(i.last_pub_date) between trunc(sysdate) - 7 and trunc(sysdate)
join dbowner.tbl_items j 
on i.parent_ref = j.orig_item_ref
and j.item_type = ''JOU'' and j.orig_system = ''JRBI'' ';

EXECUTE IMMEDIATE '
insert into tbl_temp_jrbi_published_capri 
select distinct
		  a.party_id,
          a.dedupe_id,
          a.orig_party_ref,
          ''JRBI'' AS user_group,
          a.title,
          a.firstname,
          a.lastname,
          a.email,
          i.orig_item_ref    AS article_id,
           i.item_milestone  AS item_milestone,
		i.issue_milestone AS issue_milestone,
		i.last_pub_date   AS last_pub_date,
		i.init_pub_date   AS init_pub_date,
		  CASE
            WHEN i.last_pub_date between trunc(sysdate) - 7 and trunc(sysdate)
            THEN ''Y''
            ELSE ''N''
          END AS date_flag,
          ''Y'' AS article_Published ,
		  i.class       AS publication_item,
          i.code        AS article_doi,
		  CASE
            WHEN SUBSTR(i.identifier,1,1) = ''S''
            THEN SUBSTR(regexp_replace(i.identifier,''[()-]''),1)
            ELSE regexp_replace(i.identifier,''[()-]'')
          END                                       AS article_pii,
		  i.name             AS article_name,
		  substr(j.identifier,1,4)||''-''||substr(j.identifier,5,8) as journal_issn  ,
		  j.name             AS journal_name,
          ''STMJ''          AS OWNER,
          a.ISO_COUNTRY_CODE AS ISO_COUNTRY_CODE,	
		  j.pmg_code as pmg,
		  j.pmg_descr,
		  a.CMX_ID as CMX_ID,
		  j.show_status as Journal_BU,
		  i.publisher as Item_URL,
		  ''Elsevier BV''    AS LEGAL_ENTITY
from
dbowner.tbl_parties a
join dbowner.tbl_interests u
on a.orig_party_ref = u.party_ref 
and a.orig_system = ''JRBI'' and u.orig_system = ''JRBI'' and u.interest_type = ''ART''
and a.email is not null
and a.email not in (select email from tbl_temp_jrbi_published_capri where user_group = ''JRBI'')
join dbowner.tbl_items i
on u.interest_value = i.orig_item_ref
and a.orig_system = ''JRBI'' and i.orig_system = ''JRBI'' and i.item_type = ''ART''
and (i.init_pub_date            IS NOT NULL
and i.last_pub_date             IS NOT NULL) 
and lower(i.class) in (''full length article'',''full-length article'',''review article'',''short communication'')
and trunc(i.last_pub_date) between trunc(sysdate) - 7 and trunc(sysdate)
join dbowner.tbl_items j 
on i.parent_ref = j.orig_item_ref
and j.item_type = ''JOU'' and j.orig_system = ''JRBI'' ' ; 

COMMIT;


vDataLoadID := getLoadID;

EXECUTE IMMEDIATE '
INSERT INTO TBL_MDT_JRBI_MASTER_DATA
    SELECT  DISTINCT
            '||vDataLoadID||' as DATA_LOAD_ID,
              ''Published_CAPRIData'' as DATA_ORIGIN,
              SYSDATE,
              NULL,
              ''N'',
              NULL as TO_BE_EXPORTED,
              NULL as EXPORTED,       
              TITLE,
              FIRSTNAME,
              LASTNAME,
              EMAIL,
              CAST (NULL AS VARCHAR2(20)) AS ROLE ,
              CAST (NULL AS VARCHAR2(20)) AS JOB_TITLE,
              CAST (NULL AS VARCHAR2(20)) AS ORGANIZATION,
              CAST (NULL AS VARCHAR2(20)) AS ORG_TYPE,
              ISO_COUNTRY_CODE            AS COUNTRY_CODE,
              OWNER,
              ORIG_PARTY_REF                                                      AS CAPRI_ID,
              CAST (NULL AS VARCHAR2(20))                                         AS SIS_ID,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_BUSINESS_DIVISION,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_SALES_DIVISION,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_ACCOUNT_ORG_TYPE,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_ACCOUNT_NAME,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_CONTACT_TYPE,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_CONTACT_TITLE,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_CONTACT_FIRSTNAME,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_CONTACT_LASTNAME,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_CONTACT_EMAIL,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_CONTACT_PHONE,
              CAST (NULL AS VARCHAR2(20))                                         AS LANGUAGE,
              USER_GROUP                                                          AS FLEX1,
              ''Published''                                                        AS FLEX2,
              JOURNAL_ISSN                                                        AS FLEX3,
              ARTICLE_PII                                                         AS FLEX4,
              ARTICLE_ID                                                          AS FLEX5,
              ARTICLE_DOI                                                         AS FLEX6,
              REPLACE(REPLACE(REPLACE(ARTICLE_NAME,''"'',''""''),''”'',''""''), ''“'',''""'') AS FLEX7,
              JOURNAL_NAME                                                        AS FLEX8,
              PMG                                                                 AS FLEX9,
              JOURNAL_BU                                         				  AS FLEX10,
              Item_URL                                         					  AS FLEX11,
              last_pub_date                                         			  AS FLEX12,
              CAST (NULL AS VARCHAR2(20))                                         AS FLEX13,
              CAST (NULL AS VARCHAR2(20))                                         AS FLEX14,
              CAST (NULL AS VARCHAR2(20))                                         AS FLEX15,
              CAST (NULL AS VARCHAR2(20))                                         AS LL_FIRSTNAME,
              CAST (NULL AS VARCHAR2(20))                                         AS LL_LASTNAME,
              CAST (NULL AS VARCHAR2(20))                                         AS LL_ORGANISATION,
              CAST (null as varchar2(20)) 					      				  AS CMX_ID,
			  LEGAL_ENTITY
            FROM tbl_temp_jrbi_published_capri ' ; 
COMMIT;

processLog('processCAPRIPublishedData','Information','Completed Procedure');    

EXCEPTION WHEN origin_loaded_today THEN
    processLog('processCAPRIPublishedData','Error','Data already exists for today');
          WHEN OTHERS THEN 
    vErrorMessage := SQLERRM;
    processLog('processCAPRIPublishedData','Error',vErrorMessage);
END processCAPRIPublishedData;

PROCEDURE processCAPRIMendeleyStatsData(pOverrideCheckingTodaysData IN VARCHAR2 DEFAULT 'FALSE')
IS

BEGIN

processLog('processCAPRIMendeleyStatsData','Information','Started Procedure'); 

IF upper(pOverrideCheckingTodaysData) <> 'TRUE' THEN
    vDataRecords := checkOriginForToday('MendeleyStats_JRBI_CAPRIData');
    IF vDataRecords > 0 THEN
         RAISE origin_loaded_today;
    END IF;    
END IF;

BEGIN
      EXECUTE IMMEDIATE 'DROP TABLE tbl_temp_MendeleyStats_capri purge';
EXCEPTION WHEN OTHERS THEN NULL;
END;

/* MARKAU-7310 check PIT codes are FLA, REV, SCO */

      EXECUTE IMMEDIATE '
CREATE TABLE tbl_temp_MendeleyStats_capri AS
select distinct
a.party_id,
          a.dedupe_id,
          a.orig_party_ref,
          ''JRBI'' AS user_group,
          a.title,
          a.firstname,
          a.lastname,
          a.email,
          i.orig_item_ref    AS article_id,
        i.item_milestone  AS item_milestone,
		i.issue_milestone AS issue_milestone,
		i.last_pub_date   AS last_pub_date,
		i.init_pub_date   AS init_pub_date,
		  CASE
            WHEN i.last_pub_date between trunc(sysdate) - 14 and trunc(sysdate) - 8
            THEN ''Y''
            ELSE ''N''
          END AS date_flag,
          ''Y'' AS article_accepted ,
		  i.class       AS publication_item,
          i.code AS article_doi,
		  CASE
            WHEN SUBSTR(i.identifier,1,1) = ''S''
            THEN SUBSTR(regexp_replace(i.identifier,''[()-]''),1)
            ELSE regexp_replace(i.identifier,''[()-]'')
          END                                       AS article_pii,
		  i.name             AS article_name,
		  substr(j.identifier,1,4)||''-''||substr(j.identifier,5,8) as journal_issn  ,
		  j.name             AS journal_name,
          ''STMJ''          AS OWNER,
          a.ISO_COUNTRY_CODE AS ISO_COUNTRY_CODE,	
		  j.pmg_code as pmg,
		  j.pmg_descr,
		  a.CMX_ID as CMX_ID,
		  j.show_status as Journal_BU,
		  i.publisher as Item_URL,
		  ''Elsevier BV''    AS LEGAL_ENTITY
from
dbowner.tbl_parties a 
join dbowner.tbl_items i
on a.orig_party_ref = i.authors
and a.orig_system = ''JRBI'' and i.orig_system = ''JRBI'' and i.item_type = ''ART''
and a.email is not null
and (i.init_pub_date            IS NOT NULL
and i.last_pub_date             IS NOT NULL) 
and lower(i.class) in (''full length article'',''full-length article'',''review article'',''short communication'')
and trunc(i.last_pub_date) between trunc(sysdate) - 14 and trunc(sysdate) - 8
join dbowner.tbl_items j 
on i.parent_ref = j.orig_item_ref
and j.item_type = ''JOU'' and j.orig_system = ''JRBI'' ';

EXECUTE IMMEDIATE '
insert into tbl_temp_MendeleyStats_capri 
select distinct
		  a.party_id,
          a.dedupe_id,
          a.orig_party_ref,
          ''JRBI'' AS user_group,
          a.title,
          a.firstname,
          a.lastname,
          a.email,
          i.orig_item_ref    AS article_id,
           i.item_milestone  AS item_milestone,
		i.issue_milestone AS issue_milestone,
		i.last_pub_date   AS last_pub_date,
		i.init_pub_date   AS init_pub_date,
		  CASE
            WHEN i.last_pub_date between trunc(sysdate) - 14 and trunc(sysdate) - 8
            THEN ''Y''
            ELSE ''N''
          END AS date_flag,
          ''Y'' AS article_Published ,
		  i.class       AS publication_item,
          i.code        AS article_doi,
		  CASE
            WHEN SUBSTR(i.identifier,1,1) = ''S''
            THEN SUBSTR(regexp_replace(i.identifier,''[()-]''),1)
            ELSE regexp_replace(i.identifier,''[()-]'')
          END                                       AS article_pii,
		  i.name             AS article_name,
		  substr(j.identifier,1,4)||''-''||substr(j.identifier,5,8) as journal_issn  ,
		  j.name             AS journal_name,
          ''STMJ''          AS OWNER,
          a.ISO_COUNTRY_CODE AS ISO_COUNTRY_CODE,	
		  j.pmg_code as pmg,
		  j.pmg_descr,
		  a.CMX_ID as CMX_ID,
		  j.show_status as Journal_BU,
		  i.publisher as Item_URL,
		  ''Elsevier BV''    AS LEGAL_ENTITY
from
dbowner.tbl_parties a
join dbowner.tbl_interests u
on a.orig_party_ref = u.party_ref 
and a.orig_system = ''JRBI'' and u.orig_system = ''JRBI'' and u.interest_type = ''ART''
and a.email not in (select email from tbl_temp_MendeleyStats_capri where user_group = ''JRBI'')
join dbowner.tbl_items i
on u.interest_value = i.orig_item_ref
and a.orig_system = ''JRBI'' and i.orig_system = ''JRBI'' and i.item_type = ''ART''
and (i.init_pub_date            IS NOT NULL
and i.last_pub_date             IS NOT NULL) 
and lower(i.class) in (''full length article'',''full-length article'',''review article'',''short communication'')
and trunc(i.last_pub_date) between trunc(sysdate) - 14 and trunc(sysdate) - 8
join dbowner.tbl_items j 
on i.parent_ref = j.orig_item_ref
and j.item_type = ''JOU'' and j.orig_system = ''JRBI'' ' ; 

COMMIT;

vDataLoadID := getLoadID;

EXECUTE IMMEDIATE '
INSERT INTO TBL_MDT_JRBI_MASTER_DATA
    SELECT  DISTINCT
            '||vDataLoadID||' as DATA_LOAD_ID,
              ''MendeleyStats_JRBI_CAPRIData'' as DATA_ORIGIN,
              SYSDATE,
              NULL,
              ''N'',
              NULL as TO_BE_EXPORTED,
              NULL as EXPORTED,       
              TITLE,
              FIRSTNAME,
              LASTNAME,
              EMAIL,
              CAST (NULL AS VARCHAR2(20)) AS ROLE ,
              CAST (NULL AS VARCHAR2(20)) AS JOB_TITLE,
              CAST (NULL AS VARCHAR2(20)) AS ORGANIZATION,
              CAST (NULL AS VARCHAR2(20)) AS ORG_TYPE,
              ISO_COUNTRY_CODE            AS COUNTRY_CODE,
              OWNER,
              ORIG_PARTY_REF                                                      AS CAPRI_ID,
              CAST (NULL AS VARCHAR2(20))                                         AS SIS_ID,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_BUSINESS_DIVISION,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_SALES_DIVISION,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_ACCOUNT_ORG_TYPE,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_ACCOUNT_NAME,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_CONTACT_TYPE,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_CONTACT_TITLE,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_CONTACT_FIRSTNAME,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_CONTACT_LASTNAME,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_CONTACT_EMAIL,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_CONTACT_PHONE,
              CAST (NULL AS VARCHAR2(20))                                         AS LANGUAGE,
              USER_GROUP                                                          AS FLEX1,
              ''MendeleyStats''                                                        AS FLEX2,
              JOURNAL_ISSN                                                        AS FLEX3,
              ARTICLE_PII                                                         AS FLEX4,
              ARTICLE_ID                                                          AS FLEX5,
              ARTICLE_DOI                                                         AS FLEX6,
              REPLACE(REPLACE(REPLACE(ARTICLE_NAME,''"'',''""''),''”'',''""''), ''“'',''""'') AS FLEX7,
              JOURNAL_NAME                                                        AS FLEX8,
              PMG                                                                 AS FLEX9,
              JOURNAL_BU                                         				  AS FLEX10,
              Item_URL                                         					  AS FLEX11,
              last_pub_date                                         			  AS FLEX12,
              CAST (NULL AS VARCHAR2(20))                                         AS FLEX13,
              CAST (NULL AS VARCHAR2(20))                                         AS FLEX14,
              CAST (NULL AS VARCHAR2(20))                                         AS FLEX15,
              CAST (NULL AS VARCHAR2(20))                                         AS LL_FIRSTNAME,
              CAST (NULL AS VARCHAR2(20))                                         AS LL_LASTNAME,
              CAST (NULL AS VARCHAR2(20))                                         AS LL_ORGANISATION,
              CAST (null as varchar2(20)) 					      				  AS CMX_ID,
			  LEGAL_ENTITY
            FROM tbl_temp_MendeleyStats_capri ' ; 
COMMIT;

processLog('processCAPRIMendeleyStatsData','Information','Completed Procedure');  

EXCEPTION WHEN origin_loaded_today THEN
    processLog('processCAPRIMendeleyStatsData','Error','Data already exists for today');
          WHEN OTHERS THEN 
    vErrorMessage := SQLERRM;
    processLog('processCAPRIMendeleyStatsData','Error',vErrorMessage);
END processCAPRIMendeleyStatsData;

PROCEDURE processCAPRIWebPublishedData(pOverrideCheckingTodaysData IN VARCHAR2 DEFAULT 'FALSE')

IS 

BEGIN 

processLog('processCAPRIWebPublishedData','Information','Started Procedure');  

IF upper(pOverrideCheckingTodaysData) <> 'TRUE' THEN
    vDataRecords := checkOriginForToday('WebPublished_CapriData');
    IF vDataRecords > 0 THEN
         RAISE origin_loaded_today;
    END IF;     
END IF;

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE tbl_temp_web_published_capri purge';
    EXCEPTION WHEN OTHERS THEN NULL;
END;

/* MARKAU-7310 PIT should be FLA, REV, SCO, SSU and incluede co-authors & remove flex13 from export
 filter based on author marketing_emails flag = Y */

EXECUTE IMMEDIATE '
CREATE TABLE tbl_temp_web_published_capri AS
select distinct
a.party_id,
          a.dedupe_id,
          a.orig_party_ref,
          ''JRBI'' AS user_group,
          a.title,
          a.firstname,
          a.lastname,
          a.email,
          a.marketing_emails as marketing_emails,
          i.orig_item_ref    AS article_id,
        i.item_milestone  AS item_milestone,
		i.issue_milestone AS Item_ID,
		i.last_pub_date   AS last_pub_date,
		i.init_pub_date   AS init_pub_date,
		  CASE
            WHEN i.last_pub_date between trunc(sysdate) - 7 and trunc(sysdate)
            THEN ''Y''
            ELSE ''N''
          END AS date_flag,
          ''Y'' AS article_published ,
		  i.class       AS publication_item,
          i.code AS ITEM_DOI,
		  CASE
            WHEN SUBSTR(i.identifier,1,1) = ''S''
            THEN SUBSTR(regexp_replace(i.identifier,''[()-]''),1)
            ELSE regexp_replace(i.identifier,''[()-]'')
          END                                       AS article_pii,
		  i.name             AS article_name,
		  substr(j.identifier,1,4)||''-''||substr(j.identifier,5,8) as journal_issn  ,
		  j.name             AS journal_name,
          ''STMJ''          AS OWNER,
          a.ISO_COUNTRY_CODE AS ISO_COUNTRY_CODE,
		  a.ORIG_COUNTRY as COUNTRY_NAME,		  
		  j.pmg_code as pmg,
		  j.pmg_descr,
		  a.CMX_ID as CMX_ID,
		  j.show_status as Journal_BU,
		  i.publisher as Item_URL,
          i.last_pub_date as Item_Reg_Date,
		  i.parent_ref as JOURNAL_CODE,
		  ''Elsevier BV''    AS LEGAL_ENTITY
from
dbowner.tbl_parties a 
join dbowner.tbl_items i
on a.orig_party_ref = i.authors
and a.orig_system = ''JRBI'' and i.orig_system = ''JRBI'' and i.item_type = ''ART''
and a.email is not null
and a.marketing_emails = ''Y''
and (i.init_pub_date            IS NOT NULL
and i.last_pub_date             IS NOT NULL) 
and lower(i.class) in (''full length article'',''full-length article'',''review article'',''short communication'',''short review'',''short survey'') 
and trunc(i.last_pub_date) between trunc(sysdate) - 7 and trunc(sysdate)
join dbowner.tbl_items j 
on i.parent_ref = j.orig_item_ref
and j.item_type = ''JOU'' and j.orig_system = ''JRBI'' ';

EXECUTE IMMEDIATE '
insert into tbl_temp_web_published_capri 
select distinct
a.party_id,
          a.dedupe_id,
          a.orig_party_ref,
          ''JRBI'' AS user_group,
          a.title,
          a.firstname,
          a.lastname,
          a.email,
          a.marketing_emails as marketing_emails,
          i.orig_item_ref    AS article_id,
        i.item_milestone  AS item_milestone,
		i.issue_milestone AS Item_ID,
		i.last_pub_date   AS last_pub_date,
		i.init_pub_date   AS init_pub_date,
		  CASE
            WHEN i.last_pub_date between trunc(sysdate) - 7 and trunc(sysdate)
            THEN ''Y''
            ELSE ''N''
          END AS date_flag,
          ''Y'' AS article_published ,
		  i.class       AS publication_item,
          i.code AS ITEM_DOI,
		  CASE
            WHEN SUBSTR(i.identifier,1,1) = ''S''
            THEN SUBSTR(regexp_replace(i.identifier,''[()-]''),1)
            ELSE regexp_replace(i.identifier,''[()-]'')
          END                                       AS article_pii,
		  i.name             AS article_name,
		  substr(j.identifier,1,4)||''-''||substr(j.identifier,5,8) as journal_issn  ,
		  j.name             AS journal_name,
          ''STMJ''          AS OWNER,
          a.ISO_COUNTRY_CODE AS ISO_COUNTRY_CODE,
		  a.ORIG_COUNTRY as COUNTRY_NAME,		  
		  j.pmg_code as pmg,
		  j.pmg_descr,
		  a.CMX_ID as CMX_ID,
		  j.show_status as Journal_BU,
		  i.publisher as Item_URL,
          i.last_pub_date as Item_Reg_Date,
		  i.parent_ref as JOURNAL_CODE,
		  ''Elsevier BV''    AS LEGAL_ENTITY
          FROM
dbowner.tbl_parties a
join dbowner.tbl_interests u
on a.orig_party_ref = u.party_ref 
and a.orig_system = ''JRBI'' and u.orig_system = ''JRBI'' and u.interest_type = ''ART''
and a.email is not null
and a.email not in (select email from tbl_temp_web_published_capri where user_group = ''JRBI'')
and a.marketing_emails = ''Y''
join dbowner.tbl_items i
on u.interest_value = i.orig_item_ref
and a.orig_system = ''JRBI'' and i.orig_system = ''JRBI'' and i.item_type = ''ART''
and (i.init_pub_date            IS NOT NULL
and i.last_pub_date             IS NOT NULL) 
and trunc(i.issue_date) between trunc(sysdate) - 7 and trunc(sysdate)
and lower(i.class) in (''full length article'',''full-length article'',''review article'',''short communication'',''short review'',''short survey'') 
join dbowner.tbl_items j 
on i.parent_ref = j.orig_item_ref
and j.item_type = ''JOU'' and j.orig_system = ''JRBI''  ';

commit;

vDataLoadID := getLoadID;

EXECUTE IMMEDIATE '
   INSERT INTO TBL_MDT_JRBI_MASTER_DATA
    SELECT  DISTINCT
            '||vDataLoadID||',
              ''WebPublished_CapriData'',
              SYSDATE,
              NULL,
              ''N'',
              NULL,
              NULL,       
              TITLE,
              FIRSTNAME,
              LASTNAME,
              EMAIL,
              CAST (NULL AS VARCHAR2(20)) AS ROLE ,
              CAST (NULL AS VARCHAR2(20)) AS JOB_TITLE,
              CAST (NULL AS VARCHAR2(20)) AS ORGANIZATION,
              CAST (NULL AS VARCHAR2(20)) AS ORG_TYPE,
              ISO_COUNTRY_CODE            AS COUNTRY_CODE,
              OWNER,
              ORIG_PARTY_REF                                                      AS CAPRI_ID,
              CAST (NULL AS VARCHAR2(20))                                         AS SIS_ID,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_BUSINESS_DIVISION,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_SALES_DIVISION,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_ACCOUNT_ORG_TYPE,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_ACCOUNT_NAME,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_CONTACT_TYPE,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_CONTACT_TITLE,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_CONTACT_FIRSTNAME,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_CONTACT_LASTNAME,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_CONTACT_EMAIL,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_CONTACT_PHONE,
              CAST (NULL AS VARCHAR2(20))                                         AS LANGUAGE,
              USER_GROUP                                                          AS FLEX1,
              ''WebshopPublished''                                                AS FLEX2,
              JOURNAL_ISSN                                                        AS FLEX3,
              ARTICLE_PII                                                         AS FLEX4,
              ARTICLE_ID                                                          AS FLEX5,
              ITEM_DOI                                                            AS FLEX6,
              REPLACE(REPLACE(REPLACE(ARTICLE_NAME,''"'',''""''),''”'',''""''), ''“'',''""'') AS FLEX7,
              JOURNAL_NAME                                                        AS FLEX8,
              PMG                                                                 AS FLEX9,
              JOURNAL_BU                                         				  AS FLEX10,
              ITEM_ID                                                             AS FLEX11,
              ITEM_REG_DATE                                                       AS FLEX12,
              ''Y''                                                               AS FLEX13,
              JOURNAL_CODE                                                        AS FLEX14,
              CAST (NULL AS VARCHAR2(20))                                         AS FLEX15,
              CAST (NULL AS VARCHAR2(20))                                         AS LL_FIRSTNAME,
              CAST (NULL AS VARCHAR2(20))                                         AS LL_LASTNAME,
              CAST (NULL AS VARCHAR2(20))                                         AS LL_ORGANISATION,
              CAST (null as varchar2(20)) 					      				  AS CMX_ID,
			  LEGAL_ENTITY
            FROM tbl_temp_web_published_capri';
            COMMIT;

processLog('processCAPRIWebPublishedData','Information','Completed Procedure');    

EXCEPTION WHEN origin_loaded_today THEN
    processLog('processCAPRIWebPublishedData','Error','Data already exists for today');
          WHEN OTHERS THEN 
    vErrorMessage := SQLERRM;
    processLog('processCAPRIWebPublishedData','Error',vErrorMessage);
END processCAPRIWebPublishedData;

PROCEDURE processCAPRIDatainBriefData(pOverrideCheckingTodaysData IN VARCHAR2 DEFAULT 'FALSE')
IS 

BEGIN 

processLog('processCAPRIDatainBriefData','Information','Started Procedure');  

IF upper(pOverrideCheckingTodaysData) <> 'TRUE' THEN
    vDataRecords := checkOriginForToday('DatainBrief_CapriData');
    IF vDataRecords > 0 THEN
         RAISE origin_loaded_today;
    END IF;     
END IF;

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE tbl_temp_Datainbrief_capri purge';
    EXCEPTION WHEN OTHERS THEN NULL;
END;

EXECUTE IMMEDIATE  '
create table tbl_temp_Datainbrief_capri as 
select distinct
		  a.party_id,
          a.dedupe_id,
          a.orig_party_ref,
          ''JRBI'' AS user_group,
          a.title,
          a.firstname,
          a.lastname,
          a.email,
          i.orig_item_ref    AS article_id,
          i.item_milestone   AS article_status,
          i.issue_date 		 AS article_acceptance_date,
		  i.imprint          AS doc_ms_no,
		  i.binding			 AS Revision_No,
		  i.added_date		 AS Doc_Status_Date,
		  j.medium as DIB_Site,
		  a.marketing_emails as marketing_emails,
           CASE WHEN trunc(i.issue_date) BETWEEN trunc(sysdate) - 7 and trunc(sysdate)
            THEN ''Y''
            ELSE ''N''
          END AS date_flag,
          ''Y'' AS article_Revised ,
		  i.class       AS publication_item,
          i.code        AS ITEM_DOI,
		  CASE
            WHEN SUBSTR(i.identifier,1,1) = ''S''
            THEN SUBSTR(regexp_replace(i.identifier,''[()-]''),1)
            ELSE regexp_replace(i.identifier,''[()-]'')
          END                                       AS article_pii,
		  i.name             AS article_name,
		  substr(j.identifier,1,4)||''-''||substr(j.identifier,5,8) as journal_issn  ,
		  j.name             AS journal_name,
          ''STMJ''            AS OWNER,
          a.ISO_COUNTRY_CODE AS ISO_COUNTRY_CODE,	
		  j.pmg_code as pmg,
		  j.pmg_descr,
		  a.CMX_ID as CMX_ID,
		  j.show_status as Journal_BU,
		  ''Elsevier BV''    AS LEGAL_ENTITY
		  
from
dbowner.tbl_parties a 
join dbowner.tbl_items i
on a.orig_party_ref = i.authors
and a.orig_system = ''JRBI'' and i.orig_system = ''JRBI'' and i.item_type = ''ART''
and a.email is not null
and ( i.binding = 1 OR i.binding = 2)
and i.show_status in ( ''Revise'',
''Rebuttal'',
''EES Account Upgrade Requested'',
''Minor Revision'',
''Major Revision'',
''Required Reviews Completed'',
''Evise Account Upgrade Pending'' )
and trunc(i.issue_date) between trunc(sysdate) - 7 and trunc(sysdate)
--and lower(i.class) in ( ''full length article'',''full-length article'',''review article'',''short communication'')
join dbowner.tbl_items j 
on i.parent_ref = j.orig_item_ref
and j.item_type = ''JOU'' and j.orig_system = ''JRBI'' ' ;

vDataLoadID := getLoadID;

EXECUTE IMMEDIATE '
   INSERT INTO TBL_MDT_JRBI_MASTER_DATA
    SELECT  DISTINCT
            '||vDataLoadID||',
              ''DatainBrief_CapriData'',
              SYSDATE,
              NULL,
              ''N'',
              NULL,
              NULL,       
              TITLE,
              FIRSTNAME,
              LASTNAME,
              EMAIL,
              CAST (NULL AS VARCHAR2(20)) AS ROLE ,
              CAST (NULL AS VARCHAR2(20)) AS JOB_TITLE,
              CAST (NULL AS VARCHAR2(20)) AS ORGANIZATION,
              CAST (NULL AS VARCHAR2(20)) AS ORG_TYPE,
              ISO_COUNTRY_CODE            AS COUNTRY_CODE,
              OWNER,
              ORIG_PARTY_REF                                                      AS CAPRI_ID,
              CAST (NULL AS VARCHAR2(20))                                         AS SIS_ID,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_BUSINESS_DIVISION,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_SALES_DIVISION,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_ACCOUNT_ORG_TYPE,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_ACCOUNT_NAME,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_CONTACT_TYPE,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_CONTACT_TITLE,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_CONTACT_FIRSTNAME,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_CONTACT_LASTNAME,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_CONTACT_EMAIL,
              CAST (NULL AS VARCHAR2(20))                                         AS CRM_CONTACT_PHONE,
              CAST (NULL AS VARCHAR2(20))                                         AS LANGUAGE,
              USER_GROUP                                                          AS FLEX1,
              ''DatainBrief''                                                     AS FLEX2,
              JOURNAL_ISSN                                                        AS FLEX3,
              ARTICLE_PII                                                         AS FLEX4,
              ARTICLE_ID                                                          AS FLEX5,
              ITEM_DOI                                                            AS FLEX6,
              REPLACE(REPLACE(REPLACE(ARTICLE_NAME,''"'',''""''),''”'',''""''), ''“'',''""'') AS FLEX7,
              JOURNAL_NAME                                                        AS FLEX8,
              marketing_emails                                                    AS FLEX9,
              JOURNAL_BU                                         				  AS FLEX10,
              doc_ms_no                                                           AS FLEX11,
              Revision_No                                                         AS FLEX12,
              7                                                                   AS FLEX13,
              Doc_Status_Date                                                     AS FLEX14,
              DIB_Site                                                            AS FLEX15,
              CAST (NULL AS VARCHAR2(20))                                         AS LL_FIRSTNAME,
              CAST (NULL AS VARCHAR2(20))                                         AS LL_LASTNAME,
              CAST (NULL AS VARCHAR2(20))                                         AS LL_ORGANISATION,
              CAST (null as varchar2(20)) 					      				  AS CMX_ID,
			  LEGAL_ENTITY
            FROM tbl_temp_Datainbrief_capri';
            COMMIT;

processLog('processCAPRIDatainBriefData','Information','Completed Procedure');    

EXCEPTION WHEN origin_loaded_today THEN
    processLog('processCAPRIDatainBriefData','Error','Data already exists for today');
          WHEN OTHERS THEN 
    vErrorMessage := SQLERRM;
    processLog('processCAPRIDatainBriefData','Error',vErrorMessage);
END processCAPRIDatainBriefData;


PROCEDURE processMasterData
IS

CURSOR CurOrigins IS
        SELECT  max(data_load_id) as data_load_id, 
                data_origin
        FROM   TBL_MDT_JRBI_MASTER_DATA
        WHERE   PROCESSED = 'N'  
        GROUP BY data_origin;

vPreviousLoadID NUMBER;

BEGIN

processLog('processMasterData','Information','Started Procedure'); 

FOR C IN CurOrigins LOOP

    --
    -- Mark Duplicates against previous loads
    --
         SELECT nvl(MAX(DATA_LOAD_ID),0)
         INTO   vPreviousLoadID
         FROM   TBL_MDT_JRBI_MASTER_DATA
         WHERE  data_origin = c.data_origin
         AND    data_load_id < c.data_load_id;

         UPDATE TBL_MDT_JRBI_MASTER_DATA a
         SET    DUPLICATE_RECORD = 'Y'
         WHERE  data_load_id = c.data_load_id
         AND    (email, flex3 /*ISSN*/, nvl(flex4,flex7) /*PII*/) IN (SELECT email, flex3 /*ISSN*/, nvl(flex4,flex7) /*PII*/
                                                                FROM TBL_MDT_JRBI_MASTER_DATA b
                                                                WHERE a.data_origin = b.data_origin 
                                                                and b.data_load_id = vPreviousLoadID
                                                                );
         COMMIT;                                                                

    --
    -- Mark Duplicates within the same load for the same origin
    --                                                                
         UPDATE TBL_MDT_JRBI_MASTER_DATA a
         SET    DUPLICATE_RECORD = 'Y'
         WHERE  data_load_id = c.data_load_id                                                                         
         AND    ROWID NOT IN (SELECT max(rowid)
                              FROM TBL_MDT_JRBI_MASTER_DATA b
                              WHERE a.data_origin = b.data_origin 
                              and a.data_load_id = b.data_load_id
                              and a.email = b.email
                              and a.flex3 = b.flex3
                              and nvl(a.flex4,a.flex7) = nvl(b.flex4,b.flex7)
                              );                                                                
         COMMIT;


         UPDATE TBL_MDT_JRBI_MASTER_DATA a
         SET    DUPLICATE_RECORD = nvl(DUPLICATE_RECORD,'N'),
                PROCESSED = 'Y',
                TO_BE_EXPORTED = CASE WHEN nvl(DUPLICATE_RECORD,'N') = 'N'  THEN 'Y' ELSE 'N' END
         WHERE  data_load_id = c.data_load_id;
         COMMIT;

END LOOP;

--
-- Run the dataChecks procedure to find any records where there are over 80% duplicates
--
    runDataChecks(80);

processLog('processMasterData','Information','Completed Procedure');    

EXCEPTION WHEN OTHERS THEN 
    vErrorMessage := SQLERRM;
    processLog('processMasterData','Error',vErrorMessage);
END processMasterData;



PROCEDURE processLog (pProcess IN VARCHAR2, pLogType IN VARCHAR2, pMessage IN VARCHAR2)
IS 

BEGIN 

    INSERT INTO TBL_LOG_SPC
    VALUES (SYSDATE,pProcess,pLogType,pMessage);
    COMMIT;

END processLog;


FUNCTION getLoadID 
RETURN NUMBER
IS
    vLoadId NUMBER := 0;
BEGIN
    SELECT nvl(MAX(DATA_LOAD_ID),0)+1 
    INTO vLoadId 
    FROM TBL_MDT_JRBI_MASTER_DATA;
RETURN vLoadID;
END getLoadID;

FUNCTION checkOriginForToday (pOrigin in VARCHAR2)
RETURN NUMBER
IS
    vRowCount NUMBER := 0;
BEGIN

   SELECT nvl(COUNT(*),0) 
   INTO   vRowCount
   FROM   TBL_MDT_JRBI_MASTER_DATA
   WHERE  data_origin = pOrigin
   AND    trunc(DATE_UPLOADED) =trunc(sysdate);

RETURN vRowCount;
END checkOriginForToday;


FUNCTION deleteOrigin (pOrigin in VARCHAR2, pDate DATE) 
RETURN VARCHAR2
IS

vDeleteRows NUMBER := 0;
vMessage    VARCHAR2(4000);

BEGIN

     DELETE FROM TBL_MDT_JRBI_MASTER_DATA
     WHERE  data_origin = pOrigin
     AND    trunc(DATE_UPLOADED) =trunc(pDate);

     vDeleteRows  :=  SQL%ROWCOUNT;

     COMMIT;

     vMessage := 'Deleted '||nvl(vDeleteRows,0) || ' for origin: "'||pOrigin||'" - date: "'||trim(to_char(pDate,'DD/MM/YYYY'))||'"';

processLog('deleteOrigin','Information',vMessage);    
RETURN vMessage;

EXCEPTION WHEN OTHERS THEN 
    vErrorMessage := SQLERRM;
    processLog('deleteOrigin','Error',vErrorMessage);     
END deleteOrigin;


PROCEDURE runDataChecks (vPercentage IN NUMBER)
IS

CURSOR CurData IS
        SELECT data_origin,
               data_load_id,
               total_records,
               total_duplicates,
               max_DATE_UPLOADED,
               CASE WHEN total_duplicates > 0 AND total_records > 0 THEN (total_duplicates/total_records)*100 ELSE 0 END as PERCENTAGE_DUPLICATES
        FROM (       
                SELECT data_origin,
                       data_load_id,
                       count(*) as total_records, 
                       sum(case when PROCESSED = 'Y' AND DUPLICATE_RECORD = 'Y' THEN 1 ELSE 0 END) as total_duplicates,
                       max(DATE_UPLOADED) as max_DATE_UPLOADED
                FROM TBL_MDT_JRBI_MASTER_DATA a
                WHERE data_load_id in (
                                      SELECT max(data_load_id) 
                                      FROM TBL_MDT_JRBI_MASTER_DATA b 
                                      WHERE a.data_origin = b.data_origin
                                      )
                GROUP BY data_origin, data_load_id
        );

vLogExists NUMBER := 0;

BEGIN

        FOR C IN CurData LOOP

            IF c.PERCENTAGE_DUPLICATES >= vPercentage THEN

                SELECT COUNT(*)
                INTO   vLogExists
                FROM   TBL_LOG_SPC
                WHERE  LOG_PROCESS = 'runDataChecks'
                AND    LOG_TYPE = 'Error'
                AND    LOG_MESSAGE like c.data_origin || ' (Load ID: '|| c.data_load_id||') has%';

                IF vLogExists = 0 THEN
                    processLog('runDataChecks','Error',c.data_origin || ' (Load ID: '|| c.data_load_id||') has '|| trim(to_char(c.PERCENTAGE_DUPLICATES,'999,999,999.99')) ||'% of duplicates.  This figure is outside the threshold of '||vPercentage||'%.  The remaining records have been updated with "TO_BE_EXPORTED" flag set to "F" whilst this error is investigated');   
                END IF;

                UPDATE TBL_MDT_JRBI_MASTER_DATA
                SET    TO_BE_EXPORTED = 'F'
                WHERE  DATA_LOAD_ID = c.data_load_id
                AND    TO_BE_EXPORTED = 'Y';
                COMMIT;

            ELSE

                SELECT COUNT(*)
                INTO   vLogExists
                FROM   TBL_LOG_SPC
                WHERE  LOG_PROCESS = 'runDataChecks'
                AND    LOG_TYPE = 'Information'
                AND    LOG_MESSAGE like c.data_origin || ' (Load ID: '|| c.data_load_id||') has%';

                IF vLogExists = 0 THEN
                    processLog('runDataChecks','Information',c.data_origin || ' (Load ID: '|| c.data_load_id||') has '|| trim(to_char(c.PERCENTAGE_DUPLICATES,'999,999,999.99')) ||'% of duplicates.  This figure is within the threshold of '||vPercentage||'%');   
                END IF;            

            END IF;

        END LOOP;

END runDataChecks;

END PACK_STATS_PUB_CAMPAIGN_JRBI;
/
