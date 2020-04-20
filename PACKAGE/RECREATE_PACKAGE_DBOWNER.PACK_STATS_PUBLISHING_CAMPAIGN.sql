CREATE PACKAGE DBOWNER.PACK_STATS_PUBLISHING_CAMPAIGN
  CREATE OR REPLACE EDITIONABLE PACKAGE "DBOWNER"."PACK_STATS_PUBLISHING_CAMPAIGN" 
AS

PROCEDURE processOpsAcceptedData(pOverrideCheckingTodaysData IN VARCHAR2 DEFAULT 'FALSE');
PROCEDURE processOpsRejectedData(pOverrideCheckingTodaysData IN VARCHAR2 DEFAULT 'FALSE');
PROCEDURE processOpsPublishedData(pOverrideCheckingTodaysData IN VARCHAR2 DEFAULT 'FALSE');
PROCEDURE processOpsAudioSlidesData(pOverrideCheckingTodaysData IN VARCHAR2 DEFAULT 'FALSE');

PROCEDURE processCAPRIAcceptedData(pOverrideCheckingTodaysData IN VARCHAR2 DEFAULT 'FALSE');
PROCEDURE processCAPRIRejectedData(pOverrideCheckingTodaysData IN VARCHAR2 DEFAULT 'FALSE');
PROCEDURE processCAPRIPublishedData(pOverrideCheckingTodaysData IN VARCHAR2 DEFAULT 'FALSE');

PROCEDURE processStatsOnlyData(pOverrideCheckingTodaysData IN VARCHAR2 DEFAULT 'FALSE');

PROCEDURE processMasterData;

PROCEDURE processLog (pProcess IN VARCHAR2, pLogType IN VARCHAR2, pMessage IN VARCHAR2);
FUNCTION getLoadID RETURN NUMBER;
FUNCTION checkOriginForToday (pOrigin in VARCHAR2) RETURN NUMBER;
FUNCTION deleteOrigin (pOrigin in VARCHAR2, pDate DATE) RETURN VARCHAR2;

PROCEDURE runDataChecks (vPercentage IN NUMBER);

END PACK_STATS_PUBLISHING_CAMPAIGN;
/
CREATE OR REPLACE EDITIONABLE PACKAGE BODY "DBOWNER"."PACK_STATS_PUBLISHING_CAMPAIGN" 
AS

    vErrorMessage        VARCHAR2(4000);
    vDataLoadID          NUMBER := 0;
    vDataRecords         NUMBER := 0;
    origin_loaded_today  EXCEPTION;

PROCEDURE processOpsAcceptedData(pOverrideCheckingTodaysData IN VARCHAR2 DEFAULT 'FALSE')
IS 

BEGIN 

processLog('processOpsAcceptedData','Information','Started Procedure');  

IF upper(pOverrideCheckingTodaysData) <> 'TRUE' THEN
    vDataRecords := checkOriginForToday('Accepted_OpsReport');
    IF vDataRecords > 0 THEN
         RAISE origin_loaded_today;
    END IF;     
END IF;

vDataLoadID := getLoadID;

    INSERT INTO TBL_MDT_SPC_MASTER_DATA
    SELECT  vDataLoadID,
            'Accepted_OpsReport',
            SYSDATE,
            NULL,
            'N',
            NULL,
            NULL,
            replace(TITLE,'"'),
            FIRSTNAME,
            LASTNAME,
            EMAIL,
            ROLE,
            JOB_TITLE,
            ORGANIZATION,
            ORG_TYPE,
            COUNTRY_CODE,
            OWNER,
            CAPRI_ID,
            SIS_ID,
            CRM_BUSINESS_DIVISION,
            CRM_SALES_DIVISION,
            CRM_ACCOUNT_ORG_TYPE,
            CRM_ACCOUNT_NAME,
            CRM_CONTACT_TYPE,
            CRM_CONTACT_TITLE,
            CRM_CONTACT_FIRSTNAME,
            CRM_CONTACT_LASTNAME,
            CRM_CONTACT_EMAIL,
            CRM_CONTACT_PHONE,
            LANGUAGE,
            FLEX1,
            FLEX2,
            FLEX3,
            FLEX4,
            FLEX5,
            FLEX6,
            FLEX7,
            FLEX8,
            FLEX9,
            FLEX10,
            FLEX11,
            FLEX12,
            FLEX13,
            FLEX14,
            FLEX15,
            LL_FIRSTNAME,
            LL_LASTNAME,
            LL_ORGANISATION,
            replace(CMX_ID,'"')
    FROM   TBL_TEMP_SPC_ACCPETED_OPS;
    COMMIT;

processLog('processOpsAcceptedData','Information','Completed Procedure');    

EXCEPTION WHEN origin_loaded_today THEN
    processLog('processOpsAcceptedData','Error','Data already exists for today');
          WHEN OTHERS THEN 
    vErrorMessage := SQLERRM;
    processLog('processOpsAcceptedData','Error',vErrorMessage);
END processOpsAcceptedData;


PROCEDURE processOpsAudioSlidesData(pOverrideCheckingTodaysData IN VARCHAR2 DEFAULT 'FALSE')
IS 

BEGIN 

processLog('processOpsAudioSlidesData','Information','Started Procedure');  

IF upper(pOverrideCheckingTodaysData) <> 'TRUE' THEN
    vDataRecords := checkOriginForToday('AudioSlides_OpsReport');
    IF vDataRecords > 0 THEN
         RAISE origin_loaded_today;
    END IF;     
END IF;

vDataLoadID := getLoadID;

    INSERT INTO TBL_MDT_SPC_MASTER_DATA
    SELECT  vDataLoadID,
            'AudioSlides_OpsReport',
            SYSDATE,
            NULL,
            'N',
            NULL,
            NULL,
            replace(TITLE,'"'),
            FIRSTNAME,
            LASTNAME,
            EMAIL,
            ROLE,
            JOB_TITLE,
            ORGANIZATION,
            ORG_TYPE,
            COUNTRY_CODE,
            OWNER,
            CAPRI_ID,
            SIS_ID,
            CRM_BUSINESS_DIVISION,
            CRM_SALES_DIVISION,
            CRM_ACCOUNT_ORG_TYPE,
            CRM_ACCOUNT_NAME,
            CRM_CONTACT_TYPE,
            CRM_CONTACT_TITLE,
            CRM_CONTACT_FIRSTNAME,
            CRM_CONTACT_LASTNAME,
            CRM_CONTACT_EMAIL,
            CRM_CONTACT_PHONE,
            LANGUAGE,
            trim(FLEX1),
            FLEX2,
            FLEX3,
            FLEX4,
            FLEX5,
            SUBSTR(FLEX6,8),
            FLEX7,
            FLEX8,
            FLEX9,
            FLEX10,
            FLEX11,
            FLEX12,
            FLEX13,
            FLEX14,
            FLEX15,
            LL_FIRSTNAME,
            LL_LASTNAME,
            LL_ORGANISATION,
            replace(CMX_ID,'"')
    FROM   TBL_TEMP_SPC_AUDIOSLIDES_OPS;
    COMMIT;

processLog('processOpsAudioSlidesData','Information','Completed Procedure');    

EXCEPTION WHEN origin_loaded_today THEN
    processLog('processOpsAudioSlidesData','Error','Data already exists for today');
          WHEN OTHERS THEN 
    vErrorMessage := SQLERRM;
    processLog('processOpsAudioSlidesData','Error',vErrorMessage);
END processOpsAudioSlidesData;


PROCEDURE processOpsRejectedData(pOverrideCheckingTodaysData IN VARCHAR2 DEFAULT 'FALSE')
AS

BEGIN

processLog('processOpsRejectedData','Information','Started Procedure');    

IF upper(pOverrideCheckingTodaysData) <> 'TRUE' THEN
    vDataRecords := checkOriginForToday('Rejected_OpsReport');
    IF vDataRecords > 0 THEN
         RAISE origin_loaded_today;
    END IF; 
END IF;    

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE tbl_temp_spc_items_ISSN purge';
EXCEPTION WHEN OTHERS THEN NULL;
END;

EXECUTE IMMEDIATE '
create table tbl_temp_spc_items_ISSN 
   as 
      select distinct 
             i.site as EES_ACRONYM, 
             replace(i.identifier,''-'') as ISSN,
             p.journal_issn,
             p.pmg
      from   (select * from report_owner.D_PRODUCT_MASTER where product_Type = ''Journal'') p,
             dbowner.tbl_items i
      where i.orig_system = ''EES''
      and   i.item_type = ''JOU''
      and   i.identifier is not null
      and   replace(i.identifier,''-'') = p.journal_issn';

execute immediate 'create index indx_tbl_spc_ess_items_1 on tbl_temp_spc_items_ISSN (issn)';
execute immediate 'create index indx_tbl_spc_ess_items_2 on tbl_temp_spc_items_ISSN (EES_ACRONYM)';

processLog('processOpsRejectedData','Information','Temp Table 1 of 6 build complete');

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE tbl_temp_rejected_art_temp purge';
EXCEPTION WHEN OTHERS THEN NULL;
END;


EXECUTE IMMEDIATE '
create table tbl_temp_rejected_art_temp as
select * from TBL_TEMP_SPC_REJECTED_OPS';

EXECUTE IMMEDIATE 'update tbl_temp_rejected_art_temp set eesacronym=replace(eesacronym,''"'')';
commit;

EXECUTE IMMEDIATE 'alter table tbl_temp_rejected_art_temp add ees_acronym varchar2(30)';

EXECUTE IMMEDIATE 'update tbl_temp_rejected_art_temp set ees_acronym=regexp_substr(doc_no,''^[A-Z]*'')';
commit;

EXECUTE IMMEDIATE 'alter table tbl_temp_rejected_art_temp add article_name_stripped varchar2(4000)';

EXECUTE IMMEDIATE 'update tbl_temp_rejected_art_temp set article_name_stripped=trim(lower(replace(doc_title,''"'')))';
commit;

EXECUTE IMMEDIATE 'CREATE INDEX INDX_SPC_TEMP_ART_REJECTED_1 ON tbl_temp_rejected_art_temp (article_name_stripped)';

processLog('processOpsRejectedData','Information','Temp Table 2 of 6 build complete');

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE tbl_temp_spc_items_Article purge';
EXCEPTION WHEN OTHERS THEN NULL;
END;

EXECUTE IMMEDIATE '
create table tbl_temp_spc_items_Article
as
select p.article_pii, 
       p.article_name,
       p.journal_issn,
       p.journal_name,
       p.pmg,
       trim(lower(replace(p.article_name,''"''))) as article_name_stripped
from   report_owner.d_product_master p
where  p.product_type = ''Article''
and    p.journal_issn in (select ISSN from tbl_temp_spc_items_ISSN j)';

EXECUTE IMMEDIATE 'create index indx_spc_article_1 on tbl_temp_spc_items_Article (article_name_stripped)';
EXECUTE IMMEDIATE 'create index indx_spc_article_2 on tbl_temp_spc_items_Article (journal_issn)';

processLog('processOpsRejectedData','Information','Temp Table 3 of 6 build complete');

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE M_rejected_OPS_1 purge';
EXCEPTION WHEN OTHERS THEN NULL;
END;


EXECUTE IMMEDIATE '
create table M_rejected_OPS_1 as
select distinct r.JOURNAL_NO,
r.DOC_TITLE,
r.DOC_NO,
r.FINDSPDATE,
r.AUTHOR_FULL_NAME,
r.LASTNAME,
r.FIRSTNAME,
r.TITLENAME,
r.EMAIL,
r.EESACRONYM,
''STMJ'' as owner,''EES'' as system,''Rejected'' as type,
substr(p.JOURNAL_ISSN,1,4)||''-''||substr(p.JOURNAL_ISSN,-4) as JOURNAL_ISSN,
p.PMG,
CAST(null as varchar2(20)) as cmx_id
from tbl_temp_spc_items_Article p,
     tbl_temp_spc_items_ISSN EES_Items,
     tbl_temp_rejected_art_temp r
where p.article_name_stripped=r.article_name_stripped
and p.JOURNAL_ISSN=EES_Items.ISSN
and r.EESACRONYM = EES_Items.EES_ACRONYM';

processLog('processOpsRejectedData','Information','Temp Table 4 of 6 build complete');

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE M_rejected_OPS_2 purge';
EXCEPTION WHEN OTHERS THEN NULL;
END;


EXECUTE IMMEDIATE '
create table M_rejected_OPS_2 as
select JOURNAL_NO,
DOC_TITLE,
DOC_NO,
FINDSPDATE,
AUTHOR_FULL_NAME,
LASTNAME,
FIRSTNAME,
TITLENAME,
EMAIL,
EESACRONYM,
CAST(null as varchar2(20)) as cmx_id from tbl_temp_rejected_art_temp
minus
select JOURNAL_NO,
DOC_TITLE,
DOC_NO,
FINDSPDATE,
AUTHOR_FULL_NAME,
LASTNAME,
FIRSTNAME,
TITLENAME,
EMAIL,
EESACRONYM,
CAST(null as varchar2(20)) as cmx_id from M_rejected_OPS_1';

processLog('processOpsRejectedData','Information','Temp Table 5 of 6 build complete');

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE M_rejected_OPS purge';
EXCEPTION WHEN OTHERS THEN NULL;
END;

EXECUTE IMMEDIATE '
create table M_rejected_OPS as
select * from M_rejected_OPS_1';

EXECUTE IMMEDIATE '
insert into M_rejected_OPS select JOURNAL_NO,
DOC_TITLE,
DOC_NO,
FINDSPDATE,
AUTHOR_FULL_NAME,
LASTNAME,
FIRSTNAME,
TITLENAME,
EMAIL,
EESACRONYM,
''STMJ'' as owner,
''EES'' as system,
''Rejected'' as type,
NULL as JOURNAL_ISSN,
NULL as PMG,
NULL as CMX_ID
from M_rejected_OPS_2';
commit;

processLog('processOpsRejectedData','Information','Temp Table 6 of 6 build complete');
vDataLoadID := getLoadID;

EXECUTE IMMEDIATE '
    INSERT INTO TBL_MDT_SPC_MASTER_DATA
    SELECT  DISTINCT
            '||vDataLoadID||',
           ''Rejected_OpsReport'',
            SYSDATE,
            NULL,
            ''N'',
            NULL,
            NULL,
            TITLENAME,
            FIRSTNAME,
            LASTNAME,
            Decode(instr(EMAIL,'';''),0,EMAIL,substr(EMAIL,1, instr(EMAIL,'';'')-1)),
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            owner,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            System,
            Type,
            Journal_issn,
            NULL,
            NULL,
            NULL,
            DOC_TITLE,
            NULL,
            PMG,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL
    from M_REJECTED_OPS';

processLog('processOpsRejectedData','Information','Completed Procedure');    

EXCEPTION WHEN origin_loaded_today THEN
    processLog('processOpsRejectedData','Error','Data already exists for today');
          WHEN OTHERS THEN 
    vErrorMessage := SQLERRM;
    processLog('processOpsRejectedData','Error',vErrorMessage);
END processOpsRejectedData;


PROCEDURE processOpsPublishedData(pOverrideCheckingTodaysData IN VARCHAR2 DEFAULT 'FALSE')
AS

BEGIN

processLog('processOpsPublishedData','Information','Started Procedure');    

IF upper(pOverrideCheckingTodaysData) <> 'TRUE' THEN
    vDataRecords := checkOriginForToday('Published_OpsReport');
    IF vDataRecords > 0 THEN
         RAISE origin_loaded_today;
    END IF;    
END IF;    

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE M_published_OPS purge';
EXCEPTION WHEN OTHERS THEN NULL;
END;


EXECUTE IMMEDIATE '
CREATE TABLE M_PUBLISHED_OPS
AS
SELECT DISTINCT REPLACE(PA.LASTNAME,''"'') AS LASTNAME_NEW,
                PA.*,
                ''STMJ'' AS OWNER,
                ''PTS'' AS SYSTEM,
                ''Published'' AS TYPE,
                P.PMG,
                CAST(NULL AS VARCHAR2(20)) AS CMX_ID
FROM  TBL_TEMP_SPC_PUBLISHED_OPS PA
LEFT JOIN 
       (
       SELECT JOURNAL_ISSN, ARTICLE_PII, MAX(P.PMG) AS PMG
       FROM REPORT_OWNER.D_PRODUCT_MASTER P
       WHERE PRODUCT_TYPE = ''Article''
       GROUP BY JOURNAL_ISSN, ARTICLE_PII
       ) P
ON     REPLACE(PA.JOURNAL_ISSN,''-'') = P.JOURNAL_ISSN
AND    PA.ITEM_EID = P.ARTICLE_PII';

processLog('processOpsPublishedData','Information','Temp Table 1 of 1 build complete');
vDataLoadID := getLoadID;

EXECUTE IMMEDIATE '
    INSERT INTO TBL_MDT_SPC_MASTER_DATA
    SELECT  DISTINCT
            '||vDataLoadID||',
           ''Published_OpsReport'',
            SYSDATE,
            NULL,
            ''N'',
            NULL,
            NULL,
            TITLENAME,
            NULL,
            LASTNAME_New,
            Decode(instr(EMAIL,'';''),0,EMAIL,substr(EMAIL,1, instr(EMAIL,'';'')-1)),
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            owner,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            system,
            type,
            Journal_issn,
            item_eid,
            NULL,
            NULL,
            item_title,
            journal_title,
            pmg,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL
    from M_PUBLISHED_OPS
    where email is not NULL and pmg is not null';

processLog('processOpsPublishedData','Information','Completed Procedure');    

EXCEPTION WHEN origin_loaded_today THEN
    processLog('processOpsPublishedData','Error','Data already exists for today');
          WHEN OTHERS THEN 
    vErrorMessage := SQLERRM;
    processLog('processOpsPublishedData','Error',vErrorMessage);

END processOpsPublishedData;


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
    EXECUTE IMMEDIATE 'DROP TABLE tbl_temp_spc_accepted_capri purge';
    --EXECUTE IMMEDIATE 'DROP TABLE tbl_temp_jrbi_accepted_capri purge';
EXCEPTION WHEN OTHERS THEN NULL;
END;

EXECUTE IMMEDIATE '
        CREATE TABLE tbl_temp_spc_accepted_capri AS
        SELECT DISTINCT a.party_id,
          a.dedupe_id,
          a.orig_party_ref,
          ''PTS'' AS user_group,
          a.title,
          a.firstname,
          a.lastname,
          a.email,
          i.orig_item_ref    AS article_id,
          i.item_milestone   AS article_status,
          i.orig_create_date AS article_update_date,
          CASE
            WHEN i.orig_create_date BETWEEN (sysdate - 14) AND (sysdate - 7)
            THEN ''Y''
            ELSE ''N''
          END AS date_flag,
          ''Y'' AS article_accepted ,
          --p.publication_item_type,  p.article_doi, p.article_pii, p.article_name, p.journal_issn, p.journal_name     ,
          i.class       AS publication_item,
          i.description AS article_doi,
          CASE
            WHEN SUBSTR(i.identifier,1,1) = ''S''
            THEN SUBSTR(regexp_replace(i.identifier,''[()-]''),1)
            ELSE regexp_replace(i.identifier,''[()-]'')
          END                                       AS article_pii,
          i.name             AS article_name,
          j.identifier       AS journal_issn,
          j.name             AS journal_name,
          ''STMJ''             AS OWNER,
          a.ISO_COUNTRY_CODE AS ISO_COUNTRY_CODE,
          p.pmg,
          p.pmg_desc,
          a.CMX_ID as CMX_ID
        FROM dbowner.tbl_items i,
          dbowner.tbl_items j,
          dbowner.tbl_parties a,
          --,report_owner.f_submission_count F,
          report_owner.d_product_master P
          --report_owner.d_end_users a,
        WHERE a.orig_system               = ''PTS''
        AND a.orig_party_ref              = i.authors
        AND i.orig_system                 = ''PTS''
        AND i.item_type                   = ''ART''
        AND i.item_milestone             IN ( ''ACCEPTED'',''COMPLETE'',''S100-CRE'')
        AND SUBSTR(i.issue_milestone,1,1) = ''N''
        AND i.orig_create_date BETWEEN (sysdate - 14) AND (sysdate - 7) -->  this is the date the article record is created in PTS
        AND j.orig_system                = ''PTS''
        AND j.item_type                  = ''JOU''
        AND j.orig_item_ref              = i.parent_ref
        AND lower(i.class)              IN (''full length article'',''review article'',''short communication'')
        AND (i.init_pub_date            IS NULL
        OR i.last_pub_date              IS NULL) -->  this is to make sure that NO PUBLISHED articles are selected?
        AND a.email                     IS NOT NULL
        AND REPLACE(j.identifier,''-'') = p.journal_issn (+)
        AND ''Journal''                    = p.product_type (+) '
        ;

/*
EXECUTE IMMEDIATE '
CREATE TABLE tbl_temp_jrbi_accepted_capri AS
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
            WHEN i.issue_date BETWEEN trunc(sysdate) - 7 and trunc(sysdate)
            THEN ''Y''
            ELSE ''N''
          END AS date_flag,
          ''Y'' AS article_accepted ,
		  i.class       AS publication_item,
          i.description AS article_doi,
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
		  j.show_status as Journal_BU
from
dbowner.tbl_parties a 
join dbowner.tbl_items i
on a.orig_party_ref = i.authors
and a.orig_system = ''JRBI'' and i.orig_system = ''JRBI'' and i.item_type = ''ART''
AND a.email is not null
and i.item_milestone = ''Accept''
AND (i.init_pub_date  is null
OR   i.last_pub_date  is null) 
and trunc(i.issue_date) between trunc(sysdate) - 7 and trunc(sysdate)
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
            WHEN i.issue_date BETWEEN trunc(sysdate) - 7 and trunc(sysdate)
            THEN ''Y''
            ELSE ''N''
          END AS date_flag,
          ''Y'' AS article_accepted ,
		  i.class       AS publication_item,
          i.description AS article_doi,
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
		  j.show_status as Journal_BU
from
dbowner.tbl_parties a
join dbowner.tbl_interests u
on a.orig_party_ref = u.party_ref 
and a.orig_system = ''JRBI'' and u.orig_system = ''JRBI'' and u.interest_type = ''ART''
and a.email not in (select email from tbl_temp_jrbi_accepted_capri where user_group = ''JRBI'')
join dbowner.tbl_items i
on u.interest_value = i.orig_item_ref
and a.orig_system = ''JRBI'' and i.orig_system = ''JRBI'' and i.item_type = ''ART''
and i.item_milestone = ''Accept''
and trunc(i.issue_date) between trunc(sysdate) - 7 and trunc(sysdate)
join dbowner.tbl_items j 
on i.parent_ref = j.orig_item_ref
and j.item_type = ''JOU'' and j.orig_system = ''JRBI''  ' ; 

commit;
*/

vDataLoadID := getLoadID;

EXECUTE IMMEDIATE '
    INSERT INTO TBL_MDT_SPC_MASTER_DATA
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
              ''Accepted''                                                          AS FLEX2,
              JOURNAL_ISSN                                                        AS FLEX3,
              ARTICLE_PII                                                         AS FLEX4,
              ARTICLE_ID                                                          AS FLEX5,
              ARTICLE_DOI                                                         AS FLEX6,
              REPLACE(REPLACE(REPLACE(ARTICLE_NAME,''"'',''""''),''”'',''""''), ''“'',''""'') AS FLEX7,
              JOURNAL_NAME                                                        AS FLEX8,
              PMG                                                                 AS FLEX9,
              CAST (NULL AS VARCHAR2(20))                                         AS FLEX10,
              CAST (NULL AS VARCHAR2(20))                                         AS FLEX11,
              CAST (NULL AS VARCHAR2(20))                                         AS FLEX12,
              CAST (NULL AS VARCHAR2(20))                                         AS FLEX13,
              CAST (NULL AS VARCHAR2(20))                                         AS FLEX14,
              CAST (NULL AS VARCHAR2(20))                                         AS FLEX15,
              CAST (NULL AS VARCHAR2(20))                                         AS LL_FIRSTNAME,
              CAST (NULL AS VARCHAR2(20))                                         AS LL_LASTNAME,
              CAST (NULL AS VARCHAR2(20))                                         AS LL_ORGANISATION,
              CAST (null as varchar2(20)) 					      AS CMX_ID
            FROM tbl_temp_spc_accepted_capri';
            COMMIT;

/*
EXECUTE IMMEDIATE '
    INSERT INTO TBL_MDT_SPC_MASTER_DATA
    SELECT  DISTINCT
            '||vDataLoadID||',
              ''Accepted_JRBI_CAPRIData'',
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
              CAST (NULL AS VARCHAR2(20))                                         AS FLEX11,
              CAST (NULL AS VARCHAR2(20))                                         AS FLEX12,
              CAST (NULL AS VARCHAR2(20))                                         AS FLEX13,
              CAST (NULL AS VARCHAR2(20))                                         AS FLEX14,
              CAST (NULL AS VARCHAR2(20))                                         AS FLEX15,
              CAST (NULL AS VARCHAR2(20))                                         AS LL_FIRSTNAME,
              CAST (NULL AS VARCHAR2(20))                                         AS LL_LASTNAME,
              CAST (NULL AS VARCHAR2(20))                                         AS LL_ORGANISATION,
              CAST (null as varchar2(20)) 					      				  AS CMX_ID
            FROM TBL_TEMP_JRBI_ACCEPTED_CAPRI';
            COMMIT;
*/

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
    EXECUTE IMMEDIATE 'DROP TABLE tbl_temp_spc_rejected_capri purge';
EXCEPTION WHEN OTHERS THEN NULL;
END;

EXECUTE IMMEDIATE '
    CREATE TABLE tbl_temp_spc_rejected_capri
    as
    SELECT distinct
      a.party_id, a.dedupe_id, a.orig_party_ref,
      f.user_group,
      a.title, a.firstname, a.lastname, a.email,
      f.orig_interest_ref as article_id, f.document_status as article_status,   
      i.orig_update_date as article_update_date,
      case when trunc(i.orig_update_date)  between (sysdate - 14) and (sysdate - 7)  then ''Y'' else ''N'' end as date_flag, --->between (sysdate - 14) and (sysdate - 7)
      f.article_accepted , p.publication_item_type,
      p.article_doi, p.article_pii, p.article_name, 
      replace (p.journal_issn,''-'') as journal_issn, 
      p.journal_name     ,
      ''STMJ'' AS OWNER,
      COUNTRY_CODE as ISO_COUNTRY_CODE,
      p.pmg,
      p.pmg_desc,
    a.account_cmx_id as CMX_ID
      from
      report_owner.d_end_users a ,
      report_owner.f_submission_count f,
      report_owner.d_product_master p,
      dbowner.tbl_items i
      where a.user_group in (''EES Authors'',''EVISE Authors'')
      and f.user_group in (''EES Authors'',''EVISE Authors'')
      and f.article_accepted = ''R''
      and a.end_user_skey = f.end_user_skey
      and f.product_master_id = p.product_id
      and f.orig_interest_ref = i.orig_item_ref
      and i.orig_system in (''EES'',''EVI2'')
      and trunc(i.orig_update_date)   between (sysdate - 14) and (sysdate - 7)  -->between (sysdate - 14) and (sysdate - 7)   -->  this is the date the article record is last updated
      and a.email is not null
      ';

/*
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
          i.added_date AS article_acceptance_date,
		  CASE
            WHEN i.added_date between add_months(trunc(sysdate,''mm''),-1) and last_day(add_months(trunc(sysdate,''mm''),-1))
            THEN ''Y''
            ELSE ''N''
          END AS date_flag,
          ''Y'' AS article_accepted ,
		  i.class       AS publication_item,
          i.description AS article_doi,
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

from
dbowner.tbl_parties a 
join dbowner.tbl_items i
on a.orig_party_ref = i.authors
and a.orig_system = ''JRBI'' and i.orig_system = ''JRBI'' and i.item_type = ''ART''
and a.email is not null
and i.item_milestone = ''Reject''
and trunc(i.added_date) between  add_months(trunc(sysdate,''mm''),-1) and last_day(add_months(trunc(sysdate,''mm''),-1))
join dbowner.tbl_items j 
on i.parent_ref = j.orig_item_ref
and j.item_type = ''JOU'' and j.orig_system = ''JRBI'' ';
*/

vDataLoadID := getLoadID;

EXECUTE IMMEDIATE '
    INSERT INTO TBL_MDT_SPC_MASTER_DATA
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
            cast (null as varchar2(20)) as ROLE	,
            cast (null as varchar2(20)) as  JOB_TITLE,
            cast (null as varchar2(20)) as  ORGANIZATION,
            cast (null as varchar2(20)) as  ORG_TYPE,
            ISO_COUNTRY_CODE as COUNTRY_CODE,
            OWNER,
            ORIG_PARTY_REF as CAPRI_ID,
            cast (null as varchar2(20)) as  SIS_ID,
            cast (null as varchar2(20)) as  CRM_BUSINESS_DIVISION,
            cast (null as varchar2(20)) as  CRM_SALES_DIVISION,
            cast (null as varchar2(20)) as  CRM_ACCOUNT_ORG_TYPE,
            cast (null as varchar2(20)) as  CRM_ACCOUNT_NAME,
            cast (null as varchar2(20)) as  CRM_CONTACT_TYPE,
            cast (null as varchar2(20)) as  CRM_CONTACT_TITLE,
            cast (null as varchar2(20)) as  CRM_CONTACT_FIRSTNAME,
            cast (null as varchar2(20)) as  CRM_CONTACT_LASTNAME,
            cast (null as varchar2(20)) as  CRM_CONTACT_EMAIL,
            cast (null as varchar2(20)) as  CRM_CONTACT_PHONE,
            cast (null as varchar2(20)) as  LANGUAGE,
            USER_GROUP   as FLEX1,
            ''Rejected''   as FLEX2,
            substr(JOURNAL_ISSN,1,4)||''-''||substr(JOURNAL_ISSN,-4) as FLEX3,
            ARTICLE_PII  as FLEX4,
            ARTICLE_ID   as FLEX5,
            ARTICLE_DOI  as FLEX6,
            replace(replace(replace(ARTICLE_NAME,''"'',''""''),''”'',''""''), ''“'',''""'')  as FLEX7,
            JOURNAL_NAME as FLEX8,
            PMG as  FLEX9,
            cast (null as varchar2(20)) as  FLEX10,
            cast (null as varchar2(20)) as  FLEX11,
            cast (null as varchar2(20)) as  FLEX12,
            cast (null as varchar2(20)) as  FLEX13,
            cast (null as varchar2(20)) as  FLEX14,
            cast (null as varchar2(20)) as  FLEX15,
            cast (null as varchar2(20)) as  LL_FIRSTNAME,
            cast (null as varchar2(20)) as  LL_LASTNAME,
            cast (null as varchar2(20)) as LL_ORGANISATION,
            CMX_ID as CMX_ID
     from   TBL_TEMP_SPC_REJECTED_CAPRI';
commit;

/*
EXECUTE IMMEDIATE '
INSERT INTO TBL_MDT_SPC_MASTER_DATA
    SELECT  DISTINCT
            '||vDataLoadID||',
              ''Rejected_JRBI_CAPRIData'',
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
              CAST (NULL AS VARCHAR2(20))                                         AS FLEX12,
              CAST (NULL AS VARCHAR2(20))                                         AS FLEX13,
              CAST (NULL AS VARCHAR2(20))                                         AS FLEX14,
              CAST (NULL AS VARCHAR2(20))                                         AS FLEX15,
              CAST (NULL AS VARCHAR2(20))                                         AS LL_FIRSTNAME,
              CAST (NULL AS VARCHAR2(20))                                         AS LL_LASTNAME,
              CAST (NULL AS VARCHAR2(20))                                         AS LL_ORGANISATION,
              CAST (null as varchar2(20)) 					      				  AS CMX_ID
            FROM tbl_temp_jrbi_rejected_capri' ; 
commit;
*/

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
    EXECUTE IMMEDIATE 'DROP TABLE tbl_temp_spc_published_capri1 purge';
EXCEPTION WHEN OTHERS THEN NULL;
END;

EXECUTE IMMEDIATE '
CREATE TABLE tbl_temp_spc_published_capri1 AS
SELECT DISTINCT a.party_id,
  a.dedupe_id,
  a.orig_party_ref,
  ''PTS'' AS user_group,
  a.title,
  a.firstname,
  a.lastname,
  a.email,
  i.orig_item_ref   AS article_id,
  i.item_milestone  AS item_milestone,
  i.issue_milestone AS issue_milestone,
  i.last_pub_date   AS last_pub_date,
  i.init_pub_date   AS init_pub_date,
  --case when i.last_pub_date  between (sysdate - 14) and (sysdate - 7) then ''Y'' else ''N'' end as date_flag,
  ''Y'' AS article_accepted ,
  --p.publication_item_type,  p.article_doi, p.article_pii, p.article_name, p.journal_issn, p.journal_name     ,
  i.class       AS publication_item_type,
  i.description AS article_doi,
  CASE
    WHEN SUBSTR(i.identifier,1,1) = ''S''
    THEN SUBSTR(regexp_replace(i.identifier,''[()-]''),1)
    ELSE regexp_replace(i.identifier,''[()-]'')
  END                                       AS article_pii,
  i.name             AS article_name,
  j.identifier       AS journal_issn,
  j.name             AS journal_name,
  ''STMJ''             AS OWNER,
  a.iso_country_code AS ISO_COUNTRY_CODE,
  p.pmg,
  p.pmg_desc,
  a.CMX_ID as CMX_ID
FROM dbowner.tbl_items i,
  dbowner.tbl_items j,
  dbowner.tbl_parties a,
  --,report_owner.f_submission_count F,
  report_owner.d_product_master P
  --report_owner.d_end_users a,
WHERE a.orig_system               = ''PTS''
AND a.orig_party_ref              = i.authors
AND i.orig_system                 = ''PTS''
AND i.item_type                   = ''ART''
AND SUBSTR(i.issue_milestone,1,1) = ''Y''
AND i.last_pub_date BETWEEN (sysdate - 14) AND (sysdate - 7) -->  this is the date the article record appears online in SD
AND j.orig_system                = ''PTS''
AND j.item_type                  = ''JOU''
AND j.orig_item_ref              = i.parent_ref
AND (i.init_pub_date            IS NOT NULL
AND i.last_pub_date             IS NOT NULL) -->  this is to make sure that ONLY PUBLISHED articles are selected?
AND a.email                     IS NOT NULL
AND lower(i.class) in (''review article'',''full length article'',''short communication'')  --> pick only these article types MARKAU-4266
AND REPLACE(j.identifier,''-'','''') = p.journal_issn (+)
AND ''Journal''                    = p.product_type (+) ';

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE tbl_temp_spc_published_capri2 purge';
EXCEPTION WHEN OTHERS THEN NULL;
END;

EXECUTE IMMEDIATE '
CREATE TABLE tbl_temp_spc_published_capri2 AS
SELECT DISTINCT a.party_id,
  a.dedupe_id,
  a.orig_party_ref,
  ''EES Co-Authors'' AS user_group,
  a.title,
  a.firstname,
  a.lastname,
  a.email,
  b.orig_interest_ref          AS article_id,
  CAST (NULL AS VARCHAR2(200)) AS article_status,
  b.orig_update_date           AS article_update_date,
  CASE
    WHEN b.orig_update_date BETWEEN (sysdate - 14) AND (sysdate - 7)
    THEN ''Y''
    ELSE ''N''
  END           AS date_flag,
  ''Y''           AS article_accepted ,
  pts_i.class       AS publication_item,
  pts_i.description AS article_doi,
  CASE
    WHEN SUBSTR(pts_i.identifier,1,1) = ''S''
    THEN SUBSTR(regexp_replace(pts_i.identifier,''[()-]''),1)
    ELSE regexp_replace(pts_i.identifier,''[()-]'')
  END                                       AS article_pii,
  pts_i.name                       AS article_name,
  pts_i.journal_issn                 AS journal_issn,
  pts_i.journal_name                      AS journal_name,  
  ''STMJ'' as owner,
  a.iso_country_code,
  CAST (NULL AS VARCHAR2(200)) AS PMG,
  CAST (NULL AS VARCHAR2(200)) AS PMG_DESC,
  a.cmx_id  
FROM dbowner.tbl_parties a,
  (select * from dbowner.tbl_interests b where b.orig_system = ''EES'' and interest_section||''_''||interest_sub_section in (select max(c.interest_section||''_''||c.interest_sub_section) from tbl_interests c where c.orig_system = ''EES'' and b.interest_value = c.interest_value)) b,
  (select regexp_replace(item.identifier,''[()-]'') article_pii,item.*, jou.identifier as journal_issn, regexp_replace(jou.identifier,''[()-]'') as match_journal_issn, jou.name as journal_name from dbowner.tbl_items item, dbowner.tbl_items jou where item.orig_system = ''PTS'' and item.item_type = ''ART'' and jou.orig_system = ''PTS'' and jou.item_type = ''JOU'' and item.parent_ref = jou.orig_item_ref AND lower(item.class) in (''review article'',''full length article'',''short communication'')  /* pick only these article types MARKAU-4266 */) pts_i,
  dbowner.tbl_items j,
  dbowner.tbl_items i,
  M_published_OPS x 
WHERE A.ORIG_SYSTEM = ''EES''
AND a.orig_party_ref LIKE ''EES_CAUTH%''
AND a.orig_party_ref = b.party_ref
AND b.orig_system    = ''EES''
AND i.ORIG_SYSTEM    = ''EES''
AND i.ITEM_TYPE      = ''ART''
AND b.interest_Value = i.orig_item_ref
AND j.orig_system   = ''EES''
AND j.item_type     = ''JOU''
AND j.orig_item_ref = i.parent_ref
AND   trim(replace(replace(replace(i.identifier,''-''),''(''),'')'')) =x.item_eid
--and   trim(replace(x.journal_issn,''-'')) = j.identifier
AND   a.email IS NOT NULL 
AND   pts_i.match_journal_issn = trim(replace(x.journal_issn,''-''))
AND   pts_i.article_pii = x.item_eid
UNION ALL
select distinct
       p.party_id,
       p.dedupe_id,
       p.orig_party_ref,
       ''EVISE Co-Authors'' as user_group,
       p.title,
       p.firstname,
       p.lastname,
       p.email,
       i.orig_item_ref as article_id,
       null as article_status,
       i.orig_update_date as article_update_date,
       CASE
    WHEN i.orig_update_date BETWEEN (sysdate - 14) AND (sysdate - 7)
    THEN ''Y''
    ELSE ''N''
  END           AS date_flag,
  ''Y''         AS article_accepted ,

  pts_i.class       AS publication_item,
  pts_i.description AS article_doi,
  CASE
    WHEN SUBSTR(pts_i.identifier,1,1) = ''S''
    THEN SUBSTR(regexp_replace(pts_i.identifier,''[()-]''),1)
    ELSE regexp_replace(pts_i.identifier,''[()-]'')
  END                                       AS article_pii,
  pts_i.name                       AS article_name,
  pts_i.journal_issn                 AS journal_issn,
  pts_i.journal_name                      AS journal_name,  
  ''STMJ'' as owner,
  p.iso_country_code,
  CAST (NULL AS VARCHAR2(200)) AS PMG,
  CAST (NULL AS VARCHAR2(200)) AS PMG_DESC,
  p.cmx_id
from tbl_parties p,
     tbl_items i,
     tbl_items j,
     (select regexp_replace(item.identifier,''[()-]'') article_pii,item.*, jou.identifier as journal_issn, regexp_replace(jou.identifier,''[()-]'') as match_journal_issn, jou.name as journal_name from dbowner.tbl_items item, dbowner.tbl_items jou where item.orig_system = ''PTS'' and item.item_type = ''ART'' and jou.orig_system = ''PTS'' and jou.item_type = ''JOU'' and item.parent_ref = jou.orig_item_ref AND lower(item.class) in (''review article'',''full length article'',''short communication'')  /* pick only these article types MARKAU-4266 */) pts_i,
     M_published_OPS x --tbl_temp_spc_published_capri1 x,
where p.orig_system = ''EVI2''
and   p.PRINCIPAL_FIELD in (''COAUTHOR'',''CORRUTHOR'',''CORRAUTHOR'') 
and   upper(p.URL_REFERRAL) = ''FALSE''
and   i.orig_system = ''EVI2''
and   i.item_type = ''ART''
and   ''EVI2_USER_'' ||p.user_referral  = i.authors
and   j.orig_system = ''EVI2''
and   j.item_Type = ''JOU''
and   i.parent_ref = j.orig_item_ref
--and   trim(replace(replace(replace(i.identifier,''-''),''(''),'')'')) = x.article_pii
AND   trim(replace(replace(replace(i.identifier,''-''),''(''),'')'')) =x.item_eid
and   trim(replace(x.journal_issn,''-'')) = j.identifier
AND   p.email IS NOT NULL 
AND   pts_i.match_journal_issn = trim(replace(x.journal_issn,''-''))
AND   pts_i.article_pii = x.item_eid
';

EXECUTE IMMEDIATE '
UPDATE tbl_temp_spc_published_capri2 a
SET
  (
    a.pmg,
    a.pmg_desc
  )
  =
  (SELECT pmg,
          pmg_desc
  FROM report_owner.d_product_master p
  WHERE p.product_type               = ''Journal''
  AND REPLACE(a.journal_issn,''-'','''') = p.journal_issn
  AND p.pmg is not null and p.pmg_desc is not null
  )';
COMMIT;


vDataLoadID := getLoadID;


EXECUTE IMMEDIATE '
    INSERT INTO TBL_MDT_SPC_MASTER_DATA
    SELECT DISTINCT 
            '||vDataLoadID||' as DATA_LOAD_ID,
           ''Published_CAPRIData'' as DATA_ORIGIN,
            SYSDATE AS UPLOAD_DATE,
            NULL as DUPLICATE_RECORD,
            ''N'' as PROCESSED,
            NULL as TO_BE_EXPORTED,
            NULL as EXPORTED,    
            a.*
    FROM
    (
    SELECT  DISTINCT
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
          ''Published''                                                         AS FLEX2,
          JOURNAL_ISSN                                                        AS FLEX3,
          ARTICLE_PII                                                         AS FLEX4,
          ARTICLE_ID                                                          AS FLEX5,
          ARTICLE_DOI                                                         AS FLEX6,
          REPLACE(REPLACE(REPLACE(ARTICLE_NAME,''"'',''""''),''”'',''""''), ''“'',''""'') AS FLEX7,
          JOURNAL_NAME                                                        AS FLEX8,
          PMG                                                                 AS FLEX9,
          CAST (NULL AS VARCHAR2(20))                                         AS FLEX10,
          CAST (NULL AS VARCHAR2(20))                                         AS FLEX11,
          CAST (NULL AS VARCHAR2(20))                                         AS FLEX12,
          CAST (NULL AS VARCHAR2(20))                                         AS FLEX13,
          CAST (NULL AS VARCHAR2(20))                                         AS FLEX14,
          CAST (NULL AS VARCHAR2(20))                                         AS FLEX15,
          CAST (NULL AS VARCHAR2(20))                                         AS LL_FIRSTNAME,
          CAST (NULL AS VARCHAR2(20))                                         AS LL_LASTNAME,
          CAST (NULL AS VARCHAR2(20))                                         AS LL_ORGANISATION,
          CMX_ID		 					      AS CMX_ID
        FROM tbl_temp_spc_published_capri1
        UNION
        SELECT  DISTINCT
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
          ''Published''                                                         AS FLEX2,
          JOURNAL_ISSN                                                        AS FLEX3,
          ARTICLE_PII                                                         AS FLEX4,
          ARTICLE_ID                                                          AS FLEX5,
          ARTICLE_DOI                                                         AS FLEX6,
          REPLACE(REPLACE(REPLACE(ARTICLE_NAME,''"'',''""''),''”'',''""''), ''“'',''""'') AS FLEX7,
          JOURNAL_NAME                                                        AS FLEX8,
          PMG                                                                 AS FLEX9,
          CAST (NULL AS VARCHAR2(20))                                         AS FLEX10,
          CAST (NULL AS VARCHAR2(20))                                         AS FLEX11,
          CAST (NULL AS VARCHAR2(20))                                         AS FLEX12,
          CAST (NULL AS VARCHAR2(20))                                         AS FLEX13,
          CAST (NULL AS VARCHAR2(20))                                         AS FLEX14,
          CAST (NULL AS VARCHAR2(20))                                         AS FLEX15,
          CAST (NULL AS VARCHAR2(20))                                         AS LL_FIRSTNAME,
          CAST (NULL AS VARCHAR2(20))                                         AS LL_LASTNAME,
          CAST (NULL AS VARCHAR2(20))                                         AS LL_ORGANISATION,
          CMX_ID		 					      AS CMX_ID
        FROM tbl_temp_spc_published_capri2
        ) a';
    COMMIT; 

processLog('processCAPRIPublishedData','Information','Completed Procedure');    

EXCEPTION WHEN origin_loaded_today THEN
    processLog('processCAPRIPublishedData','Error','Data already exists for today');
          WHEN OTHERS THEN 
    vErrorMessage := SQLERRM;
    processLog('processCAPRIPublishedData','Error',vErrorMessage);
END processCAPRIPublishedData;


PROCEDURE processStatsOnlyData(pOverrideCheckingTodaysData IN VARCHAR2 DEFAULT 'FALSE')
IS

BEGIN

processLog('processStatsOnlyData','Information','Started Procedure'); 

IF upper(pOverrideCheckingTodaysData) <> 'TRUE' THEN
    vDataRecords := checkOriginForToday('StatsOnlyData');
    IF vDataRecords > 0 THEN
         RAISE origin_loaded_today;
    END IF;    
END IF;    

vDataLoadID := getLoadID;

INSERT INTO TBL_MDT_SPC_MASTER_DATA
    SELECT  vDataLoadID,
            'StatsOnlyData',
            SYSDATE,
            NULL,
            'N',
            NULL,
            NULL,
            TITLE,
            FIRSTNAME,
            LASTNAME,
            EMAIL,
            ROLE,
            JOB_TITLE,
            ORGANIZATION,
            ORG_TYPE,
            COUNTRY_CODE,
            OWNER,
            CAPRI_ID,
            SIS_ID,
            CRM_BUSINESS_DIVISION,
            CRM_SALES_DIVISION,
            CRM_ACCOUNT_ORG_TYPE,
            CRM_ACCOUNT_NAME,
            CRM_CONTACT_TYPE,
            CRM_CONTACT_TITLE,
            CRM_CONTACT_FIRSTNAME,
            CRM_CONTACT_LASTNAME,
            CRM_CONTACT_EMAIL,
            CRM_CONTACT_PHONE,
            LANGUAGE,
            FLEX1,
            FLEX2,
            FLEX3,
            FLEX4,
            FLEX5,
            FLEX6,
            FLEX7,
            FLEX8,
            FLEX9,
            FLEX10,
            FLEX11,
            FLEX12,
            FLEX13,
            FLEX14,
            FLEX15,
            LL_FIRSTNAME,
            LL_LASTNAME,
            LL_ORGANISATION,
            CMX_ID
    FROM   TBL_MDT_SPC_MASTER_DATA a
    WHERE  data_origin in ('Published_OpsReport')
    AND    email is not NULL 
    AND    flex9 /* pmg */ is not null
    AND    data_load_id in (select max(b.data_load_id) from TBL_MDT_SPC_MASTER_DATA b where a.data_origin = b.data_origin)
    ;
    commit;

INSERT INTO TBL_MDT_SPC_MASTER_DATA
    SELECT  vDataLoadID,
            'StatsOnlyData',
            SYSDATE,
            NULL,
            'N',
            NULL,
            NULL,
            TITLE,
            FIRSTNAME,
            LASTNAME,
            EMAIL,
            ROLE,
            JOB_TITLE,
            ORGANIZATION,
            ORG_TYPE,
            COUNTRY_CODE,
            OWNER,
            CAPRI_ID,
            SIS_ID,
            CRM_BUSINESS_DIVISION,
            CRM_SALES_DIVISION,
            CRM_ACCOUNT_ORG_TYPE,
            CRM_ACCOUNT_NAME,
            CRM_CONTACT_TYPE,
            CRM_CONTACT_TITLE,
            CRM_CONTACT_FIRSTNAME,
            CRM_CONTACT_LASTNAME,
            CRM_CONTACT_EMAIL,
            CRM_CONTACT_PHONE,
            LANGUAGE,
            FLEX1,
            FLEX2,
            FLEX3,
            FLEX4,
            FLEX5,
            FLEX6,
            FLEX7,
            FLEX8,
            FLEX9,
            FLEX10,
            FLEX11,
            FLEX12,
            FLEX13,
            FLEX14,
            FLEX15,
            LL_FIRSTNAME,
            LL_LASTNAME,
            LL_ORGANISATION,
            CMX_ID
    FROM   TBL_MDT_SPC_MASTER_DATA a
    WHERE  data_origin in ('Published_CAPRIData')
    AND    flex1 /* user_Group */ in ('EVISE Co-Authors','EES Co-Authors')
    AND    data_load_id in (select max(b.data_load_id) from TBL_MDT_SPC_MASTER_DATA b where a.data_origin = b.data_origin)
    AND    (email, flex3 /*ISSN*/, flex4 /*PII*/) not in (select email, flex3, flex4 from TBL_MDT_SPC_MASTER_DATA where data_load_id = vDataLoadID and data_origin = 'StatsOnlyData')
    AND    flex4 /*PII*/ in (select flex4 /*PII*/ from TBL_MDT_SPC_MASTER_DATA a WHERE a.data_origin in ('Published_OpsReport') and a.data_load_id in (select max(b.data_load_id) from TBL_MDT_SPC_MASTER_DATA b where a.data_origin = b.data_origin))
    ;    
    commit;

processLog('processStatsOnlyData','Information','Completed Procedure');    

EXCEPTION WHEN origin_loaded_today THEN
    processLog('processStatsOnlyData','Error','Data already exists for today');
          WHEN OTHERS THEN 
    vErrorMessage := SQLERRM;
    processLog('processStatsOnlyData','Error',vErrorMessage);
END processStatsOnlyData;


PROCEDURE processMasterData
IS

CURSOR CurOrigins IS
        SELECT  max(data_load_id) as data_load_id, 
                data_origin
        FROM   TBL_MDT_SPC_MASTER_DATA
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
         FROM   TBL_MDT_SPC_MASTER_DATA
         WHERE  data_origin = c.data_origin
         AND    data_load_id < c.data_load_id;

         UPDATE TBL_MDT_SPC_MASTER_DATA a
         SET    DUPLICATE_RECORD = 'Y'
         WHERE  data_load_id = c.data_load_id
         AND    (email, flex3 /*ISSN*/, nvl(flex4,flex7) /*PII*/) IN (SELECT email, flex3 /*ISSN*/, nvl(flex4,flex7) /*PII*/
                                                                FROM TBL_MDT_SPC_MASTER_DATA b
                                                                WHERE a.data_origin = b.data_origin 
                                                                and b.data_load_id = vPreviousLoadID
                                                                );
         COMMIT;                                                                

    --
    -- Mark Duplicates within the same load for the same origin
    --                                                                
         UPDATE TBL_MDT_SPC_MASTER_DATA a
         SET    DUPLICATE_RECORD = 'Y'
         WHERE  data_load_id = c.data_load_id                                                                         
         AND    ROWID NOT IN (SELECT max(rowid)
                              FROM TBL_MDT_SPC_MASTER_DATA b
                              WHERE a.data_origin = b.data_origin 
                              and a.data_load_id = b.data_load_id
                              and a.email = b.email
                              and a.flex3 = b.flex3
                              and nvl(a.flex4,a.flex7) = nvl(b.flex4,b.flex7)
                              );                                                                
         COMMIT;


         UPDATE TBL_MDT_SPC_MASTER_DATA a
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
    FROM TBL_MDT_SPC_MASTER_DATA;
RETURN vLoadID;
END getLoadID;

FUNCTION checkOriginForToday (pOrigin in VARCHAR2)
RETURN NUMBER
IS
    vRowCount NUMBER := 0;
BEGIN

   SELECT nvl(COUNT(*),0) 
   INTO   vRowCount
   FROM   TBL_MDT_SPC_MASTER_DATA
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

     DELETE FROM TBL_MDT_SPC_MASTER_DATA
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
                FROM TBL_MDT_SPC_MASTER_DATA a
                WHERE data_load_id in (
                                      SELECT max(data_load_id) 
                                      FROM TBL_MDT_SPC_MASTER_DATA b 
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

                UPDATE TBL_MDT_SPC_MASTER_DATA
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


END PACK_STATS_PUBLISHING_CAMPAIGN;
/
