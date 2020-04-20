CREATE PACKAGE DBOWNER.PK_BOOKS_ACADEMIC_CAMPAIGN
  CREATE OR REPLACE EDITIONABLE PACKAGE "DBOWNER"."PK_BOOKS_ACADEMIC_CAMPAIGN" 
AS

PROCEDURE processENEWSCoreTitle(pOverrideCheckingTodaysData IN VARCHAR2 DEFAULT 'FALSE');
PROCEDURE processENEWSHybridTitle(pOverrideCheckingTodaysData IN VARCHAR2 DEFAULT 'FALSE');

PROCEDURE processLog (pProcess IN VARCHAR2, pLogType IN VARCHAR2, pMessage IN VARCHAR2);


END PK_BOOKS_ACADEMIC_CAMPAIGN;
/
CREATE OR REPLACE EDITIONABLE PACKAGE BODY "DBOWNER"."PK_BOOKS_ACADEMIC_CAMPAIGN" 
AS

    vErrorMessage        VARCHAR2(4000);
    vDataLoadID          NUMBER := 0;
    vDataRecords         NUMBER := 0;
    origin_loaded_today  EXCEPTION;

PROCEDURE processENEWSCoreTitle(pOverrideCheckingTodaysData IN VARCHAR2 DEFAULT 'FALSE')
IS

BEGIN 

processLog('processENEWSCoreTitle','Information','Started Procedure'); 

EXECUTE IMMEDIATE 'TRUNCATE TABLE TBL_TEMP_CORETITLE';


EXECUTE IMMEDIATE '
INSERT INTO TBL_TEMP_CORETITLE
with main AS (
select 
OpcoPubdateLE,
to_char(to_date(sysdate,''dd/mm/rrrr''),''rrrr'')currentyear,
to_char(to_date(sysdate,''dd/mm/rrrr''),''mm'')currentmonth ,
Tease ,
TITLE,
EDITION_NUMBER,
SUBTITLE,
Isbn13OrImpressionId ,
Author_A_Byline AUTHOR_NAMES,
PriceUS PRICE_USD ,
PriceEUR PRICE_EUR,
PriceGBP PRICE_GBP,
Pages PAGES_NUMBER,
CopyrightYear COPYRIGHT_YEAR,
General_Description,
KEY_FEATURES,
DELTA_US_STATUS,
substr(ES_Subject_Codes,4,3) ES_SUBJECT_CODE_LEVEL3,
substr(ES_Subject_Codes,1,3) ES_SUBJECT_CODE_LEVEL2,
substr(ES_Subject_Codes,1,1) ES_SUBJECT_CODE_LEVEL1,
ES_Subject_Codes,
PMG,
PMC
from TBL_TEMP_PPM_PRODUCTS
where Textbook_Flag =''Y'' and
US_Discount_Code = ''65 - Textbook'' AND
Delivery_Status IN (''Available'',''In production'') AND
VERSION_TYPE IN (''Book - Hardback'',''Book - Paperback'') AND
CopyrightYear >=  ''2017'' )
select 
Tease ,
TITLE,
EDITION_NUMBER,
SUBTITLE,
Isbn13OrImpressionId ,
AUTHOR_NAMES,
PRICE_USD ,
PRICE_EUR,
PRICE_GBP,
PAGES_NUMBER,
COPYRIGHT_YEAR,
General_Description,
KEY_FEATURES,
DELTA_US_STATUS,
ES_SUBJECT_CODE_LEVEL3,
ES_SUBJECT_CODE_LEVEL2,
ES_SUBJECT_CODE_LEVEL1,
ES_Subject_Codes,
PMG,
PMC
from main
where  OpcoPubdateLE like ''%2018%''
and currentyear = ''2018''
and  currentmonth = ''09''
UNION ALL
select 
Tease ,
TITLE,
EDITION_NUMBER,
SUBTITLE,
Isbn13OrImpressionId ,
AUTHOR_NAMES,
PRICE_USD ,
PRICE_EUR,
PRICE_GBP,
PAGES_NUMBER,
COPYRIGHT_YEAR,
General_Description,
KEY_FEATURES,
DELTA_US_STATUS,
ES_SUBJECT_CODE_LEVEL3,
ES_SUBJECT_CODE_LEVEL2,
ES_SUBJECT_CODE_LEVEL1,
ES_Subject_Codes,
PMG,
PMC
from main
where  OpcoPubdateLE like ''%2017%'' and
substr(OpcoPubdateLE,1,2) IN (''10'',''11'',''12'')
and currentyear - 1 = ''2017''
UNION ALL
select 
Tease ,
TITLE,
EDITION_NUMBER,
SUBTITLE,
Isbn13OrImpressionId,
AUTHOR_NAMES,
PRICE_USD ,
PRICE_EUR,
PRICE_GBP,
PAGES_NUMBER,
COPYRIGHT_YEAR,
General_Description,
KEY_FEATURES,
DELTA_US_STATUS,
ES_SUBJECT_CODE_LEVEL3,
ES_SUBJECT_CODE_LEVEL2,
ES_SUBJECT_CODE_LEVEL1,
ES_Subject_Codes,
PMG,
PMC
from main
where  OpcoPubdateLE like ''%2019%'' and
substr(OpcoPubdateLE,1,2) IN (''1/'',''2/'',''3/'')
and currentyear + 1 = ''2019'' ';
COMMIT;
--Added by Vijay on 12/09/2018
BEGIN
PK_CORE_Hybrid_TITLE.CreateDataCoreTitle;
COMMIT;
END;


BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE TBL_ACAD_BKS_END_USERS_V3 purge';
    EXCEPTION WHEN OTHERS THEN NULL;
END;

EXECUTE IMMEDIATE '
CREATE TABLE TBL_ACAD_BKS_END_USERS_V3 AS
SELECT DISTINCT A.TITLE , 
A.FIRSTNAME , 
A.LASTNAME, 
A.EMAIL , 
A.ORIG_ROLE ,  
A.ORIG_JOB_TITLE,
A.AG_ORG_TYPE,
A.COUNTRY_CODE,
A.END_USER_SKEY,
A.ORIG_PARTY_REF AS CAPRI_ID,
A.ACCOUNT_BUSINESS_DIVISION,
A.ACCOUNT_SALES_DIVISION,
A.ACCOUNT_NAME,
A.COMMS_LANGUAGE,
A.ACCOUNT_CMX_ID,
A.USER_GROUP
FROM  REPORT_OWNER.D_END_USER_ACCOUNTS A
WHERE A.BKS_RECORD   = 1
AND A.EMAIL IS NOT NULL
AND (A.USER_GROUP IN (''ELBA Contacts'',''PC Alert Subscribers'',''Delta Buyer'')
and A.ORIG_ROLE in (''LECTURER (HE)'',''TEACHER'',''FACULTY/RESEARCHER/SCIENTIST'',''DEAN'',''HEAD OF DEPARTMENT'',
''EX'',''HO'',''OTHER'',''SENIOR MANAGEMENT (RESEARCH ADMIN)''))
and  A.COUNTRY not in (''Finland'',''France'',''Italy'',''India'',''Greece'',''Malta'',''Spain'',''Portugal'')
AND   a.end_user_skey in (select mp.end_user_skey from report_owner.D_END_USER_MAILING_PREFS mp where mp.marketable = ''Y'' 
and a.user_group = mp.user_group)
and   CASE
    WHEN NEXT_DAY(SYSDATE-7,''FRI'') - A.CREATED_DATE <= (365.25*(36/12))
    THEN ''Y'' 
    ELSE ''N''
  END = ''Y'' ';

EXECUTE IMMEDIATE '
insert into tbl_acad_bks_end_users_v3
SELECT DISTINCT A.TITLE , 
A.FIRSTNAME , 
A.LASTNAME, 
A.EMAIL , 
A.ORIG_ROLE ,  
A.ORIG_JOB_TITLE,
A.AG_ORG_TYPE,
A.COUNTRY_CODE,
A.END_USER_SKEY,
A.ORIG_PARTY_REF,
A.ACCOUNT_BUSINESS_DIVISION,
A.ACCOUNT_SALES_DIVISION,
A.ACCOUNT_NAME,
A.COMMS_LANGUAGE,
A.ACCOUNT_CMX_ID,
A.USER_GROUP
from  REPORT_OWNER.D_END_USER_ACCOUNTS A
WHERE A.BKS_RECORD       = 1
AND A.EMAIL IS NOT NULL
AND a.user_group = ''TEC Contacts''
and  A.COUNTRY not in (''Finland'',''France'',''Italy'',''India'',''Greece'',''Malta'',''Spain'',''Portugal'')
AND   a.end_user_skey in (select mp.end_user_skey from report_owner.D_END_USER_MAILING_PREFS mp where mp.marketable = ''Y'' 
and a.user_group = mp.user_group)
and   CASE
    WHEN NEXT_DAY(SYSDATE-7,''FRI'') - A.CREATED_DATE <= (365.25*(36/12))
    THEN ''Y''
    ELSE ''N''
  END = ''Y'' ';

COMMIT;  

EXECUTE IMMEDIATE 'create index INDX_ACAD_BKS_END_USER on TBL_ACAD_BKS_END_USERS_V3(END_USER_SKEY)';

EXECUTE IMMEDIATE 'create index INDX_ACAD_BKS_USER_GROUP on TBL_ACAD_BKS_END_USERS_V3(USER_GROUP)';

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE tbl_acad_bks_promis_level4 purge';
    EXCEPTION WHEN OTHERS THEN NULL;
END;

EXECUTE IMMEDIATE '
create table tbl_acad_bks_promis_level4 as
select distinct sub.product_subject_id, pro.promis_level_4_code , pro.promis_level_3_code 
from   REPORT_OWNER.D_PRODUCT_SUBJECT sub,  REPORT_OWNER.D_PRODUCT_SUBJECT pro
where pro.item_type  in ( ''BOK'',''JOU'')
and sub.identifier = pro.promis_level_4_code
and exists
(select ''x'' from DBOWNER.tbl_temp_coretitle c where c.es_subject_code_level3 = pro.promis_level_3_code) ';

EXECUTE IMMEDIATE 'create index INDX_ACAD_BKS_PROMIS_LVL4 on tbl_acad_bks_promis_level4(promis_level_3_code)';

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE TBL_MAIN_CORETITLE purge';
    EXCEPTION WHEN OTHERS THEN NULL;
END;

EXECUTE IMMEDIATE '
CREATE TABLE TBL_MAIN_CORETITLE AS 
Select DISTINCT
A.TITLE AS USER_TITLE, 
A.FIRSTNAME , 
A.LASTNAME, 
A.EMAIL , 
A.ORIG_ROLE ,  
A.ORIG_JOB_TITLE,
A.AG_ORG_TYPE,
A.COUNTRY_CODE,
A.END_USER_SKEY,
A.CAPRI_ID,
A.ACCOUNT_BUSINESS_DIVISION,
A.ACCOUNT_SALES_DIVISION,
A.ACCOUNT_NAME,
A.COMMS_LANGUAGE,
A.ACCOUNT_CMX_ID,
A.USER_GROUP,
''ST-BKS'' AS OWNER,
to_Char(CT.TEASE) TEASE ,
to_Char(CT.TITLE )TITLE,
CT.EDITION_NUMBER ,
to_Char(CT.SUBTITLE)SUBTITLE ,
CT.Isbn13OrImpressionId,
to_Char(CT.AUTHOR_NAMES)AUTHOR_NAMES ,
CT.PRICE_USD ,
CT.PRICE_EUR ,
CT.PRICE_GBP ,
CT.PAGES_NUMBER ,
CT.COPYRIGHT_YEAR ,
to_Char(CT.General_DesCription )General_DesCription,
to_Char(CT.KEY_FEATURES)KEY_FEATURES ,
CT.DELTA_US_STATUS,
CT.ES_SUBJECT_CODE_LEVEL3,
CT.ES_SUBJECT_CODE_LEVEL2,
CT.ES_SUBJECT_CODE_LEVEL1,
CT.ES_SubjeCt_Codes,
CT.PMG,
CT.PMC,
''Elsevier BV'' As LEGAL_ENTITY
FROM DBOWNER.TBL_ACAD_BKS_END_USERS_V3 A,
REPORT_OWNER.F_SUBJECT_INTEREST_COUNT F,
DBOWNER.tbl_acad_bks_promis_level4 P,
DBOWNER.tbl_temp_coretitle CT
where A.END_USER_SKEY      = F.END_USER_SKEY
AND   A.USER_GROUP         = F.USER_GROUP
AND   F.PRODUCT_SUBJECT_ID = P.PRODUCT_SUBJECT_ID
AND   P.promis_level_3_code = CT.es_subject_code_level3
AND   F.BKS_RECORD         = ''Y''
AND   F.INTEREST_TYPE      = ''Books ENews'' ';

processLog('processENEWSCoreTitle','Information','Completed Procedure');    

EXCEPTION WHEN origin_loaded_today THEN
    processLog('processENEWSCoreTitle','Error','Data already exists for today');
          WHEN OTHERS THEN 
    vErrorMessage := SQLERRM;
    processLog('processENEWSCoreTitle','Error',vErrorMessage);
END processENEWSCoreTitle;


PROCEDURE processENEWSHybridTitle(pOverrideCheckingTodaysData IN VARCHAR2 DEFAULT 'FALSE')
IS

BEGIN 
dbms_output.put_line ('processENEWSHybridTitle...started');
processLog('processENEWSHybridTitle','Information','Started Procedure'); 

dbms_output.put_line ('TRUNCATE TABLE TBL_TEMP_HYBRIDTITLE');
EXECUTE IMMEDIATE 'TRUNCATE TABLE TBL_TEMP_HYBRIDTITLE';

dbms_output.put_line ('INSERT TABLE TBL_TEMP_HYBRIDTITLE');
EXECUTE IMMEDIATE '
INSERT INTO TBL_TEMP_HYBRIDTITLE 
with main AS (
select 
OpcoPubdateLE,
to_char(to_date(sysdate,''dd/mm/rrrr''),''rrrr'')currentyear ,
to_char(to_date(sysdate,''dd/mm/rrrr''),''mm'')currentmonth ,
Tease ,
TITLE,
EDITION_NUMBER,
SUBTITLE,
Isbn13OrImpressionId ,
Author_A_Byline AUTHOR_NAMES,
PriceUS PRICE_USD ,
PriceEUR PRICE_EUR,
PriceGBP PRICE_GBP,
Pages PAGES_NUMBER,
CopyrightYear COPYRIGHT_YEAR,
General_Description,
KEY_FEATURES,
DELTA_US_STATUS,
substr(ES_Subject_Codes,4,3) ES_SUBJECT_CODE_LEVEL3,
substr(ES_Subject_Codes,1,3) ES_SUBJECT_CODE_LEVEL2,
substr(ES_Subject_Codes,1,1) ES_SUBJECT_CODE_LEVEL1,
ES_Subject_Codes,
PMG,
PMC
from TBL_TEMP_PPM_PRODUCTS
where Textbook_Flag =''Y'' and
US_Discount_Code = ''61 - Non-serials'' AND
Delivery_Status IN (''Available'',''In production'') AND
VERSION_TYPE IN (''Book - Hardback'',''Book - Paperback'') AND
CopyrightYear >=  ''2017'' AND
TxtRefTrd = ''Hybrid'')
select 
Tease ,
TITLE,
EDITION_NUMBER,
SUBTITLE,
Isbn13OrImpressionId,
AUTHOR_NAMES,
PRICE_USD ,
PRICE_EUR,
PRICE_GBP,
PAGES_NUMBER,
COPYRIGHT_YEAR,
General_Description,
KEY_FEATURES,
DELTA_US_STATUS,
ES_SUBJECT_CODE_LEVEL3,
ES_SUBJECT_CODE_LEVEL2,
ES_SUBJECT_CODE_LEVEL1,
ES_Subject_Codes,
PMG,
PMC
from main
where  OpcoPubdateLE like ''%2018%''
and currentyear = ''2018'' 
and currentmonth = ''09''
and substr(OpcoPubdateLE,1,2) IN (''7/'',''8/'',''9/'',''10'',''11'',''12'')
UNION ALL
select 
Tease ,
TITLE,
EDITION_NUMBER,
SUBTITLE,
Isbn13OrImpressionId ,
AUTHOR_NAMES,--Need to work on this
 PRICE_USD ,
 PRICE_EUR,
PRICE_GBP,
PAGES_NUMBER,
COPYRIGHT_YEAR,
General_Description,
KEY_FEATURES,
DELTA_US_STATUS,
ES_SUBJECT_CODE_LEVEL3,
ES_SUBJECT_CODE_LEVEL2,
ES_SUBJECT_CODE_LEVEL1,
ES_Subject_Codes,
PMG,
PMC
from main
where  OpcoPubdateLE like ''%2019%'' 
and currentyear = ''2019''
and currentmonth = ''02''
and substr(OpcoPubdateLE,1,2) IN (''1/'',''2/'',''3/'',''4/'',''5/'',''6/'')
';
commit;

--Added by Vijay on 12/09/2018
dbms_output.put_line ('PK_CORE_Hybrid_TITLE.CreateDataHybridTitle');
BEGIN
PK_CORE_Hybrid_TITLE.CreateDataHybridTitle;
COMMIT;
END;
dbms_output.put_line ('DROP TABLE INDX_ACAD_BKS_PROMIS_LVL4_hyb purge');
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE INDX_ACAD_BKS_PROMIS_LVL4_hyb purge';
    EXCEPTION WHEN OTHERS THEN NULL;
END;
BEGIN
EXECUTE IMMEDIATE 'DROP TABLE tbl_acad_bks_promis_level4_hyb purge';
 EXCEPTION WHEN OTHERS THEN NULL;
END;
dbms_output.put_line ('create table tbl_acad_bks_promis_level4_hyb');
EXECUTE IMMEDIATE '
create table tbl_acad_bks_promis_level4_hyb as
select distinct sub.product_subject_id, pro.promis_level_4_code , pro.promis_level_3_code 
from   REPORT_OWNER.D_PRODUCT_SUBJECT sub,  REPORT_OWNER.D_PRODUCT_SUBJECT pro
where pro.item_type  in ( ''BOK'',''JOU'')
and sub.identifier = pro.promis_level_4_code
and exists
(select ''x'' from DBOWNER.tbl_temp_hybridtitle c where c.es_subject_code_level3 = pro.promis_level_3_code) ';

EXECUTE IMMEDIATE 'create index INDX_ACAD_BKS_PROMIS_LVL4_hyb on tbl_acad_bks_promis_level4_hyb(promis_level_3_code)';
dbms_output.put_line ('DROP TABLE TBL_MAIN_HYBRIDTITLE');
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE TBL_MAIN_HYBRIDTITLE purge';
    EXCEPTION WHEN OTHERS THEN NULL;
END;
dbms_output.put_line ('CREATE TABLE TBL_MAIN_HYBRIDTITLE');
EXECUTE IMMEDIATE '
CREATE TABLE TBL_MAIN_HYBRIDTITLE AS 
Select DISTINCT
A.TITLE AS USER_TITLE, 
A.FIRSTNAME , 
A.LASTNAME, 
A.EMAIL , 
A.ORIG_ROLE ,  
A.ORIG_JOB_TITLE,
A.AG_ORG_TYPE,
A.COUNTRY_CODE,
A.END_USER_SKEY,
A.CAPRI_ID,
A.ACCOUNT_BUSINESS_DIVISION,
A.ACCOUNT_SALES_DIVISION,
A.ACCOUNT_NAME,
A.COMMS_LANGUAGE,
A.ACCOUNT_CMX_ID,
A.USER_GROUP,
''ST-BKS'' AS OWNER,
to_Char(CT.TEASE) TEASE ,
to_Char(CT.TITLE )TITLE,
CT.EDITION_NUMBER ,
to_Char(CT.SUBTITLE)SUBTITLE ,
CT.Isbn13OrImpressionId,
to_Char(CT.AUTHOR_NAMES)AUTHOR_NAMES ,
CT.PRICE_USD ,
CT.PRICE_EUR ,
CT.PRICE_GBP ,
CT.PAGES_NUMBER ,
CT.COPYRIGHT_YEAR ,
to_Char(CT.General_DesCription )General_DesCription,
to_Char(CT.KEY_FEATURES)KEY_FEATURES ,
CT.DELTA_US_STATUS,
CT.ES_SUBJECT_CODE_LEVEL3,
CT.ES_SUBJECT_CODE_LEVEL2,
CT.ES_SUBJECT_CODE_LEVEL1,
CT.ES_SubjeCt_Codes,
CT.PMG,
CT.PMC,
''Elsevier BV'' As LEGAL_ENTITY
FROM DBOWNER.TBL_ACAD_BKS_END_USERS_V3 A,
REPORT_OWNER.F_SUBJECT_INTEREST_COUNT F,
DBOWNER.tbl_acad_bks_promis_level4_hyb P,
DBOWNER.tbl_temp_hybridtitle CT
where A.END_USER_SKEY      = F.END_USER_SKEY
AND   A.USER_GROUP         = F.USER_GROUP
AND   F.PRODUCT_SUBJECT_ID = P.PRODUCT_SUBJECT_ID
AND   P.promis_level_3_code = CT.es_subject_code_level3
AND   F.BKS_RECORD         = ''Y''
AND   F.INTEREST_TYPE      = ''Books ENews'' ';

processLog('processENEWSHybridTitle','Information','Completed Procedure');    
dbms_output.put_line ('Before Exception');
EXCEPTION WHEN origin_loaded_today THEN
dbms_output.put_line ('Inside  Exception' );
    processLog('processENEWSHybridTitle','Error','Data already exists for today');
          WHEN OTHERS THEN 
    vErrorMessage := SQLERRM;
    processLog('processENEWSHybridTitle','Error',vErrorMessage);
END processENEWSHybridTitle;

PROCEDURE processLog (pProcess IN VARCHAR2, pLogType IN VARCHAR2, pMessage IN VARCHAR2)
IS 

BEGIN 

    INSERT INTO TBL_LOG_SPC
    VALUES (SYSDATE,pProcess,pLogType,pMessage);
    COMMIT;

END processLog;

END PK_BOOKS_ACADEMIC_CAMPAIGN;
/
