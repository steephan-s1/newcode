CREATE PACKAGE DBOWNER.PK_PULL_HUB
  CREATE OR REPLACE EDITIONABLE PACKAGE "DBOWNER"."PK_PULL_HUB" AS
/*******************************************************************************
Author		: Chandrashekar Ganesh
Description	: Package to populate Staging Tables for HubSpot
Audit		:
	Version		Date		User		Description
	1		23-Feb-2016	Ganesh Shekar	Initial version
*******************************************************************************/
	PROCEDURE get_cell_contacts
				(p_orig_upd_create_date		IN	VARCHAR2
				,p_cell_contacts		OUT	SYS_REFCURSOR
				,p_error_message		OUT	VARCHAR2);

	PROCEDURE pull_source(p_source IN VARCHAR2, p_success IN OUT VARCHAR2);

END pk_pull_hub;
 
/
CREATE OR REPLACE EDITIONABLE PACKAGE BODY "DBOWNER"."PK_PULL_HUB" 
AS
  /*******************************************************************************
  Author  : Chandrashekar Ganesh
  Description : Package to populate Staging Tables
  Audit  :
  Version  Date  User  Description
  1  23-Feb-2016 Ganesh Shekar Initial version
  2  31-Aug-2017 Hariharan J   MARKAU-3425 - Adding 'First Conversation Event Name' and 'Recent Conversation Event Name' for HUBSPOT data for Segmentation
  *******************************************************************************/
  g_pos         NUMBER         := 0;
  v_message     VARCHAR2(3000) := '';
  v_err_code    NUMBER;
  v_query_count NUMBER := 0;
  PROCEDURE get_cell_contacts(
      p_orig_upd_create_date IN VARCHAR2 ,
      p_cell_contacts OUT SYS_REFCURSOR ,
      p_error_message OUT VARCHAR2)
  IS
    l_orig_upd_create_date DATE;
  BEGIN
    l_orig_upd_create_date := TRUNC(SYSDATE) - TO_NUMBER(p_orig_upd_create_date, '9999');
   
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE TBL_HUB_DETAILS PURGE';
EXCEPTION WHEN OTHERS THEN NULL;
END;
    
    EXECUTE IMMEDIATE 'Create table TBL_HUB_DETAILS as
SELECT PTYHUB.*,  
ITRHUB.INTEREST_VALUES
FROM  
(SELECT A.ORIG_PARTY_REF ,    
A.ORG_ORIG_SYSTEM_REF ,    
A.ORIG_SYSTEM ,  
A.ORIG_TITLE  ,
A.ORIG_CREATE_DATE ,    
A.ORIG_UPDATE_DATE ,    
A.PARTY_NAME ,    
A.FIRSTNAME ,    
A.LASTNAME ,    
A.EMAIL ,  
A.COUNTRY ,
A.ISO_COUNTRY_CODE , 
A.JOB_TITLE , 
A.ORG_TYPE ,
A.USER_ORGANISATION ,
A.SALES_EMAILS ,    
A.MARKETING_EMAILS      
FROM TBL_PARTIES A  
WHERE ORIG_SYSTEM = ''HUB''  
) PTYHUB
LEFT OUTER JOIN  
(SELECT PARTY_REF,    
UTL_I18N.UNESCAPE_REFERENCE(RTRIM (XMLAGG (XMLELEMENT (E, INTEREST_VALUE    
|| '';'')).EXTRACT (''//text()''), '';'')) INTEREST_VALUES  
FROM TBL_INTERESTS  
WHERE ORIG_SYSTEM = ''HUB''   
GROUP BY PARTY_REF  
) ITRHUB
ON PTYHUB.ORIG_PARTY_REF = ITRHUB.PARTY_REF';

    EXECUTE IMMEDIATE 'CREATE INDEX IDX_TBL_HUB_DETAILS_1 ON TBL_HUB_DETAILS(ORG_ORIG_SYSTEM_REF)';

    EXECUTE IMMEDIATE 'CREATE INDEX IDX_TBL_HUB_DETAILS_2 ON TBL_HUB_DETAILS(ORIG_UPDATE_DATE)';

    EXECUTE IMMEDIATE 'CREATE INDEX IDX_TBL_HUB_DETAILS_3 ON TBL_HUB_DETAILS(EMAIL)';
   
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE TBL_AE2_DETAILS PURGE';
EXCEPTION WHEN OTHERS THEN NULL;
END;    

    EXECUTE IMMEDIATE 'Create table TBL_AE2_DETAILS as
SELECT pty.orig_title title ,  
pty.firstname firstname ,  
pty.lastname lastname ,  
pty.email email ,  
pty.country country ,  
pty.iso_country_code iso_country_code ,  
pty.job_title profession ,  
DECODE(pty.principal_field,''Other'',NULL,pty.principal_field) AE2_field_of_research ,  
pty.org_type work_setting ,  
pty.user_organisation Institution ,  
pty.orig_party_ref capri_ref ,  
PTY.ORIG_UPDATE_DATE cellpress_update_date
FROM dbowner.tbl_parties pty
WHERE pty.orig_system     = ''AE2''
AND pty.orig_site         = ''Cell Press''
AND pty.dedupe_type       = ''PER''
AND pty.record_status IN (''A'', ''I'', ''U'')
AND (pty.orig_update_date > SYSDATE-7 OR pty.orig_create_date > SYSDATE-7)
AND EXISTS  
(SELECT 1  
FROM dbowner.tbl_interests MIN  
WHERE min.party_ref            = pty.org_orig_system_ref  
AND min.orig_system            = pty.orig_system  
AND min.interest_type          = ''MIN''  
AND min.interest_value_details = ''cell''  
AND min.record_status          = ''A''  
)';
    
    EXECUTE IMMEDIATE 'CREATE INDEX IDX_TBL_AE2_DETAILS_1 ON TBL_AE2_DETAILS(email)';
    
    EXECUTE IMMEDIATE 'CREATE INDEX IDX_TBL_AE2_DETAILS_2 ON TBL_AE2_DETAILS(cellpress_update_date)';
    
    EXECUTE IMMEDIATE 'CREATE INDEX IDX_TBL_AE2_DETAILS_3 ON TBL_AE2_DETAILS(capri_ref)';
       
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE TBL_AE2_OPTINS_HUB_EXPORT PURGE';
EXCEPTION WHEN OTHERS THEN NULL;
END;    
    
    
    EXECUTE IMMEDIATE ' 
CREATE TABLE TBL_AE2_OPTINS_HUB_EXPORT AS
SELECT A.TITLE,  
A.FIRSTNAME,  
A.LASTNAME,  
A.EMAIL,  
A.COUNTRY,  
A.ISO_COUNTRY_CODE,  
A.PROFESSION,  
A.AE2_FIELD_OF_RESEARCH,  
B.INTEREST_VALUES,  
FK_GET_HUB_INTEREST(A.AE2_FIELD_OF_RESEARCH,B.INTEREST_VALUES) FIELD_OF_RESEARCH_CHECKBOXES,  
A.WORK_SETTING,  
A.INSTITUTION,  
A.CAPRI_REF,  
A.CELLPRESS_UPDATE_DATE,  
B.ORIG_UPDATE_DATE HUB_UPDATE_DATE
FROM TBL_AE2_DETAILS A
LEFT OUTER JOIN TBL_HUB_DETAILS B
ON A.EMAIL = B.EMAIL';
    
    EXECUTE IMMEDIATE 'CREATE INDEX IDX_TBL_AE2_OPTINS_1 ON TBL_AE2_OPTINS_HUB_EXPORT(CELLPRESS_UPDATE_DATE)';
    
    EXECUTE IMMEDIATE 'CREATE INDEX IDX_TBL_AE2_OPTINS_2 ON TBL_AE2_OPTINS_HUB_EXPORT(HUB_UPDATE_DATE)';
    
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE TBL_AE2_OPTOUTS PURGE';
EXCEPTION WHEN OTHERS THEN NULL;
END;    

    
    EXECUTE IMMEDIATE ' 
CREATE TABLE TBL_AE2_OPTOUTS AS
SELECT B.ORIG_TITLE   TITLE
,B.FIRSTNAME   FIRSTNAME
,B.LASTNAME   LASTNAME
,B.EMAIL   EMAIL
,B.COUNTRY   COUNTRY
,B.ISO_COUNTRY_CODE  ISO_COUNTRY_CODE
,B.JOB_TITLE   PROFESSION
,DECODE(B.PRINCIPAL_FIELD,''Other'',NULL,B.PRINCIPAL_FIELD) FIELD_OF_RESEARCH_CHECKBOXES
,B.ORG_TYPE   WORK_SETTING
,B.USER_ORGANISATION   INSTITUTION
,B.ORIG_PARTY_REF  CAPRI_REF
,B.ORIG_UPDATE_DATE   CELLPRESS_UPDATE_DATE 
,B.ORG_ORIG_SYSTEM_REF 
,''Y'' AE2_OPT_OUTS
FROM TBL_INTERESTS A, TBL_PARTIES B WHERE A.ORIG_SYSTEM          = ''AE2''
AND A.INTEREST_TYPE          = ''MIN''
AND A.INTEREST_VALUE_DETAILS = ''cell''
AND A.RECORD_STATUS          = ''D''
AND A.PARTY_REF              = B.ORG_ORIG_SYSTEM_REF
AND B.ORIG_SYSTEM            = ''AE2''
AND B.ORIG_SITE              = ''Cell Press''
AND B.ORIG_UPDATE_DATE > sysdate-7';
    
    EXECUTE IMMEDIATE 'CREATE INDEX IDX_TBL_AE2_OPTOUTS_1 ON TBL_AE2_OPTOUTS(CELLPRESS_UPDATE_DATE)';
    
    EXECUTE IMMEDIATE 'CREATE INDEX IDX_TBL_AE2_OPTOUTS_2 ON TBL_AE2_OPTOUTS(ORG_ORIG_SYSTEM_REF)';
        
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE TBL_AE2_OPTOUTS_HUB_EXPORT PURGE';
EXCEPTION WHEN OTHERS THEN NULL;
END;    
    
    
    EXECUTE IMMEDIATE ' 
CREATE TABLE TBL_AE2_OPTOUTS_HUB_EXPORT AS
SELECT 
B.ORIG_TITLE TITLE
,B.FIRSTNAME
,B.LASTNAME
,B.EMAIL
,B.COUNTRY
,B.ISO_COUNTRY_CODE
,B.JOB_TITLE PROFESSION
,B.INTEREST_VALUES FIELD_OF_RESEARCH_CHECKBOXES
,B.ORG_TYPE WORK_SETTING
,B.USER_ORGANISATION INSTITUTION
,B.ORG_ORIG_SYSTEM_REF CAPRI_REF
,A.CELLPRESS_UPDATE_DATE
,B.ORIG_UPDATE_DATE HUB_UPDATE_DATE
FROM TBL_AE2_OPTOUTS A
,TBL_HUB_DETAILS B
WHERE A.CAPRI_REF = B.ORG_ORIG_SYSTEM_REF
AND B.MARKETING_EMAILS = ''Y''';
    
    EXECUTE IMMEDIATE 'CREATE INDEX IDX_TBL_AE2_OPTOUTS_3 ON TBL_AE2_OPTOUTS_HUB_EXPORT(CELLPRESS_UPDATE_DATE)';
    
    EXECUTE IMMEDIATE 'CREATE INDEX IDX_TBL_AE2_OPTOUTS_4 ON TBL_AE2_OPTOUTS_HUB_EXPORT(HUB_UPDATE_DATE)';

    OPEN p_cell_contacts FOR 
      select title 
          ,firstname 
          ,lastname 
          ,email 
          ,country 
          ,iso_country_code 
          ,profession 
          ,field_of_research_checkboxes 
          ,work_setting 
          ,institution 
          ,capri_ref
          ,null capri_opt_outs
          from tbl_ae2_optins_hub_export 
          where cellpress_update_date > hub_update_date or hub_update_date is null
        union
      select title 
          ,firstname 
          ,lastname 
          ,email 
          ,country 
          ,iso_country_code 
          ,profession 
          ,null field_of_research_checkboxes 
          ,work_setting 
          ,institution 
          ,capri_ref
          ,'true' capri_opt_outs
          from tbl_ae2_optouts_hub_export ;
  
  p_error_message  := SQLERRM;
  
  EXCEPTION
  
  WHEN OTHERS THEN
    p_error_message := SQLERRM;
    ROLLBACK;
  END get_cell_contacts;

--// Create Parties from HubSpot
  
  PROCEDURE pull_parties(
      p_source  IN VARCHAR2 ,
      p_success IN OUT VARCHAR2)
  IS
  BEGIN
    g_pos := 1;
    
    pk_main.log_time(p_source, 'pk_pull_hub', 'pull_parties', 'Creating Authors', g_pos, true);
    
    INSERT
    INTO tbl_temp_parties
      (
        status ,
        orig_system ,
        orig_system_ref ,
        party_type ,
        dedupe_type ,
        usr_created_date ,
        usr_last_visit_date ,
        usc_contact_title ,
        orig_title ,
        jobtitle ,
        usc_contact_firstname ,
        usc_contact_lastname ,
        usc_dedupe_email ,
        usc_country ,
        iso_country_code ,
        usc_org ,
        unique_inst_id ,
        usr_subscriber_code ,
        org_type ,
        sales_emails ,
        marketing_emails ,
        user_access_type ,
        run_time,
        --Start - JIRA 3425
        usr_url_ref,
        usr_other_ref
      --End - JIRA 3425
      )
    SELECT 'A' status ,
      'HUB' orig_system ,
      'HUB'
      || '_'
      || vidno orig_system_ref ,
      'EUS' party_type ,
      'PER' dedupe_type ,
      TO_DATE('19700101','yyyymmdd') + (createdate/1000/24/60/60) usr_created_date ,
      TO_DATE('19700101','yyyymmdd') + (lastmodifieddate/1000/24/60/60) usr_last_visit_date ,
      title usc_contact_title ,
      title orig_title ,
      profession jobtitle ,
      firstname usc_contact_firstname ,
      lastname usc_contact_lastname ,
      email usc_dedupe_email ,
      country usc_country ,
      iso_country_code iso_country_code ,
      institution usc_org ,
      capri_ref unique_inst_id ,
      lifecyclestage usr_subscriber_code ,
      work_setting org_type ,
      DECODE(upper(optoutallemail),'TRUE', 'N', 'Y' ) sales_emails ,
      DECODE(upper(optoutmktemail),'TRUE', 'N', 'Y' ) marketing_emails ,
      DECODE(upper(caprioptouts),'TRUE', 'N', 'Y' ) user_access_type ,
      TO_DATE('19700101','yyyymmdd') + (lastmodifieddate/1000/24/60/60) run_time,
      --Start JIRA -3425
      CASE WHEN  first_conversion_event_name LIKE '%: Name%' 
            THEN
                 substr(first_conversion_event_name, 1, instr(first_conversion_event_name,': Name') -1 )
            ELSE
                first_conversion_event_name
        END first_conversion_event_name, 
      CASE WHEN  recent_conversion_event_name LIKE '%: Name%' 
            THEN
                 substr(recent_conversion_event_name, 1, instr(recent_conversion_event_name,': Name') -1 )
            ELSE
                recent_conversion_event_name
        END recent_conversion_event_name
      --End JIRA -3425
    FROM tbl_temp_hub_parties;
    --// Saving Changes
    
    COMMIT;
    
    g_pos := 2;
    
    pk_main.log_time(p_source, 'pk_pull_hub', 'pull_parties', 'Completed Creating Authors', g_pos, true);
    
    --// Mark Comletion as SUCCESS
    p_success := 'Y';
  EXCEPTION
  WHEN OTHERS THEN
    pk_main.log_error(p_source, 'pk_pull_hub', 'pull_parties', SQLCODE, SQLERRM, 999);
    p_success := 'N';
    ROLLBACK;
    RAISE_APPLICATION_ERROR(-20001, 'Failed to Load pull_parties ');
  END pull_parties;

--// Create Interests from HubSpot
  PROCEDURE pull_interest(
      p_source  IN VARCHAR2 ,
      p_success IN OUT VARCHAR2)
  IS
    lPersons INTEGER;
    lSQL     VARCHAR2(4000);
  BEGIN
    g_pos := 1;
    
    pk_main.log_time(p_source, 'pk_pull_hub', 'interest_books', 'Start Creating Books for Authors', g_pos, true);
    
    p_success := 'N';
    --// Create Books for the rest of Books
    lPersons := 1;
    
    INSERT
    INTO tbl_temp_interests
      (
        orig_system_ref ,
        orig_system ,
        party_ref ,
        create_date ,
        update_date ,
        interest_type ,
        interest_value ,
        status
      )
    SELECT orig_system_ref ,
      orig_system ,
      party_ref ,
      create_date ,
      update_date ,
      interest_type ,
      DECODE(trim(interest_value),'Biophsyics','Biophysics','Ecology and Evolution','Ecology & Evolution','Infectious Disease','Infectious Diseases', 'Systems/Computational Biology','Systems/computational biology',trim(interest_value)) interest_value ,
      status
    FROM
      (SELECT 'HUB'
        || '_SPE_'
        || t.vidno
        || '_'
        || lower(trim(regexp_substr(regexp_replace(t.field_of_research_checkboxes,' |/|\|&',''), '[^;]+', 1, levels.column_value))) orig_system_ref ,
        'HUB' orig_system ,
        'HUB'
        || '_'
        ||vidno party_ref ,
        TO_DATE('19700101','yyyymmdd') + (t.createdate/1000/24/60/60) create_date ,
        TO_DATE('19700101','yyyymmdd') + (t.lastmodifieddate/1000/24/60/60) update_date ,
        'SPE' interest_type ,
        trim(regexp_substr(t.field_of_research_checkboxes, '[^;]+', 1, levels.column_value)) interest_value ,
        'I' status
      FROM tbl_temp_hub_parties t,
        TABLE(CAST(multiset
        (SELECT level
        FROM dual
          CONNECT BY level <= LENGTH (regexp_replace(t.field_of_research_checkboxes, '[^;]+')) + 1
        ) AS sys.OdciNumberList)) levels
      WHERE t.field_of_research_checkboxes IS NOT NULL
      );
    --// Save Changes
    COMMIT;
    --// Mark Comletion as SUCCESS
    g_pos := 100;
    pk_main.log_time(p_source, 'pk_pull_hub', 'interest_books', 'Completed Creating Books for Authors', g_pos, true);
    p_success := 'Y';
  EXCEPTION
  WHEN OTHERS THEN
    pk_main.log_error(p_source, 'pk_pull_hub', 'interest_books', SQLCODE, SQLERRM, 999);
    p_success := 'N';
    ROLLBACK;
    RAISE_APPLICATION_ERROR(-20001, 'Failed to Load interest_books ');
  END pull_interest;
--// Main
  PROCEDURE pull_source(
      p_source  IN VARCHAR2 ,
      p_success IN OUT VARCHAR2)
  IS
  BEGIN
    pk_main.log_time(p_source, 'pk_pull_hub', 'pull_source', 'Start', g_pos, true);
    p_success := 'N';
    UPDATE dbowner.tbl_sources
    SET pull_start             = SYSDATE ,
      pull_end                 = NULL ,
      pull_status              = 'R' ,
      pull_error_msg           = NULL ,
      related_transfer_session = NULL
    WHERE source               = p_source;
    --// Clean Temp Tables
    --EXECUTE IMMEDIATE 'ALTER TABLE tbl_temp_parties TRUNCATE PARTITION par_hub';
    --EXECUTE IMMEDIATE 'ALTER TABLE tbl_temp_interests TRUNCATE PARTITION par_hub';
    --// Perform Load
    pull_parties(p_source, p_success);
    pull_interest(p_source, p_success);
    --// Correct Country in Tbl_Temp_Parties
    FOR rec0 IN
    (SELECT rowid ,
      iso_country_code ,
      usc_country
    FROM dbowner.tbl_temp_parties
    WHERE orig_system = 'HUB'
    )
    LOOP
      FOR rec1 IN
      (SELECT c.iso_code ,
        c.country_name
      FROM dbowner.tbl_countries b ,
        dbowner.tbl_iso_countries c
      WHERE (upper(trim(rec0.usc_country)) = b.source_value
      OR rec0.usc_country                  = b.iso_code )
      AND b.iso_code                       = c.iso_code
      )
      LOOP
        UPDATE dbowner.tbl_temp_parties
        SET iso_country_code = rec1.iso_code ,
          usc_country        = rec1.country_name ,
          doctored           = 'Y'
        WHERE rowid          = rec0.rowid;
      END LOOP;
    END LOOP;
    -- UPDATE dbowner.tbl_temp_parties a
    -- SET (a.iso_country_code, a.usc_country, a.doctored) =
    --   (SELECT DISTINCT c.iso_code, c.country_name, 'Y'
    --    FROM dbowner.tbl_countries b, dbowner.tbl_iso_countries c
    --    WHERE a.usc_country = b.iso_code
    --    AND b.iso_code = c.iso_code
    --   )
    -- WHERE a.orig_system = p_source;
    --// Update End Time on Successful Completion
    IF p_success = 'Y' THEN
      UPDATE dbowner.tbl_sources
      SET last_marker_db1= this_marker_db1 ,
        pull_end         = SYSDATE ,
        pull_status      = 'S' ,
        pull_error_msg   = NULL
      WHERE source       = p_source;
    ELSE
      ROLLBACK;
      pk_main.log_error(p_source, 'pk_pull_hub', 'pull_source', SQLCODE, 'FAILED To Complete Load', 999);
      UPDATE dbowner.tbl_sources
      SET pull_end     = SYSDATE ,
        pull_status    = 'F' ,
        pull_error_msg = 'FAILED To Complete Load'
      WHERE source     = p_source;
    END IF;
    COMMIT;
  EXCEPTION
  WHEN OTHERS THEN
    pk_main.log_error(p_source, 'pk_pull_hub', 'pull_source', SQLCODE, SQLERRM, 999);
    p_success := 'N';
    ROLLBACK;
  END pull_source;
END pk_pull_hub;
/
