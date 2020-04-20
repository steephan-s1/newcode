CREATE PACKAGE DBOWNER.PK_PULL
  CREATE OR REPLACE EDITIONABLE PACKAGE "DBOWNER"."PK_PULL" AS
/*******************************************************************************
Author		: Michael Ranken
Description	: Package to populate Staging Tables
Audit		:
	Version		Date		User		Description
	1		01-JAN-2010	Michael Ranken	Initial version
*******************************************************************************/

	PROCEDURE pull_source(p_source IN VARCHAR2, p_success IN OUT VARCHAR2);
END pk_pull;
/
CREATE OR REPLACE EDITIONABLE PACKAGE BODY "DBOWNER"."PK_PULL" AS
/*******************************************************************************
Author		: Michael Ranken
Description	: Package to populate Staging Tables
Audit		:
	Version		Date		User		Description
	1		01-JAN-2010	Michael Ranken	Initial version
	101		29-SEP-2015	Ganesh Shekar	JIRA_670
	102		29-SEP-2015	Ganesh Shekar	JIRA_670
	103		07-Dec-2015	Ganesh Shekar	JIRA_1632
*******************************************************************************/

PROCEDURE run_query(p_source IN VARCHAR2, p_query_no IN NUMBER, p_success IN OUT VARCHAR2);

g_pos 	  		NUMBER := 0;
v_message 		VARCHAR2(3000) := '';
v_err_code 		NUMBER;
v_query_count	NUMBER := 0;

PROCEDURE pull_source(p_source IN VARCHAR2, p_success IN OUT VARCHAR2) IS

v_source_time_db1   DATE := NULL;
v_source_time_db2   DATE := NULL;
v_total_queries		NUMBER;
v_attempts			NUMBER;

BEGIN

	pk_main.log_time(p_source, 'pk_pull', 'pull_source', 'start of procedure', g_pos, true);
	UPDATE dbowner.tbl_sources
	SET pull_start = SYSDATE,
	    pull_end = NULL,
	    pull_status = 'R',
	    pull_error_msg = NULL,
	    related_transfer_session = NULL
	WHERE source = p_source;
	COMMIT;

	IF p_source = 'BMN' THEN
	   	--SELECT get_time_fk@bmnlive INTO v_source_time_db1 FROM dual@bmnlive;
	   	v_total_queries := 24;
	ELSIF p_source = 'TLI' THEN
		--SELECT get_time_fk@tlilive INTO v_source_time_db1 FROM dual@tlilive;
		NULL;
	   	v_total_queries := 9;
	ELSIF p_source = 'AE' THEN

		SELECT max(change_date)
		INTO v_source_time_db1
		FROM compowner.TBL_DATA_CHANGES
		WHERE source = 'AE';

		--SELECT max(run_time) INTO v_source_time_db2 FROM alerts_trigger@celllive;

	   	v_total_queries := 19;
	ELSIF p_source = 'SIS' THEN

		SELECT sysdate - 1
		INTO v_source_time_db1
		FROM dual;
	   	v_total_queries := 25;

	ELSIF p_source = 'SD' THEN

		SELECT this_marker_db1
		INTO v_source_time_db1
		FROM dbowner.tbl_sources
		WHERE source = 'SD';

  	v_total_queries := 41;

  ELSIF p_source = 'PPV' THEN

    SELECT last_marker_db1
    INTO v_source_time_db1
    FROM dbowner.tbl_sources
    WHERE source = 'PPV';

    v_total_queries := 5;

	ELSIF p_source = 'CW' THEN
		--SELECT get_time_fk@cwlive INTO v_source_time_db1 FROM dual@cwlive;
	   	v_total_queries := 6;

	ELSIF p_source = 'GCD' THEN

		SELECT sysdate - 1 INTO v_source_time_db1 FROM dual;
	   	v_total_queries := 11;

	ELSIF p_source = 'PTS' THEN

    SELECT this_marker_db1
		INTO v_source_time_db1
		FROM dbowner.tbl_sources
		WHERE source = 'PTS';

    v_total_queries := 7;

	ELSIF p_source = 'EA' THEN

		SELECT this_marker_db1
		INTO v_source_time_db1
		FROM dbowner.tbl_sources
		WHERE source = 'EA';

	   	v_total_queries := 5;

	ELSIF p_source = 'STR' THEN

		SELECT this_marker_db1
		INTO v_source_time_db1
		FROM dbowner.tbl_sources
		WHERE source = 'STR';
	   	v_total_queries := 14;

	ELSIF p_source = 'ELB' THEN

		SELECT sysdate
		INTO v_source_time_db1
		FROM dbowner.tbl_sources
		WHERE source = 'ELB';

	   	v_total_queries := 11;
	ELSIF p_source = 'ION' THEN

		SELECT this_marker_db1
		INTO v_source_time_db1
		FROM dbowner.tbl_sources
		WHERE source = 'ION';

	   	v_total_queries := 4;

	ELSIF p_source = 'DEL' THEN

		SELECT this_marker_db1
		INTO v_source_time_db1
		FROM dbowner.tbl_sources
		WHERE source = 'DEL';
	   	v_total_queries := 14;

	ELSIF p_source = 'ACT' THEN
		SELECT sysdate - 1 INTO v_source_time_db1 FROM dual;
	   	v_total_queries := 8;

	ELSIF p_source = 'WR' THEN

		SELECT last_marker_db1
		INTO v_source_time_db1
		FROM dbowner.tbl_sources
		WHERE source = 'WR';

	   	v_total_queries := 16;

	ELSIF p_source = 'COP' THEN

		SELECT this_marker_db1
		INTO v_source_time_db1
		FROM dbowner.tbl_sources
		WHERE source = 'COP';
	   	v_total_queries := 16;

	ELSIF p_source = 'EW2' THEN

		SELECT this_marker_db1
		INTO v_source_time_db1
		FROM dbowner.tbl_sources
		WHERE source = 'EW2';

	   	v_total_queries := 5;

  ELSIF p_source = 'TEC' THEN

		SELECT this_marker_db1
		INTO v_source_time_db1
		FROM dbowner.tbl_sources
		WHERE source = 'TEC';

    v_total_queries := 19;

  ELSIF p_source = 'CRM' THEN

		SELECT sysdate-1
    INTO v_source_time_db1
    FROM dbowner.tbl_sources
    WHERE source = 'CRM';

    v_total_queries := 35;

  ELSIF p_source = 'JCI' THEN

		SELECT this_marker_db1
		INTO v_source_time_db1
		FROM dbowner.tbl_sources
		WHERE source = 'JCI';

  	v_total_queries := 13;

  ELSIF p_source = 'COM' THEN

		SELECT this_marker_db1
		INTO v_source_time_db1
		FROM dbowner.tbl_sources
		WHERE source = 'COM';

  	v_total_queries := 10;

  ELSIF p_source = 'EST' THEN

		SELECT this_marker_db1
		INTO v_source_time_db1
		FROM dbowner.tbl_sources
		WHERE source = 'EST';

  	v_total_queries := 12;

  ELSIF p_source = 'EES' THEN

		SELECT this_marker_db1
		INTO v_source_time_db1
		FROM dbowner.tbl_sources
		WHERE source = 'EES';

  	v_total_queries := 10;

  ELSIF p_source = 'CRS' THEN

		SELECT this_marker_db1
		INTO v_source_time_db1
		FROM dbowner.tbl_sources
		WHERE source = 'CRS';

  	v_total_queries := 11;

  ELSIF p_source = 'NLN' THEN

        SELECT this_marker_db1
        INTO v_source_time_db1
        FROM dbowner.tbl_sources
        WHERE source = 'NLN';

    --// GCS: JIRA_379
    --   v_total_queries := 3;
    v_total_queries := 5;
    
 ELSIF p_source = 'LCF' THEN

        SELECT this_marker_db1
        INTO v_source_time_db1
        FROM dbowner.tbl_sources
        WHERE source = 'LCF';

    v_total_queries := 1;     
    
 ELSIF p_source = 'EVI' THEN

        SELECT this_marker_db1
        INTO v_source_time_db1
        FROM dbowner.tbl_sources
        WHERE source = 'EVI';

    v_total_queries := 3;
    
	/* Added by Gomathi */

 ELSIF p_source = 'JRBI' THEN

        SELECT this_marker_db1
        INTO v_source_time_db1
        FROM dbowner.tbl_sources
        WHERE source = 'JRBI';

    v_total_queries := 9;    
    
 ELSIF p_source = 'TLH' THEN

        SELECT this_marker_db1
        INTO v_source_time_db1
        FROM dbowner.tbl_sources
        WHERE source = 'TLH';

    v_total_queries := 2;    
    
 ELSIF p_source = 'EVI2' THEN

        SELECT this_marker_db1
        INTO v_source_time_db1
        FROM dbowner.tbl_sources
        WHERE source = 'EVI2';

    v_total_queries := 4;       

	END IF;

	UPDATE dbowner.tbl_sources SET this_marker_db1 = v_source_time_db1 WHERE source = p_source;
	UPDATE dbowner.tbl_sources SET this_marker_db2 = v_source_time_db2 WHERE source = p_source;
   	p_success := 'Y';

	WHILE v_query_count < v_total_queries AND p_success = 'Y' LOOP

		v_attempts := 0;
		v_query_count := v_query_count + 1;
		p_success := 'N';

	    pk_main.log_time(p_source, 'pk_pull', 'pull_source', p_source || ' query no: ' || v_query_count, v_query_count, FALSE);

		WHILE p_success <> 'Y' AND v_attempts <= 3 LOOP

			run_query(p_source, v_query_count, p_success);
			v_attempts := v_attempts + 1;

		END LOOP;

	END LOOP;

	IF p_success = 'Y' THEN
		UPDATE dbowner.tbl_sources
		SET last_marker_db1 = v_source_time_db1,
		    last_marker_db2 = v_source_time_db2,
		    pull_end = SYSDATE,
		    pull_status = 'S'
		WHERE source = p_source;
		COMMIT;
	ELSE
		ROLLBACK;
		IF v_message = '' THEN
			v_message := 'problem running query - ' || v_query_count || '. ' || SQLERRM;
			v_err_code := SQLCODE;
		END IF;

		pk_main.log_error(p_source, 'pk_pull', 'pull_source', v_err_code, v_message, g_pos);

		UPDATE dbowner.tbl_sources
		SET pull_end = SYSDATE,
		pull_status = 'F',
		pull_error_msg = v_message
		WHERE source = p_source;
		COMMIT;
	END IF;

	v_query_count := v_query_count + 1;
	pk_main.log_time(p_source, 'pk_pull', 'pull_source', 'end of procedure', v_query_count, true);

EXCEPTION

    WHEN OTHERS THEN
		ROLLBACK;
        p_success := 'N';
		v_message := 'query:' || v_query_count ||'. error:' || sys.dbms_utility.format_error_stack;
        pk_main.log_error(p_source, 'pull_pk', 'pull_source', SQLCODE, v_message, g_pos);
		UPDATE dbowner.tbl_sources SET pull_end = SYSDATE, pull_status = 'F', pull_error_msg = v_message WHERE source = p_source;
		COMMIT;

END pull_source;

PROCEDURE run_query(p_source IN VARCHAR2, p_query_no IN NUMBER, p_success IN OUT VARCHAR2) IS

v_in_id NUMBER(15);
v_path_name VARCHAR2(2000);
v_top_name VARCHAR2(100);
v_dummy VARCHAR2(1);

/*CURSOR c_gcd_journals IS
	SELECT 'x' FROM gcd.journals_trigger@pogcd WHERE ROWNUM = 1;
*/

BEGIN

	SAVEPOINT clean_up;

  IF p_source = 'JCI' THEN

      IF p_query_no = 1 THEN
                        INSERT INTO tbl_temp_parties(status,
                                                    Orig_System,original_site,orig_system_ref,party_type,dedupe_type,usr_created_date,usc_contact_title,Orig_Title
                                                    ,usc_contact_firstname,usc_contact_lastname,usc_addr,usc_city,usc_zip,region_name,usc_country,Orig_Country,usc_dept,usc_org
                                                    ,jobtitle,usc_phone,phone_2,web_site,usc_fax,usc_dedupe_email,unique_inst_id,desk_type,run_time)
                        SELECT NULL,
                               'JCI'                      AS orig_system ,
                               'EDITOR'                   AS orig_site,
                               'JCI_EDTR_' || e.ID        AS orig_party_ref,
                               'EUS'                      AS party_type,
                               'PER'                      AS dedupe_type,
                               nvl(e.date_created,SYSDATE)             AS orig_create_date,
                               e.personaltitle_title      AS title,
                               e.personaltitle_title      AS orig_title,
                               e.givenname                AS firstname,
                               e.surname                  AS lastname,
                               e.privatestreet            AS address1,
                               e.privatecity              AS city,
                               e.privatepostalcode        AS post_code,
                               e.privatecountrypart_title AS region,
                               e.privatecountry_title     AS country,
                               e.privatecountry_title     AS orig_country,
                               s.title                    AS user_department,
                               s.organization_title       AS user_organisation,
                               'EDITOR'                   AS job_title,
                               e.tel                      AS phone,
                               e.additionaltel            AS phone_2,
                               e.personal_website         AS website,
                               e.fax                      AS fax,
                               e.mailto                   AS email,
                               CASE WHEN s.id IS NOT NULL THEN
                                        'JCI_O_' || s.id
                                        ELSE NULL END     AS org_orig_system_ref,
                               'EDITOR'                   AS job_type,
                               e.date_changed             AS orig_update_date
                        FROM TBL_TEMP_JCI_EDITORIALCONTACTS e,
                             (
                             SELECT s.isprimary, s.editorialcontact_id, o.id, o.title, o.organization_title
                             FROM   TBL_TEMP_JCI_EC_SUBORGS s,
                                    TBL_TEMP_JCI_SUBORGANIZATIONS o
                             WHERE  s.isprimary = '1'
                             AND    s.suborganization_id = o.id (+)
                             ) s
                        WHERE e.id = s.editorialcontact_id (+);

      ELSIF p_query_no = 2 THEN
                            INSERT INTO tbl_temp_interests (status
                                                           ,orig_system_ref,orig_system,party_ref,create_date,update_date,interest_type,interest_value,interest_value_details)
                            SELECT NULL,
                                   'JCI_LNK_' || s.id                             AS orig_interest_ref,
                                   'JCI'                                          AS orig_system,
                                   'JCI_EDTR_' || s.editorialcontact_id           AS party_ref,
                                   nvl(e.date_created,SYSDATE)                                 AS orig_create_date,
                                   s.date_changed                                 AS orig_update_date,
                                   'LNK'                                          AS interest_type,
                                   'JCI_O_' || s.suborganization_id             AS interest_value,
                                   'Link from JCI Editor Contact to Orginisation' AS interest_value_details
                            FROM TBL_TEMP_JCI_EC_SUBORGS s,
                                 TBL_TEMP_JCI_EDITORIALCONTACTS e
                            WHERE e.id = s.editorialcontact_id
                            AND   s.isprimary = 1;

      ELSIF p_query_no = 3 THEN
                            INSERT INTO tbl_temp_parties(status
                                                        ,orig_system,original_site,orig_system_ref,party_type,dedupe_type,usr_created_date,usc_contact_firstname,usc_contact_lastname
                                                        ,usc_addr,usc_city,usc_zip,usc_country,orig_country,usc_state,region_name,usc_dept,Inst_No_Of_Users,run_time)
                            SELECT
                                  NULL,
                                  'JCI'                                AS orig_system ,
                                  'ORGINISATION'                       AS orig_site,
                                  'JCI_O_' || subOrganizations.ID    AS orig_party_ref,
                                  'ORG'                                AS party_type,
                                  'ORG'                                AS dedupe_type,
                                  nvl(subOrganizations.Date_Created,SYSDATE)        AS orig_create_date,
                                  subOrganizations.ORGANIZATION_TITLE  AS firstname,
                                  subOrganizations.Title               AS lastname,
                                  subOrganizations.street              AS address1,
                                  subOrganizations.city                AS city,
                                  subOrganizations.PostalCode          AS post_code,
                                  subOrganizations.COUNTRY_TITLE       AS country,
                                  subOrganizations.COUNTRY_TITLE       AS orig_country,
                                  subOrganizations.countrypart_title   AS state,
                                  subOrganizations.Region_Title        AS region,
                                  subOrganizations.SubTitle            AS user_department,
                                  (
                                  SELECT COUNT(*)
                                  FROM TBL_TEMP_JCI_EC_SUBORGS aa
                                  WHERE aa.suborganization_id = subOrganizations.Id
                                  AND   aa.isprimary = 1
                                  )                                    AS org_no_of_users,
                                  subOrganizations.Date_Changed        AS orig_update_date
                            FROM  TBL_TEMP_JCI_SUBORGANIZATIONS subOrganizations;

      ELSIF p_query_no = 4 THEN
                            INSERT INTO tbl_temp_parties(status
                                                        ,orig_system,original_site,orig_system_ref,party_type,dedupe_type,usr_created_date,usr_last_visit_date,usc_contact_title
                                                        ,orig_title,usc_contact_firstname,usc_contact_lastname,usc_dedupe_email,run_time)
                            SELECT NULL,
                                   'JCI'                                             AS orig_system ,
                                   'STAFF'                                           AS orig_site,
                                   CASE WHEN Staff.User_Id IS NOT NULL THEN
                                                   'JCI_STAFF_' || Staff.ID || '_' || Staff.User_Id
                                   ELSE
                                                   'JCI_STAFF_' || Staff.ID
                                   END                                               AS orig_party_ref,
                                   'EUS'                                             AS party_type,
                                   'PER'                                             AS dedupe_type,
                                   nvl(staff.Date_Created,SYSDATE)                   AS orig_create_date,
                                   staff.USER_DATE_LAST_LOGIN                        AS last_visit_date,
                                   staff.PERSONALTITLE_TITLE                         AS title,
                                   staff.PERSONALTITLE_TITLE                         AS orig_title,
                                   staff.GivenName                                   AS firstname,
                                   staff.surName                                     AS lastname,
                                   staff.mailto                                      AS email,
                                   Staff.Date_Changed                                AS orig_update_date
                            FROM TBL_TEMP_JCI_STAFF staff;

      ELSIF p_query_no = 5 THEN
                            INSERT INTO tbl_temp_items (status
                                                       ,orig_system_ref,orig_system,create_date,update_date,name,item_type,identifier,pmc_code,pmc_descr,pmg_code,pmg_descr
                                                       ,description,show_status,item_milestone)
                            SELECT NULL,
                                   'JCI_JOU_' || Journals.ID         AS orig_item_ref,
                                   'JCI'                             AS orig_system ,
                                   nvl(Journals.Date_Created,SYSDATE)             AS orig_create_date,
                                   Journals.Date_Changed             AS orig_update_date,
                                   Journals.Title                    AS name,
                                   'JOU'                             AS item_type,
                                   Journals.ISSN                     AS identifier,
                                   Journals.PMC_ID                   AS pmc_code,
                                   Journals.PMC_Title                AS pmc_descr,
                                   Journals.PMG_ID                   AS pmg_code,
                                   Journals.PMG_Title                AS pmg_descr,
                                   Journals.SUBTITLE                 AS description,
                                   Journals.SUBJECTCOLLECTIONS_TITLE AS show_status,
                                   substr(Journals.Portfolio_Title,1,45)          AS item_milestone
                            FROM   TBL_TEMP_JCI_JOURNALS Journals;

      ELSIF p_query_no = 6 THEN
                            INSERT INTO tbl_temp_interests (status
                                                           ,orig_system_ref,orig_system,party_ref,create_date,update_date,interest_type,interest_value,interest_value_details
                                                           ,interest_section,interest_sub_section,alert_end_date)
                            SELECT  NULL,
                                    'JCI_JOU_ED_'        ||
                                    EDITORIALCONTACT_ID || '_' ||
                                    JOURNAL_ID          || '_' ||
                                    ID                  || '_' ||
                                    EDITORIALCONROLE_ROLE_ID            AS orig_interest_ref,
                                   'JCI'                                AS orig_system ,
                                   'JCI_EDTR_' || EDITORIALCONTACT_ID   AS party_ref,
                                   nvl(date_start,SYSDATE)                           AS orig_create_date,
                                   date_changed                         AS orig_update_date,
                                   'JOU'                                AS interest_type,
                                   EDITORIALROLE_TITLE                  AS interest_value,
                                   CLASSIFICATION_TITLE                 AS interest_value_details,
                                   'JCI_JOU_' || JOURNAL_ID             AS interest_section,
                                   CASE WHEN nvl(EDITORIALCONROLE_DATE_END,SYSDATE+2) >= SYSDATE
                                        THEN 'Role Active'
                                        ELSE 'Role Inactive'
                                   END                                  AS interest_subsection,
                                   EDITORIALCONROLE_DATE_END            AS alert_end_date
                            FROM TBL_TEMP_JCI_BOARD EditorRoles;

      ELSIF p_query_no = 7 THEN
                            INSERT INTO tbl_temp_interests (status
                                                           ,orig_system_ref,orig_system,party_ref,create_date,update_date,interest_type,interest_value,interest_section)
                            SELECT  NULL,
                                    'JCI_JOU_ST_'        ||
                                    STAFF_ID            || '_' ||
                                    JOURNAL_ID          || '_' ||
                                    STAFFROLE_ID                        AS orig_interest_ref,
                                   'JCI'                                AS orig_system ,
                                   'JCI_STAFF_' || STAFF_ID             AS party_ref,
                                   nvl(date_changed,SYSDATE) AS orig_create_date,
                                   date_changed                         AS orig_update_date,
                                   'JOU'                                AS interest_type,
                                   STAFFROLE_TITLE                      AS interest_value,
                                   'JCI_JOU_' || JOURNAL_ID             AS interest_section
                            FROM TBL_TEMP_JCI_STAFF_JOU_ROLES StaffRoles;

      ELSIF p_query_no = 8 THEN
                            INSERT INTO tbl_temp_interests (status
                                                           ,orig_system_ref,orig_system,party_ref,create_date,update_date,interest_type,interest_value
                                                           ,interest_value_details,interest_section,interest_sub_section)
                            SELECT NULL,
                                   'JCI_MEET_' || Meeting.ID   AS orig_interest_ref,
                                   'JCI'                       AS orig_system ,
                                   'JCI_STAFF_' || Meeting.Staff_Id  AS party_ref,
                                   nvl(nvl(Meeting.Date_Created,Date_Changed),SYSDATE)        AS orig_create_date,
                                   Meeting.Date_Changed        AS orig_update_date,
                                   'MEETING'                   AS interest_type,
                                   Meeting.Occasion            AS interest_value,
                                   Meeting.Report              AS interest_value_details,
                                   Meeting.TripType_Title      AS interest_section,
                                   Meeting.Trip_Title          AS interest_sub_section
                            FROM TBL_TEMP_JCI_MEETING_EVENTS Meeting;

   ELSIF p_query_no = 9 THEN
    -- Identify the PARTY records that need to be marked as 'D'
      insert into dbowner.tbl_temp_parties (status, orig_system, orig_system_ref, party_type, run_time)
        select distinct 'D', orig_system, orig_party_ref, party_type, sysdate
        from dbowner.tbl_parties
        where orig_system = 'JCI'
        and orig_party_ref in
        (select orig_party_ref from dbowner.tbl_parties where orig_system = 'JCI'
         minus
         select orig_system_ref from dbowner.tbl_temp_parties where orig_system = 'JCI');

    ELSIF p_query_no = 10 THEN
    -- Identify the INTEREST records that need to be marked as 'D'
      insert into dbowner.tbl_temp_interests (orig_system_ref, orig_system, update_date, interest_type, status)
        select distinct orig_interest_ref, orig_system, sysdate, interest_type, 'D'
        from dbowner.tbl_interests
        where orig_system = 'JCI'
        and orig_interest_ref in
        (select orig_interest_ref from dbowner.tbl_interests where orig_system = 'JCI'
         minus
         select orig_system_ref from dbowner.tbl_temp_interests where orig_system = 'JCI');

    ELSIF p_query_no = 11 THEN
    -- Identify the ITEM records that need to be marked as 'D'
      insert into dbowner.tbl_temp_items (orig_system_ref, orig_system, update_date, item_type, status)
        select distinct orig_item_ref, orig_system, sysdate, item_type, 'D'
        from dbowner.tbl_items
        where orig_system = 'JCI'
        and orig_item_ref in
        (select orig_item_ref from dbowner.tbl_items where orig_system = 'JCI'
         minus
         select orig_system_ref from dbowner.tbl_temp_items where orig_system = 'JCI');

    ELSIF p_query_no = 12 THEN
    -- Identify the SUBSCRIPTION records that need to be marked as 'D'
      insert into dbowner.tbl_temp_subscriptions (status, orig_system_ref, orig_system, orig_update_date)
        select distinct 'D', orig_subscription_ref, orig_system, sysdate
        from dbowner.tbl_subscriptions
        where orig_system = 'JCI'
        and orig_subscription_ref in
        (select orig_subscription_ref from dbowner.tbl_subscriptions where orig_system = 'JCI'
         minus
         select orig_system_ref from dbowner.tbl_temp_subscriptions where orig_system = 'JCI');

    ELSIF p_query_no = 13 THEN
    -- Identify the ITEM_SUBJECTS records that need to be marked as 'D'
      insert into dbowner.tbl_temp_item_subjects (status, orig_system, orig_system_ref, item_ref, update_date)
        select distinct 'D', orig_system, orig_subject_ref, item_ref, sysdate
        from dbowner.tbl_item_subjects
        where orig_system = 'JCI'
        and orig_subject_ref in
        (select orig_subject_ref from dbowner.tbl_item_subjects where orig_system = 'JCI'
         minus
         select orig_system_ref from dbowner.tbl_temp_item_subjects where orig_system = 'JCI');


      END IF;


  ELSIF p_source = 'COM' THEN

      IF p_query_no = 1 THEN
                          INSERT INTO tbl_temp_parties ( STATUS,run_time,
                                                         orig_System, original_site,orig_system_ref,party_type,dedupe_type,usr_created_date,usr_status,usc_contact_title
                                                        ,orig_title,usc_contact_firstname,usc_contact_lastname,usc_addr,usr_address2,usc_city,usc_zip,usc_country
                                                        ,orig_country,usc_state,usc_org,jobtitle,usc_phone,usc_fax,usc_dedupe_email,unique_inst_id,chunk_size,display_srch_results
                                                        ,display_toc_in_nia,User_Access_Type,sort_preference,history_enabled,history_expand,subj_area_home_page
                                                       )
                          SELECT
                                NULL,
                                contacts.Date_Changed                AS orig_update_date,
                                'COM'                               AS orig_system ,
                                'CONTACT'                            AS orig_site,
                                'COM_BILL_' || contacts.ID          AS orig_party_ref,
                                'EUS'                                AS party_type,
                                'PER'                                AS dedupe_type,
                                contacts.Date_Created                AS orig_create_date,
                                contacts.IsActive                    AS user_status,
                                contacts.PERSONALTITLE_TITLE         AS title,
                                contacts.PERSONALTITLE_TITLE         AS orig_title,
                                contacts.FirstName                   AS firstname,
                                contacts.LastName                    AS lastname,
                                contacts.Address_1                   AS address1,
                                contacts.Address_2                   AS address2,
                                contacts.City                        AS city,
                                contacts.Postal_Code                 AS post_code,
                                contacts.COUNTRY_TITLE               AS country,
                                contacts.COUNTRY_TITLE               AS orig_country,
                                contacts.COUNTRYPART_TITLE           AS state,
                                contacts.Institute                   AS user_organisation,
                                contacts.JobTitle                    AS job_title,
                                contacts.phonenr                     AS phone,
                                contacts.faxnr                       AS fax,
                                contacts.mailto                      AS email,
                                'COM_DEL_' || contacts.ID           AS org_orig_system_ref,
                                Mailings.mailing_3rdparty_fax        AS chunk_size,
                                Mailings.mailing_3rdparty_mail       AS display_search_results,
                                Mailings.mailing_3rdparty_post       AS display_toc_in_nia,
                                Mailings.mailing_3rdparty_tel        AS access_type,
                                Mailings.mailing_elsevier_fax        AS sort_preference,
                                Mailings.mailing_elsevier_mail       AS history_enabled,
                                Mailings.mailing_elsevier_post       AS history_expand,
                                Mailings.mailing_elsevier_tel        AS subject_area_home_page
                          FROM  TBL_TEMP_COMS_CONTACTS Contacts,
                                (
                                SELECT r.delegate_id,
                                       r.mailing_3rdparty_fax,
                                       r.mailing_3rdparty_mail,
                                       r.mailing_3rdparty_post,
                                       r.mailing_3rdparty_tel,
                                       r.mailing_elsevier_fax,
                                       r.mailing_elsevier_mail,
                                       r.mailing_elsevier_post,
                                       r.mailing_elsevier_tel
                                FROM TBL_TEMP_COMS_REGISTRATIONS r
                                WHERE r.id IN (SELECT MAX(r1.id) FROM TBL_TEMP_COMS_REGISTRATIONS r1 WHERE r1.delegate_id = r.delegate_id)
                                ) Mailings
                          WHERE Contacts.ID = Mailings.delegate_id (+);

      ELSIF p_query_no = 2 THEN
                            INSERT INTO tbl_temp_parties ( STATUS,run_time,
                                                           orig_System, original_site,orig_system_ref,party_type,dedupe_type,usr_created_date,usr_status,usc_contact_title
                                                          ,orig_title,usc_contact_firstname,usc_contact_lastname,usc_addr,usr_address2,usc_city,usc_zip,usc_country
                                                          ,orig_country,usc_state,usc_org,jobtitle,usc_phone,phone_2,usc_fax,phone_3,usc_dedupe_email,chunk_size,display_srch_results
                                                          ,display_toc_in_nia,User_Access_Type,sort_preference,history_enabled,history_expand,subj_area_home_page
                                                         )
                            SELECT
                                  NULL,
                                  contacts.Date_Changed                AS orig_update_date,
                                  'COM'                               AS orig_system ,
                                  'CONTACT'                            AS orig_site,
                                  'COM_DEL_' || contacts.ID           AS orig_party_ref,
                                  'DEL'                                AS party_type,
                                  'PER'                                AS dedupe_type,
                                  contacts.Date_Created                AS orig_create_date,
                                  contacts.IsActive                    AS user_status,
                                  contacts.PERSONALTITLE_TITLE         AS title,
                                  contacts.PERSONALTITLE_TITLE         AS orig_title,
                                  contacts.FirstName                   AS firstname,
                                  contacts.LastName                    AS lastname,
                                  contacts.DELIVERY_ADDRESS_1          AS address1,
                                  contacts.DELIVERY_ADDRESS_2          AS address2,
                                  contacts.DELIVERY_CITY               AS city,
                                  contacts.DELIVERY_POSTAL_CODE        AS post_code,
                                  contacts.DELIVERY_COUNTRY_TITLE      AS country,
                                  contacts.DELIVERY_COUNTRY_TITLE      AS orig_country,
                                  contacts.DELIVERY_COUNTRYPART_TITLE  AS state,
                                  contacts.Institute                   AS user_organisation,
                                  contacts.JobTitle                    AS job_title,
                                  contacts.phonenr                     AS phone,
                                  contacts.export_phonenr              AS phone2,
                                  contacts.faxnr                       AS fax,
                                  contacts.export_faxnr                AS phone3,
                                  contacts.mailto                      AS email,
                                  Mailings.mailing_3rdparty_fax        AS chunk_size,
                                  Mailings.mailing_3rdparty_mail       AS display_search_results,
                                  Mailings.mailing_3rdparty_post       AS display_toc_in_nia,
                                  Mailings.mailing_3rdparty_tel        AS access_type,
                                  Mailings.mailing_elsevier_fax        AS sort_preference,
                                  Mailings.mailing_elsevier_mail       AS history_enabled,
                                  Mailings.mailing_elsevier_post       AS history_expand,
                                  Mailings.mailing_elsevier_tel        AS subject_area_home_page
                            FROM  TBL_TEMP_COMS_CONTACTS Contacts,
                                  (
                                  SELECT r.delegate_id,
                                         r.mailing_3rdparty_fax,
                                         r.mailing_3rdparty_mail,
                                         r.mailing_3rdparty_post,
                                         r.mailing_3rdparty_tel,
                                         r.mailing_elsevier_fax,
                                         r.mailing_elsevier_mail,
                                         r.mailing_elsevier_post,
                                         r.mailing_elsevier_tel
                                  FROM TBL_TEMP_COMS_REGISTRATIONS r
                                  WHERE r.id IN (SELECT MAX(r1.id) FROM TBL_TEMP_COMS_REGISTRATIONS r1 WHERE r1.delegate_id = r.delegate_id)
                                  ) Mailings
                            WHERE Contacts.ID = Mailings.delegate_id (+);

      ELSIF p_query_no = 3 THEN
                            INSERT INTO tbl_temp_interests (status, orig_system_ref,orig_system,party_ref,create_date,update_date,interest_type,interest_value,interest_value_details)
                            SELECT NULL,
                                   s.id                                                       AS orig_interest_ref,
                                   'COM'                                                     AS orig_system,
                                   'COM_BILL_' || s.id                                       AS party_ref,
                                   s.date_created                                             AS orig_create_date,
                                   s.date_changed                                             AS orig_update_date,
                                   'LNK'                                                      AS interest_type,
                                   'COM_DEL_' || s.id                                        AS interest_value,
                                   'Link from COMS Contact Billing Record to Delivery Record' AS interest_value_details
                            FROM TBL_TEMP_COMS_CONTACTS s
                            where s.id is not null;


      ELSIF p_query_no = 4 THEN
                            INSERT INTO tbl_temp_items (status,
                                                       orig_system_ref,orig_system,create_date,update_date,name,item_type,publisher,pmc_code,pmc_descr,pmg_code,pmg_descr
                                                       ,issue_milestone,show_status,item_milestone, imprint)
                            SELECT
                                  NULL,
                                  'COM_MEET_' || Meetings.ID          AS orig_item_ref,
                                  'COM'                               AS orig_system ,
                                  nvl(Meetings.Date_Created,SYSDATE)   AS orig_create_date,
                                  nvl(Meetings.Date_Changed,SYSDATE)   AS orig_update_date,
                                  Meetings.Title                       AS name,
                                  'MET'                                AS item_type,
                                  Meetings.certificate_signatory       AS publisher,
                                  Meetings.PMC_id                      AS pmc_code,
                                  Meetings.PMC_Title                   AS pmc_descr,
                                  Meetings.PMG_id                      AS pmg_code,
                                  Meetings.PMG_Title                   AS pmg_descr,
                                  substr(Meetings.Location,1,45)       AS issue_milestone,
                                  Meetings.PAYMENT_TERM_TITLE          AS show_status,
                                  substr(Meetings.VAT_REGISTRATION_NUMBER_TITLE,1,45) AS item_milestone,
                                  CASE WHEN meetings.SOCIETY_NAME IS NOT NULL THEN 'Y' ELSE 'N' END AS imprint
                            FROM  TBL_TEMP_COMS_MEETINGS Meetings;

      ELSIF p_query_no = 5 THEN
                            INSERT INTO tbl_temp_subscriptions (Status,
                                                               orig_system_ref,orig_system,orig_create_date,orig_update_date,party_ref,item_ref_1,start_date,end_date
                                                               ,price,sub_status,product_name,product_description,Order_Id,Inv_Currency,tax_code,Claim_Count,Ranking
                                                               ,No_Copies,Qss_Claim_Code,Claim_Code,owner)
                            SELECT
                                  NULL,
                                  CASE WHEN nvl(orderedProducts.ID,0) <> 0 THEN
                                            'COM_ORD_' || orderedProducts.ID
                                  ELSE
                                            'COM_ORD_REG_' || Registrations.ID
                                  END                                        AS orig_subscription_ref,
                                  'COM'                                      AS orig_system,
                                  nvl(orderedProducts.ORDER_DATE_CREATED,
                                      Registrations.Date_Created)            AS orig_create_date,
                                  nvl(orderedProducts.ORDER_DATE_CHANGED,
                                      Registrations.Date_Changed)            AS orig_update_date,
                                  'COM_BILL_' || Registrations.DELEGATE_ID   AS party_ref,
                                  'COM_MEET_' || Meetings.ID                 AS item_ref_1,
                                  Meetings.Start_Date                        AS start_date,
                                  Meetings.End_Date                          AS end_date,
                                  orderedProducts.ORDER_PRICE_TOTAL_CENTS/100    AS price,
                                  CASE
                                  WHEN Cancelled.Order_Booking_Id IS NOT NULL THEN
                                       'cancelled - refunded'
                                  ELSE
                                       orderedProducts.order_paymentstate_title
                                  END                                        AS sub_status,
                                  Meetings.Title                             AS product_name,
                                  Meetings.society_name                      AS product_description,
                                  orderedProducts.ORDER_ID                   AS order_id,
                                  substr(orderedProducts.currency,1,3)       AS inv_currency,
                                  orderedProducts.PRODUCT_VATCODE_TITLE      AS tax_code,
                                  orderedProducts.Order_Price_Vat_Cents/100      AS claim_count,
                                  orderedProducts.ORDER_STATE_TITLE          AS ranking,
                                  orderedProducts.Qty                        AS no_copies,
                                  orderedProducts.Product_Title              AS qss_claim_code,
                                  Registrations.ATTENDEETYPE_TITLE           AS claim_code,
                                  Registrations.Abstractids                  AS owner
                            from TBL_TEMP_COMS_ORDERDPRODUCTS  orderedProducts,
                                 TBL_TEMP_COMS_REGISTRATIONS   Registrations,
                                 TBL_TEMP_COMS_MEETINGS        Meetings,
                                 TBL_TEMP_COMS_BOOKINGS        Bookings,
                                 TBL_TEMP_COMS_CREDITS         Cancelled
                            WHERE Registrations.id                 = orderedProducts.registration_id (+)
                            AND   Registrations.Booking_Id         = Bookings.Id (+)
                            AND   Bookings.Meeting_Id              = Meetings.Id (+)
                            AND   orderedProducts.Id               = Cancelled.Ordered_Product_Id (+)
                            AND   orderedProducts.Meeting_Id       = Cancelled.Meeting_Id (+)
                            AND   orderedProducts.Order_Booking_Id = Cancelled.Order_Booking_Id (+) ;


   ELSIF p_query_no = 6 THEN
    -- Identify the PARTY records that need to be marked as 'D'
      insert into dbowner.tbl_temp_parties (status, orig_system, orig_system_ref, party_type, run_time)
        select distinct 'D', orig_system, orig_party_ref, party_type, sysdate
        from dbowner.tbl_parties
        where orig_system = 'COM'
        and orig_party_ref in
        (select orig_party_ref from dbowner.tbl_parties where orig_system = 'COM'
         minus
         select orig_system_ref from dbowner.tbl_temp_parties where orig_system = 'COM');

    ELSIF p_query_no = 7 THEN
    -- Identify the INTEREST records that need to be marked as 'D'
      insert into dbowner.tbl_temp_interests (orig_system_ref, orig_system, update_date, interest_type, status)
        select distinct orig_interest_ref, orig_system, sysdate, interest_type, 'D'
        from dbowner.tbl_interests
        where orig_system = 'COM'
        and orig_interest_ref in
        (select orig_interest_ref from dbowner.tbl_interests where orig_system = 'COM'
         minus
         select orig_system_ref from dbowner.tbl_temp_interests where orig_system = 'COM');

    ELSIF p_query_no = 8 THEN
    -- Identify the ITEM records that need to be marked as 'D'
      insert into dbowner.tbl_temp_items (orig_system_ref, orig_system, update_date, item_type, status)
        select distinct orig_item_ref, orig_system, sysdate, item_type, 'D'
        from dbowner.tbl_items
        where orig_system = 'COM'
        and orig_item_ref in
        (select orig_item_ref from dbowner.tbl_items where orig_system = 'COM'
         minus
         select orig_system_ref from dbowner.tbl_temp_items where orig_system = 'COM');

    ELSIF p_query_no = 9 THEN
    -- Identify the SUBSCRIPTION records that need to be marked as 'D'
      insert into dbowner.tbl_temp_subscriptions (status, orig_system_ref, orig_system, orig_update_date)
        select distinct 'D', orig_subscription_ref, orig_system, sysdate
        from dbowner.tbl_subscriptions
        where orig_system = 'COM'
        and orig_subscription_ref in
        (select orig_subscription_ref from dbowner.tbl_subscriptions where orig_system = 'COM'
         minus
         select orig_system_ref from dbowner.tbl_temp_subscriptions where orig_system = 'COM');

    ELSIF p_query_no = 10 THEN
    -- Identify the ITEM_SUBJECTS records that need to be marked as 'D'
      insert into dbowner.tbl_temp_item_subjects (status, orig_system, orig_system_ref, item_ref, update_date)
        select distinct 'D', orig_system, orig_subject_ref, item_ref, sysdate
        from dbowner.tbl_item_subjects
        where orig_system = 'COM'
        and orig_subject_ref in
        (select orig_subject_ref from dbowner.tbl_item_subjects where orig_system = 'COM'
         minus
         select orig_system_ref from dbowner.tbl_temp_item_subjects where orig_system = 'COM');


      END IF;



  ELSIF p_source = 'EST' THEN

      IF p_query_no = 1 THEN
                          INSERT INTO tbl_temp_parties (status, Orig_System, original_site, orig_system_ref, party_type, dedupe_type, usr_created_date, usr_status, orig_title
                                                       ,usc_contact_title, usc_contact_firstname,usc_contact_lastname,usc_addr,usr_address2,usc_city,usc_state
                                                       ,usc_zip,usc_country,orig_country,region_name,usc_dept,usc_org,usc_phone,phone_2,usc_dedupe_email,run_time,MARKETING_EMAILS)
                          SELECT
                                  decode(customers.IsActive,1,'A','D') AS record_status,
                                  'EST'                             AS orig_system ,
                                  'CUSTOMER'                         AS orig_site,
                                  'EST_' || customers.ID            AS orig_party_ref,
                                  'EUS'                              AS party_type,
                                  'PER'                              AS dedupe_type,
                                  customers.Date_Created             AS orig_create_date,
                                  decode(customers.IsActive,1,'A','D') AS user_status,
                                  customers.TITLE_TITLE              AS title,
                                  customers.TITLE_TITLE              AS orig_title,
                                  customers.FirstName                AS firstname,
                                  customers.LastName                 AS lastname,
                                  customers.Address1                 AS address1,
                                  customers.Address2                 AS address2,
                                  customers.City                     AS city,
                                  customers.State                    AS state,
                                  customers.Postal_Code              AS post_code,
                                  customers.COUNTRY_TITLE            AS country,
                                  customers.COUNTRY_TITLE            AS orig_country,
                                  customers.REGION_TITLE             AS region,
                                  customers.Department               AS user_department,
                                  customers.University               AS user_organisation,
                                  customers.phonenr                  AS phone,
                                  customers.alt_phonenr              AS phone_2,
                                  customers.mailto                   AS email,
                                  customers.Date_Changed             AS orig_update_date,
                                  customers.allow_capri_email        AS marketing_emails
                          FROM TBL_TEMP_ESTREET_CUSTOMERS customers;


      ELSIF p_query_no = 2 THEN
                            INSERT INTO tbl_temp_subscriptions (status,orig_system_ref,orig_system,orig_create_date,orig_update_date,party_ref,item_ref_1,item_ref_2,start_date,end_date
                                                               ,price,sub_status,product_name,product_description,claim_count,duration,purchase_number,Order_Id,copy_price_orig,inv_currency
                                                               ,No_Copies,tax_code, CREDIT_CARD_ALLOWED)
                            SELECT
                                    NULL,
                                    'EST_ORD_' || orderedProducts.ID AS orig_subscription_ref,
                                    'EST' AS orig_system,
                                    orderedProducts.Date_Created AS orig_create_date,
                                    orderedProducts.Date_Changed AS orig_update_date,
                                    'EST_' || orderedProducts.Customer_ID AS party_ref,
                                    CASE WHEN orderedProducts.Journal_ID IS NOT NULL THEN 'EST_JOU_' || orderedProducts.Journal_ID END AS item_ref_1,
                                    CASE WHEN orderedProducts.Article_Ref IS NOT NULL THEN 'EST_ART_' || orderedProducts.Id END AS Item_ref_2,
                                    orderedProducts.Date_Created AS start_date,
                                    orderedProducts.DATE_CANCELED AS end_date,
                                    ((orderedProducts.UNIT_PRICE * orderedProducts.QTY) + orderedProducts.VAT_AMOUNT + orderedProducts.SHIPPING_AMOUNT + orderedProducts.SHIPPING_VAT_AMOUNT)/100 AS price,
                                    orderedProducts.ORDER_PAYSTATE_TITLE AS sub_status,
                                    orderedProducts.PRODUCT_TITLE AS product_name,
                                    orderedProducts.PRODUCT_TITLE_REPORT AS product_description,
                                    orderedProducts.ORDER_PRICE_VAT_CENTS AS claim_count,
                                    orderedProducts.ORDER_PRICE_SHIPPING_CENTS AS duration,
                                    orderedProducts.pii AS purchase_number,
                                    orderedProducts.Order_ID AS order_id,
                                    orderedProducts.ORDER_PRICE_TOTAL_CENTS/100 AS copy_price_orig,
                                    orderedProducts.currency AS inv_currency,
                                    orderedProducts.qty AS no_copies,
                                    orderedProducts.Vat_Rate AS tax_code,
                                    Products.Producttype_Title AS CREDIT_CARD_ALLOWED
                            FROM TBL_TEMP_ESTREET_ORDERED_PRODS orderedProducts,
                                 TBL_TEMP_ESTREET_PRODUCTS Products
                            WHERE orderedProducts.Customer_ID IS NOT NULL
                            AND   orderedProducts.Product_Id = Products.Id (+);

      ELSIF p_query_no = 3 THEN
                            INSERT INTO tbl_temp_subscriptions (status,orig_system_ref,orig_system,orig_create_date,orig_update_date,party_ref,item_ref_1,start_date,price
                                                               ,sub_status,product_name,product_description,claim_count,inv_currency,tax_code)
                            SELECT
                                    NULL,
                                    'EST_SUB_' || ss_orders.ID AS orig_subscription_ref,
                                    'EST' AS orig_system,
                                    ss_orders.Date_Created AS orig_create_date,
                                    ss_orders.Date_Changed AS orig_update_date,
                                    'EST_' || ss_orders.Customer_ID AS party_ref,
                                    'EST_JOU_' || ss_orders.SS_JOURNAL_ID AS item_ref_1,
                                    ss_orders.Date_Created AS start_date,
                                    ss_orders.PRICE_TOTAL_CENTS/100 AS price,
                                    ss_orders.STATE_TITLE AS sub_status,
                                    ss_orders.Submission_Title AS product_name,
                                    ss_orders.JOURNAL_TITLE AS product_description,
                                    ss_orders.PRICE_VAT_CENTS/100 AS claim_count,
                                    ss_orders.currency AS inv_currency,
                                    ss_orders.Vat_Rate AS tax_code
                            FROM TBL_TEMP_ESTREET_SS_ORDERS ss_orders;

      ELSIF p_query_no = 4 THEN
                            INSERT INTO tbl_temp_items (
                                                        status,orig_system_ref,orig_system,create_date,update_date,name,item_type,
                                                        identifier,pmg_code,pmg_descr,DESCRIPTION,code
                                                        )

                            SELECT NULL,
                                   'EST_JOU_' || Journals.ID AS orig_item_ref,
                                   'EST' AS orig_system ,
                                   Journals.Date_Created AS orig_create_date,
                                   Journals.Date_Changed AS orig_update_date,
                                   Journals.Title AS name,
                                   'JOU' AS item_type,
                                   Journals.ISSN AS identifier,
                                   Journals.PMG_ID AS pmg_code,
                                   Journals.PROMISCODE AS pmg_descr,
                                   Journals.Discipline_Title AS DESCRIPTION,
                                   Journals.Subdiscipline_Title AS Code
                            FROM TBL_TEMP_ESTREET_JOURNALS Journals;

      ELSIF p_query_no = 5 THEN
                            EXECUTE IMMEDIATE 'TRUNCATE TABLE tbl_temp_estreet_ordprods_xml';

                            INSERT INTO tbl_temp_estreet_ordprods_xml
                            SELECT t.id,
                                   t.customer_id,
                                   t.personalisation,
                                   t.wddx_billing,
                                   t.wddx_shipping
                            FROM tbl_temp_estreet_ordered_prods t;
                            COMMIT;


                            INSERT INTO tbl_temp_items (
                                                       status,orig_system_ref,orig_system,parent_ref,create_date,update_date,year,volume,issue,identifier,NAME
                                                       ,show_status,issue_milestone,publisher,init_pub_date,no_of_pages,issue_date,delivery_date,authors,item_type)
                            SELECT NULL,
                                   ORIG_ITEM_REF,
                                   orig_system,
                                   parent_ref,
                                   SYSDATE,SYSDATE,
                                   year,
                                   volume,
                                   issue,
                                   identifier,
                                   name,
                                   show_status,
                                   issue_milestone,
                                   substr(publisher,1,70) AS publisher,
                                   CASE WHEN init_pub_date NOT LIKE '%NO_MONTH%' AND init_pub_date NOT LIKE '%NO_YEAR%'
                                        THEN to_date(init_pub_date,'DD-MON-YYYY')
                                   ELSE NULL
                                   END AS init_pub_date,
                                   no_of_pages,
                                   to_date(issue_date,'DD-MON-YYYY') AS issue_date,
                                   to_date(delivery_date,'DD-MON-YYYY') AS delivery_date,
                                   authors,
                                   'ART'
                            FROM
                            (
                            SELECT
                                   'EST_ART_' || ID         AS ORIG_ITEM_REF,
                                   'EST'                    AS orig_system ,
                                   'EST_JOU_' || JOURNAL_ID AS parent_ref,
                                   PUBYEAR                   AS year,
                                   VOLUME                    AS volume,
                                   ISSUE                     AS issue,
                                   ARTICLE_IDENTIFIER        AS identifier,
                                   ARTICLE_TITLE             AS name,
                                   ARTICLE_STATUS            AS show_status,
                                   ISSUE_TYPE                AS issue_milestone,
                                   ISSUE_TITLE               AS publisher,

                                   CASE WHEN trim(substr(ISSUE_COVERDATE,1,instr(ISSUE_COVERDATE,' '))) IN ('1','2','3','4','5','6','7','8','9','01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31')
                                        THEN lpad(trim(substr(ISSUE_COVERDATE,1,instr(ISSUE_COVERDATE,' '))),2,'0')
                                        ELSE '01'
                                   END -- get the date from the text - if no DAY then default to 01
                                   || '-' ||
                                   CASE WHEN lower(ISSUE_COVERDATE) LIKE '%january%' OR lower(ISSUE_COVERDATE) LIKE '%jan%' THEN 'JAN'
                                        WHEN lower(ISSUE_COVERDATE) LIKE '%february%' OR lower(ISSUE_COVERDATE) LIKE '%feb%' THEN 'FEB'
                                        WHEN lower(ISSUE_COVERDATE) LIKE '%march%' OR lower(ISSUE_COVERDATE) LIKE '%mar%' THEN 'MAR'
                                        WHEN lower(ISSUE_COVERDATE) LIKE '%april%' OR lower(ISSUE_COVERDATE) LIKE '%apr%' THEN 'APR'
                                        WHEN lower(ISSUE_COVERDATE) LIKE '%may%' OR lower(ISSUE_COVERDATE) LIKE '%may%' THEN 'MAY'
                                        WHEN lower(ISSUE_COVERDATE) LIKE '%june%' OR lower(ISSUE_COVERDATE) LIKE '%jun%' THEN 'JUN'
                                        WHEN lower(ISSUE_COVERDATE) LIKE '%july%' OR lower(ISSUE_COVERDATE) LIKE '%jul%' THEN 'JUL'
                                        WHEN lower(ISSUE_COVERDATE) LIKE '%august%' OR lower(ISSUE_COVERDATE) LIKE '%aug%' THEN 'AUG'
                                        WHEN lower(ISSUE_COVERDATE) LIKE '%september%' OR lower(ISSUE_COVERDATE) LIKE '%sep%' THEN 'SEP'
                                        WHEN lower(ISSUE_COVERDATE) LIKE '%octboer%' OR lower(ISSUE_COVERDATE) LIKE '%oct%' THEN 'OCT'
                                        WHEN lower(ISSUE_COVERDATE) LIKE '%november%' OR lower(ISSUE_COVERDATE) LIKE '%nov%' THEN 'NOV'
                                        WHEN lower(ISSUE_COVERDATE) LIKE '%december%' OR lower(ISSUE_COVERDATE) LIKE '%dec%' THEN 'DEC'
                                        ELSE 'NO_MONTH'
                                   END -- get the month from the text field - if empty then NO_MONTH
                                   || '-' ||
                                   CASE WHEN substr(trim(ISSUE_COVERDATE),-4) LIKE '19%' OR substr(trim(ISSUE_COVERDATE),-4) LIKE '20%'
                                        THEN substr(trim(ISSUE_COVERDATE),-4)
                                   ELSE 'NO_YEAR'
                                   END  -- get the year - if its not 4 digits then leave alone and default to NO_YEAR
                                   AS init_pub_date,

                                   ISSUE_NRPAGES             AS no_of_pages,
                                   ISSUE_STARTDATE           AS issue_date,
                                   ISSUE_ENDDATE             AS delivery_date,
                                   ISSUE_EDITORS             AS authors
                            FROM
                            (
                            SELECT
                                            x.id,
                                            x.customer_id,
                                            EXTRACTVALUE(VALUE(d),'/ptsref/journal_id') AS Journal_ID,
                                            EXTRACTVALUE(VALUE(d),'/ptsref/journal_title') AS Journal_Title,
                                            EXTRACTVALUE(VALUE(d),'/ptsref/journal_acronym') AS journal_acronym,
                                            EXTRACTVALUE(VALUE(d),'/ptsref/pubyear') AS pubyear,
                                            EXTRACTVALUE(VALUE(d),'/ptsref/volume') AS volume,
                                            EXTRACTVALUE(VALUE(d),'/ptsref/issue') AS issue,
                                            EXTRACTVALUE(VALUE(d),'/ptsref/article_identifier') AS article_identifier,
                                            EXTRACTVALUE(VALUE(d),'/ptsref/article_title') AS article_title,
                                            EXTRACTVALUE(VALUE(d),'/ptsref/article_status') AS article_status,
                                            EXTRACTVALUE(VALUE(d),'/ptsref/issue_type') AS issue_type,
                                            EXTRACTVALUE(VALUE(d),'/ptsref/issue_title') AS issue_title,
                                            EXTRACTVALUE(VALUE(d),'/ptsref/issue_coverdate') AS issue_coverdate,
                                            EXTRACTVALUE(VALUE(d),'/ptsref/issue_nrpages') AS issue_nrpages,
                                            EXTRACTVALUE(VALUE(d),'/ptsref/issue_conference') AS issue_conference,
                                            EXTRACTVALUE(VALUE(d),'/ptsref/issue_venue') AS issue_venue,
                                            EXTRACTVALUE(VALUE(d),'/ptsref/issue_startdate') AS issue_startdate,
                                            EXTRACTVALUE(VALUE(d),'/ptsref/issue_enddate') AS issue_enddate,
                                            EXTRACTVALUE(VALUE(d),'/ptsref/issue_editors') AS issue_editors
                                     FROM
                                            tbl_temp_estreet_ordprods_xml x,
                                            table(xmlsequence(extract(x.personalisation, '/personalisation/ptsref'))) d
                            ));


   ELSIF p_query_no = 6 THEN
       /*JIRA2923 - Added the truncate statement and Insert statement - START*/
                          EXECUTE IMMEDIATE 'TRUNCATE TABLE TBL_TEMP_ESTREET_IS_PROJECTS';
    
                          INSERT INTO TBL_TEMP_ESTREET_IS_PROJECTS
                              SELECT DBMS_LOB.SUBSTR(CAPTION,3900,2) AS CAPTION ,
                                  CUSTOMER_FIRSTNAME,
                                  CUSTOMER_ID,
                                  CUSTOMER_LASTNAME,
                                  DATE_CHANGED,
                                  DATE_CREATED,
                                  DBMS_LOB.SUBSTR(DESCRIPTION,3900,1) DESCRIPTION,
                                  HEIGHT,
                                  ID,
                                  KEYWORDS,
                                  NUMBERILLUSTRATIONSORDERED,
                                  ORDER_AMOUNT,
                                  ORDER_AMOUNT_BEFORE_DISCOUNT,
                                  ORDER_AMOUNT_VAT,
                                  ORDER_CURRENCY,
                                  ORDER_DATE_CHANGED,
                                  ORDER_DATE_CREATED,
                                  ORDER_ID,
                                  ORDER_STATE_TITLE,
                                  ORDER_VAT_PERCENTAGE,
                                  OWNER,
                                  PREFERREDFILEFORMAT_ID,
                                  PREFERREDFILEFORMAT_TITLE,
                                  PREFERREDFONT_ID,
                                  PREFERREDFONT_TITLE,
                                  DBMS_LOB.SUBSTR(REFERENCEFILESOURCEEXPLANATION,3900,1) REFERENCEFILESOURCEEXPLANATION,
                                  DBMS_LOB.SUBSTR(REFERENCEURL,3900,1) REFERENCEURL,
                                  STATE_ID,
                                  STATE_TITLE,
                                  TITLE,
                                  USAGEENVIRONMENTDESCRIPTION,
                                  WIDTH
                              FROM TBL_TEMP_ESTREET_IS_PROJECTS_C;
                          COMMIT;
    /*JIRA2923 - Added the truncate statement and Insert statement - END*/
   
   
                            INSERT INTO tbl_temp_subscriptions (status,orig_system_ref,orig_system,orig_create_date,orig_update_date,party_ref,item_ref_1,item_ref_2,start_date,end_date
                                                               ,price,sub_status,product_name,product_description,claim_count,duration,purchase_number,Order_Id,inv_currency
                                                               ,No_Copies,tax_code, CREDIT_CARD_ALLOWED)
                            SELECT
                                    NULL,
                                    'EST_IS_ORD_' || orderedProducts.ID AS orig_subscription_ref,
                                    'EST' AS orig_system,
                                    orderedProducts.Date_Created AS orig_create_date,
                                    orderedProducts.Date_Changed AS orig_update_date,
                                    'EST_' || orderedProducts.Customer_ID AS party_ref,
                                    NULL item_ref_1,
                                    NULL AS Item_ref_2,
                                    orderedProducts.Order_Date_Created AS start_date,
                                    NULL AS end_date,
                                    (orderedProducts.ORDER_AMOUNT + orderedProducts.ORDER_AMOUNT_VAT)/100 AS price,
                                    orderedProducts.ORDER_STATE_TITLE AS sub_status,
                                    orderedProducts.TITLE AS product_name,
                                    substr(orderedProducts.DESCRIPTION,1,2000) AS product_description,
                                    orderedProducts.ORDER_AMOUNT_VAT/100 AS claim_count,
                                    NULL AS duration,
                                    NULL AS purchase_number,
                                    orderedProducts.Order_ID AS order_id,
                                    orderedProducts.Order_currency AS inv_currency,
                                    orderedProducts.Numberillustrationsordered AS no_copies,
                                    orderedProducts.Order_Vat_Percentage AS tax_code,
                                    orderedProducts.State_Title AS CREDIT_CARD_ALLOWED
                            FROM tbl_temp_estreet_is_projects orderedProducts;

   ELSIF p_query_no = 7 THEN
   
       /*JIRA2923 - Added the truncate statement and Insert statement - START*/
                          EXECUTE IMMEDIATE 'TRUNCATE TABLE TBL_TEMP_ESTREET_LE_PROJECTS';
                          
                          INSERT INTO TBL_TEMP_ESTREET_LE_PROJECTS
                            SELECT DBMS_LOB.SUBSTR(ARTICLE_TITLE,3900,2) as ARTICLE_TITLE   
                                ,AUTHOR_NAME            
                                ,AUTO_WORD_COUNT        
                                ,CREATED_AT             
                                ,CUSTNOTIFIED_AT        
                                ,CUSTOMER_FIRSTNAME     
                                ,CUSTOMER_ID            
                                ,CUSTOMER_LASTNAME      
                                ,DATE_CHANGED           
                                ,DATE_CREATED           
                                ,DISKFILENAME           
                                ,DOCDELIVERED_AT        
                                ,DOCFILENAME            
                                ,DOCREADY_AT            
                                ,DOCREADY_RESOURCE_ID   
                                ,DOCSUBM_RESOURCE_ID    
                                ,EXTPROJREF             
                                ,FINISHED_AT            
                                ,ID                     
                                ,IS_PREMIUM             
                                ,JOURNAL_URL            
                                ,LANGUAGE_ID            
                                ,LANGUAGE_TITLE         
                                ,LE_SUPPLIER_ID         
                                ,MSG2CUSTOMER           
                                ,MSGFROMSUPPLIER        
                                ,NRRETRIEVALATTEMPTS    
                                ,NRSUBMISSIONATTEMPTS   
                                ,ORDERTOTAL_AMOUNT      
                                ,ORDER_AMOUNT           
                                ,ORDER_CURRENCY         
                                ,ORDER_DATE_CHANGED     
                                ,ORDER_DATE_CREATED     
                                ,ORDER_ID               
                                ,ORDER_STATE_TITLE      
                                ,ORDER_VAT_AMOUNT       
                                ,ORDER_VAT_RATE         
                                ,ORG_PROJECT_ID         
                                ,SPECIAL_INSTRUCTIONS   
                                ,STATE_ID               
                                ,STATE_TITLE            
                                ,STUDYAREA_ID           
                                ,STUDYAREA_TITLE        
                                ,SUBMFAILED_AT          
                                ,SUPPLIERSTATUS         
                                ,TURNAROUNDTIME_ID      
                                ,TURNAROUNDTIME_TITLE   
                                ,USEDSAVEPROGRESS       
                                ,WORDCOUNTCATEGORY_ID   
                                ,WORDCOUNTCATEGORY_TITLE
                            FROM TBL_TEMP_ESTREET_LE_PROJECTS_C;
                            Commit;
    /*JIRA2923 - Added the truncate statement and Insert statement - END*/
   
   
                            INSERT INTO tbl_temp_subscriptions (status,orig_system_ref,orig_system,orig_create_date,orig_update_date,party_ref,item_ref_1,item_ref_2,start_date,end_date
                                                               ,price,sub_status,product_name,product_description,claim_count,duration,purchase_number,Order_Id,inv_currency
                                                               ,No_Copies,tax_code, CREDIT_CARD_ALLOWED,owner,entitlement_type,claim_limit,password_id, password, password_status, source_code)
                            SELECT
                                    NULL,
                                    'EST_LE_ORD_' || orderedProducts.ID AS orig_subscription_ref,
                                    'EST' AS orig_system,
                                    orderedProducts.Date_Created AS orig_create_date,
                                    orderedProducts.Date_Changed AS orig_update_date,
                                    'EST_' || orderedProducts.Customer_ID AS party_ref,
                                    NULL item_ref_1,
                                    NULL AS Item_ref_2,
                                    orderedProducts.CREATED_AT AS start_date,
                                    NULL AS end_date,
                                    orderedProducts.ORDERTOTAL_AMOUNT/100 AS price,
                                    orderedProducts.ORDER_STATE_TITLE AS sub_status,
                                    orderedProducts.LANGUAGE_TITLE AS product_name,
                                    substr(orderedProducts.DOCFILENAME,1,2000) AS product_description,
                                    orderedProducts.ORDER_VAT_AMOUNT/100 AS claim_count,
                                    NULL AS duration,
                                    NULL AS purchase_number,
                                    orderedProducts.Order_ID AS order_id,
                                    orderedProducts.Order_currency AS inv_currency,
                                    NULL AS no_copies,
                                    orderedProducts.ORDER_VAT_RATE AS tax_code,
                                    orderedProducts.State_Title AS CREDIT_CARD_ALLOWED,
                                    orderedProducts.Studyarea_Title AS owner,
                                    orderedProducts.Wordcountcategory_Title AS ENTITLEMENT_TYPE,
                                    orderedProducts.Turnaroundtime_Title AS Claim_Limit,
                                    orderedProducts.le_supplier_id AS password_id,
                                    orderedProducts.msgfromsupplier AS password,
                                    orderedProducts.Usedsaveprogress AS password_status,
                                    substr(orderedProducts.article_title,1,300) AS source_code
                            FROM tbl_temp_estreet_le_projects orderedProducts;

   ELSIF p_query_no = 8 THEN
    -- Identify the PARTY records that need to be marked as 'D'
      insert into dbowner.tbl_temp_parties (status, orig_system, orig_system_ref, party_type, run_time)
        select distinct 'D', orig_system, orig_party_ref, party_type, sysdate
        from dbowner.tbl_parties
        where orig_system = 'EST'
        and orig_party_ref in
        (select orig_party_ref from dbowner.tbl_parties where orig_system = 'EST'
         minus
         select orig_system_ref from dbowner.tbl_temp_parties where orig_system = 'EST');

    ELSIF p_query_no = 9 THEN
    -- Identify the INTEREST records that need to be marked as 'D'
      insert into dbowner.tbl_temp_interests (orig_system_ref, orig_system, update_date, interest_type, status)
        select distinct orig_interest_ref, orig_system, sysdate, interest_type, 'D'
        from dbowner.tbl_interests
        where orig_system = 'EST'
        and orig_interest_ref in
        (select orig_interest_ref from dbowner.tbl_interests where orig_system = 'EST'
         minus
         select orig_system_ref from dbowner.tbl_temp_interests where orig_system = 'EST');

    ELSIF p_query_no = 10 THEN
    -- Identify the ITEM records that need to be marked as 'D'
      insert into dbowner.tbl_temp_items (orig_system_ref, orig_system, update_date, item_type, status)
        select distinct orig_item_ref, orig_system, sysdate, item_type, 'D'
        from dbowner.tbl_items
        where orig_system = 'EST'
        and orig_item_ref in
        (select orig_item_ref from dbowner.tbl_items where orig_system = 'EST'
         minus
         select orig_system_ref from dbowner.tbl_temp_items where orig_system = 'EST');

    ELSIF p_query_no = 11 THEN
    -- Identify the SUBSCRIPTION records that need to be marked as 'D'
      insert into dbowner.tbl_temp_subscriptions (status, orig_system_ref, orig_system, orig_update_date)
        select distinct 'D', orig_subscription_ref, orig_system, sysdate
        from dbowner.tbl_subscriptions
        where orig_system = 'EST'
        and orig_subscription_ref in
        (select orig_subscription_ref from dbowner.tbl_subscriptions where orig_system = 'EST'
         minus
         select orig_system_ref from dbowner.tbl_temp_subscriptions where orig_system = 'EST');

    ELSIF p_query_no = 12 THEN
    -- Identify the ITEM_SUBJECTS records that need to be marked as 'D'
      insert into dbowner.tbl_temp_item_subjects (status, orig_system, orig_system_ref, item_ref, update_date)
        select distinct 'D', orig_system, orig_subject_ref, item_ref, sysdate
        from dbowner.tbl_item_subjects
        where orig_system = 'EST'
        and orig_subject_ref in
        (select orig_subject_ref from dbowner.tbl_item_subjects where orig_system = 'EST'
         minus
         select orig_system_ref from dbowner.tbl_temp_item_subjects where orig_system = 'EST');

      END IF;


  --
  -- Insert EES Data
  --
  ELSIF p_source = 'EES' THEN


      -- User Records --
      IF p_query_no = 1 THEN

      -- Build up temp tables first --
      BEGIN
      EXECUTE IMMEDIATE 'DROP TABLE TBL_TEMP_EES_PARTIES_JOINED PURGE';
      EXCEPTION WHEN OTHERS THEN NULL;
      END;
      BEGIN
      EXECUTE IMMEDIATE 'DROP TABLE TBL_TEMP_EES_JOURNALS_JOINED PURGE';
      EXCEPTION WHEN OTHERS THEN NULL;
      END;
      BEGIN
      EXECUTE IMMEDIATE 'DROP TABLE TBL_TEMP_EES_ARTICLES_JOINED PURGE';
      EXCEPTION WHEN OTHERS THEN NULL;
      END;

      -- Temp Parties table
      EXECUTE IMMEDIATE '
      CREATE TABLE TBL_TEMP_EES_PARTIES_JOINED
      AS
      SELECT DISTINCT eesacronym, peopleid, addressid
      FROM
      (
      SELECT eesacronym, peopleid, addressid
      FROM TBL_TEMP_EES_PARTIES
      UNION
      SELECT substr(orig_party_ref,instr(orig_party_ref,''_'',1,1)+1,instr(orig_party_ref,''_'',-1,2)-instr(orig_party_ref,''_'',1,1)-1) AS eesacronym,
            to_number(trim(substr(orig_party_ref,instr(orig_party_ref,''_'',-1,2)+1,instr(orig_party_ref,''_'',-1,1)-instr(orig_party_ref,''_'',-1,2)-1))) AS peopleid,
            to_number(trim(substr(orig_party_ref,instr(orig_party_ref,''_'',-1,1)+1)))
      FROM TBL_PARTIES
      WHERE ORIG_SYSTEM = ''EES'' AND ORIG_PARTY_REF NOT LIKE ''EES_CAUTH%''
      )';
      COMMIT;

      EXECUTE IMMEDIATE 'CREATE INDEX INDX_TMP_ESS_PARTY_JOIN_1 ON TBL_TEMP_EES_PARTIES_JOINED (eesacronym)';
      EXECUTE IMMEDIATE 'CREATE INDEX INDX_TMP_ESS_PARTY_JOIN_2 ON TBL_TEMP_EES_PARTIES_JOINED (peopleid)';


        dbms_stats.gather_table_stats(
                                     ownname => 'DBOWNER',
                                     tabname => 'TBL_TEMP_EES_PARTIES_JOINED',
                                     estimate_percent => 10,
                                     method_opt => 'for all indexed columns size auto',
                                     degree => 4 ,
                                     cascade => TRUE
                                     );


      -- Temp Journals Table
      EXECUTE IMMEDIATE '
      CREATE TABLE TBL_TEMP_EES_JOURNALS_JOINED
      AS
      SELECT trim(replace(trim(substr(i1.orig_item_ref,1,instr(i1.orig_item_ref,''_'',-1,1)-1)),''EES_JOU_'')) eesacronym,
             to_number(trim(substr(orig_item_ref,instr(orig_item_ref,''_'',-1,1)+1))) AS care_sites_id,
             site as ptsacronym,
             orig_item_ref,
             orig_create_date,
             orig_update_date,
             capri_create_date,
             capri_update_date,
             MAX(capri_update_date) OVER (PARTITION BY trim(replace(trim(substr(i1.orig_item_ref,1,instr(i1.orig_item_ref,''_'',-1,1)-1)),''EES_JOU_''))) AS MAX_CAPRI_UPDATE_DATE
      FROM   dbowner.tbl_items i1
      WHERE  i1.orig_system = ''EES''
      AND    i1.item_type = ''JOU''';
      COMMIT;

      EXECUTE IMMEDIATE '
      DELETE FROM TBL_TEMP_EES_JOURNALS_JOINED
      WHERE MAX_CAPRI_UPDATE_DATE <> capri_update_date';
      COMMIT;

      EXECUTE IMMEDIATE '
      DELETE FROM TBL_TEMP_EES_JOURNALS_JOINED j1
      WHERE ROWID NOT IN (SELECT MAX(ROWID) FROM TBL_TEMP_EES_JOURNALS_JOINED j2 WHERE j1.eesacronym = j2.eesacronym)';
      COMMIT;

      EXECUTE IMMEDIATE '
      INSERT INTO TBL_TEMP_EES_JOURNALS_JOINED (care_sites_id,eesacronym,ptsacronym,orig_item_ref)
      SELECT max(j.care_sites_id) as care_sites_id,
             j.eesacronym,
             j.ptsacronym,
             ''EES_JOU_''||j.eesacronym||''_''||max(j.care_sites_id) AS orig_item_ref
      FROM TBL_TEMP_EES_JOURNALS j
      WHERE j.eesacronym NOT IN (
                                SELECT distinct eesacronym
                                FROM TBL_TEMP_EES_JOURNALS_JOINED
                                )
      GROUP BY  j.eesacronym,
             j.ptsacronym';
      COMMIT;

      EXECUTE IMMEDIATE 'CREATE INDEX INDX_TMP_ESS_JOURNAL_JOIN_1 ON TBL_TEMP_EES_JOURNALS_JOINED (eesacronym)';
      EXECUTE IMMEDIATE 'CREATE INDEX INDX_TMP_ESS_JOURNAL_JOIN_2 ON TBL_TEMP_EES_JOURNALS_JOINED (ptsacronym)';


        dbms_stats.gather_table_stats(
                                     ownname => 'DBOWNER',
                                     tabname => 'TBL_TEMP_EES_JOURNALS_JOINED',
                                     estimate_percent => 10,
                                     method_opt => 'for all indexed columns size auto',
                                     degree => 4 ,
                                     cascade => TRUE
                                     );

      -- Temp Articles Table
      EXECUTE IMMEDIATE '
      CREATE TABLE TBL_TEMP_EES_ARTICLES_JOINED
      AS
      SELECT orig_system,
             orig_item_ref,
             parent_ref,
             CASE WHEN i.identifier IS NULL THEN to_number(trim(substr(orig_item_ref,instr(orig_item_ref,''_'',-1,1)+1)))
             ELSE to_number(trim(substr(orig_item_ref,instr(orig_item_ref,''_'',-1,2)+1,instr(orig_item_ref,''_'',-1,1)-instr(orig_item_ref,''_'',-1,2)-1)))
             END AS DocumentID,

             REPLACE(
                   CASE WHEN i.identifier IS NULL THEN trim(substr(orig_item_ref,1,instr(orig_item_ref,''_'',-1,1)-1))
                   ELSE trim(substr(orig_item_ref,1,instr(orig_item_ref,''_'',-1,2)-1))
                   END,''EES_ART_'') AS eesacronym,
             CASE WHEN i.identifier IS NOT NULL THEN trim(substr(orig_item_ref,instr(orig_item_ref,''_'',-1,1)+1)) ELSE NULL END AS PTS_REF
      FROM TBL_ITEMS I
      WHERE I.orig_system = ''EES''
      AND item_type = ''ART''';
      COMMIT;

      EXECUTE IMMEDIATE '
      INSERT INTO TBL_TEMP_EES_ARTICLES_JOINED
      SELECT DISTINCT
             ''EES'' AS orig_system,
             ''EES_ART_'' || A.EESACRONYM  || ''_'' ||  A.DOCUMENTID || CASE WHEN ia.orig_item_ref IS NOT NULL THEN ''_'' || REPLACE(ia.orig_item_ref,''PTS_'') ELSE NULL END AS orig_item_ref,
             ''EES_JOU_''||j.eesacronym||''_''||j.care_sites_id AS parent_ref,
             a.documentid,
             a.eesacronym,
             CASE WHEN ia.orig_item_ref IS NOT NULL THEN REPLACE(ia.orig_item_ref,''PTS_'') ELSE NULL END AS PTS_REF
      FROM      (
                SELECT *
                FROM TBL_TEMP_EES_ARTICLES a
                WHERE a.documentid IN     (
                                          SELECT aj.documentid
                                          FROM
                                          (
                                          SELECT documentid,  eesacronym FROM  TBL_TEMP_EES_ARTICLES
                                          Minus
                                          SELECT documentid,  eesacronym FROM  TBL_TEMP_EES_ARTICLES_JOINED
                                          ) aj
                                          WHERE aj.eesacronym = a.eesacronym
                                          AND   aj.documentid = a.documentid
                                          )
                ) A
      LEFT JOIN (SELECT j.eesacronym, j.ptsacronym, MAX(j.care_sites_id) AS care_sites_id FROM TBL_TEMP_EES_JOURNALS_JOINED j GROUP BY j.eesacronym, j.ptsacronym) j
      ON        a.eesacronym = j.eesacronym
      LEFT JOIN (
                SELECT ia.orig_item_ref,
                       ia.parent_ref,
                       ia.name,
                       ia.volume,
                       ia.issue,
                       ia.year,
                       ia.identifier,
                       ia.code,
                       ia.type,
                       ia.sub_type,
                       ia.binding,
                       ia.medium,
                       ia.no_of_pages,
                       ia.page_numbers,
                       ia.item_milestone,
                       ia.issue_milestone,
                       ij.code AS PTS_JOURNAL_CODE
                FROM  tbl_items ia,
                      tbl_items ij
                WHERE ia.orig_system = ''PTS''
                AND   ia.item_type = ''ART''
                AND   ij.orig_item_ref = ia.parent_ref
                AND   ij.orig_system = ''PTS''
                AND   ij.item_type = ''JOU''
                ) ia
      ON        j.ptsacronym = ia.PTS_JOURNAL_CODE
      AND       CASE WHEN length(a.document_title) > 10 THEN lower(a.document_title) ELSE ''ABC'' END
                =
                CASE WHEN length(ia.name) > 10 THEN lower(ia.name) ELSE ''XYZ'' END
      ';
      COMMIT;

      EXECUTE IMMEDIATE 'CREATE INDEX INDX_TMP_ESS_ARTICLES_JOIN_1 ON TBL_TEMP_EES_ARTICLES_JOINED (eesacronym)';
      EXECUTE IMMEDIATE 'CREATE INDEX INDX_TMP_ESS_ARTICLES_JOIN_2 ON TBL_TEMP_EES_ARTICLES_JOINED (documentid)';

        dbms_stats.gather_table_stats(
                                     ownname => 'DBOWNER',
                                     tabname => 'TBL_TEMP_EES_ARTICLES_JOINED',
                                     estimate_percent => 10,
                                     method_opt => 'for all indexed columns size auto',
                                     degree => 4 ,
                                     cascade => TRUE
                                     );



                                INSERT INTO tbl_temp_parties (
                                                              party_type
                                                             ,dedupe_type
                                                             ,status
                                                             ,orig_system
                                                             ,orig_system_ref
                                                             ,usr_created_date
                                                             ,usr_status
                                                             ,usr_login
                                                             ,usc_contact_title
                                                             ,Orig_Title
                                                             ,usc_contact_firstname
                                                             ,usc_contact_lastname
                                                             ,usc_add_type
                                                             ,usc_addr
                                                             ,usr_address2
                                                             ,address3
                                                             ,address4
                                                             ,usc_city
                                                             ,usc_state
                                                             ,usc_zip
                                                             ,usc_country
                                                             ,orig_country
                                                             ,usc_dept
                                                             ,usc_org
                                                             ,jobtitle
                                                             ,usc_phone
                                                             ,phone_2
                                                             ,phone_3
                                                             ,usc_fax
                                                             ,usc_dedupe_email
                                                             ,user_access_type
                                                             ,history_enabled
                                                             ,subj_area_home_page
                                                             ,Toggle_Pref
                                                             ,desk_type
                                                             ,prime_type_descr
                                                             ,sales_emails
                                                             ,run_time
                                                             )

                                SELECT
                                      'EUS'   AS party_type,
                                      'PER'   AS dedupe_type,
                                      CASE WHEN PPL_INACTIVE = 'true' THEN 'D' ELSE NULL END AS Status,
                                      'EES' AS orig_system,
                                      'EES_' || nvl(P.EESACRONYM,'0') ||'_'|| nvl(P.PEOPLEID,0) ||'_'|| nvl(p.addressid,0) AS orig_system_ref,
                                      P.REG_DATE AS usr_created_date,
                                      P.PPL_INACTIVE AS usr_status,
                                      P.WLOGIN AS usr_login,
                                      P.PTITLE AS usc_contact_title,
                                      P.PTITLE AS orig_title,
                                      p.firstname AS usc_contact_firstname,
                                      p.lastname AS usc_contact_lastname,
                                      p.atype AS usc_add_type,
                                      p.address1 AS usc_addr,
                                      p.address2 AS usr_address2,
                                      p.address3 AS address3,
                                      CAST(p.address4 AS VARCHAR2(50)) AS address4,
                                      p.city AS usc_city,
                                      p.st AS usc_state,
                                      p.zipcode AS usc_zip,
                                      p.country AS usc_country,
                                      p.country AS orig_country,
                                      p.department AS usc_dept,
                                      substr(p.institute,1,250) AS usc_org,
                                      p.position AS jobtitle,
                                      p.phone AS usc_phone,
                                      p.phone2 AS phone_2,
                                      p.phone3 AS phone_3,
                                      p.fax AS usc_fax,
                                      p.email AS usc_dedupe_email,
                                      p.never_logged_in_before AS user_access_type,
                                      p.publisher AS history_enabled,
                                      p.editor AS subj_area_home_page,
                                      p.reviewer AS toggle_pref,
                                      p.position AS desk_type,
                                      p.contacttype AS prime_type_descr,
                                      CASE WHEN lower(m.reg_question) LIKE '%please tick the box if you do not wish to receive news%'
                                            AND lower(m.reg_answer) = 'true' THEN 'N'
                                           WHEN lower(m.reg_question) LIKE '%please tick the box if you do not wish to receive news%'
                                            AND lower(m.reg_answer) = 'false' THEN 'Y'
                                           WHEN lower(m.reg_question) LIKE '%please tick the box if you wish to receive news%'
                                            AND lower(m.reg_answer) = 'true' THEN 'Y'
                                           WHEN lower(m.reg_question) LIKE '%please tick the box if you wish to receive news%'
                                            AND lower(m.reg_answer) = 'false' THEN 'N'
                                            ELSE NULL
                                            END AS sales_emails,
                                      p.ppl_lastupdate
                                FROM TBL_TEMP_EES_PARTIES p,
                                     TBL_TEMP_EES_MAILING_PREFS m
                                WHERE p.eesacronym = m.eesacronym (+)
                                AND   p.peopleid   = m.peopleid (+)
                                ;

      -- Journal Records --
      ELSIF p_query_no = 2 THEN
                                 INSERT INTO tbl_temp_items (
                                                            orig_system
                                                           ,orig_system_ref
                                                           ,NAME
                                                           ,item_type
                                                           ,site
                                                           ,create_date
                                                           ,last_pub_date
                                                           ,update_date
                                                           ,IDENTIFIER
                                                           ,show_status
                                                           ,publisher
                                                           ,pmc_code
                                                           ,pmc_descr
                                                           ,pmg_code
                                                           ,pmg_descr
                                                           )
                                SELECT
                                      'EES' AS orig_system,
                                      'EES_JOU_'||j.eesacronym||'_'||j.care_sites_id AS orig_item_ref,
                                      j.journalfulltitle AS NAME,
                                      'JOU' AS item_type,
                                      j.ptsacronym AS site,
                                      j.datecreated AS orig_create_date,
                                      j.datesetlive AS LAST_pub_date,
                                      j.siteslastupdateddatetime AS orig_update_date,
                                      ii.identifier AS IDENTIFIER,
                                      ii.show_status AS show_status,
                                      ii.publisher AS publisher,
                                      ii.pmc_code AS pmc_code,
                                      ii.pmc_descr AS pmc_descr,
                                      ii.pmg_code AS pmg_code,
                                      ii.pmg_descr AS pmg_descr
                                FROM tbl_temp_ees_journals j,
                                     (
                                     SELECT ii.code,
                                            ii.identifier AS IDENTIFIER,
                                            ii.show_status AS show_status,
                                            ii.publisher AS publisher,
                                            ii.pmc_code AS pmc_code,
                                            ii.pmc_descr AS pmc_descr,
                                            ii.pmg_code AS pmg_code,
                                            ii.pmg_descr AS pmg_descr
                                     FROM tbl_items ii
                                     WHERE ii.orig_system = 'PTS'
                                     AND ii.item_type = 'JOU'
                                     AND ii.orig_item_ref IN
                                                            (
                                                            SELECT MAX(i1.orig_item_ref)
                                                            FROM tbl_items i1
                                                            WHERE i1.orig_system = 'PTS'
                                                            AND i1.item_type = 'JOU'
                                                            AND i1.code = ii.code
                                                            )
                                     ) ii
                                WHERE j.ptsacronym = ii.code (+)
                                ;

      -- Article Records --
      ELSIF p_query_no = 3 THEN

            EXECUTE IMMEDIATE 'ALTER TABLE TBL_TEMP_ITEMS MODIFY PARTITION par_EES UNUSABLE LOCAL INDEXES';

                                INSERT INTO TBL_TEMP_ITEMS (
                                                            ORIG_SYSTEM
                                                           ,ORIG_SYSTEM_REF
                                                           ,PARENT_REF
                                                           ,Update_Date
                                                           ,NAME
                                                           ,Item_Type
                                                           ,DESCRIPTION
                                                           ,Show_Status
                                                           ,Volume
                                                           ,ISSUE
                                                           ,YEAR
                                                           ,IDENTIFIER
                                                           ,Code
                                                           ,TYPE
                                                           ,Sub_Type
                                                           ,BINDING
                                                           ,Medium
                                                           ,NO_OF_PAGES
                                                           ,Page_Numbers
                                                           ,Item_Milestone
                                                           ,Issue_Milestone
                                                           ,CREATE_DATE
                                                           ,LAST_PUB_DATE
                                                           ,CLASS
                                                           )
                                SELECT DISTINCT
                                       'EES',
                                       'EES_ART_' || A.EESACRONYM  || '_' || A.DOCUMENTID || CASE WHEN ia.orig_item_ref IS NOT NULL THEN '_' || REPLACE(ia.orig_item_ref,'PTS_') ELSE NULL END AS orig_item_ref,
                                       'EES_JOU_'||j.eesacronym||'_'||j.care_sites_id AS parent_ref,
                                       a.lastudpate AS orig_update_date,
                                       a.document_title AS NAME,
                                       'ART' AS item_type,
                                       a.category AS DESCRIPTION,
                                       a.status_name AS show_status,
                                       ia.volume,
                                       ia.issue,
                                       ia.year,
                                       ia.identifier,
                                       ia.code,
                                       ia.type,
                                       ia.sub_type,
                                       ia.binding,
                                       ia.medium,
                                       ia.no_of_pages,
                                       ia.page_numbers,
                                       ia.item_milestone,
                                       ia.issue_milestone,
                                       a.submission_start_date,
                                       a.ddisposidate,
                                       a.ddiposistatus
                                FROM      TBL_TEMP_EES_ARTICLES A
                                LEFT JOIN TBL_TEMP_EES_JOURNALS_JOINED j
                                ON        a.eesacronym = j.eesacronym
                                LEFT JOIN (
                                          SELECT *
                                          FROM tbl_items ii
                                          WHERE ii.orig_system = 'PTS'
                                          AND ii.item_type = 'JOU'
                                          ) ij
                                ON        j.ptsacronym = ij.code
                                LEFT JOIN (
                                          SELECT ia.orig_item_ref,
                                                 ia.parent_ref,
                                                 ia.name,
                                                 ia.volume,
                                                 ia.issue,
                                                 ia.year,
                                                 ia.identifier,
                                                 ia.code,
                                                 ia.type,
                                                 ia.sub_type,
                                                 ia.binding,
                                                 ia.medium,
                                                 ia.no_of_pages,
                                                 ia.page_numbers,
                                                 ia.item_milestone,
                                                 ia.issue_milestone
                                          FROM  tbl_items ia
                                          WHERE ia.orig_system = 'PTS'
                                          AND   ia.item_type = 'ART'
                                          ) ia
                                ON        ij.orig_item_ref = ia.parent_ref
                                        AND       CASE WHEN length(a.document_title) > 10 THEN lower(a.document_title) ELSE 'ABC' END
                                                  =
                                                  CASE WHEN length(ia.name) > 10 THEN lower(ia.name) ELSE 'XYZ' END
                                ;
                       COMMIT;
         EXECUTE IMMEDIATE 'ALTER TABLE TBL_TEMP_ITEMS MODIFY PARTITION par_EES REBUILD UNUSABLE LOCAL INDEXES';


      -- Article Author Records --
      ELSIF p_query_no = 4 THEN

                                INSERT INTO TBL_TEMP_INTERESTS (
                                                                ORIG_SYSTEM
                                                               ,PARTY_REF
                                                               ,ORIG_SYSTEM_REF
                                                               ,INTEREST_TYPE
                                                               ,INTEREST_VALUE
                                                               ,INTEREST_VALUE_DETAILS
                                                               ,INTEREST_SUB_SECTION
                                                               ,ALERT_END_DATE
                                                               ,CREATE_DATE
                                                               )
                                SELECT DISTINCT
                                       'EES',
                                       'EES_' || nvl(P.EESACRONYM,'0') ||'_'|| nvl(P.PEOPLEID,0) ||'_'|| nvl(p.addressid,0) AS orig_party_ref,
                                       'EES_AUTH_' ||   r.eesacronym || '_' ||
                                                        r.documentid || '_' ||
                                                        r.peopleid   || '_' ||
                                                        p.addressid  || '_' ||
                                                        r.roleid     || '_' ||
                                                        r.revision   ||
                                                        CASE WHEN a.PTS_REF IS NOT NULL THEN
                                                                                           '_'|| a.PTS_REF
                                                             ELSE
                                                                                            NULL
                                                             END
                                                                       AS orig_interest_ref,
                                       'ART' AS interest_type,
                                       a.orig_item_ref AS interest_value,
                                       r.role_result AS interest_value_details,
                                       max(r.primary_author) AS interest_sub_section,
                                       r.role_stop AS alert_end_date,
                                       r.role_start
                                FROM TBL_TEMP_EES_ROLEAUTH r,
                                     TBL_TEMP_EES_PARTIES_JOINED p,
                                     TBL_TEMP_EES_ARTICLES_JOINED a
                                WHERE r.eesacronym = p.eesacronym (+)
                                AND   r.peopleid   = p.peopleid   (+)
                                AND   r.eesacronym = a.eesacronym (+)
                                AND   r.documentid = a.documentid (+)
                                GROUP BY 'EES_' || nvl(P.EESACRONYM,'0') ||'_'|| nvl(P.PEOPLEID,0) ||'_'|| nvl(p.addressid,0),
                                      'EES_AUTH_' ||   r.eesacronym || '_' ||
                                                        r.documentid || '_' ||
                                                        r.peopleid   || '_' ||
                                                        p.addressid  || '_' ||
                                                        r.roleid     || '_' ||
                                                        r.revision   ||
                                                        CASE WHEN a.PTS_REF IS NOT NULL THEN
                                                                                           '_'|| a.PTS_REF
                                                             ELSE
                                                                                            NULL
                                                             END,
                                       a.orig_item_ref,
                                       r.role_result,
                                       r.role_stop,
                                       r.role_start
                                ;


      -- Article Editor Records --
      ELSIF p_query_no = 5 THEN
                                INSERT INTO TBL_TEMP_INTERESTS (
                                                                ORIG_SYSTEM
                                                               ,PARTY_REF
                                                               ,ORIG_SYSTEM_REF
                                                               ,INTEREST_TYPE
                                                               ,INTEREST_VALUE
                                                               ,INTEREST_VALUE_DETAILS
                                                               ,INTEREST_SUB_SECTION
                                                               ,ALERT_END_DATE
                                                               ,CREATE_DATE
                                                               )
                                SELECT DISTINCT
                                       'EES',
                                       'EES_' || nvl(P.EESACRONYM,'0') ||'_'|| nvl(P.PEOPLEID,0) ||'_'|| nvl(p.addressid,0) AS orig_party_ref,
                                       'EES_EDIT_' ||   r.eesacronym || '_' ||
                                                        r.documentid || '_' ||
                                                        r.peopleid   || '_' ||
                                                        p.addressid  || '_' ||
                                                        r.roleid     || '_' ||
                                                        r.revision   ||
                                                        CASE WHEN a.PTS_REF IS NOT NULL THEN
                                                                                           '_'|| a.PTS_REF
                                                             ELSE
                                                                                            NULL
                                                             END
                                                                       AS orig_interest_ref,
                                       'ART' AS interest_type,
                                       a.orig_item_ref AS interest_value,
                                       r.role_result AS interest_value_details,
                                       r.editor_description AS interest_sub_section,
                                       r.role_stop AS alert_end_date,
                                       r.role_start
                                FROM TBL_TEMP_EES_ROLEEDIT r,
                                     TBL_TEMP_EES_PARTIES_JOINED p,
                                     TBL_TEMP_EES_ARTICLES_JOINED a
                                WHERE r.eesacronym = p.eesacronym (+)
                                AND   r.peopleid   = p.peopleid   (+)
                                AND   r.eesacronym = a.eesacronym (+)
                                AND   r.documentid = a.documentid (+)
                                ;

      -- Article Reviewer Records --
      ELSIF p_query_no = 6 THEN
                                INSERT INTO TBL_TEMP_INTERESTS (
                                                                ORIG_SYSTEM
                                                               ,PARTY_REF
                                                               ,ORIG_SYSTEM_REF
                                                               ,INTEREST_TYPE
                                                               ,INTEREST_VALUE
                                                               ,INTEREST_VALUE_DETAILS
                                                               ,INTEREST_SUB_SECTION
                                                               ,ALERT_END_DATE
                                                               ,CREATE_DATE
                                                               )
                                SELECT DISTINCT
                                       'EES',
                                       'EES_' || nvl(P.EESACRONYM,'0') ||'_'|| nvl(P.PEOPLEID,0) ||'_'|| nvl(p.addressid,0) AS orig_party_ref,
                                       'EES_REVIEW_' || r.eesacronym || '_' ||
                                                        r.documentid || '_' ||
                                                        r.peopleid   || '_' ||
                                                        p.addressid  || '_' ||
                                                        r.roleid     || '_' ||
                                                        r.revision   ||
                                                        CASE WHEN a.PTS_REF IS NOT NULL THEN
                                                                                           '_'|| a.PTS_REF
                                                             ELSE
                                                                                            NULL
                                                             END
                                                                       AS orig_interest_ref,
                                       'ART' AS interest_type,
                                       a.orig_item_ref AS interest_value,
                                       r.role_result AS interest_value_details,
                                       NULL AS interest_sub_section,
                                       r.role_stop AS alert_end_date,
                                       r.role_start
                                FROM TBL_TEMP_EES_ROLEREV r,
                                     TBL_TEMP_EES_PARTIES_JOINED p,
                                     TBL_TEMP_EES_ARTICLES_JOINED a
                                WHERE r.eesacronym = p.eesacronym (+)
                                AND   r.peopleid   = p.peopleid   (+)
                                AND   r.eesacronym = a.eesacronym (+)
                                AND   r.documentid = a.documentid (+)
                                ;

      -- Get distinct list of Articles that have reviewers either in TBL_TEMP_INTERESTS or TBL_INTERESTS --
      ELSIF p_query_no = 7 THEN

                            BEGIN

                            BEGIN
                            EXECUTE IMMEDIATE 'DROP TABLE TBL_TEMP_EES_REVIEWED_ARTICLES PURGE';
                            EXCEPTION WHEN OTHERS THEN NULL;
                            END;

                            EXECUTE IMMEDIATE '
                            CREATE TABLE TBL_TEMP_EES_REVIEWED_ARTICLES
                              AS
                                SELECT DISTINCT interest_value
                                FROM
                                (
                                    SELECT ii.interest_value,
                                           CASE WHEN orig_interest_ref LIKE ''EES_REVIEW%'' THEN ''Y'' ELSE NULL END AS REVIEWED
                                    FROM   tbl_interests ii
                                    WHERE  ii.orig_system = ''EES''
                                    AND    ii.interest_type = ''ART''
                                )
                                WHERE REVIEWED = ''Y''
                                UNION
                                SELECT DISTINCT ii.interest_value
                                FROM   tbl_temp_interests ii
                                WHERE  ii.orig_system = ''EES''
                                AND    ii.interest_type = ''ART''
                                AND    ii.orig_system_ref LIKE ''EES_REVIEW%''';


                            EXECUTE IMMEDIATE 'create index INDX_TEMPREVIEWART_ID on TBL_TEMP_EES_REVIEWED_ARTICLES (INTEREST_VALUE)';

                            BEGIN
                            dbms_stats.gather_table_stats(
                                                         ownname => 'DBOWNER',
                                                         tabname => 'TBL_TEMP_EES_REVIEWED_ARTICLES',
                                                         estimate_percent => 10,
                                                         method_opt => 'for all indexed columns size auto',
                                                         degree => 4 ,
                                                         cascade => TRUE
                                                         );
                            END;

                            END;

      -- Update Articles to show if there are reviewer records for the given Article in TBL_TEMP_INTERESTS or TBL_INTERESTS --
      ELSIF p_query_no = 8 THEN
                              COMMIT;
                              EXECUTE IMMEDIATE '
                              UPDATE tbl_temp_items t
                              SET t.imprint = ''Y''
                              WHERE t.orig_system = ''EES''
                              AND  t.orig_system_ref IN
                                                       (
                                                       SELECT a.interest_value
                                                       FROM TBL_TEMP_EES_REVIEWED_ARTICLES a
                                                       WHERE a.interest_value = t.orig_system_ref
                                                       )';
                               COMMIT;
      --COAUTHOR PARTY RECORDS                         
      ELSIF p_query_no = 9 THEN
                            INSERT
                            INTO TBL_TEMP_PARTIES
                              (
                                STATUS,    ORIG_SYSTEM,    ORIG_SYSTEM_REF,    PARTY_TYPE,    USR_CREATED_DATE,    USR_STATUS,    USR_LOGIN,	USR_OTHER_REF,	USC_CONTACT_TITLE,
                                ORIG_TITLE,    USC_CONTACT_FIRSTNAME,    USC_CONTACT_LASTNAME,    USC_ADD_TYPE,    USC_ADDR,    USR_ADDRESS2,    ADDRESS3,
                                ADDRESS4,    USC_CITY,    USC_STATE,    USC_ZIP,    USC_COUNTRY,    ORIG_COUNTRY,    USC_DEPT,    USC_ORG,
                                JOBTITLE,    USC_PHONE,    PHONE_2,    PHONE_3,    USC_FAX,    USC_DEDUPE_EMAIL,    UNIQUE_INST_ID,    USER_ACCESS_TYPE,
                                HISTORY_ENABLED,    SUBJ_AREA_HOME_PAGE,    TOGGLE_PREF,    DESK_TYPE,    PRIME_TYPE_DESCR,    RUN_TIME
                              )
                            SELECT DISTINCT 'A' AS STATUS,
                              'EES'             AS ORIG_SYSTEM,
                              'EES_CAUTH_'
                              || A.EESACRONYM
                              ||'_'
                              ||A.AUTHID              AS ORIG_SYSTEM_REF,
                              'EUS'                   AS PARTY_TYPE,
                              NVL(A.REG_DATE,SYSDATE) AS USR_CREATED_DATE,
                              A.INACTIVE              AS USR_STATUS,
                              A.WLOGIN                AS USR_LOGIN,
                              A.PEOPLEID 			  AS USR_OTHER_REF,
                              A.PTITLE                AS USC_CONTACT_TITLE,
                              A.PTITLE                AS ORIG_TITLE,
                              A.FIRSTNAME             AS USC_CONTACT_FIRSTNAME,
                              A.LASTNAME              AS USC_CONTACT_LASTNAME,
                              lower(A.ATYPE)        AS USC_ADD_TYPE,
                              A.ADDRESS1              AS USC_ADDR,
                              A.ADDRESS2              AS USR_ADDRESS2,
                              A.ADDRESS3              AS ADDRESS3,
                              A.ADDRESS4              AS ADDRESS4,
                              A.CITY                  AS USC_CITY,
                              A.ST                    AS USC_STATE,
                              A.ZIPCODE               AS USC_ZIP,
                              A.COUNTRY               AS USC_COUNTRY,
                              A.COUNTRY               AS ORIG_COUNTRY,
                              A.DEPARTMENT            AS USC_DEPT,
                              A.INSTITUTE             AS USC_ORG,
                              A.POSITION              AS JOBTITLE,
                              A.PHONE                 AS USC_PHONE,
                              A.PHONE2                AS PHONE_2,
                              A.PHONE3                AS PHONE_3,
                              A.FAX                   AS USC_FAX,
                              A.EMAIL                 AS USC_DEDUPE_EMAIL,
                              'EES_'
                              || A.EESACRONYM
                              ||'_'
                              || A.PEOPLEID
                              ||'_'
                              || A.ADDRESSID           AS UNIQUE_INST_ID,
                              A.NEVER_LOGGED_IN_BEFORE AS USER_ACCESS_TYPE,
                              A.PUBLISHER              AS HISTORY_ENABLED,
                              A.EDITOR                 AS SUBJ_AREA_HOME_PAGE,
                              A.REVIEWER               AS TOGGLE_PREF,
                              A.POSITION               AS DESK_TYPE,
                              A.CONTACTTYPE            AS PRIME_TYPE_DESCR,
                              A.LASTUPDATE             AS RUN_TIME
                            FROM TBL_TEMP_EES_COAUTHORS A;
                            COMMIT;
      --COAUTHOR INTEREST RECORDS                         
      ELSIF p_query_no = 10 THEN
                            INSERT
                                INTO TBL_TEMP_INTERESTS
                                  (
                                    ORIG_SYSTEM_REF,
                                    ORIG_SYSTEM,
                                    PARTY_REF,
                                    CREATE_DATE,
                                    UPDATE_DATE,
                                    INTEREST_TYPE,
                                    INTEREST_VALUE,
                                    INTEREST_VALUE_DETAILS,
                                    INTEREST_SECTION,
                                    INTEREST_SUB_SECTION
                                  )
                                SELECT DISTINCT 'EES_CAUTH_' || A.EESACRONYM  ||'_'  ||A.DOCUMENTID  ||'_' ||A.REVISION || '_' ||A.AUTHID  ||
                                  CASE
                                    WHEN C.PTS_REF IS NOT NULL
                                    THEN '_'
                                      || C.PTS_REF
                                    ELSE NULL
                                  END   AS ORIG_SYSTEM_REF,
                                  'EES' AS ORIG_SYSTEM,
                                  'EES_CAUTH_'  || A.EESACRONYM  || '_'  || A.AUTHID     AS PARTY_REF,
                                  SYSDATE AS CREATE_DATE,
                                  A.ROW_LASTMODIFIED_TIMESTAMP AS UPDATE_DATE,
                                  'ART'           AS INTEREST_TYPE,
                                  C.ORIG_ITEM_REF AS INTEREST_VALUE,
                                  A.DOCUMENTID    AS INTEREST_VALUE_DETAILS,
                                  A.REVISION AS INTEREST_SECTION,
                                  A.AUTHID        AS INTEREST_SUB_SECTION
                                FROM TBL_TEMP_EES_COAUTHORS A,
                                  DBOWNER.TBL_TEMP_EES_ARTICLES_JOINED C
                                WHERE A.EESACRONYM = C.EESACRONYM (+)
                                AND A.DOCUMENTID   = C.DOCUMENTID (+);
                                COMMIT;

      END IF;

ELSIF p_source = 'NLN' THEN

    IF p_query_no = 1 THEN
               -- insert Preference Centre Registrants 'EUS'

      EXECUTE IMMEDIATE 'TRUNCATE TABLE TBL_NL_MDT_PC_RECIPIENT';

      INSERT INTO TBL_NL_MDT_PC_RECIPIENT
      SELECT * FROM EMROWNER.TBL_NL_MDT_RECIPIENT WHERE DATA_TYPE = 'PC';

            INSERT INTO dbowner.tbl_temp_parties
                SELECT DISTINCT NULL, 'NLN', DATA_TYPE, 'NLN_' || RECIPIENT_ID, 'EUS', 'PER', CREATION_DATE, NULL, NULL, NULL, NULL, NULL, NULL, LANGUAGE, CREATION_OWNER, LAST_MODIFIED_OWNER, NULL,
                    TITLE, TITLE, FIRST_NAME, LAST_NAME, NULL, NULL, ADDRESS1, ADDRESS2, ADDRESS3, ADDRESS4, CITY, STATE, POSTCODE, COUNTRY, NULL, COUNTRY,
                    NULL, NULL, NULL, NULL, DEPARTMENT, ORGANIZATION_NAME, JOB_TITLE, PHONE, NULL, NULL, NULL, NULL, EMAIL_ADDRESS, NULL, NULL, NULL, DECODE(BLACKLIST,1,'N','Y'), NULL, NULL, NULL,
                    NULL, SIS_ID, NULL, NULL, NULL, NULL, NULL, NULL, NULL, PUBLISHING_ROLE, NULL, NULL, NULL, NULL, NULL, ORGANIZATION_TYPE, ROLE, NULL, NULL, NULL,
                    DECODE(BLACKLIST_EMAIL,1,'N','Y'), NULL, NULL, NULL, LAST_MODIFICATION_DATE, null, NULL
                FROM TBL_NL_MDT_PC_RECIPIENT;

        ELSIF p_query_no = 2 THEN
            -- insert Declared Interests 'DIN'
            INSERT INTO dbowner.tbl_temp_interests
                SELECT DISTINCT 'NLN_DIN_' || RECIPIENT_ID || '_' || INTEREST_NAME, 'NLN', 'NLN_' || RECIPIENT_ID, CREATION_DATE, LAST_MODIFICATION_DATE, 'DIN', INTEREST_VALUE, INTEREST_NAME, CREATED_BY, MODIFIED_BY, NULL, NULL
                FROM EMROWNER.VW_NL_DECLARED_INTERESTS;

        ELSIF p_query_no = 3 THEN
            -- insert Service Subscriptions 'SER'
            INSERT INTO dbowner.tbl_temp_interests
                SELECT DISTINCT 'NLN_SER_' || subs.RECIPIENT_ID || '_' || subs.SERVICE_ID, 'NLN', 'NLN_' || subs.RECIPIENT_ID, subs.CREATION_DATE, NULL, 'SER', subs.SERVICE_ID, serv.LABEL, NULL, NULL, NULL, NULL
                FROM EMROWNER.TBL_NL_MDT_SUBSCRIPTIONS subs,
                     TBL_NL_MDT_PC_RECIPIENT pc,
                     EMROWNER.TBL_NL_MDT_SERVICES serv
                WHERE subs.recipient_id = pc.recipient_id
                AND subs.service_id = serv.service_id;
--// GCS Changes for creating 'Declared Interest' Deleted entries
        ELSIF p_query_no = 4 THEN
            INSERT
            INTO    dbowner.tbl_temp_interests
            SELECT    orig_interest_ref    orig_system_ref
                ,orig_system
                ,party_ref
                ,orig_create_date    create_date
                ,orig_update_date    update_date
                ,interest_type
                ,interest_value
                ,interest_value_details
                ,interest_section
                ,interest_sub_section
                ,alert_end_date
                ,status
            FROM    (
                 SELECT     intr.orig_interest_ref
                    ,intr.orig_system
                    ,intr.party_ref
                    ,intr.orig_create_date
                    ,intr.orig_update_date
                    ,intr.interest_type
                    ,intr.interest_value
                    ,intr.interest_value_details
                    ,intr.interest_section
                    ,intr.interest_sub_section
                    ,intr.alert_end_date
                    ,CASE WHEN tint.orig_system_ref IS NULL THEN 'D' ELSE NULL END status
                 FROM    tbl_interests    intr
                 LEFT JOIN tbl_temp_interests    tint
                 ON    (    tint.orig_system    = 'NLN'
                     AND    tint.interest_type    = 'DIN'
                     AND    tint.orig_system    = intr.orig_system
                     AND    tint.interest_type    = intr.interest_type
                     AND    tint.orig_system_ref    = intr.orig_interest_ref
                    )
                 WHERE    intr.orig_system    = 'NLN'
                 AND    intr.interest_type    = 'DIN'
                 AND    intr.record_status    = 'A'
                )
            WHERE    status    = 'D';

--// GCS Changes for creating 'Services' Deleted entries
        ELSIF p_query_no = 5 THEN
            INSERT
            INTO    dbowner.tbl_temp_interests
            SELECT    orig_interest_ref    orig_system_ref
                ,orig_system
                ,party_ref
                ,orig_create_date    create_date
                ,orig_update_date    update_date
                ,interest_type
                ,interest_value
                ,interest_value_details
                ,interest_section
                ,interest_sub_section
                ,alert_end_date
                ,status
            FROM    (
                 SELECT     intr.orig_interest_ref
                    ,intr.orig_system
                    ,intr.party_ref
                    ,intr.orig_create_date
                    ,intr.orig_update_date
                    ,intr.interest_type
                    ,intr.interest_value
                    ,intr.interest_value_details
                    ,intr.interest_section
                    ,intr.interest_sub_section
                    ,intr.alert_end_date
                    ,CASE WHEN tint.orig_system_ref IS NULL THEN 'D' ELSE NULL END status
                 FROM    tbl_interests    intr
                 LEFT JOIN tbl_temp_interests    tint
                 ON    (    tint.orig_system    = 'NLN'
                     AND    tint.interest_type    = 'SER'
                     AND    tint.orig_system    = intr.orig_system
                     AND    tint.interest_type    = intr.interest_type
                     AND    tint.orig_system_ref    = intr.orig_interest_ref
                    )
                 WHERE    intr.orig_system    = 'NLN'
                 AND    intr.interest_type    = 'SER'
                 AND    intr.record_status    = 'A'
                )
            WHERE    status    = 'D';

      END IF;

	ELSIF p_source = 'SD' THEN

    IF p_query_no = 1 THEN
		   	-- insert End User Contacts 'EUS'

      EXECUTE IMMEDIATE 'ALTER INDEX DBOWNER.IDX_TEMP_SD_USER_1 REBUILD COMPUTE STATISTICS';

      INSERT INTO dbowner.tbl_temp_parties
                SELECT DISTINCT NULL, 'SD', fk_append_platform_codes(a.web_user_id) platform_code, 'SD2_' || a.web_user_id, 'EUS', 'PER', a.create_date, a.status, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                    account_admin, NULL, a.title, a.title,a.user_first_name, a.user_last_name, NULL, NULL, a.address1, a.address2, a.address3, NULL, a.city, a.stateorprov, a.zip, a.country, NULL,
                    a.country, NULL, NULL, NULL, NULL, NULL, NULL, a.job_title, a.user_phone, NULL, NULL, NULL, a.user_fax, a.user_email, decode(a.email_html,'Y','HTML','Text'), NULL,NULL, NULL,
                    issue_alert_count, NULL, NULL, NULL, NULL, NULL, NULL, search_alert_count, topic_alert_count, chunk_size, display_srch_results, NULL, user_creation_type,
                    NULL, NULL, NULL, NULL, NULL, a.org_type, a.user_role_name, NULL, NULL, NULL,decode(marketing_opt_out, 'N', 'Y', 'N'), NULL, a.developer_flag, NULL, a.update_date,
                    NULL, NULL
                FROM dbowner.tbl_temp_sd_user a
                ORDER BY a.status;

		ELSIF p_query_no = 2 THEN
			-- insert Organisational Contacts 'OCT'
			INSERT INTO dbowner.tbl_temp_parties
                SELECT DISTINCT decode(a.logical_deleted, 'Y', 'D', NULL), 'SD', NULL, 'SD2_OCT_' || a.acct_no, 'OCT', 'PER', a.create_date, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                    a.loc_title, a.loc_title, a.loc_name, NULL, NULL, NULL, a.loc_add1, a.loc_add2, a.loc_add3, NULL, a.loc_city, a.loc_storprv, a.loc_zip, a.loc_country_id, NULL, a.loc_country_id,
                    NULL, NULL, NULL, NULL, a.loc_dept, a.name, a.loc_job_title, a.loc_phone, NULL, NULL, NULL, a.loc_fax, a.loc_email, NULL, NULL, NULL, NULL, NULL, 'SD2_BCT_' || a.acct_no, a.loc_company,
                    NULL, a.sis, NULL, 'SD2_ACCT_' || acct_id, NULL, NULL, NULL, NULL, NULL,NULL, NULL, NULL, NULL, NULL, NULL, a.org_type, a.loc_desk_type, a.sd_customer_type_name, a.sc_customer_type_name,
                    NULL,NULL, NULL, NULL, NULL, a.update_date,
                    ELS_CUSTOMER_ID, NULL
                FROM dbowner.tbl_temp_sd_acct a;

		ELSIF p_query_no = 3 THEN
		    -- insert Billing Contacts 'BCT'
			INSERT INTO dbowner.tbl_temp_parties
                SELECT DISTINCT decode(a.logical_deleted, 'Y', 'D', NULL), 'SD', NULL, 'SD2_BCT_' || acct_no, 'BCT', 'PER', a.create_date, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, a.title,
                    a.title, a.contact, NULL, NULL, NULL, a.address1, a.address2, a.address3, NULL, a.city, a.stateorprov, a.zip, a.tax_country_id, NULL, a.tax_country_id, NULL, NULL, NULL, NULL, a.dept, a.name,
                    a.job_title, a.phone, NULL, NULL, NULL, a.fax, a.email, NULL, NULL, NULL, NULL, NULL, NULL, a.company, NULL, a.sis, NULL, 'SD2_ACCT_' || acct_id, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                    NULL, NULL, NULL, a.org_type, a.desk_type, a.sd_customer_type_name, a.sc_customer_type_name, NULL, NULL, NULL, NULL, NULL, a.update_date,
                    ELS_CUSTOMER_ID, NULL
                FROM dbowner.tbl_temp_sd_acct a;

		ELSIF p_query_no = 4 THEN
            -- insert Departments 'DEP'
			INSERT INTO dbowner.tbl_temp_parties
                SELECT DISTINCT decode(a.logical_deleted,'Y','D',NULL), 'SD', null, 'SD2_DEPT_' || dept_id, 'DEP', 'DEP', create_date, NULL, removed_date, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, dept_name, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                    athens_org_id, athens_site_prefix, user_count, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, update_date,
                    NULL, NULL
                FROM dbowner.tbl_temp_sd_dept a;

		ELSIF p_query_no = 5 THEN
            -- insert Specialisations 'SPE'
			INSERT INTO dbowner.tbl_temp_interests
                SELECT DISTINCT 'SD2_SPE_' || web_user_id || '_' || category_abbrev, 'SD', 'SD2_' || web_user_id, this_marker_db1, this_marker_db1, 'SPE', category_abbrev, NULL, NULL, NULL, NULL, NULL
                FROM dbowner.tbl_temp_sd_subj a, dbowner.tbl_sources b
                WHERE b.source = 'SD';

		ELSIF p_query_no = 6 THEN
            -- insert Favourite Lists 'FAV'
			INSERT INTO dbowner.tbl_temp_interests
                SELECT DISTINCT 'SD2_FAV_' || web_user_id || '_' || nvl(issn,isbn), 'SD', 'SD2_' || web_user_id, this_marker_db1, this_marker_db1, 'FAV', nvl(issn,isbn), NULL, publication_type, NULL, NULL, NULL
                FROM dbowner.tbl_temp_sd_favl a, dbowner.tbl_sources b
                WHERE b.source = 'SD';

		ELSIF p_query_no = 7 THEN
            -- insert Volume Issue Alerts 'VIAL' (EMA)
			INSERT INTO DBOWNER.TBL_TEMP_INTERESTS
                SELECT DISTINCT 'SD2_EMA_' || web_user_id || '_' || issn, 'SD', 'SD2_' || web_user_id, created_date, this_marker_db1, 'EMA', issn, NULL, publication_type, NULL, NULL, NULL
                FROM dbowner.tbl_temp_sd_vial a, dbowner.tbl_sources b
                WHERE b.source = 'SD';

		ELSIF p_query_no = 8 THEN
            -- insert Department Users 'DU'
			INSERT INTO dbowner.tbl_temp_interests
                SELECT DISTINCT 'SD2_DU_' || dept_id || '_' || web_user_id || '_' || user_node_assoc_id, 'SD', 'SD2_' || web_user_id, this_marker_db1, this_marker_db1, 'DU', 'SD2_DEPT_' || dept_id, rank, assoc_begin_date, logically_deleted, assoc_end_date, inactive
                FROM dbowner.tbl_temp_sd_dept_user a, dbowner.tbl_sources b
                WHERE b.source = 'SD';

		ELSIF p_query_no = 9 THEN
            -- insert Account Departments 'AD'
			INSERT INTO dbowner.tbl_temp_interests
                SELECT DISTINCT 'SD2_AD_' || acct_id ||'_' || dept_id || '_' || rank, 'SD', 'SD2_DEPT_' || dept_id, this_marker_db1, this_marker_db1, 'AD', 'SD2_ACCT_' || acct_id, rank, assoc_begin_date, logically_deleted, assoc_end_date, inactive
                FROM dbowner.tbl_temp_sd_acct_dept a, dbowner.tbl_sources b
                WHERE b.source = 'SD';

		ELSIF p_query_no = 10 THEN
            -- insert Product Accounts 'PA'
			INSERT INTO dbowner.tbl_temp_interests
                SELECT DISTINCT 'SD2_PA_' || product_id ||'_' || acct_id || '_' || rank, 'SD', 'SD2_ACCT_' || acct_id, this_marker_db1, this_marker_db1, 'PA', 'SD2_PRO_' || product_id, rank, assoc_begin_date, logically_deleted, assoc_end_date, inactive
                FROM dbowner.tbl_temp_sd_product_acct a, dbowner.tbl_sources b
                WHERE b.source = 'SD';

		ELSIF p_query_no = 11 THEN
            -- insert SuperAccount Accounts 'SA'
			INSERT INTO dbowner.tbl_temp_interests
                SELECT DISTINCT 'SD2_SA_' || super_acct_id ||'_' || acct_id || '_' || rank, 'SD', 'SD2_ACCT_' || acct_id, this_marker_db1, this_marker_db1, 'SA', 'SD2_SA_' || super_acct_id, rank, assoc_begin_date, logically_deleted, assoc_end_date, inactive
                FROM dbowner.tbl_temp_sd_superacct_acct a, dbowner.tbl_sources b
                WHERE b.source = 'SD';

		ELSIF p_query_no = 12 THEN
            -- insert SuperAccount Products 'SP'
			INSERT INTO dbowner.tbl_temp_interests
                SELECT DISTINCT 'SD2_SP_' || product_id  ||'_' || super_acct_id || '_' || rank, 'SD', 'SD2_SA_' || super_acct_id, this_marker_db1, this_marker_db1, 'SP', 'SD2_PRO_' || product_id, rank, assoc_begin_date, logically_deleted, assoc_end_date, inactive
                FROM dbowner.tbl_temp_sd_superacct_product a, dbowner.tbl_sources b
                WHERE b.source = 'SD';

		ELSIF p_query_no = 13 THEN
            -- insert SuperAccount SuperAccount 'SS'
			INSERT INTO dbowner.tbl_temp_interests
                SELECT DISTINCT 'SD2_SS_' || parent_super_acct_id ||'_' || child_super_acct_id || '_' || rank, 'SD', 'SD2_SA_' || child_super_acct_id, this_marker_db1, this_marker_db1, 'SS', 'SD2_SA_' || parent_super_acct_id, rank, assoc_begin_date, logically_deleted, assoc_end_date, inactive
                FROM dbowner.tbl_temp_sd_superac_superac a, dbowner.tbl_sources b
                WHERE b.source = 'SD';

		ELSIF p_query_no = 14 THEN
            -- insert SD Preferences 'PRE'
			INSERT INTO dbowner.tbl_temp_interests
                SELECT DISTINCT 'SD2_PRE_' || web_user_id || '_' || product_id, 'SD', 'SD2_' || web_user_id, this_marker_db1, this_marker_db1, 'PRE', 'SD2_PRO_' || product_id, history_enabled, history_expand, sort_preference, NULL, NULL
                FROM dbowner.tbl_temp_sd_user_pref a, dbowner.tbl_sources b
                WHERE b.source = 'SD';

		ELSIF p_query_no = 15 THEN
            -- insert SD User Platform Access 'PLA'
			INSERT INTO dbowner.tbl_temp_interests
                SELECT DISTINCT 'SD2_PLA_' || web_user_id || '_' || platform_code || '_' || site_id, 'SD', 'SD2_' || web_user_id, this_marker_db1, this_marker_db1, 'PLA', platform_code, site_name, last_access_browser, last_access_ip, last_access_timestamp, NULL
                FROM dbowner.tbl_temp_sd_platform_access a, dbowner.tbl_sources b
                WHERE b.source = 'SD';

		ELSIF P_QUERY_NO = 16 THEN
			-- insert SD User Search Alerts 'SEA'
            INSERT INTO DBOWNER.TBL_TEMP_INTERESTS
                SELECT DISTINCT 'SD2_SEA_' || CLIENT_ID || '_'  || SS_TASK_ID, 'SD', 'SD2_' || WEB_USER_ID, CREATE_DATE, UPDATE_DATE, 'SEA', SEARCH_TITLE, SEARCH_PARAMETERS, SEARCH_STRING, FREQUENCY, NULL, NULL
                FROM DBOWNER.TBL_TEMP_SD_USER_SEARCH_ALERTS;

		ELSIF P_QUERY_NO = 17 THEN
			-- insert SD User Topic Alerts 'TOP'
			INSERT INTO DBOWNER.TBL_TEMP_INTERESTS
                SELECT DISTINCT 'SD2_TOP_' || WEB_USER_ID || '_' || TOPIC_ALERT_ID, 'SD', 'SD2_' || WEB_USER_ID, CREATE_DATE, UPDATE_DATE, 'TOP', TOPIC_ALERT_ID, TOPIC_ALERT_NAME, NULL, NULL, NULL, NULL
                FROM DBOWNER.TBL_TEMP_SD_USER_TOPIC_ALERTS;

		ELSIF P_QUERY_NO = 18 THEN
			-- insert SD User Platform First Logged 'REG'
			INSERT INTO DBOWNER.TBL_TEMP_INTERESTS
                SELECT DISTINCT 'SD2_REG_' || A.WEB_USER_ID || '_' || A.PLATFORM_CODE || '_' || A.SITE_ID, 'SD', 'SD2_' || A.WEB_USER_ID, A.FIRST_REGISTERED_TIMESTAMP, B.THIS_MARKER_DB1, 'REG', A.PLATFORM_CODE, A.SITE_NAME, A.SITE_ID, NULL, A.FIRST_REGISTERED_TIMESTAMP, NULL
                FROM (SELECT DISTINCT * FROM DBOWNER.TBL_TEMP_SD_USR_PLAT_FST_RGSTR) A, DBOWNER.TBL_SOURCES B
                WHERE B.SOURCE = 'SD';

		ELSIF P_QUERY_NO = 19 THEN
			-- insert SD User Credentials 'CRE'
			INSERT INTO DBOWNER.TBL_TEMP_INTERESTS
                SELECT DISTINCT 'SD2_CRE_' || A.WEB_USER_ID || '_' || A.USER_ID, 'SD', 'SD2_' || A.WEB_USER_ID, B.THIS_MARKER_DB1, B.THIS_MARKER_DB1, 'CRE', A.USER_ID, A.USER_TYPE, NULL, NULL, NULL, NULL
                FROM (SELECT DISTINCT * FROM DBOWNER.TBL_TEMP_SD_USER_CREDENTIALS) A, DBOWNER.TBL_SOURCES B
                WHERE B.SOURCE = 'SD';

        ELSIF P_QUERY_NO = 20 THEN
            -- insert SD User IP Credentials 'IPC'
            INSERT INTO DBOWNER.TBL_TEMP_INTERESTS
                SELECT DISTINCT 'SD2_IPC_' || A.WEB_USER_ID, 'SD', 'SD2_' || A.WEB_USER_ID, B.THIS_MARKER_DB1, B.THIS_MARKER_DB1, 'IPC', A.USER_ID, A.USER_TYPE, NULL, NULL, NULL, NULL
                FROM (SELECT DISTINCT * FROM DBOWNER.TBL_TEMP_SD_USR_IP_CREDENTIALS) A, DBOWNER.TBL_SOURCES B
                WHERE B.SOURCE = 'SD';

		ELSIF p_query_no = 21 THEN
            -- insert Accounts
			INSERT INTO dbowner.tbl_temp_subscriptions
                SELECT DISTINCT decode(a.logical_deleted,'Y','D',NULL), 'SD2_' || acct_no, 'SD', b.this_marker_db1, b.this_marker_db1, 'SD2_BCT_' || a.acct_no, NULL, NULL, NULL, a.removed_date, NULL, a.logical_deleted, a.account_type_desc,
                    NULL, NULL, NULL, NULL, a.athens_enabled, NULL, NULL, NULL, NULL, NULL, a.price_plan_id, NULL, NULL, NULL, NULL, NULL, a.athens_enabled, a.name, a.sis, NULL, a.create_date, NULL, NULL, NULL, NULL, NULL, NULL, a.currency, NULL,
                    NULL, NULL, NULL, NULL, NULL, NULL, a.creditcard_allowed, a.tax_code, a.tax_exempted, NULL, NULL
                FROM dbowner.tbl_temp_sd_acct a, dbowner.tbl_sources b
                WHERE b.source = 'SD';

		ELSIF p_query_no = 22 THEN
            -- insert Products 'PRO'
			INSERT INTO dbowner.tbl_temp_items
                SELECT DISTINCT 'SD2_PRO_' || product_id, 'SD', sysdate, sysdate, product_name, 'PRO', NULL, NULL, NULL, NULL, NULL, NULL, platform_code, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
                FROM dbowner.tbl_temp_sd_product a;

		ELSIF p_query_no = 23 THEN
            -- insert Content 'CON'
			INSERT INTO dbowner.tbl_temp_items
				SELECT DISTINCT 'SD2_CON_'|| content_id, 'SD', sysdate, sysdate, content_title, 'CON', NULL, NULL, NULL, NULL, mrw_acronym, nvl(issn,isbn), code, NULL, NULL, NULL, NULL, NULL, content_type,
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
                FROM dbowner.tbl_temp_sd_content a;

		ELSIF P_QUERY_NO = 24 THEN
      -- insert Volume Issue Live Date 'VIDATE'
			INSERT INTO DBOWNER.TBL_TEMP_ITEMS
                SELECT DISTINCT 'SD2_VIDATE_' || A.ISSN  || '_' || A.VOLUME || '_' || A.ISSUE, 'SD', B.THIS_MARKER_DB1, B.THIS_MARKER_DB1, A.DISPALY_NAME, 'JOU', 'SD_JOU_' || A.ISSN, A.VOLUME, A.ISSUE, NULL, NULL, A.ISSN, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, FIRST_DATE_OF_AVAILABILITY, NULL, NULL, NULL
                FROM DBOWNER.VW_TEMP_SD_VI_LIVE_DATE A, DBOWNER.TBL_SOURCES B
                WHERE B.SOURCE = 'SD';

		ELSIF p_query_no = 25 THEN
			--DELETE dbowner.tbl_temp_sd_user;
      NULL;
		ELSIF p_query_no = 26 THEN
			--DELETE dbowner.tbl_temp_sd_acct;
      NULL;
		ELSIF p_query_no = 27 THEN
			--DELETE dbowner.tbl_temp_sd_dept;
      null;
		ELSIF p_query_no = 28 THEN
			--DELETE dbowner.tbl_temp_sd_subj;
      null;
		ELSIF p_query_no = 29 THEN
			--DELETE dbowner.tbl_temp_sd_favl;
      null;
		ELSIF p_query_no = 30 THEN
			--DELETE dbowner.tbl_temp_sd_vial;
      null;
    ELSIF p_query_no = 31 THEN
      --DELETE dbowner.tbl_temp_sd_dept_user;
      NULL;
    ELSIF p_query_no = 32 THEN
      --DELETE dbowner.tbl_temp_sd_acct_dept;
      null;
    ELSIF p_query_no = 33 THEN
      --DELETE dbowner.tbl_temp_sd_product_acct;
      NULL;
    ELSIF p_query_no = 34 THEN
      --DELETE dbowner.tbl_temp_sd_superacct_acct;
      null;
    ELSIF p_query_no = 35 THEN
      --DELETE dbowner.tbl_temp_sd_superacct_product;
      NULL;
    ELSIF p_query_no = 36 THEN
      --DELETE dbowner.tbl_temp_sd_superac_superac;
      null;
    ELSIF p_query_no = 37 THEN
      --DELETE dbowner.tbl_temp_sd_user_pref;
      null;
    ELSIF p_query_no = 38 THEN
      --DELETE dbowner.tbl_temp_sd_product;
      null;
    ELSIF p_query_no = 39 THEN
      --DELETE dbowner.tbl_temp_sd_content;
      null;
		ELSIF p_query_no = 40 THEN
      --DELETE dbowner.tbl_temp_sd_platform_access;
      null;
      
    -- User Deletes
    ELSIF p_query_no = 41 THEN 

            --
            -- Delete records from the MAIN table where the WEB_USER_ID exists in TEMP table
            --
            DELETE FROM TBL_MDT_SD_USER_DELETES 
            WHERE WEB_USER_ID IN (
                                 SELECT WEB_USER_ID 
                                 FROM TBL_TEMP_SD_USER_DELETES
                                 );
            
            --
            -- Insert records into MAIN table from TEMP table
            --
            INSERT INTO TBL_MDT_SD_USER_DELETES
            SELECT * 
            FROM TBL_TEMP_SD_USER_DELETES;
            COMMIT;
            
            --
            -- Update TBL_TEMP_PARTIES to update any records not marked for DELETED to DELETE
            --
            UPDATE TBL_TEMP_PARTIES
            SET    STATUS = 'D'
            WHERE  ORIG_SYSTEM = 'SD'
            AND    STATUS <> 'D'
            AND    ORIG_SYSTEM_REF IN (SELECT 'SD2_'||WEB_USER_ID FROM TBL_MDT_SD_USER_DELETES);
            COMMIT;             
            
            --
            -- Update TBL_PARTIES to update any records not marked for DELETED to DELETE
            --
            UPDATE TBL_PARTIES
            SET    RECORD_STATUS = 'D'
            WHERE  ORIG_SYSTEM = 'SD'
            AND    RECORD_STATUS <> 'D'
            AND    ORIG_PARTY_REF IN (SELECT 'SD2_'||WEB_USER_ID FROM TBL_MDT_SD_USER_DELETES);
            COMMIT;      
      
		END IF;

 ELSIF p_source = 'CRS' THEN

    IF p_query_no = 1 THEN

          --
          -- CRS Parties into TBL_TEMP_PARTIES
          --
          INSERT INTO tbl_temp_parties (orig_system,orig_system_ref,party_type,dedupe_type,usr_status,usr_login,usr_ref,usr_url_ref,usc_contact_firstname
                                       ,usc_contact_lastname,usc_country,iso_country_code,orig_country,usc_dedupe_email,unique_inst_id
                                       ,usr_subscriber_code,User_Access_Type,history_enabled,prime_type_descr,second_type_descr
                                       ,usr_created_date,run_time)
          SELECT
                  'CRS' as orig_system ,
                  'CRS_EUS_' || USER_ID as orig_party_ref,
                  'EUS' as party_type,
                  'PER' as dedupe_type,
                  decode(INACTIVATED, 'Y', 'D', NULL) as user_status,
                  USER_CREDENTIAL as login,
                  NODE_TYPE_ID as user_referral,
                  LOGICALLY_DELETED as url_referral,
                  FIRST_NAME as firstname,
                  LAST_NAME as lastname,
                  c.COUNTRY_NAME as country,
                  c.iso_code as iso_country_code,
                  u.COUNTRY_NAME as orig_country,
                  EMAIL_ADDRESS as email,
                  'SD2_' || CS_USER_ID as org_orig_system_ref,
                  'CRS_EUS_' || CS_USER_ID as subscriber_code,
                  ADMINISTRATOR as access_type,
                  USER_ID as history_enabled,
                  GROUP_USER_FLAG as org_primary_type,
                  USER_PASSWORD_RESET_FLAG as org_secondary_type,
                  p.orig_create_date,
                  s.this_marker_db1 AS RUN_TIME
          FROM TBL_TMP_CRS_USERS u,
               TBL_ISO_COUNTRIES c,
               (SELECT * FROM TBL_PARTIES WHERE ORIG_SYSTEM = 'SD') p,
               TBL_SOURCES S
          WHERE u.country_name = c.country_name (+)
          AND   'SD2_' || u.CS_USER_ID = p.orig_party_ref (+)
          AND   s.source = 'CRS';

    ELSIF p_query_no = 2 THEN
          --
          -- CRS Departments
          --
          INSERT INTO tbl_temp_parties (orig_system,orig_system_ref,party_type,dedupe_type,usr_status,usr_ref,usr_url_ref,usc_dept,unique_inst_id
                                       ,usr_subscriber_code,history_enabled
                                       ,usr_created_date,run_time)
          SELECT
                  'CRS' as orig_system ,
                  'CRS_DEPT_' || DEPARTMENT_ID as orig_party_ref,
                  'DEP' as party_type,
                  'DEP' as dedupe_type,
                  decode(INACTIVATED, 'Y', 'D', NULL) as user_status,
                  NODE_TYPE_ID as user_referral,
                  LOGICALLY_DELETED as url_referral,
                  DEPARTMENT_NAME as user_department,
                  'SD2_DEPT_' || CS_DEPARTMENT_ID as org_orig_system_ref,
                  'CRS_DEPT_' || CS_DEPARTMENT_ID as subscriber_code,
                  department_id as history_enabled,
                  p.orig_create_date,
                  s.this_marker_db1 AS RUN_TIME
          FROM  TBL_TMP_CRS_DEPARTMENTS u,
                (SELECT * FROM TBL_PARTIES WHERE ORIG_SYSTEM = 'SD') p,
               TBL_SOURCES S
          WHERE 'SD2_DEPT_' || CS_DEPARTMENT_ID = p.orig_party_ref (+)
          AND   s.source = 'CRS';


    ELSIF p_query_no = 3 THEN
          --
          -- CRS Accounts
          --
          INSERT INTO tbl_temp_parties (orig_system,original_site,orig_system_ref,party_type,dedupe_type,usr_status,usr_ref,usr_url_ref
                                       ,usr_other_ref,usc_contact_firstname,usc_contact_lastname,usc_addr,usr_address2,usc_city,usc_state
                                       ,usc_zip,usc_country,iso_country_code,orig_country,rso_region,usc_dept,usc_institute_url,unique_inst_id
                                       ,Inst_Name,related_sis_id,usr_subscriber_code,User_Access_Type,history_enabled,org_type,prime_type_descr
                                       ,usr_created_date,run_time)
          SELECT
                  'CRS' as orig_system ,
                  'CRS_ACCT_' || ACCOUNT_NUMBER as orig_site,
                  'CRS_ACCT_' || ACCOUNT_ID as orig_party_ref,
                  'ACC' as party_type,
                  'ORG' as dedupe_type,
                  decode(INACTIVATED, 'Y', 'D', NULL) as user_status,
                  NODE_TYPE_ID as user_referral,
                  LOGICALLY_DELETED as url_referral,
                  ACCT_MGR_FIRST_NAME ||' ' || ACCT_MGR_LAST_NAME as other_referral,
                  CONTACT_FIRST_NAME as firstname,
                  CONTACT_LAST_NAME as lastname,
                  CONTACT_ADDRESS1 as address1,
                  CONTACT_ADDRESS2 as address2,
                  CONTACT_CITY as city,
                  CONTACT_STATE_PROV as state,
                  CONTACT_ZIP as post_code,
                  c.country_name as country,
                  c.iso_code as iso_country_code,
                  CONTACT_COUNTRY_NAME as orig_country,
                  RSO_CODE  as rso_region, -- We need to have a lookup in place for this
                  ACCOUNT_NAME as user_department,
                  ACCOUNT_NUMBER as org_url,
                  'SD2_BCT_' || ACCOUNT_NUMBER as org_orig_system_ref,
                  ACCOUNT_NAME as org_name,
                  SIS as related_sis_id,
                  'CRS_ACCT_' || CS_ACCOUNT_ID as subscriber_code,
                  USER_LEVEL_STAT_FLAG as access_type,
                  ACCOUNT_ID as history_enabled,
                  a.ORG_TYPE as org_type,
                  b.account_typedesc as org_primary_type,
                  p.orig_create_date,
                  s.this_marker_db1 AS RUN_TIME
          FROM TBL_TMP_CRS_ACCOUNTS A,
               TBL_TMP_CRS_ACCOUNT_TYPES B,
               TBL_ISO_COUNTRIES c,
                (SELECT * FROM TBL_PARTIES WHERE ORIG_SYSTEM = 'SD') p,
               TBL_SOURCES S
          WHERE a.CONTACT_COUNTRY_NAME = c.country_name (+)
          AND   a.account_type_id = b.account_type_id (+)
          AND   'SD2_BCT_' || ACCOUNT_NUMBER = p.orig_party_ref (+)
          AND   s.source = 'CRS';


    ELSIF p_query_no = 4 THEN
          --
          -- CRS Super Accounts
          --
          INSERT INTO tbl_temp_parties (orig_system,original_site,orig_system_ref,party_type,dedupe_type,usr_status,usr_ref,usr_url_ref
                                       ,usr_other_ref,usc_contact_firstname,usc_contact_lastname,usc_addr,usr_address2,usc_city
                                       ,usc_state,usc_zip,usc_country,iso_country_code,orig_country,rso_region,usc_dept,usc_institute_url
                                       ,unique_inst_id,Inst_Name,related_sis_id,usr_subscriber_code,history_enabled,Org_Type,prime_type_descr
                                       ,usr_created_date,run_time)
          SELECT
                'CRS' as orig_system ,
                'CRS_SUP_' || SUPER_ACCOUNT_NUMBER as orig_site,
                'CRS_SUP_' || SUPER_ACCOUNT_ID as orig_party_ref,
                'ACC' as party_type,
                'ORG' as dedupe_type,
                decode(INACTIVATED, 'Y', 'D', NULL) as user_status,
                NODE_TYPE_ID as user_referral,
                LOGICALLY_DELETED as url_referral,
                ACCT_MGR_FIRST_NAME ||' ' || ACCT_MGR_LAST_NAME as other_referral,
                CONTACT_FIRST_NAME as firstname,
                CONTACT_LAST_NAME as lastname,
                CONTACT_ADDRESS1 as address1,
                CONTACT_ADDRESS2 as address2,
                CONTACT_CITY as city,
                CONTACT_STATE_PROV as state,
                CONTACT_ZIP as post_code,
                c.country_name as country,
                c.iso_code as iso_country_code,
                CONTACT_COUNTRY_NAME as orig_country,
                RSO_CODE  as rso_region, -- We need to have a lookup in place for this
                SUPER_ACCOUNT_NAME as user_department,
                SUPER_ACCOUNT_NUMBER as org_url,
                'SD2_' || CS_SUPER_ACCOUNT_ID as org_orig_system_ref,
                SUPER_ACCOUNT_NAME as org_name,
                SIS as related_sis_id,
                'CRS_SUP_' || CS_SUPER_ACCOUNT_ID as subscriber_code,
                SUPER_ACCOUNT_ID as history_enabled,
                a.ORG_TYPE as org_type,
                b.account_typedesc as org_primary_type,
                  p.orig_create_date,
                  s.this_marker_db1 AS RUN_TIME
          FROM  TBL_TMP_CRS_SUPER_ACCOUNTS A,
                TBL_TMP_CRS_ACCOUNT_TYPES B,
                TBL_ISO_COUNTRIES c,
                (SELECT * FROM TBL_PARTIES WHERE ORIG_SYSTEM = 'SD') p,
               TBL_SOURCES S
          WHERE A.CONTACT_COUNTRY_NAME = C.COUNTRY_NAME (+)
          AND   A.ACCOUNT_TYPE_ID = B.ACCOUNT_TYPE_ID (+)
          AND   'SD2_' || CS_SUPER_ACCOUNT_ID = p.orig_party_ref (+)
          AND   s.source = 'CRS';


    ELSIF p_query_no = 5 THEN
          --
          -- CRS Super Account to Account Link (Interests)
          --
          INSERT INTO TBL_TEMP_INTERESTS (ORIG_SYSTEM_REF,ORIG_SYSTEM,PARTY_REF,CREATE_DATE,INTEREST_TYPE,INTEREST_VALUE,INTEREST_VALUE_DETAILS
                                         ,INTEREST_SUB_SECTION,ALERT_END_DATE)
          SELECT
                'CRS_SA_' || PARENT_SUPER_ACCOUNT_ID || '_' || CHILD_ACCOUNT_ID as orig_interest_ref,
                'CRS' as orig_system ,
                'CRS_ACCT_' || CHILD_ACCOUNT_ID as party_ref,
                ASSOC_BEGIN_DATE as orig_create_date,
                'SA' as interest_type,
                'CRS_SUP_' || PARENT_SUPER_ACCOUNT_ID as interest_value,
                RANK as interest_value_details,
                LOGICALLY_DELETED as interest_sub_section,
                ASSOC_END_DATE as alert_end_date
          FROM  TBL_TMP_CRS_ACC_SUPER_ACC;


    ELSIF p_query_no = 6 THEN
          --
          -- CRS Account to Product Link (Interests)
          --
          INSERT INTO TBL_TEMP_INTERESTS (ORIG_SYSTEM_REF,ORIG_SYSTEM,PARTY_REF,CREATE_DATE,INTEREST_TYPE,INTEREST_VALUE,INTEREST_VALUE_DETAILS
                                         ,INTEREST_SUB_SECTION,ALERT_END_DATE)
          SELECT
                'CRS_PA_' || PARENT_PRODUCT_ID || '_' || CHILD_ACCOUNT_ID as orig_interest_ref,
                'CRS' as orig_system ,
                'CRS_ACCT_' || CHILD_ACCOUNT_ID as party_ref,
                ASSOC_BEGIN_DATE as orig_create_date,
                'PA' as interest_type,
                'CRS_PRO_' || PARENT_PRODUCT_ID as interest_value,
                RANK as interest_value_details,
                LOGICALLY_DELETED as interest_sub_section,
                ASSOC_END_DATE as alert_end_date
          FROM  TBL_TMP_CRS_ACC_PRODUCT;


    ELSIF p_query_no = 7 THEN
          --
          -- CRS Account to Department Link (Interests)
          --
          INSERT INTO TBL_TEMP_INTERESTS (ORIG_SYSTEM_REF,ORIG_SYSTEM,PARTY_REF,CREATE_DATE,INTEREST_TYPE,INTEREST_VALUE,INTEREST_VALUE_DETAILS
                                         ,INTEREST_SUB_SECTION,ALERT_END_DATE)
          SELECT
                'CRS_AD_' || PARENT_ACCOUNT_ID || '_' || CHILD_DEPARTMENT_ID as orig_interest_ref,
                'CRS' as orig_system ,
                'CRS_DEPT_' || CHILD_DEPARTMENT_ID as party_ref,
                ASSOC_BEGIN_DATE as orig_create_date,
                'AD' as interest_type,
                'CRS_ACCT_' || PARENT_ACCOUNT_ID as interest_value,
                RANK as interest_value_details,
                LOGICALLY_DELETED as interest_sub_section,
                ASSOC_END_DATE as alert_end_date
          FROM  TBL_TMP_CRS_DEPT_ACC;


    ELSIF p_query_no = 8 THEN
          --
          -- CRS Super Account To Super Account (Interests)
          --
          INSERT INTO TBL_TEMP_INTERESTS (ORIG_SYSTEM_REF,ORIG_SYSTEM,PARTY_REF,CREATE_DATE,INTEREST_TYPE,INTEREST_VALUE,INTEREST_VALUE_DETAILS
                                         ,INTEREST_SUB_SECTION,ALERT_END_DATE)
          SELECT
                'CRS_SS_' || PARENT_SUPER_ACCOUNT_ID || '_' || CHILD_SUPER_ACCOUNT_ID as orig_interest_ref,
                'CRS' as orig_system ,
                'CRS_SUP_' || CHILD_SUPER_ACCOUNT_ID as party_ref,
                ASSOC_BEGIN_DATE as orig_create_date,
                'SS' as interest_type,
                'CRS_SUP_' || PARENT_SUPER_ACCOUNT_ID as interest_value,
                RANK as interest_value_details,
                LOGICALLY_DELETED as interest_sub_section,
                ASSOC_END_DATE as alert_end_date
          FROM  TBL_TMP_CRS_SUP_ACC_SUP_ACC;


    ELSIF p_query_no = 9 THEN
          --
          -- CRS Super Account To Product (Interests)
          --
          INSERT INTO TBL_TEMP_INTERESTS (ORIG_SYSTEM_REF,ORIG_SYSTEM,PARTY_REF,CREATE_DATE,INTEREST_TYPE,INTEREST_VALUE,INTEREST_VALUE_DETAILS
                                         ,INTEREST_SUB_SECTION,ALERT_END_DATE)
          SELECT
                'CRS_SP_' || PARENT_PRODUCT_ID || '_' || CHILD_SUPER_ACCOUNT_ID as orig_interest_ref,
                'CRS' as orig_system ,
                'CRS_SUP_' || CHILD_SUPER_ACCOUNT_ID as party_ref,
                ASSOC_BEGIN_DATE as orig_create_date,
                'SP' as interest_type,
                'CRS_PRO_' || PARENT_PRODUCT_ID as interest_value,
                RANK as interest_value_details,
                LOGICALLY_DELETED as interest_sub_section,
                ASSOC_END_DATE as alert_end_date
          FROM  TBL_TMP_CRS_SUPER_ACC_PRODUCT;


    ELSIF p_query_no = 10 THEN
          --
          -- CRS Department to User (Interests)
          --
          INSERT INTO TBL_TEMP_INTERESTS (ORIG_SYSTEM_REF,ORIG_SYSTEM,PARTY_REF,CREATE_DATE,INTEREST_TYPE,INTEREST_VALUE,INTEREST_VALUE_DETAILS
                                         ,INTEREST_SECTION,INTEREST_SUB_SECTION,ALERT_END_DATE)
          SELECT
                'CRS_DU_' || PARENT_DEPARTMENT_ID ||'_' || CHILD_USER_ID ||'_' || USER_NODE_ASSOC_ID as orig_interest_ref,
                'CRS' as orig_system ,
                'CRS_EUS_' || CHILD_USER_ID as party_ref,
                ASSOC_BEGIN_DATE as orig_create_date,
                'DU' as interest_type,
                'CRS_DEPT_' || PARENT_DEPARTMENT_ID as interest_value,
                RANK as interest_value_details,
                USER_ACCESS_TYPE as interest_section,
                LOGICALLY_DELETED as interest_sub_section,
                ASSOC_END_DATE as alert_end_date
          FROM  TBL_TMP_CRS_USER_DEPT;


    ELSIF p_query_no = 11 THEN
          --
          -- CRS Products
          --
          INSERT INTO TBL_TEMP_ITEMS (ORIG_SYSTEM_REF,ORIG_SYSTEM,NAME,ITEM_TYPE,PARENT_REF,VOLUME,DESCRIPTION,IDENTIFIER,CODE,SITE,TYPE)
          SELECT
                'CRS_PRO_' || PRODUCT_ID as orig_item_ref,
                'CRS' as orig_system ,
                PRODUCT_NAME as name,
                'PRO' as item_type,
                'CRS_LIST_' || ALL_LIST_ID as parent_ref,
                'SD2_PRO_' || CS_PRODUCT_ID as volume,
                CS_SITE_ID as description,
                CS_PRODUCT_ID as identifier,
                PLATFORM_CODE as code,
                SITE_FAMILY_NAME as site,
                SITE_URL_QUALIFIER as TYPE
          FROM  TBL_TMP_CRS_PRODUCTS;
    END IF;

  ELSIF p_source = 'PPV' THEN

        IF p_query_no = 1 THEN

            INSERT INTO dbowner.tbl_temp_parties
              SELECT DISTINCT NULL, 'PPV', NULL, 'SD_' || a.web_user_id || '_' || UPPER(substr(a.firstname,1,1)) || a.lastname, 'PPV', NULL, SYSDATE, NULL, NULL, a.user_id, NULL, NULL, NULL, NULL, NULL, NULL, NULL, a.title, a.title, a.firstname,
                a.lastname, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, a.country, NULL, NULL, NULL, NULL, NULL, NULL, NULL, a.name, NULL, NULL, NULL, NULL, NULL, NULL, a.user_email,
                NULL, NULL, NULL, NULL, NULL, 'SD2_' || a.web_user_id, 'SD2_ACCT_' || a.acct_no, NULL, NULL, NULL, NULL, NULL, NULL, null, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, a.user_role, NULL,
                NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                    NULL, NULL
              FROM dbowner.tbl_temp_sd_ppv a;

        ELSIF p_query_no = 2 THEN

            INSERT INTO dbowner.tbl_temp_subscriptions
              SELECT DISTINCT NULL, 'SD_PPV_' || a.confirmation_code || '_' || a.pii, 'PPV', a.purchase_date, SYSDATE, 'SD_' || a.web_user_id || '_' || UPPER(substr(a.firstname,1,1)) || a.lastname, 'SD_ART_' || a.pii, 'SD_JOU_' || a.issn, NULL, NULL, a.purchase_value,
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, a.confirmation_code,
                NULL, NULL, NULL, NULL, NULL, NULL, a.price_category, NULL, NULL, a.payment_method, NULL, NULL, NULL, NULL
              FROM dbowner.tbl_temp_sd_ppv a;

        ELSIF p_query_no = 3 THEN

            INSERT INTO dbowner.tbl_temp_items
              SELECT DISTINCT 'SD_JOU_' || a.issn, 'PPV', SYSDATE, SYSDATE, REPLACE(journal_name,'amp;',''), 'JOU', NULL, NULL, NULL, NULL, NULL, a.issn, a.journal, NULL, NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
              FROM dbowner.tbl_temp_sd_ppv a;

        ELSIF p_query_no = 4 THEN

            INSERT INTO dbowner.tbl_temp_items
              SELECT DISTINCT 'SD_ART_' || a.pii, 'PPV', SYSDATE, SYSDATE, NULL, 'ART', 'SD_JOU_' || a.issn, a.volume, a.issue, substr(publication_date,length(publication_date)-3,4), NULL, a.pii, NULL, NULL, NULL, NULL, a.publication_date, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,NULL
              FROM dbowner.tbl_temp_sd_ppv a;

        ELSIF p_query_no = 5 THEN

            DELETE dbowner.tbl_temp_sd_ppv;

        END IF;

	ELSIF p_source = 'SIS' THEN
NULL;
/*
		IF p_query_no = 1 THEN
		   	--NULL;
			INSERT INTO dbowner.tbl_temp_parties
				SELECT DISTINCT decode(status, 'D', 'D', NULL), 'SIS', a.orig_sys_id, 'SIS_' || g.orig_sys_id || '_' || g.orig_sys_cust_num, 'OPA', 'ORG', a.creation_date, NULL, a.amnd_date,
					NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, a.address1, NULL, NULL, address_type_descr, a.address4, a.address5,
					a.address6, a.address7, NULL, state_name, a.post_code, country_name, NULL, country_name, NULL, a.country_code, NULL, NULL, a.address2, a.address3, NULL, NULL, NULL, NULL, NULL,
					NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, d.node_descr, NULL, a.related_sis_id, a.top_level_sis_id, e.book_disc_code_descr,  NULL, NULL, NULL, NULL,
					NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NUll, NULL, NULL, NULL, NULL, NULL, a.amnd_date,
                    NULL, NULL
				FROM operational_addresses@POSIS_AWS a,
						 countries@POSIS_AWS b,
						 states@POSIS_AWS c,
						 organisation_units@POSIS_AWS d,
						 book_disc_codes@POSIS_AWS e,
						 address_type@POSIS_AWS f,
						 capri_operational_addresses@POSIS_AWS g
				WHERE a.country_code = b.country_code (+)
				AND a.state_code = c.state_code (+)
				AND a.country_code = c.country_code (+)
				AND a.related_sis_id = d.org_sis_id (+)
				AND a.book_disc_code = e.book_disc_code (+)
				AND a.address_type = f.address_type (+)
				AND g.orig_sys_id IN (1,2,5,6,7,8,9,10,12,15,18,24,25,30)
				AND g.orig_sys_id = a.orig_sys_id (+)
				AND g.orig_sys_cust_num = a.orig_sys_cust_num (+)
				AND g.run_time < (SELECT this_marker_db1 FROM dbowner.tbl_sources WHERE source = 'SIS');

		ELSIF p_query_no = 2 THEN
			--NULL;
			INSERT INTO dbowner.tbl_temp_parties
				SELECT DISTINCT decode(status, 'D', 'D', NULL), 'SIS', NULL, 'SIS_ORG_' || g.org_sis_id, 'ORG', 'ORG', a.node_created_date, NULL, a.node_modify_date,
					NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,  a.contact_name, NULL, NULL, NULL, a.street, a.address_2,
					NULL, NULL, a.town, state_name, post_code, country_name, NULL, country_name, NULL, a.country_code, NULL, NULL,
					a.node_descr, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, a.node_descr, NULL, a.parent_id, a.top_level_sis_id,
					decode(h.sisorgid, NULL,'N','Y'), NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, funct_descr, NULL, prime_type_descr, second_type_descr, NULL,
					NULL, NULL, NULL, NULL, a.node_modify_date,
                    NULL, NULL
				FROM organisation_units@POSIS_AWS a,
						 countries@POSIS_AWS b,
						 states@POSIS_AWS c,
						 org_unit_functions@POSIS_AWS d,
						 org_secondary_types@POSIS_AWS e,
						 org_primary_types@POSIS_AWS f,
						 capri_organisation_units@POSIS_AWS g,
						 sd_account@POSIS_AWS h
				WHERE a.country_code = b.country_code (+)
				AND a.state_code = c.state_code (+)
				AND a.country_code = c.country_code (+)
				AND a.unit_funct_code = d.unit_funct_code (+)
				AND a.prime_type_code = e.prime_type_code (+)
				AND a.second_type_code = e.second_type_code (+)
				AND e.prime_type_code = f.prime_type_code (+)
				AND g.org_sis_id = a.org_sis_id (+)
				AND g.org_sis_id = h.sisorgid (+)
				AND g.run_time < (SELECT this_marker_db1 FROM dbowner.tbl_sources WHERE source = 'SIS');
		ELSIF p_query_no = 3 THEN
			--NULL;
			INSERT INTO dbowner.tbl_temp_parties
				SELECT DISTINCT NULL, 'SIS', NULL, 'SIS_CAR_' || UNIT_ID, 'CAR', 'ORG', LAST_UPDATED_DATE, NULL, NULL, CONTROL, NULL, NULL, NULL,
            UGPROFILE2005, ENRPROFILE2005, SIZESET2005, NULL, NULL, NULL, NAME, NULL, NULL, NULL, NULL, NULL, NULL, NULL, CITY, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NAME, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, IPUG2005,
            'SIS_ORG_' || ORG_SIS_ID, NAME, NULL, ORG_SIS_ID, NULL, IPGRAD2005, NULL, NULL, NULL, NULL, NULL, ICLEVEL, NULL, BASIC2005, NULL,
            HSI, MEDICAL, NULL, NULL, LOCALE, ACCRED, NULL, NULL, NULL, NULL, NULL, LAST_UPDATED_DATE,
                    NULL, NULL
        FROM SIS.SIS_CARNEGIE@POSIS_AWS;
		ELSIF p_query_no = 4 THEN
			--NULL;
			UPDATE dbowner.tbl_temp_parties a SET region_name =
	   			(SELECT region_name
					FROM regions@POSIS_AWS b, country_in_regions@POSIS_AWS c
			   		WHERE b.region_code = c.region_code
			   		AND b.region_type_id = 'GO'
			   		AND c.region_type_id = 'GO'
			   		AND a.rso_region = trim(to_char(c.country_code)))
	   		WHERE orig_system = 'SIS' AND doctored IS NULL;
		ELSIF p_query_no = 5 THEN
			--NULL;
	   		UPDATE dbowner.tbl_temp_parties a SET rso_region =
	   			(SELECT region_name
					FROM regions@POSIS_AWS b, country_in_regions@POSIS_AWS c
					WHERE b.region_code = c.region_code
			   		AND b.region_type_id = 'RS'
			   		AND c.region_type_id = 'RS'
			   		AND a.rso_region = trim(to_char(c.country_code)))
	   		WHERE orig_system = 'SIS' AND doctored IS NULL;
		ELSIF p_query_no = 6 THEN
			INSERT INTO dbowner.tbl_temp_interests
				SELECT DISTINCT 'SIS_IDT_' || a.org_sis_id || '_' || a.industry_code, 'SIS', 'SIS_ORG_' || a.org_sis_id, run_time, run_time, 'IDT', industry_name, NULL, NULL, NULL, NULL, decode(status, 'D', 'D', NULL)
				FROM capri_org_unit_in_industry@POSIS_AWS a,
					   industries@POSIS_AWS b
				WHERE a.industry_code = b.industry_code (+)
				AND a.run_time < (SELECT this_marker_db1 FROM dbowner.tbl_sources WHERE source = 'SIS');
		ELSIF p_query_no = 7 THEN
			INSERT INTO dbowner.tbl_temp_items
				SELECT DISTINCT 'SIS_JOU_' || j.orig_sys_jnl_num, 'SIS', run_time, run_time, title, 'JOU', NULL, last_vol, last_issue, year,
					upper(abrv_title), issn, NULL, NULL, NULL, NULL, pblshr_name, jnl_type_descr, sbscrptn_type_desc, NULL, medium_type_descr, a.pmc_code, pmc_descr, a.pmg_code, pmg_descr,
					jnl_class_descr, substr(imprint_name,1,100), NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, last_change_date, NULL, decode(j.status, 'D', 'D', NULL)
				FROM journals@POSIS_AWS a,
						 publishers@POSIS_AWS b,
						 journal_types@POSIS_AWS c,
						 subscription_types@POSIS_AWS d,
						 medium@POSIS_AWS e,
						 product_market_combinations@POSIS_AWS f,
						 product_market_groups@POSIS_AWS g,
						 journal_classes@POSIS_AWS h,
						 imprints@POSIS_AWS i,
						 capri_journals@POSIS_AWS j
				WHERE a.pblshr_code = b.pblshr_code (+)
				AND a.jnl_type_code = c.jnl_type_code (+)
				AND a.sbscrptn_type = d.sbscrptn_type (+)
				AND a.medium = e.medium_type_id (+)
				AND a.pmc_code = f.pmc_code (+)
				AND a.pmg_code = f.pmg_code (+)
				AND a.pmg_code = g.pmg_code (+)
				AND a.jnl_class = h.jnl_class (+)
				AND a.imprint_id = i.imprint_id (+)
				AND j.orig_sys_jnl_num = a.orig_sys_jnl_num (+)
				AND j.run_time < (SELECT this_marker_db1 FROM dbowner.tbl_sources WHERE source = 'SIS');
		ELSIF p_query_no = 8 THEN
			INSERT INTO dbowner.tbl_temp_items
				SELECT DISTINCT 'SIS_BOK_' || h.isbn13, 'SIS', run_time, run_time, title, 'BOK', NULL, NULL, NULL, NULL, NULL, 'SIS_BOK_' || h.isbn10,
					NULL, author_edtr, NULL, NULL, pblshr_name, NULL, NULL, binding_type_descr, NULL, a.pmc_code, pmc_descr, a.pmg_code, pmg_descr,
					book_class_name, substr(imprint_name,1,100), NULL, NULL, NULL, NULL, NULL, NULL, NULL, launch_date, pblctn_date, SYSDATE, decode(h.status, 'D', 'D', NULL)
				FROM books@POSIS_AWS a,
				     publishers@POSIS_AWS b,
				     binding_types@POSIS_AWS c,
				     product_market_combinations@POSIS_AWS d,
				     product_market_groups@POSIS_AWS e,
				     book_classes@POSIS_AWS f,
				     imprints@POSIS_AWS g,
				     capri_books@POSIS_AWS h
				WHERE a.pblshr_code = b.pblshr_code (+)
				AND a.binding_type = c.binding_type (+)
				AND a.pmc_code = d.pmc_code (+)
				AND a.pmg_code = d.pmg_code (+)
				AND a.pmg_code = e.pmg_code (+)
				AND a.book_class = f.book_class (+)
				AND a.imprint_id = g.imprint_id (+)
				AND h.orig_sys_id = a.orig_sys_id (+)
				AND h.isbn13 = a.isbn13 (+)
				AND h.run_time < (SELECT this_marker_db1 FROM dbowner.tbl_sources WHERE source = 'SIS');
		ELSIF p_query_no = 9 THEN
			INSERT INTO dbowner.tbl_temp_ips
				SELECT DISTINCT 'SIS_' || a.ip_block_id || '_' || ip_id, 'SIS', NULL, 'SIS_ORG_' || a.org_sis_id, a.created_date, a.last_updated_date,
					ip_comp1 || '.' || ip_comp2 || '.' || ip_comp3_begin || '.' || ip_comp4_begin,
					ip_comp1 || '.' || ip_comp2 || '.' || ip_comp3_end || '.' || ip_comp4_end,
					NULL, NULL, ip_comp1, ip_comp2, ip_comp3_begin, ip_comp3_end, ip_comp4_begin, ip_comp4_end, NULL, NULL
				FROM ip_info_block@POSIS_AWS a,
				ip_address_ranges@POSIS_AWS b
	        	WHERE a.ip_block_id = b.ip_block_id
				AND (a.ip_block_id IN (SELECT ip_block_id FROM capri_ip_info_block@POSIS_AWS WHERE status <> 'D' AND run_time < (SELECT this_marker_db1 FROM dbowner.tbl_sources WHERE source = 'SIS'))
					OR a.ip_block_id IN (SELECT ip_block_id FROM capri_ip_address_ranges@POSIS_AWS WHERE status <> 'D' AND run_time < (SELECT this_marker_db1 FROM dbowner.tbl_sources WHERE source = 'SIS')));
		ELSIF p_query_no = 10 THEN
			INSERT INTO dbowner.tbl_temp_ips
				SELECT DISTINCT 'SIS_' || ip_block_id || '_' || ip_id, 'SIS', NULL, NULL, run_time, run_time,
					NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'D'
				FROM capri_ip_address_ranges@POSIS_AWS
				WHERE status = 'D'
				AND run_time < (SELECT this_marker_db1 FROM dbowner.tbl_sources WHERE source = 'SIS');
		ELSIF p_query_no = 11 THEN
			INSERT INTO dbowner.tbl_temp_subscriptions
				SELECT DISTINCT decode(status, 'D', 'D', NULL), 'SIS_JOU_' || a.orig_sys_id || '_' || a.orig_sys_sale_ref || '_' || a.orig_sys_jnl_num  || '_' || a.orig_sys_cust_num || '_' || a.start_vol || '_' || a.start_issue || '_' || a.end_vol || '_' || a.end_issue,
					'SIS', run_time, run_time, 'SIS_' || a.orig_sys_id || '_' || a.orig_sys_cust_num, 'SIS_JOU_' || a.orig_sys_jnl_num, NULL,
					NULL, NULL, amnt_inv_orig, c.cnclltn_descr, e.sale_type_descr, NULL, NULL, iss_sale, NULL, NULL, NULL, d.status_descr, NULL, a.num_issues, NULL, a.paymnt_period,
					NULL, NULL, NULL, f.gratis_reason, NULL, NULL, NULL, NULL, substr(a.orig_sys_sale_ref, 1, length(a.orig_sys_sale_ref) - 2), NULL, NULL, a.end_issue, NULL,
					a.start_issue, NULL, copy_price_orig, inv_currency, orig_agent, a.start_vol, a.end_vol, a.year, a.source_code, a.num_copies, a.amnd_date,
					b.sub_sale_type_descr, NULL, NULL, NULL, NULL
				FROM journal_sales@POSIS_AWS a,
					   journal_sub_sale_types@POSIS_AWS b,
					   cancellation_reasons@POSIS_AWS c,
					   subscription_status@POSIS_AWS d,
					   journal_sale_types@POSIS_AWS e,
					   gratis_reasons@POSIS_AWS f,
					   capri_journal_sales@POSIS_AWS g
				WHERE a.sub_sale_type = b.sub_sale_type (+)
				AND a.cnclltn_code = c.cnclltn_code (+)
				AND a.sbscrptn_status = d.sbscrptn_status (+)
				AND a.sale_type = e.sale_type (+)
				AND a.gratis_code = f.gratis_code (+)
				AND a.orig_sys_id in (1,2,5,6,7,8,9,10,12,15,18,24,25,30)
				AND NOT(a.orig_sys_id = 5 AND a.orig_sys_cust_num IN (02215, 02216, 02218, 02228, 02229, 04228))
				AND g.orig_sys_jnl_num = a.orig_sys_jnl_num (+)
				AND g.orig_sys_cust_num = a.orig_sys_cust_num (+)
				AND g.orig_sys_id = a.orig_sys_id (+)
				AND g.orig_sys_sale_ref = a.orig_sys_sale_ref (+)
				AND g.start_vol = a.start_vol (+)
				AND g.start_issue = a.start_issue (+)
				AND g.end_vol = a.end_vol (+)
				AND g.end_issue = a.end_issue (+)
				AND g.run_time < (SELECT this_marker_db1 FROM dbowner.tbl_sources WHERE source = 'SIS');
			--NULL;
		ELSIF p_query_no = 12 THEN
			INSERT INTO dbowner.tbl_temp_subscriptions
				SELECT DISTINCT decode(status, 'D', 'D', NULL), 'SIS_BOK_' || a.orig_sys_id || '_' || a.orig_sys_sale_ref || '_' || a.sales_ref_seq_num  || '_' || a.isbn13 || '_' || a.orig_sys_cust_num,
					'SIS', run_time, run_time, 'SIS_' || a.orig_sys_id || '_' || a.orig_sys_cust_num, 'SIS_BOK_' || a.isbn13, 'SIS_BOK_' || a.isbn10, NULL, NULL, amnt_inv_orig, NULL, b.sale_type_descr,
					NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, a.paymnt_period, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, substr(a.orig_sys_sale_ref, 1, length(a.orig_sys_sale_ref) - 2),
					NULL, NULL, NULL, NULL, NULL, NULL, copy_price_orig, NULL, orig_agent, NULL, NULL, NULL, source_code, a.num_copies, NULL, NULL, NULL, NULL, NULL, NULL
				FROM book_sales@POSIS_AWS a,
						 book_sale_types@POSIS_AWS b,
						 capri_book_sales@POSIS_AWS c
				WHERE a.sale_type = b.sale_type (+)
				AND c.orig_sys_id in (1,2,5,6,7,8,9,10,12,15,18,24,25,30)
				AND c.orig_sys_id = a.orig_sys_id (+)
				AND c.orig_sys_sale_ref = a.orig_sys_sale_ref (+)
				AND c.sales_ref_seq_num = a.sales_ref_seq_num (+)
				AND c.isbn13 = a.isbn13 (+)
				AND c.orig_sys_cust_num = a.orig_sys_cust_num (+)
				AND c.run_time < (SELECT this_marker_db1 FROM dbowner.tbl_sources WHERE source = 'SIS');
		ELSIF p_query_no = 13 THEN
		    INSERT INTO dbowner.tbl_temp_item_subjects
				SELECT DISTINCT decode(status, 'D', 'D', NULL), 'SIS', 'SIS_SBJ_' || a.orig_sys_id || '_' || a.isbn13 || '_' || a.subject_cat_code, 'SIS_BOK_' || a.isbn13,
					run_time, run_time, a.subject_cat_code, subject_cat_descr, c.subject_grp_code, subject_grp_descr, d.subject_area_code, subject_area_descr, e.super_area_code, super_area_descr, scaling_fctr
				FROM book_in_subject_cats@POSIS_AWS a,
						 subject_categories@POSIS_AWS b,
						 subject_groups@POSIS_AWS c,
						 subject_areas@POSIS_AWS d,
						 subject_super_areas@POSIS_AWS e,
						 capri_book_in_subject_cats@POSIS_AWS f
				WHERE a.subject_cat_code = b.subject_cat_code (+)
				AND b.subject_grp_code = c.subject_grp_code (+)
				AND c.subject_area_code = d.subject_area_code (+)
				AND d.super_area_code = e.super_area_code (+)
				AND f.orig_sys_id = a.orig_sys_id (+)
				AND f.isbn13 = a.isbn13 (+)
				AND f.subject_cat_code = a.subject_cat_code (+)
				AND f.run_time < (SELECT this_marker_db1 FROM dbowner.tbl_sources WHERE source = 'SIS');
		ELSIF p_query_no = 14 THEN
		    INSERT INTO dbowner.tbl_temp_item_subjects
				SELECT DISTINCT decode(status, 'D', 'D', NULL), 'SIS', 'SIS_SBJ_' || a.orig_sys_id || '_' || a.orig_sys_jnl_num || '_' || a.subject_cat_code, 'SIS_JOU_' || a.orig_sys_jnl_num,
					run_time, run_time, a.subject_cat_code, subject_cat_descr, c.subject_grp_code, subject_grp_descr, d.subject_area_code, subject_area_descr, e.super_area_code, super_area_descr, scaling_fctr
				FROM journal_in_subject_cats@POSIS_AWS a,
						 subject_categories@POSIS_AWS b,
						 subject_groups@POSIS_AWS c,
						 subject_areas@POSIS_AWS d,
						 subject_super_areas@POSIS_AWS e,
						 capri_journal_in_subject_cats@POSIS_AWS f
				WHERE a.subject_cat_code = b.subject_cat_code (+)
				AND b.subject_grp_code = c.subject_grp_code (+)
				AND c.subject_area_code = d.subject_area_code (+)
				AND d.super_area_code = e.super_area_code (+)
				AND f.orig_sys_id = a.orig_sys_id (+)
				AND f.orig_sys_jnl_num = a.orig_sys_jnl_num (+)
				AND f.subject_cat_code = a.subject_cat_code (+)
				AND f.run_time < (SELECT this_marker_db1 FROM dbowner.tbl_sources WHERE source = 'SIS');
		ELSIF p_query_no = 15 THEN
			DELETE capri_book_sales@POSIS_AWS WHERE run_time < (SELECT this_marker_db1 FROM dbowner.tbl_sources WHERE source = 'SIS');
		ELSIF p_query_no = 16 THEN
			DELETE capri_books@POSIS_AWS WHERE run_time < (SELECT this_marker_db1 FROM dbowner.tbl_sources WHERE source = 'SIS');
		ELSIF p_query_no = 17 THEN
			DELETE capri_ip_info_block@POSIS_AWS WHERE run_time < (SELECT this_marker_db1 FROM dbowner.tbl_sources WHERE source = 'SIS');
		ELSIF p_query_no = 18 THEN
		  	DELETE capri_ip_address_ranges@POSIS_AWS WHERE run_time < (SELECT this_marker_db1 FROM dbowner.tbl_sources WHERE source = 'SIS');
		ELSIF p_query_no = 19 THEN
		  	DELETE capri_journal_sales@POSIS_AWS WHERE run_time < (SELECT this_marker_db1 FROM dbowner.tbl_sources WHERE source = 'SIS');
		ELSIF p_query_no = 20 THEN
		  	DELETE capri_journals@POSIS_AWS WHERE run_time < (SELECT this_marker_db1 FROM dbowner.tbl_sources WHERE source = 'SIS');
		ELSIF p_query_no = 21 THEN
		  	DELETE capri_operational_addresses@POSIS_AWS WHERE run_time < (SELECT this_marker_db1 FROM dbowner.tbl_sources WHERE source = 'SIS');
		ELSIF p_query_no = 22 THEN
		  	DELETE capri_organisation_units@POSIS_AWS WHERE run_time < (SELECT this_marker_db1 FROM dbowner.tbl_sources WHERE source = 'SIS');
		ELSIF p_query_no = 23 THEN
		  	DELETE capri_book_in_subject_cats@POSIS_AWS WHERE run_time < (SELECT this_marker_db1 FROM dbowner.tbl_sources WHERE source = 'SIS');
		ELSIF p_query_no = 24 THEN
		  	DELETE capri_journal_in_subject_cats@POSIS_AWS WHERE run_time < (SELECT this_marker_db1 FROM dbowner.tbl_sources WHERE source = 'SIS');
		ELSIF p_query_no = 25 THEN
		  	DELETE capri_org_unit_in_industry@POSIS_AWS WHERE run_time < (SELECT this_marker_db1 FROM dbowner.tbl_sources WHERE source = 'SIS');
		END IF;
*/
	ELSIF p_source = 'AE' THEN

		IF p_query_no = 1 THEN

			INSERT INTO dbowner.tbl_temp_parties
				SELECT DISTINCT DECODE(b.status,'D','D',NULL), 'AE', a.httphost, 'AE_' || b.prim_key, 'EUS', 'PER', a.createdate, a.status, a.modifydate, a.login,
					a.PASSWORD, fk_date_convert(a.lastauthenticationdate,'DD/MM/YYYY HH24:MI:SS'), NULL, 'userid=' || a.prim_key || ',userattributes,sid=store_JDBC', a.designator,
					a.dupcheck, Fk_Combine_Values(a.principalfield1, a.principalfield2), a.title, a.title, a.firstname, a.lastname, NULL, NULL, a.address1, a.address2, NULL, NULL, a.city,
					a.state, a.zip, a.country, NULL, a.country, NULL, NULL, NULL, NULL, a.department, a.organisation, a.jobtitle, a.telephone, NULL, NULL, NULL, a.fax, a.email, NULL, NULL,
					NULL, NULL, NULL, NULL, a.name, NULL, NULL, NULL, a.importid, NULL, NULL, NULL, NULL, NULL, a.suspend, NULL, NULL, NULL, NULL, NULL, Fk_Combine_Values(a.placeofwork1, a.placeofwork2),
					NULL, NULL, NULL, a.specialoffers, a.announcements, NULL, NULL, NULL, a.modifydate,
                    NULL, NULL
				FROM compowner.users_old a, compowner.TBL_DATA_CHANGES b
				WHERE b.prim_key = a.prim_key (+)
				AND b.source = 'AE'
				AND b.table_name = 'users';

		ELSIF p_query_no = 2 THEN

			INSERT INTO dbowner.tbl_temp_parties
				SELECT DISTINCT DECODE(b.status,'D','D',NULL), 'AE', NULL, 'AE_GRP_' || b.prim_key, 'ORG', NULL, a.createdate, NULL, a.modifydate, NULL, NULL, NULL,
					NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
					NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'AE_GRP_' || a.group_dn, 'AE_GRP_' || a.parentdn, a.name,
					NULL, NULL, NULL, a.associatedgroupsetid, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
					NULL, NULL, NULL, a.modifydate,
                    NULL, NULL
				FROM compowner.groups_old a, compowner.TBL_DATA_CHANGES b
				WHERE b.prim_key = a.prim_key (+)
				AND b.source = 'AE'
				AND b.table_name = 'groups';

		ELSIF p_query_no = 3 THEN

			INSERT INTO dbowner.tbl_temp_interests
				SELECT DISTINCT 'AE_MBS_' || b.prim_key, 'AE', 'AE_' || a.userid, change_date, change_date, 'MBS', 'AE_GRP_' || a.groupid,
				a.name, 'AE_GRP_' || a.parentdn, 'AE_GRP_' || a.groupdn, NULL, DECODE(b.status,'D','D',NULL)
				FROM compowner.group_memberships_old a, compowner.TBL_DATA_CHANGES b
				WHERE b.prim_key = a.prim_key (+)
				AND b.source = 'AE'
				AND b.table_name = 'group_memberships';

		ELSIF p_query_no = 4 THEN

			INSERT INTO dbowner.tbl_temp_interests
				SELECT DISTINCT 'AE_EMA_CELL_' || b.prim_key, 'AE', 'AE_' || ale_usr_id, ale_creation_date, ale_creation_date, 'EMA', aty_name,
					REPLACE(REPLACE(ale_xml,'<journal alias=''',NULL),'''/>',NULL), afo_name, afr_name, NULL, DECODE(b.status,'D','D',NULL)
				FROM compowner.cell_alerts_old a, compowner.TBL_DATA_CHANGES b
				WHERE b.prim_key = a.prim_key (+)
				AND b.source = 'AE'
				AND b.table_name = 'cell_alerts';

		ELSIF p_query_no = 5 THEN

			INSERT INTO dbowner.tbl_temp_interests
				SELECT DISTINCT 'AE_EMA_PNX_' || b.prim_key, 'AE', 'AE_' || a.userid, a.creationdate, a.creationdate, 'EMA', a.typename, a.journal,
					a.frmtname, a.freqname, NULL, DECODE(b.status,'D','D',NULL)
				FROM compowner.phoenix_alerts_old a,
						 compowner.TBL_DATA_CHANGES b
				WHERE b.prim_key = a.prim_key (+)
				AND b.source = 'AE'
				AND b.table_name = 'phoenix_alerts';

		ELSIF p_query_no = 6 THEN

			INSERT INTO dbowner.tbl_temp_interests
				SELECT DISTINCT 'AE_EMA_LAN_' || b.prim_key, 'AE', 'AE_' || a.userid, a.creationdate, a.creationdate, 'EMA', a.typename, a.alertxml,
					a.alertname, a.frmtname || '_' || a.freqname, NULL, DECODE(b.status,'D','D',NULL)
				FROM compowner.lancet_alerts_old a,
						 compowner.TBL_DATA_CHANGES b
				WHERE b.prim_key = a.prim_key (+)
				AND b.source = 'AE'
				AND b.table_name = 'lancet_alerts';

		ELSIF p_query_no = 7 THEN

			INSERT INTO dbowner.tbl_temp_interests
				SELECT DISTINCT 'AE_' || DECODE(name,'phoenix_lancetyearqualified','PFS_','MIN_') || b.prim_key, 'AE', 'AE_' || a.userid, change_date,
					change_date, DECODE(name,'phoenix_lancetyearqualified','PFS','MIN'), name, value, NULL, NULL, NULL, DECODE(status,'D','D',NULL)
				FROM compowner.user_properties_old a, compowner.TBL_DATA_CHANGES b
				WHERE b.prim_key = a.prim_key (+)
				AND b.source = 'AE'
				AND b.table_name = 'user_properties';

		ELSIF p_query_no = 8 THEN

			INSERT INTO dbowner.tbl_temp_items
				SELECT DISTINCT 'AE_PRO_' || b.prim_key, 'AE', change_date, change_date, name, DECODE(SUBSTR(name,-1,1),')','JOU','PRO'),
					NULL, NULL, NULL, NULL, NULL, sku, DECODE(SUBSTR(name,-1,1),')',REPLACE(SUBSTR(name,INSTR (name,'(')+1),')',NULL),acronym), NULL, NULL, NULL, NULL, NULL, NULL,
					NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, DECODE(status,'D','D',NULL)
				FROM compowner.products_old a,
				     compowner.TBL_DATA_CHANGES b
				WHERE b.prim_key = a.prim_key (+)
				AND b.source = 'AE'
				AND b.table_name = 'products';

		ELSIF p_query_no = 9 THEN

			INSERT INTO dbowner.tbl_temp_items
				SELECT DISTINCT 'AE_ART_'|| article, 'AE', NULL, NULL, name, 'ART', 'AE_PRO_' || productid, DECODE(volume,'*',NULL,volume),
					DECODE(issue,'*',NULL,issue), NULL, description, article, NULL, DECODE(SUBSTR(description,1,2),'24',NULL,info), NULL, NULL, NULL, TYPE, NULL, NULL, NULL, NULL,
					NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
				FROM compowner.orders_old a, compowner.TBL_DATA_CHANGES b
				WHERE article <> '*'
				AND b.prim_key = a.prim_key (+)
				AND b.source = 'AE'
				AND b.table_name = 'orders';

		ELSIF p_query_no = 10 THEN

			INSERT INTO dbowner.tbl_temp_items
				SELECT DISTINCT	'AE_COU_' || b.prim_key, 'AE', a.created_date, NULL, NULL, 'COU', NULL, NULL, NULL, NULL, NULL, a.claim_code, a.source, NULL, a.upload, NULL, NULL, NULL,
					NULL, a.batch_id, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, a.redemption_ceiling, a.expiration_dispatched, NULL, NULL, NULL, NULL,
					a.expiration_date, NULL, DECODE(b.status,'D','D',NULL)
				FROM compowner.coupons_old a,
				     compowner.TBL_DATA_CHANGES b
				WHERE b.prim_key = a.prim_key (+)
				AND b.source = 'AE'
				AND b.table_name = 'coupons';

		ELSIF p_query_no = 11 THEN

			INSERT INTO dbowner.tbl_temp_subscriptions
				SELECT DISTINCT DECODE(b.status,'D','D',NULL), 'AE_LIC_' || b.prim_key, 'AE', change_date, change_date, 'AE_' || DECODE(SUBSTR(a.dn,1,6),'userid',SUBSTR(a.dn,8,LENGTH(a.dn) -37 ),'GRP_'|| a.dn),
					'AE_SET_' || a.productsetid, NULL, decode(attributelabel,'Start Date',dateval,NULL), decode(attributelabel,'End Date',dateval,NULL), NULL, DECODE('objecttypes.label', 'Unconditional access license', 'COUPON', DECODE(SUBSTR(dn,1,6), 'userid','PERSONAL','GROUP')),
					NULL, a.name, NULL, a.label, a.coupenprodclaimsid, a.claim_id, a.cep_id, NULL, a.objecttype, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, a.licenseid,
					NULL, NULL, NULL, NULL, NULL, NULL, a.dn,NULL, NULL, NULL, NULL, NULL, NULL, NULL
				FROM compowner.licenses_old a, compowner.TBL_DATA_CHANGES b
				WHERE b.prim_key = a.prim_key (+)
				AND b.source = 'AE'
				AND b.table_name = 'licenses';

		ELSIF p_query_no = 12 THEN

			INSERT INTO dbowner.tbl_temp_subscriptions
				SELECT DISTINCT DECODE(b.status,'D','D',NULL), 'AE_CLA_' || b.prim_key, 'AE', claim_date, claim_date, 'AE_' || DECODE(SUBSTR(claimant_dn,1,6),'userid',SUBSTR(claimant_dn,8,LENGTH(claimant_dn) -37 ),claimant_dn),
					'AE_COU_' || coupon_id, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
					NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, claimant_dn, NULL, NULL, NULL, NULL, NULL, NULL, NULL
				FROM compowner.coupon_claims_old a, compowner.TBL_DATA_CHANGES b
				WHERE b.prim_key = a.prim_key (+)
				AND b.source = 'AE'
				AND b.table_name = 'coupon_claims';

		ELSIF p_query_no = 13 THEN

			INSERT INTO dbowner.tbl_temp_subscriptions
				SELECT DISTINCT DECODE(b.status,'D','D',NULL), 'AE_ORD_' || b.prim_key, 'AE', a.itemcreatedate, a.itemmodifydate, 'AE_' || a.customerid, 'AE_PRO_' || a.productid,
					DECODE(article,'*',NULL,'AE_ART_'||a.article), a.ordercreatedate, NULL, a.total, a.status, a.product, a.description,a.TYPE, a.extrainfo, NULL, a.info, NULL, NULL, ordertype, NULL, NULL, NULL, NULL, a.region,
					NULL, NULL, a.orderref, a.pricetoken, a.price_code, NULL, a.licenceid, a.startdate, a.enddate, DECODE(a.issue,'*',NULL,a.issue), NULL, NULL, a.id, a.listprice, a.currencycode, NULL,
					DECODE(a.volume,'*',NULL,a.volume), NULL, NULL, a.promocode, a.quantity, NULL, a.paymentmethod, NULL, NULL, NULL, NULL
				FROM compowner.orders_old a, compowner.TBL_DATA_CHANGES b
				WHERE b.prim_key = a.prim_key (+)
				AND b.source = 'AE'
				AND b.table_name = 'orders';

		ELSIF p_query_no = 14 THEN

			INSERT INTO dbowner.tbl_temp_item_subjects
				SELECT DISTINCT DECODE(b.status,'D','D',NULL), 'AE', 'AE_SET_' || b.prim_key, 'AE_PRO_' || a.productid, NULL, NULL, NULL, 'AE_SET_' || id, NULL, NULL, NULL, NULL, NULL, NULL, NULL
				FROM compowner.product_sets_old a, compowner.TBL_DATA_CHANGES b
				WHERE b.prim_key = a.prim_key (+)
				AND b.source = 'AE'
				AND b.table_name = 'product_sets';

		ELSIF p_query_no = 15 THEN

			INSERT INTO dbowner.tbl_temp_item_subjects
				SELECT DISTINCT DECODE(b.status,'D','D',NULL), 'AE', 'AE_CEP_' || b.prim_key, 'AE_COU_' || coupon_id, NULL, NULL, NULL, 'AE_PRO_' || product_id, scheduled_activation, scheduled_revocation, revocation_dispatched,
					activation_dispatched, NULL, NULL, NULL
				FROM compowner.coupon_prod_ents_old a,
						 compowner.TBL_DATA_CHANGES b
				WHERE b.prim_key = a.prim_key (+)
				AND b.source = 'AE'
				AND b.table_name = 'coupon_prod_ents';

		ELSIF p_query_no = 16 THEN

      INSERT INTO dbowner.tbl_temp_item_subjects
        SELECT DISTINCT DECODE(b.status,'D','D',NULL), 'AE', 'AE_CEG_' || b.prim_key, 'AE_COU_' || coupon_id, NULL, NULL, NULL, 'AE_GRP_' || group_id,
               trim(to_char(scheduled_activation,'DD/MM/YYYY HH24:MI:SS')),
               trim(to_char(scheduled_revocation,'DD/MM/YYYY HH24:MI:SS')),
               revocation_dispatched, activation_dispatched, NULL, NULL, NULL
        FROM compowner.coupon_group_ents_old a,
             compowner.TBL_DATA_CHANGES b
        WHERE b.prim_key = a.prim_key (+)
        AND b.source = 'AE'
        AND b.table_name = 'coupon_group_ents';

		ELSIF p_query_no = 17 THEN

			INSERT INTO dbowner.tbl_temp_item_subjects
        SELECT DISTINCT DECODE(b.status,'D','D',NULL), 'AE', 'AE_COL_' || b.prim_key, 'AE_', NULL, NULL, a.prim_key, a.name, NULL, NULL, NULL, NULL, NULL, NULL, NULL
        FROM compowner.collection_codes_old a,
             compowner.TBL_DATA_CHANGES b
        WHERE b.prim_key = a.prim_key (+)
        AND b.source = 'AE'
        AND b.table_name = 'collection_codes';

		ELSIF p_query_no = 18 THEN

			INSERT INTO dbowner.tbl_temp_interests
				SELECT DISTINCT 'AE_EMA_GMR_' || b.prim_key, 'AE', 'AE_' || party_ref, ORIG_CREATE_DATE, ORIG_CREATE_DATE, 'EMA', INTEREST_VALUE,
					INTEREST_VALUE_DETAILS, INTEREST_SECTION, INTEREST_SUB_SECTION, NULL, DECODE(b.status,'D','D',NULL)
				FROM compowner.ehc_alerts_old a, compowner.TBL_DATA_CHANGES b
				WHERE b.prim_key = a.prim_key (+)
				AND b.source = 'AE'
				AND b.table_name = 'ehc_alerts';

		ELSIF p_query_no = 19 THEN
             COMMIT;

             DELETE FROM compowner.tbl_data_changes;

		END IF;

	ELSIF p_source = 'GCD' THEN
		null;
		/*IF p_query_no = 1 THEN
			INSERT INTO dbowner.tbl_temp_parties
				SELECT DISTINCT decode(email_address,'DELETED','D',NULL), 'GCD', NULL, 'GCD_' || a.id, 'EUS', 'PER', a.create_date, NULL, a.modify_date, b.username, NULL, NULL, NULL, NULL,
					NULL, NULL, NULL, a.title, a.title, a.first_name, a.surname, NULL, NULL, a.street_number_pob, NULL, NULL, NULL, a.city_town_state, NULL,
					a.post_zip_code, trim(decode(instr(country_name,' - '), 0, country_name, substr(country_name,1,instr(country_name,' - ')-1))), NULL,
					country_name, d.region_name, NULL, NULL, NULL, a.department, a.organisation, NULL, a.telephone, NULL, NULL, NULL, a.fax, a.email_address, NULL, NULL, NULL,
					NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, other_organisation_type,
					g.job_function_description, e.org_prime_type_descr, f.org_second_type_descr, NULL, decode(interested_other_mail, 'Y', 'Y', 'N'),
					NULL, NULL, NULL, greatest(a.modify_date, a.create_date)
				FROM registrants@pogcd a,
						 gcd.users_username@pogcd b,
						 countries@pogcd c,
						 regions@pogcd d,
						 org_primary_types@pogcd e,
						 org_secondary_types@pogcd f,
						 job_functions@pogcd g
				WHERE a.id = b.reg_id (+)
				AND a.country_code_es = c.country_code_es
				AND c.region_code = d.region_code (+)
				AND a.org_prime_type_code = e.org_prime_type_code
				AND a.org_second_type_code = f.org_second_type_code
				AND e.org_prime_type_code = f.org_prime_type_code
				AND a.job_function_code = g.job_function_code
				AND (a.modify_date >= (SELECT last_marker_db1 FROM dbowner.tbl_sources WHERE source = 'GCD') OR a.create_date >= (SELECT last_marker_db1 FROM dbowner.tbl_sources WHERE source = 'GCD'));
				-- not worried about specifying '< this pull' above as bringing in records that may come in again next time doesn't cause problems. this means query will
				-- not filter so much and therefore run quicker

		ELSIF p_query_no = 2 THEN

			OPEN c_gcd_journals;

			FETCH c_gcd_journals INTO v_dummy;

			IF (c_gcd_journals%FOUND) THEN

				INSERT INTO dbowner.tbl_temp_items
					SELECT DISTINCT 'GCD_JOU_' || a.journal_number, 'GCD', NULL, NULL, a.journal_title, 'JOU', NULL, NULL, NULL, NULL, NULL, b.issn, upper(b.acronym),
						NULL, b.journal, NULL, b.publication_id, b.product_type, NULL, NULL, NULL, b.pmc, NULL, decode(length(b.pmg),2,'0'||b.pmg,b.pmg), NULL, NULL,
						NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, SYSDATE, NULL
					FROM journals@pogcd a,
							 journal_details@pogcd b
					WHERE a.journal_number = b.journal (+);
			END IF;
			CLOSE c_gcd_journals;

		ELSIF p_query_no = 3 THEN

			UPDATE dbowner.tbl_temp_items a
			SET pmg_descr = (SELECT b.pmg_descr FROM product_market_groups@POSIS_AWS b WHERE a.pmg_code = b.pmg_code)
			WHERE a.orig_system = 'GCD'
			AND a.item_type = 'JOU';

		ELSIF p_query_no = 4 THEN
			UPDATE dbowner.tbl_temp_items a
			SET pmc_descr = (SELECT b.pmc_descr FROM product_market_combinations@POSIS_AWS b,
																							 product_market_groups@POSIS_AWS c
											 WHERE a.pmc_code = b.pmc_code (+)
												 AND a.pmg_code = b.pmg_code (+)
												 AND a.pmg_code = c.pmg_code (+))
			WHERE a.orig_system = 'GCD'
			AND a.item_type = 'JOU';

		ELSIF p_query_no = 5 THEN

			INSERT INTO dbowner.tbl_temp_items
				SELECT DISTINCT 'GCD_BOK_' || b.isbn, 'GCD', b.status_date, b.status_date, a.full_title, 'BOK', NULL, NULL, NULL, a.year, a.proceeding_sub_title,
					b.isbn, NULL, NULL, NULL, NULL, a.publisher_name, NULL, NULL, b.physical_descr, NULL, pmc, NULL, pmg, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
					decode(a.year,NULL,NULL,0,NULL,to_date('01 January ' || a.year, 'DD Month YYYY')),	decode(release_month,NULL,NULL,to_date('01' || REPLACE(release_month,'Publication:',''), 'DD Month YYYY')),
					b.status_date, NULL
			FROM books@pogcd a,
					 book_instances@pogcd b,
					 gcd.books_trigger@pogcd c
			WHERE c.book_id = b.book_id (+)
			AND c.book_id = a.book_id (+)
			AND status_date < (SELECT this_marker_db1 FROM dbowner.tbl_sources WHERE source = 'GCD');

		ELSIF p_query_no = 6 THEN

			INSERT INTO dbowner.tbl_temp_interests
				SELECT DISTINCT 'GCD_JIN_' || a.reg_id || '_' || a.journal_number, 'GCD', 'GCD_' || a.reg_id, b.create_date, b.create_date, 'JIN', c.journal_title, 'GCD_JOU_' || a.journal_number,
					NULL, NULL, NULL, decode(action, 'D', 'D', NULL)
				FROM gcd.journal_interests_trigger@pogcd a,
						 interests@pogcd b,
						 journals@pogcd c
				WHERE a.reg_id = b.reg_id (+)
				AND a.journal_number = b.journal_number (+)
				AND a.journal_number = c.journal_number (+)
				AND a.run_time < (SELECT this_marker_db1 FROM dbowner.tbl_sources WHERE source = 'GCD');
		ELSIF p_query_no = 7 THEN
			INSERT INTO dbowner.tbl_temp_interests
				SELECT DISTINCT 'GCD_BIN_' || a.reg_id || '_' || b.hierarchy_level || '_' || b.hierarchy_code, 'GCD', 'GCD_' || a.reg_id, b.created_date, b.created_date, 'BIN', a.hierarchy_level, a.hierarchy_code,
					NULL, NULL, NULL, decode(action, 'D', 'D', NULL)
				FROM gcd.book_interests_trigger@pogcd a,
				     book_interests@pogcd b
				WHERE a.reg_id = b.reg_id (+)
				AND a.hierarchy_level = b.hierarchy_level (+)
				AND a.hierarchy_code = b.hierarchy_code (+)
				AND a.run_time < (SELECT this_marker_db1 FROM dbowner.tbl_sources WHERE source = 'GCD');
		ELSIF p_query_no = 8 THEN
			DELETE gcd.journals_trigger@pogcd WHERE run_time < (SELECT this_marker_db1 FROM dbowner.tbl_sources WHERE source = 'GCD');
		ELSIF p_query_no = 9 THEN
			DELETE gcd.books_trigger@pogcd WHERE run_time < (SELECT this_marker_db1 FROM dbowner.tbl_sources WHERE source = 'GCD');
		ELSIF p_query_no = 10 THEN
			DELETE gcd.journal_interests_trigger@pogcd WHERE run_time < (SELECT this_marker_db1 FROM dbowner.tbl_sources WHERE source = 'GCD');
		ELSIF p_query_no = 11 THEN
			DELETE gcd.book_interests_trigger@pogcd WHERE run_time < (SELECT this_marker_db1 FROM dbowner.tbl_sources WHERE source = 'GCD');
		END IF;
		*/
	ELSIF p_source = 'PTS' THEN

		IF p_query_no = 1 THEN
			INSERT INTO dbowner.tbl_temp_parties
				SELECT DISTINCT NULL, 'PTS', NULL, 'PTS_' || nr, 'AUT', 'PER', creation_date, NULL, user_status_date, NULL, NULL, NULL,
					NULL, NULL, NULL, NULL, NULL, description, description, forename, surname, NULL, ads_type, address, address2, NULL, NULL, city, NULL,
					zip_code, name_iso, cod_iso, name_iso, region_code, NULL, NULL, NULL, department, organization, NULL, telephone_nr, NULL, NULL, NULL, fax_nr,
					email_address, NULL, NULL, NULL, NULL, NULL, NULL, institute, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
					NULL, NULL, NULL, NULL, NULL, NULL, ind_marketing_mail, NULL, NULL, NULL, last_update_date,
                    NULL, NULL
			FROM dbowner.tbl_temp_pts_aut;

		ELSIF p_query_no = 2 THEN
			INSERT INTO dbowner.tbl_temp_items
				SELECT DISTINCT 'PTS_JOU_' || id, 'PTS', creation_date, last_update_date, title, 'JOU', NULL, NULL, NULL, NULL, NULL,
					issn, code, NULL, ste_id2, NULL, ste_id, NULL, NULL, NULL, NULL, pmc_id, pmc_description, pmg_id, pmg_description, NULL, NULL,
					NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, last_update_date, NULL
			FROM dbowner.tbl_temp_pts_jnl;

		ELSIF p_query_no = 3 THEN
			INSERT INTO dbowner.tbl_temp_items
            SELECT DISTINCT 'PTS_' || id,
                   'PTS',
                   creation_date,
                   last_update_date,
                   title,
                   'ART',
                   'PTS_JOU_' || jin_id,
                   volume_number,
                   issue_number,
                   cover_date,
                   doi,
                   pii_nr,
                   nr,
                   'PTS_' || atr_nr_corresponding,
                   status,
                   NULL,
                   NULL,
                   physical_issue_type,
                   page_type,
                   sequence_in_issue,
                   issue_id,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   CASE
                      WHEN PIT = 'ABS' THEN 'Abstract only'
                      WHEN PIT = 'ADD' THEN 'Addendum'
                      WHEN PIT = 'ADV' THEN 'Advertisement'
                      WHEN PIT = 'ANN' THEN 'Announcement'
                      WHEN PIT = 'BPH' THEN 'Batch place holder'
                      WHEN PIT = 'BRV' THEN 'Book review'
                      WHEN PIT = 'CRP' THEN 'Case Report'
                      WHEN PIT = 'CNF' THEN 'Conference'
                      WHEN PIT = 'CON' THEN 'Contents list'
                      WHEN PIT = 'COR' THEN 'Correspondence'
                      WHEN PIT = 'DIS' THEN 'Discussion'
                      WHEN PIT = 'DUP' THEN 'Duplicate'
                      WHEN PIT = 'EDI' THEN 'Editorial'
                      WHEN PIT = 'EDB' THEN 'Editorial Board'
                      WHEN PIT = 'ERR' THEN 'Erratum'
                      WHEN PIT = 'EXM' THEN 'Exam'
                      WHEN PIT = 'FLA' THEN 'Full length article'
                      WHEN PIT = 'IND' THEN 'Index'
                      WHEN PIT = 'LIT' THEN 'Literature alert'
                      WHEN PIT = 'CAL' THEN 'Meetings calendar'
                      WHEN PIT = 'MIS' THEN 'Miscellaneous'
                      WHEN PIT = 'NWS' THEN 'News'
                      WHEN PIT = 'ZZZ' THEN 'Non manuscript item - Default'
                      WHEN PIT = 'OCN' THEN 'Other contents'
                      WHEN PIT = 'PNT' THEN 'Patent report'
                      WHEN PIT = 'PRP' THEN 'Personal report'
                      WHEN PIT = 'PGL' THEN 'Practice Guideline'
                      WHEN PIT = 'PRV' THEN 'Product review'
                      WHEN PIT = 'PUB' THEN 'Publisher''s note'
                      WHEN PIT = 'REM' THEN 'Removal'
                      WHEN PIT = 'REQ' THEN 'Request for assistance'
                      WHEN PIT = 'RET' THEN 'Retraction'
                      WHEN PIT = 'REV' THEN 'Review article'
                      WHEN PIT = 'SCO' THEN 'Short communication'
                      WHEN PIT = 'SSU' THEN 'Short survey'
                      WHEN PIT = 'DAT' THEN 'data'
                      WHEN PIT = 'LST' THEN 'list'
                      WHEN PIT = 'MIC' THEN 'micro-article'
                   ELSE PIT END,
                   NULL,
                   no_pages,
                   issue_start_end_page,
                   milestone,
                   ind_complete_milestone,
                   NULL,
                   delivery_date,
                   eo_receive_date,
                   NULL,
                   NULL,
                   eo_accept_date,
                   NULL
            FROM dbowner.tbl_temp_pts_art;
/*				SELECT DISTINCT 'PTS_' || id, 'PTS', creation_date, last_update_date, title, 'ART', 'PTS_JOU_' || jin_id,
					volume_number, issue_number, cover_date, doi, pii_nr, nr, 'PTS_' || atr_nr_corresponding, status, NULL, NULL, physical_issue_type,
					page_type, sequence_in_issue, issue_id, NULL, NULL, NULL, NULL, NULL, NULL, no_pages, issue_start_end_page,
					milestone, ind_complete_milestone, NULL, delivery_date, eo_receive_date, NULL, NULL, eo_accept_date, NULL
			FROM dbowner.tbl_temp_pts_art;
*/

		ELSIF p_query_no = 4 THEN
			UPDATE dbowner.tbl_temp_items a SET a.init_pub_date =
				(SELECT b.issue_realized_dt FROM
					(SELECT pie_id,
						max(end_date) issue_realized_dt
					FROM dbowner.tbl_temp_pts_evt
					WHERE evt_name = 'DESPATCH'
					group by pie_id) b
				WHERE a.medium = b.pie_id)
			WHERE a.orig_system = 'PTS'
			AND a.item_type = 'ART';

		ELSIF p_query_no = 5 THEN
			UPDATE dbowner.tbl_temp_items a SET a.last_pub_date =
				(SELECT b.issue_online_dt FROM
					(SELECT pie_id,
						coalesce(
						min(case when evt_name = 'SD-ON-WEB' then end_date end),
						min(case when evt_name = 'SD-IMPORTED' then end_date end),
						min(case when evt_name = 'SD-RECEIVED' then end_date end)) issue_online_dt
					FROM dbowner.tbl_temp_pts_evt
					WHERE evt_name in ('SD-ON-WEB','SD-IMPORTED','SD-RECEIVED')
					group by pie_id) b
				WHERE a.medium = b.pie_id)
			WHERE a.orig_system = 'PTS'
			AND a.item_type = 'ART';

    ELSIF p_query_no = 6 THEN
			UPDATE dbowner.tbl_items a SET a.init_pub_date =
				(SELECT b.issue_realized_dt FROM
					(SELECT pie_id,
						max(end_date) issue_realized_dt
					FROM dbowner.tbl_temp_pts_evt
					WHERE evt_name = 'DESPATCH'
					group by pie_id) b
				WHERE a.medium = b.pie_id)
			WHERE a.orig_system = 'PTS'
			AND a.item_type = 'ART'
      AND a.init_pub_date is null;

    ELSIF p_query_no = 7 THEN
			UPDATE dbowner.tbl_items a SET a.last_pub_date =
				(SELECT b.issue_online_dt FROM
					(SELECT pie_id,
						coalesce(
						min(case when evt_name = 'SD-ON-WEB' then end_date end),
						min(case when evt_name = 'SD-IMPORTED' then end_date end),
						min(case when evt_name = 'SD-RECEIVED' then end_date end)) issue_online_dt
					FROM dbowner.tbl_temp_pts_evt
					WHERE evt_name in ('SD-ON-WEB','SD-IMPORTED','SD-RECEIVED')
					group by pie_id) b
				WHERE a.medium = b.pie_id)
			WHERE a.orig_system = 'PTS'
			AND a.item_type = 'ART'
      AND a.last_pub_date is null;

		END IF;

	ELSIF p_source = 'EA' THEN

		IF p_query_no = 1 THEN
			INSERT INTO dbowner.tbl_temp_parties
				SELECT DISTINCT decode(status,'D','D',NULL), 'EA', NULL, 'EA_' || webcustid, 'EUS', 'PER', NULL, NULL, NULL, NULL, password,
					NULL, NULL, NULL, NULL, NULL, NULL, title, title, firstname, lastname, NULL, NULL, address1, address2, address3, NULL, town,
					state, postcode, country, NULL, country, NULL, NULL, NULL, NULL, NULL, organisation, jobtitle, tel, NULL, NULL, NULL, fax, email, emailformat,
					NULL, NULL, NULL, NULL, 'EA_BCT_' || webcustid, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, subscription, NULL,
					NULL, NULL, NULL, NULL, NULL, occupation, NULL, NULL, NULL, NULL, NULL, NULL, NULL, run_time,
                    NULL, NULL
				FROM dbowner.tbl_temp_ea_webcust;
		ELSIF p_query_no = 2 THEN
			INSERT INTO dbowner.tbl_temp_parties
				SELECT DISTINCT decode(status,'D','D',NULL), 'EA', NULL, 'EA_BCT_' || webcustid, 'BCT', 'PER', NULL, NULL, NULL, NULL, NULL,
					NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, bill_name, NULL, NULL, NULL, bill_address1, bill_address2, bill_address3, NULL, bill_town,
					NULL, bill_postcode, bill_country, NULL, bill_country, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
					NULL, NULL, NULL, NULL, 'EA_' || webcustid, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
					NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, run_time,
                    NULL, NULL
				FROM dbowner.tbl_temp_ea_webcust;
		ELSIF p_query_no = 3 THEN
			INSERT INTO dbowner.tbl_temp_interests
				SELECT DISTINCT 'EA_EMA_' || webcustid || '_' || subjectid, 'EA', 'EA_' || webcustid, run_time, run_time, 'EMA', subjectname,
					NULL, NULL, NULL, NULL, decode(status, 'D', 'D', NULL)
				FROM dbowner.tbl_temp_ea_webcust_topics;
		ELSIF p_query_no = 4 THEN
			DELETE dbowner.tbl_temp_ea_webcust;
		ELSIF p_query_no = 5 THEN
			DELETE dbowner.tbl_temp_ea_webcust_topics;
		END IF;

	/*ELSIF p_source = 'STR' THEN

		IF p_query_no = 1 THEN
			INSERT INTO tbl_temp_parties
				SELECT DISTINCT decode(status,'D','D',NULL), 'STR', source, 'STR_' || customeraccount, 'GCN', 'PER', dateopened, active, NULL,
					NULL, NULL, NULL, NULL, NULL, NULL, sourcedescription, NULL, title, title, forename, surname, NULL, NULL, address1, address2, street,
					address3, town, county, postcode, trim(country), NULL, trim(country), countrygroup, NULL, NULL, NULL, fullname, companyname,
					jobdescription, phone#, NULL, NULL, NULL, NULL, email, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, bookshopflag, NULL, NULL,
					noofcopies, NULL, NULL, ealert, NULL, NULL, NULL, NULL, NULL, NULL, student, NULL, NULL, thirdpartymailing, elseviergroupmail,
					NULL, NULL, NULL, run_time
				FROM tbl_temp_str_custci;
		ELSIF p_query_no = 2 THEN
			INSERT INTO tbl_temp_parties
				SELECT DISTINCT decode(status,'D','D',NULL), 'STR', NULL, 'STR_ACN_' || contact_id, 'ACN', 'PER', NULL, NULL, NULL,
					NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, title, title, first_name, surname, NULL, NULL, address1, address2, address3,
					NULL, city, county, postcode, trim(country), NULL, trim(country), brick, NULL, NULL, NULL, department_id, institution_id, NULL, NULL,
					NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
					NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, run_time
				FROM tbl_temp_str_custac;
		ELSIF p_query_no = 3 THEN
			INSERT INTO tbl_temp_interests
				SELECT DISTINCT 'STR_GIN_' || customeraccount || '_' || subjectcode, 'STR', 'STR_' || customeraccount, run_time, run_time, 'GIN',
					subjectcode, subjectdescription, NULL, NULL, NULL, decode(status,'D','D',NULL)
				FROM tbl_temp_str_custpi;
		ELSIF p_query_no = 4 THEN
			INSERT INTO tbl_temp_interests
				SELECT DISTINCT 'STR_IPC_' || contact_id || '_' || inspection_id, 'STR', 'STR_ACN_' || contact_id, run_time, run_time, 'IPC', isbn,
					NULL, NULL, NULL, insp_orderdate, decode(status,'D','D',NULL)
				FROM tbl_temp_str_custai;
		ELSIF p_query_no = 5 THEN
			INSERT INTO tbl_temp_items
				SELECT DISTINCT 'STR_' || decode(cover_type,'JO','JOU','HB','BOK','PB','BOK','PRO') || '_' || isbn, 'STR', run_time, run_time, title,
					decode(cover_type,'JO','JOU','HB','BOK','PB','BOK','PRO'), NULL, NULL, NULL, NULL, NULL, isbn, NULL, author, textbook_flag,
					d_editor_code, d_product_manager_code, d_answer_code, d_product_group, NULL, d_cover_type, NULL, NULL, NULL, NULL, d_division,
					d_imprint, NULL, NULL, sbu_profit_centre, d_sbu, NULL, NULL, NULL, NULL, publish_date, usa_publish_date, decode(status,'D','D',NULL)
				FROM tbl_temp_str_custbib;
		ELSIF p_query_no = 6 THEN
		    INSERT INTO tbl_temp_item_subjects
				SELECT DISTINCT decode(status,'D','D',NULL), 'STR',
					'STR_SBJ_' || isbn || '_' || subject_code_1 || decode(trim(subject_code_2),'','','_'||subject_code_2) || decode(trim(subject_code_3),'','','_'||subject_code_3) || decode(trim(subject_code_4),'','','_'||subject_code_4),
					'STR_'||decode(cover_type,'JO','JOU','HB','BOK','PB','BOK','PRO')|| '_' || isbn, run_time, run_time, subject_code_4, d_subject_code_4, subject_code_3,
					d_subject_code_3, subject_code_2, d_subject_code_2, subject_code_1,  d_subject_code_1, NULL
				FROM tbl_temp_str_custbib
				WHERE trim(nvl(subject_code_1,'x')) NOT IN ('x', '----');
		ELSIF p_query_no = 7 THEN
			INSERT INTO tbl_temp_subscriptions
				SELECT decode(status,'D','D',NULL), NULL, 'STR', run_time, run_time, 'STR_' || customeraccount,
					isbn, NULL, hitdate, NULL, ordervalue, NULL, NULL, NULL, NULL, ordertype, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
					NULL, NULL, reasoncode, NULL, NULL, mailshotcode, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, linevalue, NULL, NULL, NULL, NULL, NULL,
					ordersource, quantity, NULL, NULL, NULL, NULL, NULL, NULL
				FROM tbl_temp_str_custsales;
		ELSIF p_query_no = 8 THEN
			UPDATE tbl_temp_subscriptions SET orig_system_ref = 'STR_' || seq_str_custsales.nextval WHERE orig_system = 'STR';
		ELSIF p_query_no = 9 THEN
			DELETE tbl_temp_str_custci;
		ELSIF p_query_no = 10 THEN
			DELETE tbl_temp_str_custac;
		ELSIF p_query_no = 11 THEN
			DELETE tbl_temp_str_custpi;
		ELSIF p_query_no = 12 THEN
			DELETE tbl_temp_str_custai;
		ELSIF p_query_no = 13 THEN
			DELETE tbl_temp_str_custbib;
		ELSIF p_query_no = 14 THEN
			DELETE tbl_temp_str_custsales;
		END IF;*/
	ELSIF p_source = 'ELB' THEN

		IF p_query_no = 1 THEN
			INSERT INTO dbowner.tbl_temp_parties
				SELECT DISTINCT NULL, 'ELB', NULL, 'ELB_' || a.contact_id, 'PRO', 'PER', a.create_date, status, a.update_date,
					a.western_firstname, a.western_lastname, NULL, NULL, a.comments, a.middle_name, a.division, g.user_type, title, title, firstname, lastname,
					NULL, NULL, address1, address2, address3, address4, town, county, post_code, country_name, country_code, country_name, NULL,
					NULL, NULL, NULL, department, organisation, job_title, phone_h, phone_w, phone_m, web_site, fax, email, NULL, NULL, NULL, NULL, a.phone_a,
					NULL, a.acc_suffix, NULL, a.sis_id, NULL, legacy_ref, NULL, NULL, NULL, NULL, NULL, a.local_organisation, NULL, NULL, NULL, NULL, NULL, b.org_type, c.job_type,
					h.adm_territory, i.ms_name, NULL, NULL, NULL, NULL, NULL, a.update_date,
                    /*a.cmx_id, case when a.cmx_id is not null then a.cmx_id
                                  when a.sis_id is not null then a.sis_id
                              else null
                              end
                   */ null, null
				FROM elbaowner.tbl_elba_contacts a,
						 elbaowner.tbl_elba_org_types b,
						 elbaowner.tbl_elba_job_types c,
						 dbowner.tbl_iso_countries d,
						 elbaowner.tbl_elba_user_types g,
						 elbaowner.tbl_elba_adm_territories h,
						 elbaowner.tbl_elba_market_segments i
				WHERE a.org_type = b.org_type_id (+)
				AND a.job_type = c.job_type_id (+)
				AND a.country_code = d.iso_code (+)
				AND a.user_type = g.user_type_id (+)
				AND a.adm_territory = h.adm_territory_id (+)
				AND a.market_segment = i.ms_code (+)
				AND (a.update_date > (SELECT last_marker_db1 FROM dbowner.tbl_sources WHERE source = 'ELB') OR g.update_date > (SELECT last_marker_db1 FROM dbowner.tbl_sources WHERE source = 'ELB'));
		ELSIF p_query_no = 2 THEN
			INSERT INTO dbowner.tbl_temp_parties
				SELECT DISTINCT 'D', 'ELB', NULL, 'ELB_' || contact_id, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
					NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
					NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
					NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
					NULL, NULL, NULL, run_time,
                    NULL, NULL
				FROM elbaowner.tbl_elba_au_contacts
				WHERE action = 'delete'
				AND run_time > (SELECT last_marker_db1 FROM dbowner.tbl_sources WHERE source = 'ELB');
		ELSIF p_query_no = 3 THEN
			INSERT INTO dbowner.tbl_temp_interests
				SELECT DISTINCT 'ELB_GIN_' || contact_id || '_' || a.code || '_' || a.code_source, 'ELB', 'ELB_' || contact_id, run_time, run_time, 'GIN',
					a.code, a.description, parent_description || ' (' || b.parent_code || ')', top_level_description || ' (' || b.top_level_code || ')',
					NULL, decode(action,'delete','D',NULL)
				FROM elbaowner.tbl_elba_au_interests a,
						 elbaowner.tbl_elba_interest_codes b
				WHERE a.code = b.code
				AND run_time > (SELECT last_marker_db1 FROM dbowner.tbl_sources WHERE source = 'ELB');
		ELSIF p_query_no = 4 THEN
			INSERT INTO dbowner.tbl_temp_interests
				SELECT DISTINCT 'ELB_LIM_' || contact_id || '_' || a.lst_id || '_' || b.business_unit, 'ELB', 'ELB_' || contact_id, run_time, run_time, 'LIM',
					decode(type_name, 'Event / Training', 'ELB_LST_' || b.lst_id, b.reference), b.name, type_name, copies_required, decode(b.status, 'I', '01 jan 2000', NULL), decode(action,'delete','D',NULL)
				FROM elbaowner.tbl_elba_au_list_memberships a,
						 elbaowner.tbl_elba_lists b,
						 elbaowner.tbl_elba_list_types c
				WHERE a.lst_id = b.lst_id
				AND b.lst_type = c.type_id
				AND (run_time > (SELECT last_marker_db1 FROM dbowner.tbl_sources WHERE source = 'ELB')
        OR b.update_date > (SELECT last_marker_db1 FROM dbowner.tbl_sources WHERE source = 'ELB'));
		ELSIF p_query_no = 5 THEN
			/*INSERT INTO dbowner.tbl_temp_interests
				SELECT DISTINCT 'ELB_PFS_' || contact_id , 'ELB', 'ELB_' || contact_id, run_time, run_time, 'PFS',
					b.firstname || ' ' || b.lastname, c.acc_manager, d.librarian_cat, e.trainer, NULL, decode(action,'delete','D',NULL)
				FROM elbaowner.tbl_elba_au_profiles a,
						 elbaowner.tbl_elba_users b,
						 elbaowner.tbl_elba_acc_managers c,
						 elbaowner.tbl_elba_librarian_cats d,
						 elbaowner.tbl_elba_trainers e
				WHERE a.acc_dev_man = b.user_id (+)
				AND a.acc_man = c.acc_manager_id (+)
				AND a.librarian_cat = d.librarian_cat_id (+)
				AND a.freelance_trainer = e.trainer_id (+)
				AND run_time > (SELECT last_marker_db1 FROM dbowner.tbl_sources WHERE source = 'ELB');*/
			NULL;
		ELSIF p_query_no = 6 THEN
			INSERT INTO dbowner.tbl_temp_interests
				SELECT DISTINCT 'ELB_BUS_' || a.contact_id || '_' || a.bus_unit, 'ELB', 'ELB_' || a.contact_id,
					a.create_date, a.update_date, 'BUS', b.unit_code, b.unit_name, a.originating_ref, c.source_description, NULL,
					decode(a.status,'I','D',NULL)
				FROM elbaowner.tbl_elba_bus_profiles a,
						 elbaowner.tbl_elba_business_units b,
						 elbaowner.tbl_elba_sources c
				WHERE a.bus_unit = b.unit_code
				AND a.source = c.source_id (+)
				AND a.update_date > (SELECT last_marker_db1 FROM dbowner.tbl_sources WHERE source = 'ELB');
		ELSIF p_query_no = 7 THEN
			INSERT INTO dbowner.tbl_temp_interests
				SELECT DISTINCT 'ELB_MIN_' || a.contact_id || '_' || a.bus_unit || '_' || a.contact_method || '_' || a.contact_domain, 'ELB', 'ELB_' || a.contact_id,
					a.run_time, a.run_time, 'MIN', a.bus_unit, b.cm_name, c.cd_name, NULL, NULL, decode(a.action,'delete','D',NULL)
				FROM elbaowner.tbl_elba_au_preferences a,
						 elbaowner.tbl_elba_contact_methods b,
             elbaowner.tbl_elba_contact_domains c
				WHERE a.contact_method = b.cm_ref
        AND a.contact_domain = c.cd_ref
        AND a.run_time > (SELECT last_marker_db1 FROM dbowner.tbl_sources WHERE source = 'ELB');
		ELSIF p_query_no = 8 THEN
			INSERT INTO dbowner.tbl_temp_items
				SELECT DISTINCT 'ELB_LST_' || a.lst_id, 'ELB', run_time, run_time, a.name, 'LST', NULL, NULL, NULL, NULL,
					b.town || ', ' || b.county, b.products, a.reference, f.firstname || '_' || f.lastname, NULL, NULL, b.host_inst, c.type_name, d.event_type,
					b.country_code, e.language, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, b.event_date,
					NULL, NULL, NULL, NULL, decode(a.status,'D','D',NULL)
				FROM elbaowner.tbl_elba_au_lists a,
						 elbaowner.tbl_elba_event_lists b,
						 elbaowner.tbl_elba_list_types c,
						 elbaowner.tbl_elba_event_types d,
						 elbaowner.tbl_elba_languages e,
						 elbaowner.tbl_elba_users f
				WHERE a.lst_id = b.lst_id
				AND a.lst_type = c.type_id
				AND b.event_type = d.event_type_id
				AND b.language = e.language_id (+)
				AND b.trainer = f.user_id (+)
				AND a.run_time > (SELECT last_marker_db1 FROM dbowner.tbl_sources WHERE source = 'ELB');
		ELSIF p_query_no = 9 THEN
			INSERT INTO dbowner.tbl_temp_subscriptions
				SELECT DISTINCT NULL, 'ELB_' || a.order_id, 'ELB', a.create_date, a.update_date, 'ELB_' || a.contact_id, 'ELB_' || a.product_id,
					'ELB_' || a.issn, a.required_by, NULL, NULL, c.description, a.title, a.acronym, NULL, d.description, NULL, NULL, NULL, NULL,
					NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, a.issues, NULL, NULL, NULL,
					NULL, decode(instr(volumes,' - '), 0, volumes, substr(volumes, 1, instr(volumes,' - '))), substr(volumes, 1, instr(volumes,' - ')),
					NULL, a.exhibition_id, a.number_of_copies, NULL, NULL, NULL, NULL, NULL, NULL
				FROM elbaowner.tbl_elba_promotional_orders a,
						 elbaowner.tbl_elba_contacts b,
						 elbaowner.tbl_elba_promotion_status c,
						 elbaowner.tbl_elba_promotion_types d
				WHERE a.contact_id = b.contact_id
				AND a.type_code = d.type_code
				AND a.status_code = c.status_code
				AND a.update_date > (SELECT last_marker_db1 FROM dbowner.tbl_sources WHERE source = 'ELB');
		ELSIF p_query_no = 10 THEN
			INSERT INTO dbowner.tbl_temp_items
				SELECT DISTINCT 'ELB_' || a.product_id, 'ELB', sysdate, sysdate, a.title, 'JOU', NULL, a.volumes, a.issues, NULL, NULL,
					'ELB_' || a.issn, a.acronym, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
					NULL, NULL, NULL, NULL, NULL, NULL, NULL
				FROM elbaowner.tbl_elba_promotional_orders a
				WHERE a.update_date > (SELECT last_marker_db1 FROM dbowner.tbl_sources WHERE source = 'ELB');
		ELSIF p_query_no = 11 THEN
		    INSERT INTO dbowner.tbl_temp_item_subjects
				SELECT DISTINCT NULL, 'ELB', 'ELB_' || a.code_source || '_' || a.code, 'ELB_0', sysdate, sysdate, a.code, a.description,
				a.parent_code, a.parent_description, NULL, NULL, a.top_level_code, a.top_level_description, a.hierarchy_pos
				FROM elbaowner.tbl_elba_interest_codes a;
		END IF;

	/*ELSIF p_source = 'ION' THEN

		IF p_query_no = 1 THEN
		   	INSERT INTO tbl_temp_parties
		   		SELECT NULL, 'ION', portal, 'ION_' || contactid, 'EUS', 'PER', NULL, NULL, NULL, users04, users04, lastcontact, NULL, NULL,
		   			NULL, fax2, users06, title, title, firstname, lastname, NULL, NULL, address1, address2, city, NULL, NULL, state, postalcode,
		   			country, NULL, country, NULL, NULL, NULL, NULL, users01, users00, users07, NULL, NULL, NULL, NULL, NULL, email1, emailformat,
		   			NULL, NULL, bounce1, NULL, NULL, NULL, NULL, NULL, NULL, assistant, NULL, NULL, NULL, NULL, NULL, usern08, NULL, NULL, NULL,
		   			NULL, NULL, users05, users02, NULL, NULL, usern02, usern00, usern01, NULL, NULL, this_marker_db1
				FROM tbl_temp_ion_contact, dbowner.tbl_sources b
				WHERE b.source = 'ION';
		ELSIF p_query_no = 2 THEN
			INSERT INTO tbl_temp_interests
				SELECT 'ION_' || chistoryid, 'ION', 'ION_' || contactid, this_marker_db1, this_marker_db1, 'EMA',
					trim(lower(param)), trim(lower(chs1)), code, decode(a.source,'Chemweb','Chemistry',a.source), NULL, decode(chs0, 'deleted', 'D', NULL)
				FROM tbl_temp_ion_chistory a, dbowner.tbl_sources b
				WHERE b.source = 'ION';
		ELSIF p_query_no = 3 THEN
			DELETE tbl_temp_ion_contact;
		ELSIF p_query_no = 4 THEN
			DELETE tbl_temp_ion_chistory;
		END IF;*/
	ELSIF p_source = 'DEL' THEN

		IF p_query_no = 1 THEN
		   	INSERT INTO dbowner.tbl_temp_parties
		   		SELECT decode(status,'D','D',NULL), 'DEL', source, 'DEL_' || customerreference, 'BUY', 'PER', created_date,
					customer_type_desc, amended_date, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
				--// JIRA 670
					-- ,NULL
					,title            --// usc_contact_title
					,NULL
					-- ,contact ,fullname
					,CASE
						WHEN NOT NVL(forname, initials) IS NULL THEN
							NVL(forname, initials)
						WHEN NVL(forname, initials) IS NULL AND surname IS NULL THEN
							CASE
								WHEN NOT NVL(UPPER(contact),'X') = NVL(UPPER(fullname),'X') AND NOT fullname IS NULL THEN
									contact
							ELSE
								NULL
							END
						ELSE
							NULL
					 END            --// usc_contact_firstname
					,CASE
						WHEN NOT surname IS NULL THEN
							surname
						WHEN NVL(forname, initials) IS NULL AND surname IS NULL AND NOT NVL(UPPER(contact),'X') = NVL(UPPER(fullname),'X') THEN
							NVL(fullname, contact)
						WHEN NVL(forname, initials) IS NULL AND surname IS NULL AND NVL(UPPER(contact),'X') = NVL(UPPER(fullname),'X') THEN
							fullname
						ELSE
							NULL
					 END            --// usc_contact_lastname
				--// End JIRA 670
					,NULL, address_type, address1, address2, address3, address4, address5, county_desc, postcode,
					country_desc, NULL, country_desc, countrygroup_desc, brick_desc_Nxxx, brick_desc_NNxx, NULL, NULL, NULL, NULL,
					phone, NULL, NULL, NULL, fax, email, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, bookshopflag,
					NULL, NULL, noofcopies, NULL, NULL, contact_flag, NULL, NULL, NULL, NULL, NULL, NULL, account_type, NULL, NULL,
					thirdpartymailing, data_protect, elseviergroupmail, NULL, NULL, run_time,
                    NULL, NULL
				FROM dbowner.tbl_temp_del_contactinfo;
		ELSIF p_query_no = 2 THEN
			INSERT INTO dbowner.tbl_temp_items
				SELECT 'DEL_' || pin, 'DEL', run_time, run_time, title, 'BOK', 'DEL_' || isbn,NULL, NULL, NULL, NULL, 'DEL_' || isbn13, subject_class_1,
					author, subject_1_desc, NULL, pub_person_desc, d_answer_code, product_type_desc, book_type_desc, medium_desc, pmc_cd, pmc_desc, pmg_cd,
					pmg_desc, d_division, imprint_desc, NULL, NULL, NULL, NULL, answer_date, NULL, NULL, NULL, publish_date, usa_publish_date, decode('D','D',NULL)
				FROM dbowner.tbl_temp_del_biblio
				WHERE isbn13 is not null;
	    ELSIF p_query_no = 3 THEN
			INSERT INTO dbowner.tbl_temp_items
				SELECT 'DEL_' ||pin	,'DEL',run_time	,run_time	,TITLE	,'JOU'	,'DEL_' || isbn	,substr(volume_issue,1,instr(volume_issue,'/',-1)-1),substr(volume_issue,instr(volume_issue,'/',+1)+1)	,
                                null	,null	,'DEL_' || JOURNAL_NO,JOURNAL_ACRONYM,null,subject_1_desc,supply_site_desc,pub_person_desc,d_answer_code,product_type_desc	,
                                book_type_desc	,medium_desc,pmc_cd	,pmc_desc,pmg_cd,pmg_desc,d_division,imprint_desc,null,null,full_set_flag,null,answer_date,
                                 null,null,null,publish_date	,usa_publish_date,decode('D','D',NULL)
				FROM dbowner.tbl_temp_del_biblio
				WHERE isbn13 is null;
		ELSIF p_query_no = 4 THEN
			INSERT INTO dbowner.tbl_temp_subscriptions
				SELECT decode(status,'D','D',NULL), 'DEL_' || invoicenumber || '_' || linenumber, 'DEL', hitdate, NULL, 'DEL_' || customeraccount,
					'DEL_' || pin, 'DEL_' || isbn13, NULL, NULL, linevaluegbp, NULL, NULL, NULL, NULL, ordertype_desc, NULL, substr(invoicenumber,2),
					'DEL_' || invoice_customer, 'DEL_' || statement_customer, cust_ref, NULL, NULL, NULL, NULL,promotion_code, NULL, gratisreason_desc, NULL, NULL,
					mailshotcode, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, linevalue, NULL, NULL, fiscalmonth, NULL, fiscalyear, ordersourcedesc,
					quantity, NULL, NULL, NULL, NULL, NULL, NULL
				FROM dbowner.tbl_temp_del_sales;
		ELSIF p_query_no = 5 THEN
			INSERT INTO dbowner.tbl_temp_item_subjects
				SELECT decode('D','D',NULL), 'DEL', 'DEL_' || pin || '_' || subject_class_1, 'DEL_' || pin, run_time, run_time,
					subject_class_1, subject_class_1_desc, NULL, NULL, NULL, NULL, NULL, NULL, NULL
				FROM dbowner.tbl_temp_del_biblio
				WHERE subject_class_1 is not null;
		ELSIF p_query_no = 6 THEN
			INSERT INTO dbowner.tbl_temp_item_subjects
				SELECT decode('D','D',NULL), 'DEL', 'DEL_' || pin || '_' || subject_class_2, 'DEL_' || pin, run_time, run_time,
					subject_class_2, subject_class_2_desc, NULL, NULL, NULL, NULL, NULL, NULL, NULL
				FROM dbowner.tbl_temp_del_biblio
				WHERE subject_class_2 is not null;
		ELSIF p_query_no = 7 THEN
			INSERT INTO dbowner.tbl_temp_item_subjects
				SELECT decode('D','D',NULL), 'DEL', 'DEL_' || pin || '_' || subject_class_3, 'DEL_' || pin, run_time, run_time,
					subject_class_3, subject_class_3_desc, NULL, NULL, NULL, NULL, NULL, NULL, NULL
				FROM dbowner.tbl_temp_del_biblio
				WHERE subject_class_3 is not null;
		ELSIF p_query_no = 8 THEN
			INSERT INTO dbowner.tbl_temp_item_subjects
				SELECT decode('D','D',NULL), 'DEL', 'DEL_' || pin || '_' || subject_class_4, 'DEL_' || pin, run_time, run_time,
					subject_class_4, subject_class_4_desc, NULL, NULL, NULL, NULL, NULL, NULL, NULL
				FROM dbowner.tbl_temp_del_biblio
				WHERE subject_class_4 is not null;
		ELSIF p_query_no = 9 THEN
			INSERT INTO dbowner.tbl_temp_item_subjects
				SELECT decode('D','D',NULL), 'DEL', 'DEL_' || pin || '_' || subject_class_5, 'DEL_' || pin, run_time, run_time,
					subject_class_5, subject_class_5_desc, NULL, NULL, NULL, NULL, NULL, NULL, NULL
				FROM dbowner.tbl_temp_del_biblio
				WHERE subject_class_5 is not null;
		ELSIF p_query_no = 10  THEN
		    INSERT INTO dbowner.TBL_TEMP_SUBSCRIPTIONS
		      SELECT distinct NULL,'DEL_SUB_' || doc_ref || '_' || sequence_no || '_' || customer_no || '_' || suffix || '_' || pin || '_' || trim(to_char(trunc(start_date),'DD-MON-YY'))
,'DEL'	,created_date	,run_time	,
                   'DEL_'|| customer_no	,'DEL_' || pin	,'DEL_' || journal_no,start_date,end_date,paid	,cancelled_reason_desc	,price_category_desc	,
                   price_category_group	,null,renewal_type,null,null, 'DEL_' || inv_cust,null,cust_ref,null,null,null,null,null,null,gratis_reason_desc	,
                   po_ref,null,null,null,null,activation_date,cancelled_date	,null,null,null,null,pub_price,currency_code,null,null,null,null,source_desc,
                   qty,last_cancelled_date,null	,null,null,null,null
              FROM  dbowner.tbl_temp_del_subs;
		ELSIF p_query_no = 11 THEN
			DELETE dbowner.tbl_temp_del_contactinfo;
		ELSIF p_query_no = 12 THEN
			DELETE dbowner.tbl_temp_del_biblio;
		ELSIF p_query_no = 13 THEN
			DELETE dbowner.tbl_temp_del_sales;
	    ELSIF p_query_no = 14 THEN
			DELETE dbowner.tbl_temp_del_subs;
		END IF;

	ELSIF p_source = 'ACT' THEN

		IF p_query_no = 1 THEN
			INSERT INTO dbowner.tbl_temp_parties
				SELECT DISTINCT NULL, 'ACT', NULL, 'ACT_' || contact_id, 'ACN', 'PER', NULL, NULL, NULL,NULL, NULL, NULL, NULL, NULL, NULL, NULL,
					NULL, title, title, first_name, surname, NULL, NULL, address1, address2, address3, NULL, city, county, postcode,
					trim(country), NULL, trim(country), brick, NULL, NULL, NULL, department_id, institution_id, NULL, NULL, NULL, NULL,
					NULL, NULL, email_address, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
					NULL, contact_post, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, decode(lower(contact_email),'yes','Y','N'),
					NULL, NULL, run_time,
                    NULL, NULL
				FROM dbowner.tbl_temp_act_custac
				WHERE run_time < (SELECT this_marker_db1 FROM dbowner.tbl_sources WHERE source = 'ACT');
		ELSIF p_query_no = 2 THEN
			INSERT INTO dbowner.tbl_temp_parties
				SELECT DISTINCT 'D', 'ACT', NULL, 'ACT_' || contact_id, 'ACN', 'PER', NULL, NULL, NULL,
					NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, title, title, first_name, surname, NULL, NULL, address1, address2, address3,
					NULL, city, county, postcode, trim(country), NULL, trim(country), brick, NULL, NULL, NULL, department_id, institution_id, NULL, NULL,
					NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, contact_post, NULL, NULL,
					NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, contact_email, NULL, NULL, run_time,
                    NULL, NULL
				FROM dbowner.tbl_temp_act_custac_deleted
				WHERE run_time < (SELECT this_marker_db1 FROM dbowner.tbl_sources WHERE source = 'ACT');
		ELSIF p_query_no = 3 THEN
			INSERT INTO dbowner.tbl_temp_interests
				SELECT DISTINCT 'ACT_' || contact_id || '_' || inspection_id, 'ACT', 'ACT_' || contact_id, run_time, run_time, 'IPC', isbn,
					isbn13, inspection_status, NULL, insp_orderdate, NULL
				FROM dbowner.tbl_temp_act_custai
				WHERE run_time < (SELECT this_marker_db1 FROM dbowner.tbl_sources WHERE source = 'ACT');
		ELSIF p_query_no = 4 THEN
			INSERT INTO dbowner.tbl_temp_interests
				SELECT DISTINCT 'ACT_' || contact_id || '_' || inspection_id, 'ACT', 'ACT_' || contact_id, run_time, run_time, 'IPC', isbn,
					NULL, inspection_status, NULL, insp_orderdate, 'D'
				FROM dbowner.tbl_temp_act_custai_deleted
				WHERE run_time < (SELECT this_marker_db1 FROM dbowner.tbl_sources WHERE source = 'ACT');
		ELSIF p_query_no = 5 THEN
			DELETE dbowner.tbl_temp_act_custac WHERE run_time < (SELECT this_marker_db1 FROM dbowner.tbl_sources WHERE source = 'ACT');
		ELSIF p_query_no = 6 THEN
			DELETE dbowner.tbl_temp_act_custai WHERE run_time < (SELECT this_marker_db1 FROM dbowner.tbl_sources WHERE source = 'ACT');
		ELSIF p_query_no = 7 THEN
			DELETE dbowner.tbl_temp_act_custac_deleted WHERE run_time < (SELECT this_marker_db1 FROM dbowner.tbl_sources WHERE source = 'ACT');
		ELSIF p_query_no = 8 THEN
			DELETE dbowner.tbl_temp_act_custai_deleted WHERE run_time < (SELECT this_marker_db1 FROM dbowner.tbl_sources WHERE source = 'ACT');
		END IF;

	ELSIF p_source = 'WR' THEN

		IF p_query_no = 1 THEN
		   	INSERT INTO dbowner.tbl_temp_parties
		   		SELECT decode(deleteflag,1,'D',NULL), 'WR', db, 'WR_' || db  || '_' || shopper_id, 'EUS', 'PER', created, shoppertype,
					modified, NULL, password, NULL, NULL, source, NULL, NULL, community, title, title, first_name, last_name, NULL, NULL,
					street, NULL, NULL, NULL, city, state, zip, country, NULL, country, NULL, NULL, NULL, NULL, NULL, institution,
					job_title, home_STD || home_phone, work_STD || work_phone, NULL, NULL, fax_STD || fax, email, OKtoHTMLEmail, NULL,
					NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, decode(OKtoMail,0,'N','Y'), NULL, NULL, NULL, NULL,
					NULL, NULL, ApprovedAcademic, NULL, NULL, decode(OKtoEmail,0,'N','Y'), decode(DataProtection,0,'Y','N'), NULL, NULL,
					NULL, modified,
                    NULL, NULL
				FROM dbowner.tbl_temp_wr_shopper;
		ELSIF p_query_no = 2 THEN
			INSERT INTO dbowner.tbl_temp_parties
				SELECT decode(deleteflag,1,'D',NULL), 'WR', db, 'WR_' || db  || '_ECN_' || id || '_' || accesslist, 'ECN', 'PER', DateAdded, customer, modified, NULL, NULL, DateLastMailed,
					NULL, NULL, NULL, NULL, NULL, title, title, first_name, last_name, NULL, NULL, address, NULL, NULL, NULL, NULL, NULL, NULL,
					NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, position, NULL, NULL, NULL, NULL, NULL, email, NULL, NULL, NULL,
					NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, accesslist, NULL, NULL,
					academic, press, author, NULL, NULL, NULL, NULL, NULL, modified,
                    NULL, NULL
				FROM dbowner.tbl_temp_wr_shoppereal;
		ELSIF p_query_no = 3 THEN
			INSERT INTO dbowner.tbl_temp_parties
				SELECT decode(deleteflag,1,'D',NULL), 'WR', db, 'WR_' || db  || '_REG_' || shopper_id, 'REG', 'PER', created, shoppertype, modified,
					NULL, password, NULL, NULL, source, NULL, NULL, community, null, NULL, first_name, last_name, NULL, NULL, NULL, NULL, NULL, NULL,
					NULL, NULL, NULL, country, NULL, country, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, email, OKtoHTMLEmail,
					NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
					NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, modified,
                    NULL, NULL
				FROM dbowner.tbl_temp_wr_registration;
		ELSIF p_query_no = 4 THEN
			INSERT INTO dbowner.tbl_temp_interests
				SELECT 'WR_' || db || '_EMA_' || shopper_id || '_' || list_id || '_' || community, 'WR', 'WR_' || db || '_' || shopper_id,
				created, modified, 'EMA', list_id, list, community, NULL, lastmailed, decode(deleteflag,1,'D',NULL)
				FROM dbowner.tbl_temp_wr_shopperel;
		ELSIF p_query_no = 5 THEN
			INSERT INTO dbowner.tbl_temp_items
				SELECT 'WR_' || db || '_' || productid, 'WR', NULL, NULL, title, 'BOK', isbn10, volumenumber, editionnumber, NULL, subtitle,
					isbn, series, NULL, serial, NULL, NULL, producttypedescription, titletype, NULL, NULL, NULL, NULL, NULL, NULL,
					profitcenterteamtypeid, imprint, pagecount, pagecountLE, NULL, fulleditionnumber, NULL, NULL, NULL, pubdateUK, pubdateUS,
					NULL, NULL
				FROM dbowner.tbl_temp_wr_book;
		ELSIF p_query_no = 6 THEN
			INSERT INTO dbowner.tbl_temp_subscriptions
				SELECT decode(deleteflag,1,'D',NULL), 'WR_' || db || '_REQ_' || shopper_id || '_' || isbn, 'WR', created, modified,
					'WR_' || db || '_' || shopper_id, isbn, NULL, NULL, NULL, NULL, requeststatus, NULL, notes, NULL, requesttype,
					numberofstudents, coursename, courselevel, changingtextbook, coursenumber, NULL, NULL, NULL, NULL, NULL, NULL, NULL, coursedate,
					NULL, adoptiondate, NULL, neededby, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
					NULL, NULL, NULL, NULL, NULL
				FROM dbowner.tbl_temp_wr_shopperr;
		ELSIF p_query_no = 7 THEN
			INSERT INTO dbowner.tbl_temp_subscriptions
				SELECT NULL, 'WR_' || a.db || '_' || order_id || item_id, 'WR', created, modified, 'WR_' || a.db || '_' || shopper_id, sku, b.isbn10, NULL,
				NULL, total_total, order_status, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
				NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, item_id, product_list_price, NULL, NULL, NULL,
				NULL, NULL, source_code, quantity, NULL, NULL, NULL, NULL, NULL, NULL
				FROM dbowner.tbl_temp_wr_receipt a,
						 dbowner.tbl_temp_wr_book b
				WHERE a.sku = b.isbn (+);
		ELSIF p_query_no = 8 THEN
			INSERT INTO dbowner.tbl_temp_item_subjects
				SELECT NULL, 'WR', 'WR_' || db || '_' || productid || '_' || subjectcodeid, 'WR_' || db || '_' ||  productid, NULL, NULL,
					subjectcode, description, NULL, NULL, NULL, NULL, NULL, NULL, priority
				FROM dbowner.tbl_temp_wr_essubjectcodes;
		ELSIF p_query_no = 9 THEN
			DELETE dbowner.tbl_temp_wr_shopper;
		ELSIF p_query_no = 10 THEN
			DELETE dbowner.tbl_temp_wr_shoppereal;
		ELSIF p_query_no = 11 THEN
			DELETE dbowner.tbl_temp_wr_shopperel;
		ELSIF p_query_no = 12 THEN
			DELETE dbowner.tbl_temp_wr_receipt;
		ELSIF p_query_no = 13 THEN
			DELETE dbowner.tbl_temp_wr_book;
		ELSIF p_query_no = 14 THEN
			DELETE dbowner.tbl_temp_wr_shopperr;
		ELSIF p_query_no = 15 THEN
			DELETE dbowner.tbl_temp_wr_essubjectcodes;
		ELSIF p_query_no = 16 THEN
			DELETE dbowner.tbl_temp_wr_registration;
		END IF;

  ELSIF p_source = 'TEC' THEN

		IF p_query_no = 1 THEN
		   	insert into dbowner.tbl_temp_parties
				select distinct NULL, 'TEC', locale, 'TEC_' || userid, 'EUS', 'PER', created, usertype, NULL, username, null,--pwd, 
        NULL, NULL, comments, NULL, faculty, NULL, salutation, NULL, fname, lname, NULL, NULL,
					inst_address1, inst_address2, NULL, NULL, inst_city, inst_state, inst_zip,
          CASE upper(inst_country)
          WHEN 'UK' THEN 'UNITED KINGDOM'
          WHEN 'VG' THEN 'BRITISH VIRGIN ISLANDS'
          WHEN 'FK' THEN 'FALKLAND ISLANDS (MALVINAS)'
          WHEN 'YU' THEN 'YUGOSLAVIA'
          WHEN 'RS' THEN 'SERBIA (TEC)'
          WHEN 'ME' THEN 'MONTENEGRO (TEC)'
          WHEN 'FX' THEN 'FRANCE METROPOL'
          WHEN 'SF' THEN 'SERBIA (TEC)'
          ELSE trim(countrydescription)
          END,
          upper(inst_country), NULL,
          NULL, NULL, NULL, NULL, department, inst_name, job_title, phone, NULL, NULL, faculty_link, NULL,
					inst_email, NULL, NULL, NULL, NULL, professor_link, 'TEC_REP_' || rep_id, NULL, NULL, NULL, NULL,
					personal_email, NULL, NULL, NULL, NULL, NULL, applybehalf, NULL, NULL, NULL, NULL, NULL, NULL,
					NULL, NULL, NULL, NULL, receivepromo, NULL, NULL, NULL, modified,
                    NULL, NULL
				from dbowner.tbl_temp_tec_user, dbowner.tbl_temp_tec_country
        where upper(trim(inst_country)) = countrycode(+);

		ELSIF p_query_no = 2 THEN
			insert into dbowner.tbl_temp_parties
				select distinct NULL, 'TEC', NULL, 'TEC_SHP_' || user_id || '_' || ship_id, 'STC', 'PER', this_marker_db1,
					NULL, NULL, name, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
					address1, address2, NULL, NULL, city, state, zip,
          CASE upper(country)
          WHEN 'UK' THEN 'UNITED KINGDOM'
          WHEN 'VG' THEN 'BRITISH VIRGIN ISLANDS'
          WHEN 'FK' THEN 'FALKLAND ISLANDS (MALVINAS)'
          WHEN 'YU' THEN 'YUGOSLAVIA'
          WHEN 'RS' THEN 'SERBIA (TEC)'
          WHEN 'ME' THEN 'MONTENEGRO (TEC)'
          WHEN 'FX' THEN 'FRANCE METROPOL'
          WHEN 'SF' THEN 'SERBIA (TEC)'
          ELSE trim(countrydescription)
          END,
          upper(country),
          NULL, NULL, NULL, NULL, NULL,
					department, institution, NULL, phone, NULL, NULL, NULL, NULL, email, NULL, NULL, NULL, NULL, NULL,
					'TEC_' || user_id, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
					NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, this_marker_db1,
                    NULL, NULL
				from dbowner.tbl_temp_tec_shippinginfo, dbowner.tbl_sources, dbowner.tbl_temp_tec_country
				where source = 'TEC' and upper(trim(country)) = countrycode(+);

		ELSIF p_query_no = 3 THEN
			insert into dbowner.tbl_temp_parties
				select distinct NULL, 'TEC', locale, 'TEC_REP_' || repid, 'REP', 'PER', this_marker_db1, NULL, NULL,
					repnum, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, fname, lname, NULL, NULL, NULL, NULL,
					NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, division, NULL, NULL, NULL,
					NULL, NULL, NULL, NULL, email, NULL, NULL, NULL, NULL, NULL, accountnum, NULL, NULL, NULL, NULL,
					emailsignature, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
					NULL, NULL, NULL, NULL, NULL, NULL, this_marker_db1,
                    NULL, NULL
				from dbowner.tbl_temp_tec_rep, dbowner.tbl_sources
				where source = 'TEC';

		ELSIF p_query_no = 4 THEN
			insert into dbowner.tbl_temp_interests
				select distinct 'TEC_DES_' || trim(userid) ||'_'|| isbn, 'TEC', 'TEC_' || trim(userid), created, this_marker_db1,
					'DES', 'TEC_' || isbn, NULL, NULL, NULL, NULL, NULL
				from dbowner.tbl_temp_tec_userdesktop, dbowner.tbl_sources
				where source = 'TEC';

		ELSIF p_query_no = 5 THEN
			insert into dbowner.tbl_temp_interests
				select distinct 'TEC_ADO_' || userid || '_' || isbn, 'TEC', 'TEC_' || userid, adoptiondate,
					this_marker_db1, 'ADO', 'TEC_' || isbn, adoptionsource, NULL, NULL, NULL, NULL
				from dbowner.tbl_temp_tec_adoption, dbowner.tbl_sources
				where source = 'TEC';

		ELSIF p_query_no = 6 THEN
			insert into dbowner.tbl_temp_interests
				select distinct 'TEC_ADA_' || userid || '_' || isbn, 'TEC', 'TEC_' || userid, startdate,
					this_marker_db1, 'ADA', 'TEC_' || isbn, canrenew, NULL, NULL, expirationdate, NULL
				from dbowner.tbl_temp_tec_adoptionaccess, dbowner.tbl_sources
				where source = 'TEC';

		ELSIF p_query_no = 7 THEN
			insert into dbowner.tbl_temp_interests
				select distinct 'TEC_INS_' || userid || '_' || isbn, 'TEC', 'TEC_' || userid, startdate,
					this_marker_db1, 'INS', 'TEC_' || isbn, NULL, NULL, canrenew, expirationdate, NULL
				from dbowner.tbl_temp_tec_inspectionaccess, dbowner.tbl_sources
				where source = 'TEC';

		ELSIF p_query_no = 8 THEN
			insert into dbowner.tbl_temp_interests
				select distinct 'TEC_REP_US_' || a.repid || '_' || a.subsitecode, 'TEC', 'TEC_' || a.repid,
					c.this_marker_db1, c.this_marker_db1, 'REP', a.subsitecode, b.subsitename, b.us, b.uk,
					b.division, NULL
				from dbowner.tbl_temp_tec_repmappingus a, dbowner.tbl_temp_tec_subjectarea b, dbowner.tbl_sources c
				where a.subsitecode = b.subsitecode and c.source = 'TEC';

		ELSIF p_query_no = 9 THEN
			insert into dbowner.tbl_temp_interests
				select distinct 'TEC_REP_UK_' || a.repid || '_' || a.countrycode, 'TEC', 'TEC_' || a.repid,
					c.this_marker_db1, c.this_marker_db1, 'REP', a.countrycode, b.countrydescription, a.region,
					NULL, NULL, a.active
				from dbowner.tbl_temp_tec_repmappinguk a, dbowner.tbl_temp_tec_country b, dbowner.tbl_sources c
				where a.countrycode = b.countrycode(+) and c.source = 'TEC';

		ELSIF p_query_no = 10 THEN
			insert into dbowner.tbl_temp_items
				select distinct 'TEC_' || a.productid, 'TEC', c.this_marker_db1, c.this_marker_db1, a.title, 'BOK',
					NULL, a.volumenumber, a.editionnumber, a.priceeuro, a.subtitle, 'TEC_' || a.isbn, a.isbn,
					a.primaryauthorname, a.ukprice, a.usprice, NULL, a.producttypedescription, a.titletype,
					a.subjectareauk, a.subjectareaus, a.pmc, NULL, a.pmg, NULL, a.division, a.imprint, a.pagecount,
					b.hasprintcopy, b.hascompanion, b.hasmanual, NULL, NULL, NULL, a.pubdateuk, a.pubdateus,
					NULL, NULL
				from dbowner.tbl_temp_tec_products a, dbowner.tbl_temp_tec_isbnresource b, dbowner.tbl_sources c
				where a.isbn = b.isbn(+) and c.source = 'TEC';

		ELSIF p_query_no = 11 THEN
			insert into dbowner.tbl_temp_subscriptions
				select distinct a.active, 'TEC_REQ_' || a.request_id, 'TEC', a.opendate, e.this_marker_db1,
					'TEC_' || a.user_id, 'TEC_' || a.isbn, NULL, a.opendate, a.closedate, NULL, c.description,
					a.sa_code, b.description, NULL, b.code, NULL,
					'TEC_SHP_' || a.user_id || '_' || a.ship_id, NULL, d.description, NULL, NULL, NULL, NULL, NULL,
					NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'TEC_REP_' || a.rep_id, NULL, NULL, NULL, NULL, NULL,
					NULL, NULL, NULL, NULL, NULL, NULL, NULL, a.source, NULL, NULL, NULL, NULL, NULL, NULL, NULL
				from dbowner.tbl_temp_tec_request a, dbowner.tbl_temp_tec_masterrequesttype b,
					(select * from dbowner.tbl_temp_tec_masterstatus where user_type = 'U') c,
					(select * from dbowner.tbl_temp_tec_masterstatus where user_type = 'R') d,
					dbowner.tbl_sources e
				where a.request_type = b.code(+)
				and a.status_user = c.code(+)
				and a.status_rep = d.code(+)
				and e.source = 'TEC';

		ELSIF p_query_no = 12 THEN
			insert into dbowner.tbl_temp_subscriptions
				select distinct NULL, 'TEC_RQD_' || request_id, 'TEC', this_marker_db1, this_marker_db1, NULL,
					'TEC_REQ_' || request_id, NULL, adoption_date, NULL, NULL, current_book_info, faculty_link,
					professor_link, NULL, NULL, course_num, course_name, NULL, course_level, courses_taught, NULL,
					NULL, NULL, NULL, department, NULL, NULL, po, NULL, NULL, NULL, num_students, course_date, NULL,
					NULL, rep_comments, shipping_option, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, quantity,
					NULL, NULL, NULL, NULL, NULL, NULL
				from dbowner.tbl_temp_tec_requestdetails, dbowner.tbl_sources
				where source = 'TEC';

    ELSIF p_query_no = 13 THEN
			insert into dbowner.tbl_temp_item_subjects
				select distinct NULL, 'TEC', 'TEC_ISBN_' || a.isbn || '_' || a.code,
          'TEC_' || a.isbn, b.this_marker_db1, b.this_marker_db1, a.code, NULL, NULL, NULL,
          NULL, NULL, NULL, NULL, NULL
        from dbowner.tbl_temp_tec_subjareaisbnmap a, dbowner.tbl_sources b
        where b.source = 'TEC';

		ELSIF p_query_no = 14 THEN
			insert into dbowner.tbl_temp_item_subjects
				select distinct NULL, 'TEC', 'TEC_CAT_' || a.categorycode || '_' || a.category || '_' || a.subjectcode,
          'TEC_', c.this_marker_db1, c.this_marker_db1, a.categorycode, a.category, a.subjectcode, a.subjectarea,
          NULL, b.us, b.uk, b.division, NULL
        from dbowner.tbl_temp_tec_categories a, dbowner.tbl_temp_tec_subjectarea b,
             dbowner.tbl_sources c
        where a.subjectcode = b.subsitecode(+)
        and c.source = 'TEC';

    ELSIF p_query_no = 15 THEN
    -- Identify the PARTY records that need to be marked as 'D'
      insert into dbowner.tbl_temp_parties (status, orig_system, orig_system_ref, party_type, run_time)
        select distinct 'D', orig_system, orig_party_ref, party_type, sysdate
        from dbowner.tbl_parties
        where orig_system = 'TEC'
        and orig_party_ref in
        (select orig_party_ref from dbowner.tbl_parties where orig_system = 'TEC'
         minus
         select orig_system_ref from dbowner.tbl_temp_parties where orig_system = 'TEC');

    ELSIF p_query_no = 16 THEN
    -- Identify the INTEREST records that need to be marked as 'D'
      insert into dbowner.tbl_temp_interests (orig_system_ref, orig_system, update_date, interest_type, status)
        select distinct orig_interest_ref, orig_system, sysdate, interest_type, 'D'
        from dbowner.tbl_interests
        where orig_system = 'TEC'
        and orig_interest_ref in
        (select orig_interest_ref from dbowner.tbl_interests where orig_system = 'TEC'
         minus
         select orig_system_ref from dbowner.tbl_temp_interests where orig_system = 'TEC');

    ELSIF p_query_no = 17 THEN
    -- Identify the ITEM records that need to be marked as 'D'
      insert into dbowner.tbl_temp_items (orig_system_ref, orig_system, update_date, item_type, status)
        select distinct orig_item_ref, orig_system, sysdate, item_type, 'D'
        from dbowner.tbl_items
        where orig_system = 'TEC'
        and orig_item_ref in
        (select orig_item_ref from dbowner.tbl_items where orig_system = 'TEC'
         minus
         select orig_system_ref from dbowner.tbl_temp_items where orig_system = 'TEC');

    ELSIF p_query_no = 18 THEN
    -- Identify the SUBSCRIPTION records that need to be marked as 'D'
      insert into dbowner.tbl_temp_subscriptions (status, orig_system_ref, orig_system, orig_update_date)
        select distinct 'D', orig_subscription_ref, orig_system, sysdate
        from dbowner.tbl_subscriptions
        where orig_system = 'TEC'
        and orig_subscription_ref in
        (select orig_subscription_ref from dbowner.tbl_subscriptions where orig_system = 'TEC'
         minus
         select orig_system_ref from dbowner.tbl_temp_subscriptions where orig_system = 'TEC');

    ELSIF p_query_no = 19 THEN
    -- Identify the ITEM_SUBJECTS records that need to be marked as 'D'
      insert into dbowner.tbl_temp_item_subjects (status, orig_system, orig_system_ref, item_ref, update_date)
        select distinct 'D', orig_system, orig_subject_ref, item_ref, sysdate
        from dbowner.tbl_item_subjects
        where orig_system = 'TEC'
        and orig_subject_ref in
        (select orig_subject_ref from dbowner.tbl_item_subjects where orig_system = 'TEC'
         minus
         select orig_system_ref from dbowner.tbl_temp_item_subjects where orig_system = 'TEC');

		END IF;

  ELSIF p_source = 'CRM' THEN


		IF p_query_no = 1 THEN
		   	insert into dbowner.tbl_temp_parties
select distinct
decode(a.x_delete,'Y','D','A'), 'CRM', a.x_tier, 'CRM_ACC_' || a.row_id, 'ACC', 'ORG', a.created, a.cust_stat_cd,
NULL, 'CRM_PRI_' || a.pr_postn_id, a.int_org_flg, a.x_ddp_start_dt, a.x_ddp_end_dt, a.x_buss_division, a.x_sales_division, a.accnt_type_cd,
a.ou_type_cd, NULL, NULL, a.name, NULL, NULL, 'CRM_ADD_' ||a.x_pr_addr_id, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'CRM_ACC_' || a.par_ou_id, a.name, NULL, a.main_ph_num, a.x_els_disc_eligibility,
a.x_els_ext_disc, a.x_els_ecomm_flg, a.x_ddp_eligible, NULL, NULL, NULL, NULL, NULL, 'CRM_CON_' || a.pr_con_id, 'CRM_ACC_' || a.master_ou_id, a.desc_text,
b.attrib_16 ||'_' ||a.emp_count, substr(CASE WHEN a.X_ELS_SIS_ID_BACKUP IS NULL AND a.X_ELS_CMX_ID IS NULL THEN a.LOC ELSE (CASE WHEN a.LOC NOT LIKE 'ECR%' THEN a.LOC ELSE a.X_ELS_SIS_ID_BACKUP END) END,1,15), NULL, a.x_ttl_subscr_value, a.x_ddp_percent, NULL, b.attrib_27, c.ann_rev,
NULL, b.attrib_15, a.x_ttl_researchers, b.attrib_01, a.x_rd_exp_bdgt, a.prtnr_flg, a.accnt_flg,
a.x_accnt_type_cd, a.x_agent_flg, NULL, a.expertise_cd, NULL, NULL, a.x_atg_subs_allow_flg, a.x_escl_dept_flg, NULL, a.last_upd,
                    a.X_ELS_CMX_ID, a.LOC
from
vw_s_org_ext a,
vw_s_org_ext_x b,
vw_s_org_ext_fnx c
where
a.row_id = b.par_row_id (+)
and a.row_id = c.par_row_id (+)
and (a.last_upd > (select last_marker_db1 from dbowner.tbl_sources where source = 'CRM')
  or b.last_upd > (select last_marker_db1 from dbowner.tbl_sources where source = 'CRM')
  or c.last_upd > (select last_marker_db1 from dbowner.tbl_sources where source = 'CRM'));

		ELSIF p_query_no = 2 THEN
			insert into dbowner.tbl_temp_parties
select distinct
decode(a.x_delete,'Y','D','A'), 'CRM', NULL, 'CRM_CON_' ||a.row_id, 'EUS', 'PER', a.created, a.cust_stat_cd,
NULL, a.pr_held_postn_id, NULL, NULL, NULL, b.name, a.emp_flg, a.mid_name, NULL, a.per_title, NULL, a.fst_name,
a.last_name, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, NULL, a.job_title, a.work_ph_num, a.cell_ph_num, NULL, NULL, a.fax_ph_num, substr(a.email_addr,1,250), NULL, NULL, NULL,
NULL, a.pr_affl_id, 'CRM_ACC_' || a.pr_dept_ou_id, a.per_title_suffix, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, a.pref_comm_media_cd, NULL, NULL, NULL, NULL, NULL, NULL, a.x_buying_influence, a.con_cd, NULL,
a.suppress_call_flg, decode(a.suppress_email_flg,'N','Y','N'), a.suppress_mail_flg, NULL, NULL, a.last_upd,
                    NULL, NULL
from
vw_s_contact a,
vw_s_lang b
where
a.pref_lang_id = b.row_id (+);

		ELSIF p_query_no = 3 THEN
			insert into dbowner.tbl_temp_parties
select distinct
NULL, 'CRM', NULL, 'CRM_PRO_' ||a.row_id, 'EUS', 'PER', a.created, NULL, a.promoted_ts, b.login, NULL, NULL, NULL,
a.source_type_cd, a.alias_name, a.source_name, NULL, NULL, NULL, a.fst_name, a.last_name, NULL, NULL, a.addr,
a.addr_line_2, NULL, NULL, a.city, a.state, a.zipcode, a.country, NULL, NULL, NULL, NULL, NULL, NULL,
a.con_pr_accnt_loc, a.con_pr_acct_name, a.job_title, NULL, NULL, NULL, NULL, NULL, substr(a.email_addr,1,250), NULL, NULL, NULL, NULL, NULL,
'CRM_CON_' ||a.promo_to_con_id, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, a.pref_comm_media_cd,
NULL, NULL, NULL, NULL, NULL, NULL, a.x_buying_influence, NULL, NULL, a.suppress_call_flg, decode(a.suppress_email_flg,'N','Y','N'),
a.suppress_mail_flg, NULL, NULL, a.last_upd,
                    NULL, NULL
from
vw_s_prsp_contact a,
vw_s_user b
where
a.promo_by_per_id = b.par_row_id (+);

		ELSIF p_query_no = 4 THEN
			insert into dbowner.tbl_temp_parties
select distinct
NULL, 'CRM', NULL, 'CRM_ADD_' || row_id, 'ADD', 'PER', created, NULL, NULL, NULL, NULL, start_dt, end_dt, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, addr_type_cd, addr, addr_line_2, addr_line_3, NULL, city, state, zipcode,
country, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, NULL, 'CRM_ACC_' || x_per_ou_id, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, last_upd,
                    NULL, NULL
from
vw_s_addr_per
where
x_per_ou_id is not null
and last_upd > (select last_marker_db1 from dbowner.tbl_sources where source = 'CRM');

		ELSIF p_query_no = 5 THEN
			insert into dbowner.tbl_temp_interests
select distinct
'CRM_POS_' || a.ou_ext_Id || '_' || b.row_id || '_' || c.person_id, 'CRM', 'CRM_ACC_' || a.ou_ext_id, b.created,
b.last_upd, 'POS', 'CRM_CON_' || c.person_id, b.postn_type_cd, b.name, to_char(c.start_dt,'dd/mm/yyyy hh24:mi:ss'),
c.end_dt, NULL
from
vw_s_accnt_postn a,
vw_s_postn b,
vw_s_party_per c
where
b.postn_type_cd in ('Account Manager','ADM','Product Sales Manager')
and a.position_id = b.row_id
and b.row_id = c.party_id;

    ELSIF p_query_no = 6 THEN
			insert into dbowner.tbl_temp_interests
select distinct
'CRM_PRI_' || a.row_id, 'CRM', 'CRM_CON_' || a.pr_emp_id, a.created, a.last_upd,
'PRI', b.par_party_id, a.postn_type_cd, a.name, a.par_row_id, NULL, NULL
from
vw_s_postn a, vw_s_party b
where
a.par_row_id = b.row_id;

		ELSIF p_query_no = 7 THEN
			insert into dbowner.tbl_temp_interests
select distinct
'CRM_DOM_' || row_Id, 'CRM', 'CRM_ACC_' || par_row_id, created, last_upd, 'DOM', name, NULL, NULL, NULL, NULL, NULL
from
vw_s_org_ext_xm
where
type = 'DOMAIN';

		ELSIF p_query_no = 8 THEN
			--// JIRA_1632
			/*
			insert into dbowner.tbl_temp_interests
			select distinct
			'CRM_CON_' || a.row_id, 'CRM', 'CRM_ACC_' || a.rel_party_id, a.created, a.last_upd, 'CON', 'CRM_ACC_' || a.party_id,
			NULL, to_char(a.start_dt,'dd/mm/yyyy hh24:mi:ss'), 'CRM_ACC_' || b.x_pr_aff_account_from, a.end_dt, NULL
			from
			vw_s_party_rel a,
			vw_s_party b
			where
			a.rel_party_id = b.row_id
			and a.rel_type_cd = 'Consortia-Account'
			and (a.last_upd > (select last_marker_db1 from dbowner.tbl_sources where source = 'CRM')
			  or b.last_upd > (select last_marker_db1 from dbowner.tbl_sources where source = 'CRM'));
			***/
			--// JIRA_1632
			insert into dbowner.tbl_temp_interests
			WITH d AS
				(SELECT	/*+ CACHE */ last_marker_db1
				 FROM	dbowner.tbl_sources
				 WHERE	source	= 'CRM'
				)
			select	distinct
				'CRM_CON_' || a.row_id, 'CRM', 'CRM_ACC_' || a.rel_party_id, a.created, a.last_upd, 'CON', 'CRM_ACC_' || a.party_id
				,NULL, to_char(a.start_dt,'dd/mm/yyyy hh24:mi:ss'), 'CRM_ACC_' || b.x_pr_aff_account_from, a.end_dt, NULL
			from	 vw_s_party_rel	a
				,vw_s_party	b
				,d
			where	a.rel_party_id	= b.row_id
			and	a.rel_type_cd	= 'Consortia-Account'
			and	(a.last_upd	> d.last_marker_db1	OR
				 b.last_upd	> d.last_marker_db1
				 );

		ELSIF p_query_no = 9 THEN
			insert into dbowner.tbl_temp_interests
select distinct
'CRM_SIC_' || b.ou_id || '_' || b.indust_id, 'CRM', 'CRM_ACC_' || a.row_id, NULL, NULL, 'SIC', c.sic, c.name,
c.sub_type, a.pr_indust_id, NULL, NULL
from
vw_s_org_ext a,
vw_s_org_indust b,
vw_s_indust c
where
a.row_id = b.ou_id
and b.indust_id = c.row_id;

		ELSIF p_query_no = 10 THEN
			insert into dbowner.tbl_temp_interests
select distinct
'CRM_ACN_' || a.row_Id, 'CRM', 'CRM_ACC_' || a.party_id, a.created, a.last_upd, 'ACN', 'CRM_CON_' || a.person_id,
NULL, to_char(a.start_dt,'dd/mm/yyyy hh24:mi:ss'), 'CRM_CON_' || b.pr_con_id, a.end_dt, NULL
from
vw_s_party_per a,
vw_s_org_ext b
where
a.party_id = b.row_id (+)
and (a.last_upd > (select last_marker_db1 from dbowner.tbl_sources where source = 'CRM')
  or b.last_upd > (select last_marker_db1 from dbowner.tbl_sources where source = 'CRM'));

		ELSIF p_query_no = 11 THEN
			insert into dbowner.tbl_temp_interests
select distinct
'CRM_OPC_' || a.row_id, 'CRM', 'CRM_CON_' || a.per_id, a.created, a.last_upd, 'OPC', 'CRM_OPP_' || a.opty_id,
a.x_ws_influence_role, NULL, 'CRM_CON_' || b.pr_con_id, NULL, NULL
from
vw_s_opty_con a,
vw_s_opty b
where
a.opty_id = b.row_id;

		ELSIF p_query_no = 12 THEN
			insert into dbowner.tbl_temp_interests
select distinct
'CRM_EXT_' || row_id, 'CRM', 'CRM_ACC_' || par_row_id, created, last_upd, 'EXT', name, attrib_34, NULL, NULL, NULL, NULL
from
vw_s_org_ext_xm
where
type = 'ExternalAccountNr';

		ELSIF p_query_no = 13 THEN
			insert into dbowner.tbl_temp_interests
select distinct
'CRM_LIM_' || b.row_id, 'CRM',
case when b.con_per_id is not null then 'CRM_CON_' || b.con_per_id
     when b.prsp_con_per_id is not null then 'CRM_PRO_' || b.prsp_con_per_id
     else NULL
     end,
b.created, b.last_upd, 'LIM', a.name, a.file_src_type, a.subtype_cd, to_char(a.file_date,'dd/mm/yyyy hh24:mi:ss'),
a.expiration_dt, decode(a.status_cd,'Active','A','D')
from
vw_s_call_lst a,
vw_s_call_lst_con b
where
a.row_id = b.call_lst_id;

		ELSIF p_query_no = 14 THEN
			insert into dbowner.tbl_temp_interests
select distinct
'CRM_POF_' || row_id, 'CRM', 'CRM_PRO_' || prod_id, created, last_upd, 'POF', system_cd, NULL, NULL, NULL, NULL, NULL
from
vw_s_proc_sys;

		ELSIF p_query_no = 15 THEN
			insert into dbowner.tbl_temp_interests
select distinct
'CRM_CLK_' || ROW_ID, 'CRM', 'CRM_CAM_' || SRC_ID, CREATED, LAST_UPD, 'CLK',
'CRM_OFF_' || DCP_ID, NULL, NULL, NULL, NULL, NULL
from
VW_S_SRC_DCP;

		ELSIF p_query_no = 16 THEN
			insert into dbowner.tbl_temp_interests
select distinct
'CRM_MAC_' || ROW_ID, 'CRM', 'CRM_ACC_' || OU_EXT_ID, CREATED, LAST_UPD, 'MAC', 'CRM_CAM_' || SRC_ID,
COMMENTS, ATTRIB_01, REV_NUM, START_DT, NULL
from
VW_S_ACCNT_SRC;

		ELSIF p_query_no = 17 THEN
			insert into dbowner.tbl_temp_interests
SELECT DISTINCT
'CRM_STG_' || ROW_ID, 'CRM', 'CRM_OPP_' || RECORD_ID, CREATED, LAST_UPD, 'STG', OLD_VAL, NEW_VAL,
LAG(OPERATION_DT, 1) OVER (PARTITION BY RECORD_ID, FIELD_NAME ORDER BY OPERATION_DT) OLD_VALUE_DATE,
NULL, OPERATION_DT, NULL
FROM
VW_S_AUDIT_ITEM
WHERE BUSCOMP_NAME = 'Opportunity'
AND FIELD_NAME = 'Sales Stage';

		ELSIF p_query_no = 18 THEN
			insert into dbowner.tbl_temp_items
select distinct
'CRM_PRO_' || a.row_Id, 'CRM', a.created, a.last_upd, a.name,
CASE WHEN a.gtin IS NOT NULL THEN 'BOK'
     WHEN a.vendr_part_num IS NOT NULL THEN 'JOU'
     ELSE NULL
     END,
'CRM_LN_' || a.pr_prod_ln_id, NULL, NULL, NULL, a.desc_text, nvl(a.gtin, a.vendr_part_num), NULL, NULL, NULL, b.name, NULL, b.x_type,
a.x_renewable, a.x_rolling_year_flg, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, NULL, NULL, NULL, NULL
from
vw_s_prod_int a,
vw_s_prod_ln b
where a.pr_prod_ln_id = b.row_id (+)
and (a.last_upd > (select last_marker_db1 from dbowner.tbl_sources where source = 'CRM')
  or b.last_upd > (select last_marker_db1 from dbowner.tbl_sources where source = 'CRM'));

    ELSIF p_query_no = 19 THEN
                insert into dbowner.tbl_temp_subscriptions
                select distinct
                              decode(a.x_delete,'Y','D','A'),
                              'CRM_AGG_'|| a.row_id,
                              'CRM',
                              a.created,
                              a.last_upd,
                              'CRM_ACC_' || a.target_ou_id,
                              'CRM_AGG_' || a.par_agree_id,
                              'CRM_ADD_' || a.agree_addr_id,
                              a.eff_start_dt,
                              a.eff_end_dt,
                              NULL,
                              a.stat_cd,
                              a.name,
                              a.x_offering,
                              NULL,
                              a.agree_cd,
                              a.ship_to_addr_id,
                              a.bill_to_addr_id,
                              a.x_ren_type,
                              a.agr_active_flg,
                              a.x_sales_type,
                              NULL,
                              a.x_max_user,
                              NULL,
                              NULL,
                              'CRM_ACC_' || a.x_consortium_id,
                              NULL,
                              'CRM_PRI_' || a.sales_rep_postn_id,
                              NULL,
                              c.name,
                              a.x_ren_stat_cd,
                              'CRM_CON_' || a.ship_to_con_id,
                              'CRM_CON_' || a.bill_to_con_id,
                              a.x_subscr_period_start_dt,
                              a.x_subscr_period_end_dt,
                              a.rev_num,
                              b.x_attrib_45,
                              'CRM_CON_' || a.con_per_id,
                              a.agree_num,
                              a.x_total_net_inv_value,
                              trim(a.bl_curcy_cd),
                              NULL,
                              NULL,
                              NULL,
                              b.x_attrib_42,
                              'CRM_OPP_' || a.opty_id,
                              NULL,
                              a.x_ren_exp_compl_dt,
                              NULL,
                              NULL,
                              NULL,
                              NULL,
                              NULL
                FROM vw_s_doc_agree a,
                     vw_cx_doc_agree_x b,
                     vw_s_pri_lst c
                where a.row_id = b.par_row_id (+)
                and a.pri_lst_id = c.row_id (+)
                and (a.last_upd > (select last_marker_db1 from dbowner.tbl_sources where source = 'CRM')
                  or b.last_upd > (select last_marker_db1 from dbowner.tbl_sources where source = 'CRM')
                  or c.last_upd > (select last_marker_db1 from dbowner.tbl_sources where source = 'CRM'));

		--// JIRA_1632
		--INSERT
		--INTO	dbowner.tbl_temp_subscriptions
    --            WITH	r	AS
		--		(SELECT	/*+ CACHE */ last_marker_db1
		--		 FROM	dbowner.tbl_sources
		--		 WHERE	source = 'CRM')
		--SELECT	-- distinct
		--	decode(a.x_delete,'Y','D','A'),
		--	'CRM_AGG_'|| a.row_id,
		--	'CRM',
		--	a.created,
		--	a.last_upd,
		--	'CRM_ACC_' || a.target_ou_id,
		--	'CRM_AGG_' || a.par_agree_id,
		--	'CRM_ADD_' || a.agree_addr_id,
		--	a.eff_start_dt,
		--	a.eff_end_dt,
		--	NULL,
		--	a.stat_cd,
		--	a.name,
		--	a.x_offering,
		--	NULL,
		--	a.agree_cd,
		--	a.ship_to_addr_id,
		--	a.bill_to_addr_id,
		--	a.x_ren_type,
		--	a.agr_active_flg,
		--	a.x_sales_type,
		--	NULL,
		--	a.x_max_user,
		--	NULL,
		--	NULL,
		--	'CRM_ACC_' || a.x_consortium_id,
		--	NULL,
		--	'CRM_PRI_' || a.sales_rep_postn_id,
		--	NULL,
		--	c.name,
		--	a.x_ren_stat_cd,
		--	'CRM_CON_' || a.ship_to_con_id,
		--	'CRM_CON_' || a.bill_to_con_id,
		--	a.x_subscr_period_start_dt,
		--	a.x_subscr_period_end_dt,
		--	a.rev_num,
		--	b.x_attrib_45,
		--	'CRM_CON_' || a.con_per_id,
		--	a.agree_num,
		--	a.x_total_net_inv_value,
		--	trim(a.bl_curcy_cd),
		--	NULL,
		--	NULL,
		--	NULL,
		--	b.x_attrib_42,
		--	'CRM_OPP_' || a.opty_id,
		--	NULL,
		--	a.x_ren_exp_compl_dt,
		--	NULL,
		--	NULL,
		--	NULL,
		--	NULL,
		--	NULL
		--FROM	 vw_s_doc_agree		a
		--	,vw_cx_doc_agree_x	b
		--	,vw_s_pri_lst		c
		--	,r
		--WHERE	b.par_row_id	(+)	= a.row_id
		--AND	c.row_id	(+)	= a.pri_lst_id
		--AND NOT	(a.last_upd	> r.last_marker_db1	OR
		--	 b.last_upd	> r.last_marker_db1	OR
		--	 c.last_upd	> r.last_marker_db1)
		--AND EXISTS
		--	(SELECT	1
		--	 FROM	vw_s_agree_item	ai		-- Agreement Lines has changed
		--	 WHERE	ai.doc_agree_id	= a.row_id
		--	 AND	ai.last_upd	> r.last_marker_db1);

-- Bug Fix -- MARKAU-5563
INSERT INTO dbowner.tbl_temp_subscriptions
  SELECT  -- distinct
                     decode(a.x_delete,'Y','D','A'),
                     'CRM_AGG_'|| a.row_id,
                     'CRM',
                     a.created,
                     a.last_upd,
                     'CRM_ACC_' || a.target_ou_id,
                     'CRM_AGG_' || a.par_agree_id,
                     'CRM_ADD_' || a.agree_addr_id,
                     a.eff_start_dt,
                     a.eff_end_dt,
                     NULL,
                     a.stat_cd,
                     a.name,
                     a.x_offering,
                     NULL,
                     a.agree_cd,
                     a.ship_to_addr_id,
                     a.bill_to_addr_id,
                     a.x_ren_type,
                     a.agr_active_flg,
                     a.x_sales_type,
                     NULL,
                     a.x_max_user,
                     NULL,
                     NULL,
                     'CRM_ACC_' || a.x_consortium_id,
                     NULL,
                     'CRM_PRI_' || a.sales_rep_postn_id,
                     NULL,
                     c.name,
                     a.x_ren_stat_cd,
                     'CRM_CON_' || a.ship_to_con_id,
                     'CRM_CON_' || a.bill_to_con_id,
                     a.x_subscr_period_start_dt,
                     a.x_subscr_period_end_dt,
                     a.rev_num,
                     b.x_attrib_45,
                     'CRM_CON_' || a.con_per_id,
                     a.agree_num,
                     a.x_total_net_inv_value,
                     trim(a.bl_curcy_cd),
                     NULL,
                     NULL,
                     NULL,
                     b.x_attrib_42,
                     'CRM_OPP_' || a.opty_id,
                     NULL,
                     a.x_ren_exp_compl_dt,
                     NULL,
                     NULL,
                     NULL,
                     NULL,
                     NULL
             FROM     vw_s_doc_agree         a
                     ,vw_cx_doc_agree_x      b
                     ,vw_s_pri_lst           c
                     ,vw_s_agree_item        ai 
             WHERE   b.par_row_id    (+)     = a.row_id
             AND     c.row_id        (+)     = a.pri_lst_id
             AND     ai.doc_agree_id = a.row_id
             AND     ai.last_upd     > (select last_marker_db1 from dbowner.tbl_sources where source = 'CRM');

		ELSIF p_query_no = 20 THEN
			insert into dbowner.tbl_temp_subscriptions
select distinct
NULL, 'CRM_AGL_' || a.row_id, 'CRM', a.created, a.last_upd, 'CRM_AGG_' || a.doc_agree_id, 'CRM_PRO_' || a.prod_int_id,
'CRM_PRO_' || a.cvrd_item_prod_id, a.eff_start_dt, a.eff_end_dt, a.x_orig_content_fee, a.itm_stat_cd, 'CRM_INV_' || a.x_invoice_id, 'CRM_INT_' || a.prod_int_id,
a.net_pri, a.x_subscr_type, 'CRM_AGL_' || a.par_agree_item_id, NULL, a.x_entitled, NULL, a.x_pricing_opt, a.x_con_search,
a.x_user_num, a.x_tax_amount, a.x_price_cap, NULL, NULL, a.x_avg_download, NULL, a.x_non_pro_eonly_fee, a.x_non_pro_cont_fee, a.x_original_ext_invoice_num,
NULL, NULL, NULL, NULL, a.x_price_override_flg, a.x_available_online, a.ln_num, a.x_price_cap_prev, a.agree_itm_curcy_cd, a.x_agent_net_price, a.x_content_fee,
a.x_eonly_fee, NULL, a.action_cd, a.qty_req, NULL, NULL, a.x_pro_content_fee, NULL, NULL, NULL
from
vw_s_agree_item a
where a.last_upd > (select last_marker_db1 from dbowner.tbl_sources where source = 'CRM');

			--// JIRA_1632
			INSERT
			INTO	dbowner.tbl_temp_subscriptions
			WITH	r	AS
				(SELECT	/*+ CACHE */ last_marker_db1
				 FROM	dbowner.tbl_sources
				 WHERE	source = 'CRM')
			SELECT	-- DISTINCT
				decode(b.x_delete,'Y','D','A'), 'CRM_AGL_' || a.row_id, 'CRM', a.created, a.last_upd, 'CRM_AGG_' || a.doc_agree_id, 'CRM_PRO_' || a.prod_int_id,
				'CRM_PRO_' || a.cvrd_item_prod_id, a.eff_start_dt, a.eff_end_dt, a.x_orig_content_fee, a.itm_stat_cd, 'CRM_INV_' || a.x_invoice_id, 'CRM_INT_' || a.prod_int_id,
				a.net_pri, a.x_subscr_type, 'CRM_AGL_' || a.par_agree_item_id, NULL, a.x_entitled, NULL, a.x_pricing_opt, a.x_con_search,
				a.x_user_num, a.x_tax_amount, a.x_price_cap, NULL, NULL, a.x_avg_download, NULL, a.x_non_pro_eonly_fee, a.x_non_pro_cont_fee, a.x_original_ext_invoice_num,
				NULL, NULL, NULL, NULL, a.x_price_override_flg, a.x_available_online, a.ln_num, a.x_price_cap_prev, a.agree_itm_curcy_cd, a.x_agent_net_price, a.x_content_fee,
				a.x_eonly_fee, NULL, a.action_cd, a.qty_req, NULL, NULL, a.x_pro_content_fee, NULL, NULL, NULL
			FROM	 vw_s_agree_item	a
				,vw_s_doc_agree		b		-- Agreement
				,r
			WHERE	a.last_upd	< r.last_marker_db1
			AND	b.row_id	= a.doc_agree_id
			AND	b.last_upd	> r.last_marker_db1	-- Agreement has changed
			;

		ELSIF p_query_no = 21 THEN
			insert into dbowner.tbl_temp_subscriptions
select distinct
NULL, 'CRM_OPP_' || a.row_id, 'CRM', a.created, a.last_upd, 'CRM_ACC_' || a.pr_dept_ou_id, 'CRM_OFF_' || a.x_offer_id,
'CRM_RES_' || a.x_response_id, a.asgn_dt, a.x_agr_end_dt, a.sum_revn_amt, b.name, a.name, a.desc_text, NULL, a.opty_cd,
NULL, NULL, NULL, b.stage_status_cd, NULL, NULL, NULL, NULL, NULL, a.sum_win_prob, NULL, NULL, NULL, a.x_els_closure_desc, a.reason_won_lost_cd, NULL, NULL, NULL,
a.sum_effective_dt, NULL, NULL, NULL, NULL, NULL, trim(a.curcy_cd), NULL, NULL, NULL, NULL, 'CRM_CAM_' || a.pr_src_id, NULL,
NULL, NULL, NULL, NULL, NULL, NULL
from
vw_s_opty a,
vw_s_stg b
where
a.curr_stg_id = b.row_id (+)
and (a.last_upd > (select last_marker_db1 from dbowner.tbl_sources where source = 'CRM')
  or b.last_upd > (select last_marker_db1 from dbowner.tbl_sources where source = 'CRM'));

		ELSIF p_query_no = 22 THEN
			insert into dbowner.tbl_temp_subscriptions
select distinct
NULL, 'CRM_CAM_' || ROW_ID, 'CRM', CREATED, LAST_UPD, 'CRM_OFF_' || PR_DMND_CRT_PRG_ID,
'CRM_LN_' || PR_PROD_LN_ID, 'CRM_INT_' || PR_PROD_INT_ID, PROG_START_DT, PROG_END_DT, NULL,
STATUS_CD, NAME, DESC_TEXT, NULL, CUST_TRGT_TYPE_CD, NULL, CAMP_TYPE_CD, SUB_TYPE, CAMP_CAT_CD,
X_CAMP_OFFERING, NULL, NULL, NULL, NULL, X_ELS_SALES_SUP_MTRLS, NULL, NULL, SRC_NUM, X_ELS_MARK_GRP,
X_CAMP_MARKETING_UNIT, X_EXTERNAL_REF, NULL, NULL, NULL, X_ELS_MARK_GRP_CONTACT, NULL,
X_ELS_MARK_OPS_CONTACT, NULL, REQUESTED_BDGT_AMT, REVN_GOAL_CURCY_CD, NULL, NULL, NULL, NULL,
X_CAMP_PRODUCT, NULL, NULL, NULL, NULL, NULL, NULL, NULL
from
VW_S_SRC;

		ELSIF p_query_no = 23 THEN
			insert into dbowner.tbl_temp_subscriptions
select distinct
NULL, 'CRM_OFF_' || ROW_ID, 'CRM', CREATED, LAST_UPD, NULL, NULL, NULL, START_DT, END_DT, NULL,
OFFER_TYPE_CD, NAME, COMMENTS, NULL, MEDIA_TYPE_CD, NULL, NULL, NULL, X_EXTERNAL_REF,
X_ELS_AUDIENCE, NULL, NULL, NULL, NULL, X_ELS_COMM_TYPE, NULL, NULL, OFFER_NUM, NULL, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, X_CAMPAIGN_URL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL
from
VW_S_DMND_CRTN_PRG;

		ELSIF p_query_no = 24 THEN
			insert into dbowner.tbl_temp_subscriptions
select distinct
NULL, 'CRM_RES_' || ROW_ID, 'CRM', CREATED, LAST_UPD, 'CRM_ACC_' || ACCNT_ID, 'CRM_CON_' || PR_CON_ID,
'CRM_PRO_' || PRSP_CON_ID, COMM_DATE, NULL, NULL, STATUS_CD, 'CRM_PRO_' || X_PR_PROD_LN_ID, DESC_TEXT,
NULL, RESP_TYPE_CD, 'CRM_CAM_' || SRC_ID, 'CRM_OFF_' || DCP_ID, 'CRM_OPP_' || PR_OPTY_ID,
'CRM_CLK_' || CAMP_MEDIA_ID, NULL, NULL, NULL, NULL, NULL, CS_SCORE, NULL, NULL, OFFER_NUM,
'CRM_EVT_' || SRC_EVT_ID, OUTCOME_CD, X_EXT_RESPONSE_CD, NULL, NULL, NULL, NULL, NULL, NULL, COMM_UID, NULL, NULL,
NULL, NULL, NULL, NULL, X_SOURCE_CD, NULL, NULL, NULL, NULL, NULL, NULL, NULL
from
VW_S_COMMUNICATION;

		ELSIF p_query_no = 25 THEN
			insert into dbowner.tbl_temp_subscriptions
select distinct
NULL, 'CRM_INV_' || a.row_id, 'CRM', a.created, a.last_upd, 'CRM_ACC_' || a.accnt_id, NULL,
NULL, a.invc_dt, NULL, a.ttl_invc_amt, a.status_cd, NULL, NULL,
a.tax_amt, a.invc_type_cd, NULL, NULL, NULL, a.x_invoiced, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, a.vendr_invoice_num, NULL, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, a.invc_num, NULL, a.amt_curcy_cd, NULL, NULL,
NULL, NULL, 'CRM_AGG_' || a.agreement_id, NULL, NULL, NULL, NULL, NULL, NULL, NULL
from
vw_s_invoice a;

		ELSIF p_query_no = 26 THEN
			insert into dbowner.tbl_temp_subscriptions
select distinct
NULL, 'CRM_REV_' || a.row_id, 'CRM', a.created, a.last_upd, 'CRM_OPP_' || a.opty_id, 'CRM_LN_' || a.prod_ln_id,
NULL, b.attrib_12, b.attrib_13, a.revn_amt, a.class_cd, NULL, NULL,
NULL, a.type_cd, NULL, NULL, NULL, a.summary_flg, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, a.revn_amt_curcy_cd, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
from
vw_s_revn a, vw_s_revn_x b
where a.row_id = b.par_row_id (+);

    ELSIF p_query_no = 27 THEN

          insert into dbowner.tbl_temp_items
          select DISTINCT
          'CRM_LN_'|| a.row_id AS orig_item_ref,
          'CRM' AS orig_system ,
          a.created AS orig_create_date,
          a.last_upd AS orig_update_date,
          a.name AS name,
          'PRO' AS item_type,
          'CRM_LN_' || par_prod_ln_id AS parent_ref,
          null AS volume,
          null AS issue,
          null AS year,
          a.desc_text AS description,
          null AS identifier,
          null AS code,
          null AS authors,
          null AS show_status,
          null AS site,
          null AS publisher,
          a.x_type AS type,
          null AS sub_type,
          null AS binding,
          null AS medium,
          null AS pmc_code,
          null AS pmc_descr,
          null AS pmg_code,
          null AS pmg_descr,
          null AS class,
          null AS imprint,
          null AS no_of_pages,
          null AS page_numbers,
          null AS item_milestone,
          null AS issue_milestone,
          null AS issue_date,
          null AS delivery_date,
          null AS receive_date,
          null AS init_pub_date,
          null AS last_pub_date,
          null AS added_date,
          null AS record_status
          from
          vw_s_prod_ln a;

    ELSIF p_query_no = 28 THEN
          insert into dbowner.tbl_temp_items
          select DISTINCT
          'CRM_INT_' || a.row_Id AS orig_item_ref,
          'CRM' AS orig_system ,
          a.created AS orig_create_date,
          a.last_upd AS orig_update_date,
          a.name/* (Product Name)*/ AS name,
           CASE WHEN a.gtin IS NOT NULL THEN 'BOK' WHEN a.vendr_part_num IS NOT NULL THEN 'JOU' ELSE NULL END AS item_type,
          'CRM_LN_'  || pr_prod_ln_id AS parent_ref,
          null AS volume,
          null AS issue,
          null AS year,
          a.desc_text AS description,
          nvl(nvl(a.gtin,a.vendr_part_num),a.gtin) AS identifier,
          a.x_isbn_ten AS code,
          null AS authors,
          null AS show_status,
          null AS site,
          null AS publisher,
          null AS type,
          a.x_renewable /*(Renewable)*/ AS sub_type,
          a.x_rolling_year_flg /*(Rolling Year Flag)*/ AS binding,
          null AS medium,
          null AS pmc_code,
          null AS pmc_descr,
          null AS pmg_code,
          null AS pmg_descr,
          null AS class,
          b.name AS imprint,
          null AS no_of_pages,
          null AS page_numbers,
          null AS item_milestone,
          null AS issue_milestone,
          null AS issue_date,
          null AS delivery_date,
          null AS receive_date,
          null AS init_pub_date,
          null AS last_pub_date,
          null AS added_date,
          null AS record_status
          from
          vw_s_prod_int a, vw_cx_els_prod_inv b
          where a.x_els_inv_grp_id = b.row_id (+);

    ELSIF p_query_no = 29 THEN
    -- Identify the PARTY records that need to be marked as 'D'
      insert into dbowner.tbl_temp_parties (status, orig_system, orig_system_ref, party_type, run_time)
        select distinct 'D', orig_system, orig_party_ref, party_type, sysdate
        from dbowner.tbl_parties
        where orig_system = 'CRM'
        and orig_party_ref in
        (select orig_party_ref from dbowner.tbl_parties
         where orig_system = 'CRM' and record_status <> 'D' and party_type = 'EUS'
         minus
         select orig_system_ref from dbowner.tbl_temp_parties where orig_system = 'CRM' and party_type = 'EUS');

    ELSIF p_query_no = 30 THEN
    -- Identify the INTEREST records that need to be marked as 'D'
      insert into dbowner.tbl_temp_interests (orig_system_ref, orig_system, update_date, interest_type, status)
        select distinct orig_interest_ref, orig_system, sysdate, interest_type, 'D'
        from dbowner.tbl_interests
        where orig_system = 'CRM'
        and orig_interest_ref in
        (select orig_interest_ref from dbowner.tbl_interests where orig_system = 'CRM' and record_status <> 'D'
         and interest_type in ('POS','PRI','DOM','SIC','OPC','EXT','LIM','POF','CLK','MAC','STG')
         minus
         select orig_system_ref from dbowner.tbl_temp_interests where orig_system = 'CRM'
         and interest_type in ('POS','PRI','DOM','SIC','OPC','EXT','LIM','POF','CLK','MAC','STG'));

    ELSIF p_query_no = 31 THEN
    -- Identify the SUBSCRIPTION records that need to be marked as 'D'
      insert into dbowner.tbl_temp_subscriptions (orig_system_ref, orig_system, orig_update_date, status)
        select distinct orig_subscription_ref, orig_system, sysdate, 'D'
        from dbowner.tbl_subscriptions
        where orig_system = 'CRM'
        and orig_subscription_ref in
        (select orig_subscription_ref from dbowner.tbl_subscriptions where orig_system = 'CRM' and record_status <> 'D'
         and substr(orig_subscription_ref,1,7) in ('CRM_CAM','CRM_OFF','CRM_RES','CRM_INV','CRM_REV')
         minus
         select orig_system_ref from dbowner.tbl_temp_subscriptions where orig_system = 'CRM'
         and substr(orig_system_ref,1,7) in ('CRM_CAM','CRM_OFF','CRM_RES','CRM_INV','CRM_REV'));

    ELSIF p_query_no = 32 THEN
    -- Identify the ACCOUNT records that need to be marked as 'D'
      insert into dbowner.tbl_temp_parties (status, orig_system, orig_system_ref, party_type, run_time)
        select distinct 'D', 'CRM', 'CRM_ACC_' || record_id, 'ACC', sysdate
        from
        vw_s_audit_item
        where operation_cd = 'Delete'
        and buscomp_name = 'Account'
        and last_upd > (select last_marker_db1 from dbowner.tbl_sources where source = 'CRM');

    ELSIF p_query_no = 33 THEN
    -- Identify the AGREEMENT records that need to be marked as 'D'
      insert into dbowner.tbl_temp_subscriptions (status, orig_system_ref, orig_system, orig_update_date)
        select distinct 'D', 'CRM_AGG_' || record_id, 'CRM', sysdate
        from
        vw_s_audit_item
        where operation_cd = 'Delete'
        and buscomp_name = 'Service Agreement'
        and last_upd > (select last_marker_db1 from dbowner.tbl_sources where source = 'CRM');

    ELSIF p_query_no = 34 THEN
    -- Identify the AGREEMENT LINE ITEM records that need to be marked as 'D'
      insert into dbowner.tbl_temp_subscriptions (status, orig_system_ref, orig_system, orig_update_date)
        select distinct 'D', 'CRM_AGL_' || record_id, 'CRM', sysdate
        from
        vw_s_audit_item
        where operation_cd = 'Delete'
        and buscomp_name = 'FS Agreement Item'
        and last_upd > (select last_marker_db1 from dbowner.tbl_sources where source = 'CRM');

    ELSIF p_query_no = 35 THEN
    -- Identify the OPPORTUNITY records that need to be marked as 'D'
      insert into dbowner.tbl_temp_subscriptions (status, orig_system_ref, orig_system, orig_update_date)
        select distinct 'D', 'CRM_OPP_' || record_id, 'CRM', sysdate
        from
        vw_s_audit_item
        where operation_cd = 'Delete'
        and buscomp_name = 'Opportunity'
        and last_upd > (select last_marker_db1 from dbowner.tbl_sources where source = 'CRM');

   END IF;


  ELSIF p_source = 'COP' THEN

    IF p_query_no = 1 THEN
    -- AMR 04-Nov-08 Added dbowner.tbl_temp_cop_customers.i_email to insert statement
    --               (against tbl_temp_parties.email)
    --               Added Values list to insert statement
         INSERT INTO dbowner.tbl_temp_parties (ORIG_SYSTEM, ORIG_SYSTEM_REF, PARTY_TYPE, DEDUPE_TYPE, USR_CREATED_DATE,
                                              USR_STATUS, USR_STATUS_DATE, USR_LAST_VISIT_DATE, USC_CONTACT_LASTNAME,
                                              USC_ADDR, USR_ADDRESS2, ADDRESS3, USC_CITY, USC_STATE, USC_ZIP, USC_COUNTRY,
                                              USC_ORG, USC_DEDUPE_EMAIL, USR_SUBSCRIBER_CODE, ORG_TYPE, DESK_TYPE,
                                              READ_REGULARLY, RUN_TIME)
        SELECT DISTINCT 'COP', 'COP_' || to_number(trim(I_CUST_NUM)) || '_' || to_number(trim(I_BILL_TO)),'BUY', 'PER',to_date(E_INIT,'YYYY-MM-DD'),
                        trim(BC00017A.value), to_date(H_LAST_CHNG_TMSTMP,'YYYY-MM-DD'), to_date(E_LAST_ORD,'YYYY-MM-DD'), trim(N_CUST),
                        trim(N_BILLTO_ATTN_1), trim(N_BILLTO_MISC_AD), trim(N_BILLTO_STR_ADDR), trim(N_BILLTO_CITY), trim(BC00254A.value), trim(C_BILLTO_ZIP), trim(BC00328A.value),
                        trim(BCCOMPNY.value), trim(a.i_email), trim(BC00053A.value), trim(BC00329A.value), trim(BC00465A.value),
                        trim(F_CUST_VOID), sysdate
                   FROM dbowner.tbl_temp_cop_customers a,
                        dbowner.tbl_temp_cop_reference BC00017A,
                        dbowner.tbl_temp_cop_reference BC00254A,
                        dbowner.tbl_temp_cop_reference BC00328A,
                        dbowner.tbl_temp_cop_reference BC00329A,
                        dbowner.tbl_temp_cop_reference BC00465A,
                        dbowner.tbl_temp_cop_reference BCCOMPNY,
                        dbowner.tbl_temp_cop_reference BC00053A
                  WHERE a.c_cust_type = BC00017A.code (+)
                    AND a.c_billto_st_abrv = BC00254A.code (+)
                    AND a.c_disc = BC00053A.code (+)
                    AND a.c_billto_cntry  = BC00328A.code (+)
                    AND a.c_cust_class  = BC00329A.code (+)
                    AND a.c_acct_type  = BC00465A.code (+)
                    AND a.c_co  = BCCOMPNY.code (+)
                    AND BC00017A.tablename (+) = 'BC00017A'
                    AND BC00254A.tablename (+) = 'BC00254A'
                    AND BC00328A.tablename (+) = 'BC00328A'
                    AND BC00053A.tablename (+) = 'BC00053A'
                    AND BC00329A.tablename (+) = 'BC00329A'
                    AND BC00465A.tablename (+) = 'BC00465A'
                    AND BCCOMPNY.tablename (+) = 'BCCOMPNY';

    ELSIF p_query_no = 2 THEN
         INSERT INTO dbowner.tbl_temp_parties
        SELECT DISTINCT NULL, 'COP', NULL, 'COP_STC_' || to_number(trim(I_CUST_NUM)) || '_' || to_number(trim(I_BILL_TO)) || '_' || to_number(trim(I_SHIP_TO)), 'STC', 'PER', NULL, c_ord_ent_type,
          NULL, 'COP_' || to_number(trim(I_CUST_NUM)) || '_' || to_number(trim(I_BILL_TO)), NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, trim(N_SHIPTO_CUST), NULL, NULL, trim(N_SHIPTO_ATTN_1),
          trim(N_SHIPTO_MISC_AD), trim(N_SHPTO_STR_ADDR), NULL, trim(N_SHIPTO_CITY), trim(BC00254A.value), trim(C_SHIPTO_ZIP), trim(BC00328A.value), NULL,
          NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
          NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
          NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, sysdate,
                    NULL, NULL
        FROM dbowner.tbl_temp_cop_sales a,
             dbowner.tbl_temp_cop_reference BC00254A,
             dbowner.tbl_temp_cop_reference BC00328A
        WHERE a.c_shipto_st_abrv = BC00254A.code (+)
        AND a.c_shipto_cntry  = BC00328A.code (+)
        AND BC00254A.tablename (+) = 'BC00254A'
        AND BC00328A.tablename (+) = 'BC00328A';
    ELSIF p_query_no = 3 THEN
        NULL;
    -- AMR 05-Nov-08 Commented out UPDATE statement below - source table not populated and update overwrites the usc_dedupe_email field

--      UPDATE dbowner.tbl_temp_parties a
--        SET (usr_last_visit_date_remembered, usc_add_type, usc_org, jobtitle, usc_phone, usc_fax, usc_dedupe_email, user_access_type, subj_area_home_page, toggle_pref, sales_emails, marketing_emails, member_search)
--           = (SELECT to_date(first_tr, 'YYYYMMDD'), ncoadt, company, prof_title, phone_num, fax_num, email_address, dte_pr, psm1, psm2, decode(nrent||'x','1x','N','Y'), decode(nmail||'x','1x','N','Y'), decode(s||'x','Nx','N','Y')
--                FROM dbowner.tbl_temp_cop_cc3 b
--               WHERE a.orig_system_ref = b.cop_id)
--         WHERE orig_system = 'COP';
    ELSIF p_query_no = 4 THEN
      INSERT INTO dbowner.tbl_temp_items
        SELECT DISTINCT 'COP_' || trim(I_PROD), 'COP', NULL, NULL, trim(N_TITLE), 'BOK', NULL, trim(I_VOL), trim(I_BOOK_ED), NULL, trim(N_SHORT_TITLE), 'COP_' || trim(I_PROD_CODE),
          trim(BC00008A.value), trim(N_AUTHR), trim(BC00043A.value), trim(BC00142A.value), trim(BC00512A.value), trim(BC00082A.value),
          trim(F_GRTS), trim(BC00060A.value), trim(BC00084A.value), trim(C_PMC), NULL, trim(C_PMG), NULL, trim(BC00047A.value), trim(BC00048A.value),
          Q_NUM_PAGES_BOOK, NULL, trim(BC00041F.value), trim(BC00013A.value), NULL, NULL, NULL, to_date(trim(E_PROD_PUB),'YYYY-MM-DD'), NULL, NULL, NULL
        FROM dbowner.tbl_temp_cop_products a,
             dbowner.tbl_temp_cop_reference BC00008A,
             dbowner.tbl_temp_cop_reference BC00013A,
             dbowner.tbl_temp_cop_reference1 BC00041F,
              dbowner.tbl_temp_cop_reference BC00043A,
              dbowner.tbl_temp_cop_reference BC00047A,
              dbowner.tbl_temp_cop_reference BC00048A,
              dbowner.tbl_temp_cop_reference BC00060A,
             dbowner.tbl_temp_cop_reference BC00082A,
             dbowner.tbl_temp_cop_reference BC00084A,
             dbowner.tbl_temp_cop_reference BC00142A,
             dbowner.tbl_temp_cop_reference BC00512A
        WHERE a.C_SUBJ_INT_SPCL1= BC00008A.code (+)
        AND a.C_PUBL_STAT = BC00013A.code (+)
        AND a.C_PRFT_CTR = BC00041F.code (+)
        AND a.C_MAJ_SUBJ = BC00043A.code (+)
        AND a.C_PROD_CLASS = BC00047A.code (+)
        AND a.C_PROD_DISC = BC00048A.code (+)
        AND a.C_BNDG = BC00060A.code (+)
        AND a.C_PROD_TYPE = BC00082A.code (+)
        AND a.C_TEXT_REF = BC00084A.code (+)
        AND a.C_DISCP = BC00142A.code (+)
        AND a.C_COURSE = BC00512A.code (+)
        AND BC00008A.tablename (+) = 'BC00008A'
        AND BC00013A.tablename (+) = 'BC00013A'
        AND BC00041F.tablename (+) = 'BC00041F'
        AND BC00043A.tablename (+) = 'BC00043A'
        AND BC00047A.tablename (+) = 'BC00047A'
        AND BC00048A.tablename (+) = 'BC00048A'
        AND BC00060A.tablename (+) = 'BC00060A'
        AND BC00082A.tablename (+) = 'BC00082A'
        AND BC00084A.tablename (+) = 'BC00084A'
        AND BC00142A.tablename (+) = 'BC00142A'
        AND BC00512A.tablename (+) = 'BC00512A';

    ELSIF p_query_no = 5 THEN
    -- AMR 04-Nov-08 Added dbowner.tbl_temp_cop_sales.c_web_site to insert statement
    --               (against tbl_temp_subscriptions.access_issue)
    --               Added Values list to insert statement
      INSERT INTO dbowner.tbl_temp_subscriptions (ORIG_SYSTEM, ORIG_CREATE_DATE, PARTY_REF,  ITEM_REF_1,
                                                  ITEM_REF_2, PRICE, SUB_STATUS, ENTITLEMENT_TYPE,
                                                  PASSWORD,
                                                  FREE_ALERTS,  PURCHASE_NUMBER, CLAIM_CODE, ACCESS_ISSUE, ORDER_ID,
                                                  COPY_PRICE_ORIG, SOURCE_CODE, NO_COPIES)
      SELECT DISTINCT 'COP',to_date(E_INV,'YYYY-MM-DD'),'COP_' || trim(to_number(I_CUST_NUM)) || '_' || trim(to_number(I_BILL_TO)),'COP_' || trim(I_PROD),
                      'COP_' || trim(I_PROD_CODE),trim(A_ITEM),trim(BC00082A.value),trim(BC00122B.value),
                      'COP_STC_' || trim(to_number(I_CUST_NUM)) || '_' || trim(to_number(I_BILL_TO)) || '_' || trim(to_number(I_SHIP_TO)),
                      trim(BC00128A.value),trim(I_PO_NUM),trim(BC00247A.value),trim(a.c_web_site),trim(I_INV_NUM),
                      trim(BC00044A.value),trim(BC00205A.value),trim(Q_SHPD_QTY)
                 FROM dbowner.tbl_temp_cop_sales a,
                      dbowner.tbl_temp_cop_reference BC00044A,
                      dbowner.tbl_temp_cop_reference BC00082A,
                      dbowner.tbl_temp_cop_reference BC00122B,
                      dbowner.tbl_temp_cop_reference BC00128A,
                      dbowner.tbl_temp_cop_reference BC00205A,
                      dbowner.tbl_temp_cop_reference BC00247A
                WHERE a.C_PROD_PRICE = BC00044A.code (+)
                  AND a.C_PROD_TYPE = BC00082A.code (+)
                  AND a.C_ORD_ENT_TYPE = BC00122B.code (+)
                  AND a.C_CR_RSN = BC00128A.code (+)
                  AND a.C_ORD_SRC_CODE = BC00205A.code (+)
                  AND a.C_SLS_REF = BC00247A.code (+)
                  AND BC00044A.tablename (+) = 'BC00044A'
                  AND BC00082A.tablename (+) = 'BC00082A'
                  AND BC00122B.tablename (+) = 'BC00122B'
                  AND BC00128A.tablename (+) = 'BC00128A'
                  AND BC00205A.tablename (+) = 'BC00205A'
                  AND BC00247A.tablename (+) = 'BC00247A';

    ELSIF p_query_no = 6 THEN
      UPDATE dbowner.tbl_temp_subscriptions SET orig_system_ref = 'COP_' || seq_cop_sales.nextval WHERE orig_system = 'COP';
    ELSIF p_query_no = 7 THEN
        INSERT INTO dbowner.tbl_temp_item_subjects
        SELECT DISTINCT decode(status, 'D', 'D', NULL), 'COP', 'COP_SBJ_' || trim(a.i_prod) || '_' || c_so_cat_1, 'COP_' || trim(I_PROD),
          sysdate, sysdate, trim(c_so_cat_1), b.description, NULL, NULL, NULL, NULL, NULL, NULL, NULL
        FROM dbowner.tbl_temp_cop_products a,
             dbowner.tbl_temp_cop_so_codes b
        WHERE a.c_so_cat_1 = b.code
        AND a.c_so_cat_1 IS NOT NULL;
    ELSIF p_query_no = 8 THEN
        INSERT INTO dbowner.tbl_temp_item_subjects
        SELECT DISTINCT decode(status, 'D', 'D', NULL), 'COP', 'COP_SBJ_' || trim(a.i_prod) || '_' || c_so_cat_2, 'COP_' || trim(I_PROD),
          sysdate, sysdate, trim(c_so_cat_2), b.description, NULL, NULL, NULL, NULL, NULL, NULL, NULL
        FROM dbowner.tbl_temp_cop_products a,
             dbowner.tbl_temp_cop_so_codes b
        WHERE a.c_so_cat_2 = b.code
        AND a.c_so_cat_2 IS NOT NULL;
    ELSIF p_query_no = 9 THEN
        INSERT INTO dbowner.tbl_temp_item_subjects
        SELECT DISTINCT decode(status, 'D', 'D', NULL), 'COP', 'COP_SBJ_' || trim(a.i_prod) || '_' || c_so_cat_3, 'COP_' || trim(I_PROD),
          sysdate, sysdate, trim(c_so_cat_3), b.description, NULL, NULL, NULL, NULL, NULL, NULL, NULL
        FROM dbowner.tbl_temp_cop_products a,
             dbowner.tbl_temp_cop_so_codes b
        WHERE a.c_so_cat_3 = b.code
        AND a.c_so_cat_3 IS NOT NULL;
    ELSIF p_query_no = 10 THEN
        INSERT INTO dbowner.tbl_temp_item_subjects
        SELECT DISTINCT decode(status, 'D', 'D', NULL), 'COP', 'COP_SBJ_' || trim(a.i_prod) || '_' || c_so_cat_4, 'COP_' || trim(I_PROD),
          sysdate, sysdate, trim(c_so_cat_4), b.description, NULL, NULL, NULL, NULL, NULL, NULL, NULL
        FROM dbowner.tbl_temp_cop_products a,
             dbowner.tbl_temp_cop_so_codes b
        WHERE a.c_so_cat_4 = b.code
        AND a.c_so_cat_4 IS NOT NULL;
    ELSIF p_query_no = 11 THEN
        INSERT INTO dbowner.tbl_temp_item_subjects
        SELECT DISTINCT decode(status, 'D', 'D', NULL), 'COP', 'COP_SBJ_' || trim(a.i_prod) || '_' || c_so_cat_5, 'COP_' || trim(I_PROD),
          sysdate, sysdate, trim(c_so_cat_5), b.description, NULL, NULL, NULL, NULL, NULL, NULL, NULL
        FROM dbowner.tbl_temp_cop_products a,
             dbowner.tbl_temp_cop_so_codes b
        WHERE a.c_so_cat_5 = b.code
        AND a.c_so_cat_5 IS NOT NULL;
    ELSIF p_query_no = 12 THEN
      DELETE dbowner.tbl_temp_cop_reference;
    ELSIF p_query_no = 13 THEN
      DELETE dbowner.tbl_temp_cop_reference1;
    ELSIF p_query_no = 14 THEN
      DELETE dbowner.tbl_temp_cop_customers;
    ELSIF p_query_no = 15 THEN
      DELETE dbowner.tbl_temp_cop_products;
    ELSIF p_query_no = 16 THEN
      DELETE dbowner.tbl_temp_cop_sales;
    END IF;


  ELSIF p_source = 'EW2' THEN

    IF p_query_no = 1 THEN
        INSERT INTO dbowner.tbl_temp_items
        (SELECT DISTINCT
               'EW2_ART_' || pii,
               'EW2',sysdate,sysdate,article_title,'ART',null,volume,issue,null,null,pii,
          journal_acronym,first_author,null,null,null,article_type,null,null,
          null,null,null,null,null,null,null,null,start_page_no || ',' || end_page_no,
          null,null,null,null,null,publication_date,null,null,null
        FROM dbowner.tbl_temp_ew2_lancet
        UNION
        SELECT DISTINCT
               'EW2_ART_' || pii,
               'EW2',sysdate,sysdate,article_title,'ART',null,volume,issue,null,null,pii,
          journal_acronym,first_author,null,null,null,article_type,null,null,
          null,null,null,null,null,null,null,null,start_page_no || ',' || end_page_no,
          null,null,null,null,null,publication_date,null,null,null
        FROM dbowner.tbl_temp_ew2_lanonc
        UNION
        SELECT DISTINCT
               'EW2_ART_' || pii,
               'EW2',sysdate,sysdate,article_title,'ART',null,volume,issue,null,null,pii,
          journal_acronym,first_author,null,null,null,article_type,null,null,
          null,null,null,null,null,null,null,null,start_page_no || ',' || end_page_no,
          null,null,null,null,null,publication_date,null,null,null
        FROM dbowner.tbl_temp_ew2_laninf
        UNION
        SELECT DISTINCT
               'EW2_ART_' || pii,
               'EW2',sysdate,sysdate,article_title,'ART',null,volume,issue,null,null,pii,
          journal_acronym,first_author,null,null,null,article_type,null,null,
          null,null,null,null,null,null,null,null,start_page_no || ',' || end_page_no,
          null,null,null,null,null,publication_date,null,null,null
        FROM dbowner.tbl_temp_ew2_laneur);

    ELSIF p_query_no = 2 THEN
        DELETE dbowner.tbl_temp_ew2_lancet;
    ELSIF p_query_no = 3 THEN
        DELETE dbowner.tbl_temp_ew2_lanonc;
    ELSIF p_query_no = 4 THEN
        DELETE dbowner.tbl_temp_ew2_laninf;
    ELSIF p_query_no = 5 THEN
        DELETE dbowner.tbl_temp_ew2_laneur;

    END IF;
    
    
    ELSIF p_source = 'LCF' THEN
    
          IF p_query_no = 1 THEN
              INSERT INTO dbowner.tbl_temp_parties (STATUS, 
                                                    ORIG_SYSTEM, 
                                                    ORIGINAL_SITE,
                                                    ORIG_SYSTEM_REF,
                                                    PARTY_TYPE,
                                                    DEDUPE_TYPE, 
                                                    USR_CREATED_DATE,
                                                    USC_CONTACT_TITLE, 
                                                    ORIG_TITLE, 
                                                    USC_CONTACT_FIRSTNAME, 
                                                    USC_CONTACT_LASTNAME, 
                                                    JOBTITLE, 
                                                    USC_DEDUPE_EMAIL, 
                                                    usc_addr, 
                                                    usc_city, 
                                                    usc_state, 
                                                    usc_zip,
                                                    usc_country,
                                                    iso_country_code, 
                                                    orig_country, 
                                                    usc_org, 
                                                    usc_institute_url, 
                                                    unique_inst_id, 
                                                    related_sis_id, 
                                                    org_type,
                                                    usr_last_visit_date,
                                                    RUN_TIME
                                                    )
              SELECT DISTINCT NULL, 
                              'LCF', 
                              'LCF', 
                              'LCF_'|| r.RECIPIENT_ID, 
                              'EUS',
                              'PER',
                              r2.First_Date, 
                              TITLE, 
                              TITLE, 
                              FIRSTNAME, 
                              LASTNAME, 
                              JOBTITLE, 
                              EMAIL, 
                              ORGANIZATIONSTREETNAME,
                              ORGANIZATIONCITY,
                              ORGANIZATIONSTATECODE,
                              ORGANIZATIONPOSTCODE,
                              iso.Country_name,
                              iso.iso_code,
                              iso.Country_name,
                              CRMACCOUNTNAME,
                              ORGANIZATIONWEBSITE,
                              r.RECIPIENT_ID,
                              SIS_ID,
                              ORGTYPEDESCRIPTION,
                              r2.Last_Date,
                              r2.Last_Date
              FROM EMROWNER.TBL_TEMP_NL_LCF_REGISTRANTS r,
                   (
                   select r2.RECIPIENT_ID, min(r2.DateSubmitted) as First_Date, max(r2.DateSubmitted) as Last_Date
                   FROM   EMROWNER.TBL_TEMP_NL_LCF_REGISTRANTS r2
                   group by r2.RECIPIENT_ID
                   ) r2,
                   TBL_ISO_COUNTRIES iso
              WHERE r.DateSubmitted IN (SELECT max(r1.DateSubmitted) FROM EMROWNER.TBL_TEMP_NL_LCF_REGISTRANTS r1 where r1.RECIPIENT_ID = r.RECIPIENT_ID)
              and   r.RECIPIENT_ID = r2.RECIPIENT_ID (+)
              and   r.ORGANIZATIONCOUNTRYCODE = iso.iso_code (+);
        END IF;
        
    ELSIF p_source = 'EVI' THEN

          IF p_query_no = 1 THEN
              -- UPDATING THE INCORRECT DATES IN SOURCE TABLE TO NULL
              UPDATE TBL_TEMP_EVISE_AUTHORS SET EDOD_DATE_TIME = NULL WHERE EDOD_DATE_TIME = '0-00-00 00:00:00';
              COMMIT;
              
              -- TRUNCATE TABLE TBL_TEMP_EVISE_AUTHORS_DAT
              EXECUTE IMMEDIATE 'TRUNCATE TABLE TBL_TEMP_EVISE_AUTHORS_DAT';
              
              -- Inserting Into Table Tbl_Temp_Evise_Authors_Dat Using Correct Formating
              -- Changes Made
              -- ,Dbms_Lob.Substr(Crev_Full_Title,1995) 
              -- ,To_Date(Sbd_Datetime,'dd-mm-YYYY HH24:MI:SS')
              -- ,To_Date(Edod_Date_Time,'dd-mm-YYYY HH24:MI:SS')
              -- ,Initcap(Auth_Country_Of_Origin) As Auth_Country_Of_Origin
              -- ,Upper(Auth_Country_Iso_Code) As Auth_Country_Iso_Code
              -- ,Initcap(Auth_Institution)  
              
              INSERT INTO TBL_TEMP_EVISE_AUTHORS_DAT
                SELECT CREV_PII_ID ,
                  DBMS_LOB.SUBSTR(CREV_FULL_TITLE,1990) ,
                  CREV_JNL_ARTICLE_TYPE ,
                  CREV_PIT_ID ,
                  TO_DATE(SBD_DATETIME,'YYYY-MM-dd HH24:MI:SS') ,
                  MANUSCRIPT_NUMBER ,
                  EDITORIAL_OUTCOME ,
                  STATUS_CODE ,
                  STATUS_LABEL ,
                  TO_DATE(EDOD_DATE_TIME,'YYYY-MM-DD HH24:MI:SS') ,
                  JR_PTS_JOURNAL_NO ,
                  JR_TITLE ,
                  JR_PTS_ISSN ,
                  JR_PTS_ACRONYM ,
                  JR_EVISE_ACRONYM ,
                  JR_PTS_PMG ,
                  JR_PTS_PMC ,
                  AUTH_TITLE ,
                  AUTH_FIRST_NAME ,
                  AUTH_LAST_NAME ,
                  AUTH_EMAIL ,
                  initcap(AUTH_COUNTRY_OF_ORIGIN) AS AUTH_COUNTRY_OF_ORIGIN ,
                  AUTH_USER_ID ,
                  upper(AUTH_COUNTRY_ISO_CODE) AS AUTH_COUNTRY_ISO_CODE ,
                  Initcap(AUTH_INSTITUTION) ,
                  AUTH_ROW_WID
                FROM TBL_TEMP_EVISE_AUTHORS;
              COMMIT;
              
              --Inserting Party Records Into Temporary Capri Table
              INSERT INTO TBL_TEMP_PARTIES (STATUS, ORIG_SYSTEM, ORIG_SYSTEM_REF, PARTY_TYPE, DEDUPE_TYPE, USR_CREATED_DATE, USC_CONTACT_TITLE, USC_CONTACT_FIRSTNAME,
                                  USC_CONTACT_LASTNAME, USC_COUNTRY, ISO_COUNTRY_CODE, ORIG_COUNTRY, USC_ORG, USC_DEDUPE_EMAIL, UNIQUE_INST_ID)
                SELECT DISTINCT 'A' AS STATUS,
                'EVI' AS ORIG_SYSTEM,
                'EVI_AUTH_' || AUTH_ROW_WID AS ORIG_SYSTEM_REF,
                'AUT' AS PARTY_TYPE,
                'PER' AS DEDUPE_TYPE,
                SYSDATE AS USR_CREATED_DATE,
                AUTH_TITLE AS USC_CONTACT_TITLE,
                AUTH_FIRST_NAME AS USC_CONTACT_FIRSTNAME,
                AUTH_LAST_NAME AS USC_CONTACT_LASTNAME,
                AUTH_COUNTRY_OF_ORIGIN AS USC_COUNTRY,
                AUTH_COUNTRY_ISO_CODE AS ISO_COUNTRY_CODE,
                AUTH_COUNTRY_OF_ORIGIN AS ORIG_COUNTRY,
                AUTH_INSTITUTION AS USC_ORG,
                AUTH_EMAIL AS USC_DEDUPE_EMAIL,
                AUTH_USER_ID AS UNIQUE_INST_ID
                FROM TBL_TEMP_EVISE_AUTHORS_DAT;
              
              --For Correcting Country Codes  
              BEGIN
                FOR REC0 IN
                (SELECT ROWID, ISO_COUNTRY_CODE,USC_COUNTRY
                  FROM DBOWNER.TBL_TEMP_PARTIES
                  WHERE ORIG_SYSTEM = 'EVI')
                LOOP
                  FOR REC1 IN
                  (SELECT C.ISO_CODE, C.COUNTRY_NAME
                    FROM DBOWNER.TBL_COUNTRIES B,DBOWNER.TBL_ISO_COUNTRIES C
                  WHERE (UPPER(TRIM(REC0.USC_COUNTRY)) = B.SOURCE_VALUE
                  OR REC0.ISO_COUNTRY_CODE             = B.ISO_CODE )
                  AND B.ISO_CODE                       = C.ISO_CODE
                  )
                  LOOP
                    UPDATE DBOWNER.TBL_TEMP_PARTIES
                    SET ISO_COUNTRY_CODE = REC1.ISO_CODE,
                      USC_COUNTRY        = REC1.COUNTRY_NAME,
                      DOCTORED           = 'Y'
                    WHERE ROWID          = REC0.ROWID;
                  END LOOP;
                END LOOP;
              END;
              
         END IF;
         
         IF p_query_no = 2 THEN
             
             INSERT INTO TBL_TEMP_ITEMS (ORIG_SYSTEM_REF, ORIG_SYSTEM, CREATE_DATE, UPDATE_DATE, NAME, ITEM_TYPE, PARENT_REF, IDENTIFIER, AUTHORS,
                                SHOW_STATUS,TYPE,BINDING,ITEM_MILESTONE,ISSUE_MILESTONE,ISSUE_DATE)
                SELECT DISTINCT 'EVI_ART_' || MANUSCRIPT_NUMBER || '_' || CREV_PII_ID  AS ORIG_SYSTEM_REF,
                        'EVI' AS ORIG_SYSTEM,
                        SBD_DATETIME AS CREATE_DATE,
                        SYSDATE AS UPDATE_DATE,
                        CREV_FULL_TITLE AS NAME,
                        'ART' AS ITEM_TYPE,
                        'EVI_JOU_' || JR_PTS_ISSN AS PARENT_REF,
                        CREV_PII_ID AS IDENTIFIER,
                        'EVI_AUTH_' || AUTH_ROW_WID AS AUTHORS,
                        EDITORIAL_OUTCOME AS SHOW_STATUS,
                        CREV_JNL_ARTICLE_TYPE AS TYPE,
                        MANUSCRIPT_NUMBER AS BINDING,
                        STATUS_CODE AS ITEM_MILESTONE,
                        STATUS_LABEL AS ISSUE_MILESTONE,
                        EDOD_DATE_TIME AS ISSUE_DATE
                FROM TBL_TEMP_EVISE_AUTHORS_DAT;
             
         END IF;
         
         IF p_query_no = 3 THEN
             
            INSERT INTO TBL_TEMP_ITEMS (ORIG_SYSTEM_REF, ORIG_SYSTEM, CREATE_DATE, UPDATE_DATE, NAME, ITEM_TYPE, PARENT_REF,IDENTIFIER, CODE, PMC_CODE, PMG_CODE )
                SELECT DISTINCT 'EVI_JOU_' || JR_PTS_ISSN AS ORIG_SYSTEM_REF,
                      'EVI' AS ORIG_SYSTEM,
                      SYSDATE AS CREATE_DATE,
                      SYSDATE AS UPDATE_DATE,
                      JR_TITLE AS NAME,
                      'JOU' AS ITEM_TYPE,
                      JR_PTS_JOURNAL_NO AS PARENT_REF,
                      JR_PTS_ISSN AS IDENTIFIER, 
                      JR_EVISE_ACRONYM AS CODE,
                      JR_PTS_PMC AS PMC_CODE,
                      JR_PTS_PMG AS  PMG_CODE
                FROM TBL_TEMP_EVISE_AUTHORS_DAT;
                
         END IF;

    ELSIF p_source = 'EVI2' THEN

          IF p_query_no = 1 THEN
             INSERT INTO TBL_TEMP_PARTIES (
                            orig_system,
                            orig_system_ref,
                            party_type,
                            dedupe_type,
                            status,
                            usr_created_date,
                            usr_status,
                            usr_ref,
                            usr_principal_field,
                            usc_contact_title,
                            usc_contact_firstname,
                            usc_contact_lastname,
                            usc_dedupe_email,
                            unique_inst_id,
                            usr_subscriber_code,
                            run_time,
                                sales_emails,
                                marketing_emails,
                                member_search
                            )
	SELECT  
	        'EVI2',
	        'EVI2_USER_' || e.SUB_REV_ID,
	        'AUT',
	        'PER',
	        case when e.delete_ind = 'Y' then 'D' else null end,
	        e.CREATION_DATE, -- Amended frfom mappings
	        e.delete_ind,
	        e.SUB_USER_ID,
	        e.ROLE_ID,
	        e.TITLE,
	        e.FIRST_NAME,
	        e.LAST_NAME,
	        e.EMAIL_ID,
	        e.USER_ID,
	        'EVI2_' || e.SUB_REV_ID ||'_' || e.USER_ID,
	        e.UPDATED_DATE,
	        p.sales_emails,
	        p.marketing_emails,
	        p.member_search
	FROM  EVI_SUB_REV_USERS@JRBI e
	LEFT JOIN (SELECT * FROM TBL_PARTIES WHERE ORIG_SYSTEM = 'SD' AND DEDUPE_TYPE = 'PER' AND ORIG_SITE LIKE '%EVS%') p
	ON    'SD2_'||e.USER_ID = p.orig_party_ref
	WHERE   e.SUB_USER_ID IN (
                         SELECT MAX(b.SUB_USER_ID)
                         FROM   EVI_SUB_REV_USERS@JRBI b
                         WHERE  e.SUB_REV_ID = b.SUB_REV_ID
                         AND    e.EMAIL_ID = b.EMAIL_ID
                         )
	and     e.dal_created_date >= sysdate-7;
              
         END IF;
         
         IF p_query_no = 2 THEN
             
             INSERT INTO TBL_TEMP_PARTIES (
                                orig_system,
                                orig_system_ref,
                                party_type,
                                dedupe_type,
				status,
                                usr_created_date,
                                usr_status,
                                usr_ref,
                                usr_url_ref,
                                usr_other_ref,
                                usr_principal_field,
                                usc_contact_title,
                                usc_contact_firstname,
                                usc_contact_lastname,
                                usc_country,
                                orig_country,
                                usc_org,
                                usc_dedupe_email,
                                unique_inst_id,
                                usr_subscriber_code,
                                run_time,
                                sales_emails,
                                marketing_emails,
                                member_search
                                )
	SELECT
	        'EVI2',
	        'EVI2_ADD_' || e.ADDED_AUTHOR_ID,
	        'AUT',
        	'PER',
	        case when e.delete_ind = 'Y' then 'D' else null end,
	        e.CREATION_DATE,
	        e.DELETE_IND,
	        e.SUB_REV_ID,
	        upper(e.FIRST_AUTHOR),
	        e.ORCID_ID,
	        e.USER_TYPE,
	        e.TITLE,
	        e.FIRST_NAME,
	        e.LAST_NAME,
	        e.COUNTRY,
	        e.COUNTRY,
	        e.INSTITUTION,
	        e.EMAIL_ID,
	        e.USER_ID,
	        'EVI2_' || e.SUB_REV_ID ||'_' || e.USER_ID,
	        e.UPDATION_DATE,
	        p.sales_emails,
	        p.marketing_emails,
	        p.member_search
	FROM  EVI_SUB_REV_ADD_USERS@JRBI e
	LEFT JOIN (SELECT * FROM TBL_PARTIES WHERE ORIG_SYSTEM = 'SD' AND DEDUPE_TYPE = 'PER' AND ORIG_SITE LIKE '%EVS%') p
	ON    'SD2_'||e.USER_ID = p.orig_party_ref
	WHERE e.dal_created_date >= sysdate-7;
             
         END IF;
         
         IF p_query_no = 3 THEN
             
            INSERT INTO TBL_TEMP_ITEMS (
                                orig_system_ref,
                                orig_system ,
                                create_date,
                                update_date,
                                name,
                                item_type,
                                parent_ref,
                                volume,
                                issue,
                                description,
                                identifier,
                                authors,
                                show_status,
                                type,
                                sub_type,
                                binding,
                                medium,
                                item_milestone,
                                issue_date,
                                status
                                )
	SELECT
	        'EVI2_ART_' || A.SUBMISSION_ID,
	        'EVI2',
	        A.CREATION_DATE,
	        B.UPDATED_DATE,
	        B.FULL_TITLE,
	        'ART',
	        'EVI2_JOU_' || a.JRNL_ID,
	        B.REVISION_NUM,
	        B.VERSION_NUM,
	        B.KEYWORDS,
	        A.PII_ID,
	        'EVI2_USER_' || b.SUB_REV_ID,
	        null, --FUNC_JRBI_ARTICLE_STATUS_MAP(b.status_cd,b.version_num,b.revision_num), --**derived via mappings
	        C.JRNL_ARTICLE_NAME,
	        'EVI2_' || A.SUBMISSION_ID ||'_' || B.AUTHOR_ID, -- Amended from mappings
	        a.JRNL_SUBMISSION_ID,
	        'EVI2_' || B.SUB_REV_ID ||'_' || B.AUTHOR_ID,
	        B.STATUS_CD,
	        B.UPDATED_DATE,
	        A.DELETE_IND -- Amended from mappings
	FROM    EVI_SUBMISSION@JRBI A,
	        EVI_SUBMISSION_REV@JRBI B,
	        EVI_JRNL_ART_MAP@JRBI C
	WHERE   A.SUBMISSION_ID = B.SUBMISSION_ID
	AND     A.AUTHOR_REV_ID = B.SUB_REV_ID -- Amended from mappings
	AND     A.JRNL_ARTICLE_ID = C.JRNL_ARTICLE_ID
	AND     (
	         a.dal_created_date >= sysdate-7
	         or
	         b.dal_created_date >= sysdate-7
	         or
	         c.dal_created_date >= sysdate-7
	         );

	update tbl_temp_items
	set    show_status = FUNC_JRBI_ARTICLE_STATUS_MAP(item_milestone,issue,volume)
	where  orig_system = 'EVI2'
	and    item_type = 'ART';
                
         END IF;

         IF p_query_no = 4 THEN
             
            INSERT INTO TBL_TEMP_ITEMS (
                                orig_system_ref,
                                orig_system ,
                                create_date,
                                update_date,
                                name,
                                item_type,
                                identifier,
                                code,
                                status
                                )
	SELECT 
	        'EVI2_JOU_' || JRNL_ID,
	        'EVI2',
	        CREATION_DATE,
	        UPDATED_DATE,
	        JRNL_TITLE,
	        'JOU',
	        ISSN_NUM,
	        JRNL_ACR,
	        DELETE_IND
	FROM    EVI_JOURNAL_MASTER@JRBI
	WHERE   dal_created_date >= sysdate-7;    
                
         END IF;
         
		 /* Added by Gomathi - JRBI source code start */ 

	ELSIF p_source = 'JRBI' THEN	 

		IF p_query_no = 1 THEN
insert into tbl_temp_parties
							(status,
							orig_system,
							original_site,
							orig_system_ref,
							party_type,
							dedupe_type,
							usr_created_date,
							usr_status,
							usr_status_date,
							usr_other_ref,
							usc_contact_title,
							usc_contact_firstname,
							usc_contact_lastname,
							usc_primary_address,
							usc_add_type,
							usc_city,
							usc_state,
							usc_country,
							iso_country_code,
							orig_country,
							usc_dept,
							usc_org,
							usc_dedupe_email,
                            usc_institute_url,
							unique_inst_id,
							inst_name,
                            usr_subscriber_code,
							marketing_emails,
							run_time)

			Select	distinct		

					case when e.delete_ind = 'Y' then 'D' else null end as status,
					'JRBI' as orig_system ,
					d.SOURCE_SYSTEM_CODE as original_site,                         
					'JRBI_USER_'||e.USER_ID,
					'EUS',
					'PER',
					e.REC_CREATE_DATE,
					e.USER_STATUS,
					e.DELETE_DATE,
					e.USER_ORCID,
					e.USER_TITLE,
					e.USER_FIRST_NAME,
					e.USER_LAST_NAME,
					e.ADDRESS_PRIMARY_IND,
					e.ADDRESS_TYPE_NAME,
					e.ADDRESS_CITY,
					e.USER_STATE_NAME,
					e.USER_COUNTRY_NAME,
					e.USER_COUNTRY_CODE,
					e.USER_COUNTRY_NAME,
					e.ADDRESS_DEPARTMENT,
					e.ADDRESS_INSTITUTION,
					e.USER_EMAIL,
                    e.scopus_profile_code,
					e.WEB_USER_ID,
					e.ADDRESS_ORGANIZATION,
                    e.user_type_desc ,
					e.MARKETING_MAIL_IND,
					e.REC_UPDATE_DATE
				FROM JRBI_USER_DIM@JRBI e 
                join jrbi_source_system_dim@jrbi d on e.SOURCE_SYSTEM_ID = d.SOURCE_SYSTEM_ID
                join jrbi_article_lifecycle_fact@jrbi f on e.user_id = f.author_id;
COMMIT;				

END IF;

         IF p_query_no = 2 THEN
		 insert into tbl_temp_parties
							(status,
							orig_system,
							original_site,
							orig_system_ref,
							party_type,
							dedupe_type,
							usr_created_date,
							usr_status,
							usr_status_date,
							usr_ref,
							usr_url_ref, 
							usr_other_ref,
							usc_contact_title,
							usc_contact_firstname,
							usc_contact_lastname,
							usc_country,
							iso_country_code,
							orig_country,
							usc_org,
							usc_dedupe_email,
							run_time)

			Select	distinct		
					case when c.delete_ind = 'Y' then 'D' else null end as status,
					'JRBI' as orig_system ,
					d.SOURCE_SYSTEM_CODE as original_site,                         
					'JRBI_CONTR_'||c.CONTRIBUTOR_ID,
					'EUS',
					'PER',
					c.REC_CREATE_DATE,
                    c.CONTRIBUTOR_STATUS,
                    c.DELETE_DATE,
                    c.CORR_AUTH_IND,
                    c.FIRST_AUTHOR_IND,
                    c.CONTRIBUTOR_ORCID,
                    c.CONTRIBUTOR_TITLE,
                    c.CONTRIBUTOR_FIRST_NAME,
                    c.CONTRIBUTOR_LAST_NAME,
					c.CONTRIBUTOR_COUNTRY_NAME,
                    t.ISO_CODE,
                    c.CONTRIBUTOR_COUNTRY_NAME,
                    c.CONTRIBUTOR_INSTITUTION,
                    c.CONTRIBUTOR_EMAIL,
                    c.REC_UPDATE_DATE
				FROM JRBI_CONTRIBUTOR_DIM@JRBI c 
				left join JRBI_CONTRIBUTOR_BRIDGE@JRBI b on c.contributor_id = b.contributor_id
				left join jrbi_source_system_dim@jrbi d  on  c.SOURCE_SYSTEM_ID = d.SOURCE_SYSTEM_ID
				left join tbl_iso_countries t on ( c.CONTRIBUTOR_COUNTRY_CODE = t.ISO_CODE or c.CONTRIBUTOR_COUNTRY_CODE = t.COUNTRY_NAME ) 
				where c.corr_auth_ind not in ( 'Y','ON','on' )
				and  b.LATEST_REVISION_IND = 'Y';
COMMIT;				

END IF;

		IF p_query_no = 3 THEN

		insert into tbl_temp_parties
							(status,
							orig_system,
							original_site,
							orig_system_ref,
							party_type,
							dedupe_type,
							usr_created_date,
							usr_status,
							usr_status_date,
							usr_other_ref,
							usc_contact_title,
							usc_contact_firstname,
							usc_contact_lastname,
							usc_primary_address,
							usc_add_type,
							usc_city,
							usc_state,
							usc_country,
							iso_country_code,
							orig_country,
							usc_dept,
							usc_org,
							usc_dedupe_email,
                            usc_institute_url,
							unique_inst_id,
							inst_name,
                            usr_subscriber_code,
							marketing_emails,
							run_time)

select distinct		
case when e.delete_ind = 'Y' then 'D' else null end as status,
'JRBI' as orig_system ,
d.SOURCE_SYSTEM_CODE as original_site,
'JRBI_REV_'||e.user_id,
'EUS',
'PER',
e.REC_CREATE_DATE,
e.USER_STATUS,
e.DELETE_DATE,
e.USER_ORCID,
e.USER_TITLE,
e.USER_FIRST_NAME,
e.USER_LAST_NAME,
e.ADDRESS_PRIMARY_IND,
e.ADDRESS_TYPE_NAME,
e.ADDRESS_CITY,
e.USER_STATE_NAME,
e.USER_COUNTRY_NAME,
e.USER_COUNTRY_CODE,
e.USER_COUNTRY_NAME,
e.ADDRESS_DEPARTMENT,
e.ADDRESS_INSTITUTION,
e.USER_EMAIL,
e.scopus_profile_code,
e.WEB_USER_ID,
e.ADDRESS_ORGANIZATION,
e.user_type_desc ,
e.MARKETING_MAIL_IND,
e.REC_UPDATE_DATE
FROM JRBI_USER_DIM@JRBI e join jrbi_source_system_dim@jrbi d on e.SOURCE_SYSTEM_ID = d.SOURCE_SYSTEM_ID
join jrbi_review_lifecycle_fact@jrbi r on e.user_id = r.reviewer_id;
COMMIT;

END IF;

	IF p_query_no = 4 THEN	

	insert into tbl_temp_parties
							(status,
							orig_system,
							original_site,
							orig_system_ref,
							party_type,
							dedupe_type,
							usr_created_date,
							usr_status,
							usr_status_date,
							usr_other_ref,
							usc_contact_title,
							usc_contact_firstname,
							usc_contact_lastname,
							usc_primary_address,
							usc_add_type,
							usc_city,
							usc_state,
							usc_country,
							iso_country_code,
							orig_country,
							usc_dept,
							usc_org,
							usc_dedupe_email,
                            usc_institute_url,
							unique_inst_id,
							inst_name,
                            usr_subscriber_code,
							marketing_emails,
							run_time)

select 	distinct		
case when e.delete_ind = 'Y' then 'D' else null end as status,
'JRBI' as orig_system ,
d.SOURCE_SYSTEM_CODE as original_site,
'JRBI_EDIT_'||e.USER_ID,
'EUS',
'PER',
e.REC_CREATE_DATE,
e.USER_STATUS,
e.DELETE_DATE,
e.USER_ORCID,
e.USER_TITLE,
e.USER_FIRST_NAME,
e.USER_LAST_NAME,
e.ADDRESS_PRIMARY_IND,
e.ADDRESS_TYPE_NAME,
e.ADDRESS_CITY,
e.USER_STATE_NAME,
e.USER_COUNTRY_NAME,
e.USER_COUNTRY_CODE,
e.USER_COUNTRY_NAME,
e.ADDRESS_DEPARTMENT,
e.ADDRESS_INSTITUTION,
e.USER_EMAIL,
e.scopus_profile_code,
e.WEB_USER_ID,
e.ADDRESS_ORGANIZATION,
e.user_type_desc ,
e.MARKETING_MAIL_IND,
e.REC_UPDATE_DATE
FROM JRBI_USER_DIM@JRBI e join jrbi_source_system_dim@jrbi d on e.SOURCE_SYSTEM_ID = d.SOURCE_SYSTEM_ID
join jrbi_review_lifecycle_fact@jrbi r on e.user_id = r.editor_id;
COMMIT;
END IF;

		IF p_query_no = 5 THEN
			INSERT INTO tbl_temp_items
(orig_system_ref,
orig_system ,
create_date,
update_date,
name,
item_type,
parent_ref,
volume,
issue,
description,
identifier,
code,
authors,
show_status,
site,
publisher,
type,
sub_type,
binding,
medium,
class,
imprint,
item_milestone,
issue_milestone,
issue_date,
receive_date,
init_pub_date,
last_pub_date,
added_date,
status )

select distinct
'JRBI_ART_'||ar.ARTICLE_ID as orig_system_ref,
'JRBI' as orig_system ,
ar.REC_CREATE_DATE,
ar.REC_UPDATE_DATE,
ar.FULL_ARTICLE_NAME,
'ART' as item_type,
'JRBI_JOU_'||juar.JOURNAL_ID ,
substr(ISS.volume_issue_code,1,instr(ISS.volume_issue_code,'/')-1),
substr(ISS.volume_issue_code,instr(ISS.volume_issue_code,'/') + 1),
ar.ARTICLE_KEYWORDS,
ar.PII_ID_UNFORMATTED,
ar.DOI,
'JRBI_USER_'||juar.AUTHOR_ID,
st.STATUS_NAME,
ar.special_content_ind,
ar.sd_url,
ar.open_access_ind,
'JRBI_USER_'||juar.PROD_FIRST_AUTHOR_ID,
ar.revision_seqno,
ar.publishing_category_desc,
ar.PUBLICATION_ITEM_TYPE_DESC,
ar.ARTICLE_EDITORIAL_REF,
ot.outcome_name,
ar.ARTICLE_PRODUCTION_JNL_REF,
juar.acceptance_date,
juar.submission_date,
juar.PUBLICATION_FIRST_ON_DATE,
juar.PUBLICATION_VOR_DATE,
juar.EDITORIAL_OUTCOME_date,
ar.DELETE_IND
from JRBI_article_dim@jrbi ar 
join JRBI_ARTICLE_LIFECYCLE_FACT@jrbi juar on ar.ARTICLE_ID = juar.ARTICLE_ID
join JRBI_ISSUE_DIM@jrbi iss on iss.ISSUE_ID = juar.ISSUE_ID
join JRBI_OUTCOME_DIM@jrbi ot on ot.outcome_id = juar.EDITORIAL_OUTCOME_ID
join JRBI_STATUS_DIM@jrbi st on  st.status_id = juar.status_id
where ar.active_ind=1 
and juar.active_ind = 1
and iss.active_ind = 1;
COMMIT;
END IF;

IF p_query_no = 6 THEN

Insert into tbl_temp_items
(orig_system_ref,
orig_system ,
create_date,
update_date,
name,
item_type,
parent_ref,
identifier,
code,
authors,
show_status,
site,
publisher,
type,
sub_type,
medium,
pmc_code,
pmc_descr,
pmg_code,
pmg_descr,
class,
item_milestone,
issue_milestone,
issue_date,
status)

select distinct
'JRBI_JOU_'||jou.JOURNAL_ID,
'JRBI',
jou.REC_CREATE_DATE,
jou.REC_UPDATE_DATE,
jou.TITLE,
'JOU',
jou.PTS_ACRONYM,
jou.ISSN,
jou.JOURNAL_CODE,
jou.Current_pts_acronym,
jou.BUSINESS_UNIT_CODE,
jou.BUSINESS_UNIT_DESC,
jou.BUSINESS_UNIT_TYPE,
jou.Content_type,
jou.JOURNAL_PUBL_CATEGORY_GROUP,
jp.ATTR_DIB_IND,
jou.PMC_CODE,
jou.PMC_DESC,
jou.PMG_CODE,
jou.PMG_DESC,
jp.ATTR_AUDIO_SLIDES_IND,
jou.Journal_full_oa_ind,
jou.Oa_policy,
jp.ATTR_AUDIO_SLIDES_DATE_FROM,
jou.DELETE_IND
from JRBI_JOURNAL_DIM@JRBI jou
join JRBI_JOURNAL_PRODUCTION_DIM@jrbi jp
on jou.journal_id = jp.journal_id
and jou.active_ind = 1
and jp.active_ind = 1;
COMMIT;

 END IF;
 
 IF p_query_no = 7 THEN

 INSERT INTO tbl_temp_interests
(orig_system_ref,
orig_system ,
party_ref,
create_date,
update_date,
interest_type,
interest_value,
interest_value_details,
interest_section)

select distinct
'JRBI_ART_'||b.ARTICLE_ID||'_'||b.CONTRIBUTOR_ID,
'JRBI',
'JRBI_CONTR_'||b.CONTRIBUTOR_ID,
b.REC_CREATE_DATE,
b.REC_UPDATE_DATE,
'ART',
'JRBI_ART_'||b.ARTICLE_ID,
b.REVISION_SEQNO,
b.LATEST_REVISION_IND
FROM JRBI_CONTRIBUTOR_BRIDGE@jrbi b
left join JRBI_CONTRIBUTOR_DIM@JRBI c on b.contributor_id = c.contributor_id
WHERE b.LATEST_REVISION_IND = 'Y'
and c.corr_auth_ind not in ( 'Y','ON','on' );
COMMIT;
END IF;

IF p_query_no = 8 THEN
INSERT INTO tbl_temp_interests
(orig_system_ref,
orig_system ,
party_ref,
create_date,
update_date,
interest_type,
interest_value,
interest_value_details,
interest_section,
interest_sub_section,
alert_end_date,
status
)
select distinct 
'JRBI_REV_'||rv.ARTICLE_Id||'_'||rv.reviewer_id||'_'||rv.revision_seqno||rv.INTEGRATION_ID,
'JRBI',
'JRBI_REV_'||rv.reviewer_id,
rv.REC_CREATE_DATE,
rv.REC_UPDATE_DATE,
'REV',
'JRBI_ART_'||rv.ARTICLE_ID,
rv.revision_seqno,
rv.REVIEW_DECISION,
st.STATUS_NAME, 
rv.DELETE_DATE,
rv.DELETE_IND
FROM JRBI_REVIEW_LIFECYCLE_FACT@JRBI rv
join JRBI_STATUS_DIM@jrbi st on  st.status_id = rv.status_id
WHERE rv.REVIEWER_ID >0;
COMMIT;
END IF;

IF p_query_no = 9 THEN
INSERT INTO tbl_temp_interests
(orig_system_ref,
orig_system ,
party_ref,
create_date,
update_date,
interest_type,
interest_value,
interest_value_details,
interest_sub_section,
alert_end_date,
status
)
select distinct 
'JRBI_EDIT_'||rv.ARTICLE_Id||'_'||rv.editor_id||'_'||rv.revision_seqno||'_'||rv.INTEGRATION_ID,
'JRBI',
'JRBI_EDIT_'||rv.editor_id,
rv.REC_CREATE_DATE,
rv.REC_UPDATE_DATE,
'EDIT',
'JRBI_ART_'||rv.ARTICLE_ID,
rv.revision_seqno,
st.STATUS_NAME, 
rv.DELETE_DATE,
rv.DELETE_IND
FROM JRBI_REVIEW_LIFECYCLE_FACT@JRBI rv
join JRBI_STATUS_DIM@jrbi st on  st.status_id = rv.status_id
WHERE rv.EDITOR_ID >0;
COMMIT;
END IF;

 /* Added by Gomathi - JRBI source code End */ 
         
        
    ELSIF p_source = 'TLH' THEN
    
          IF p_query_no = 1 THEN
                insert into tbl_temp_parties
                            (orig_system,
                             original_site,
                             orig_system_ref,
                             party_type,
                             dedupe_type,
                             usr_created_date,
                             usr_last_visit_date,
                             usc_contact_title,
                             orig_title,
                             usc_contact_firstname,
                             usc_contact_lastname,
                             usc_country,
                             usc_org,
                             jobtitle,
                             usc_dedupe_email,
                             usr_subscriber_code,
                             org_type,
                             desk_type,
                             sales_emails,
                             marketing_emails,
                             usr_last_visit_date_remembered,
                             run_time
                             )
                select 
                        'TLH' as orig_system,
                        Case when nvl(marketing_optin,'X') = 'Yes' AND nvl(press_contact,'No') <> 'No' then 'Marketing and Press Contact' 
                             when nvl(marketing_optin,'X') = 'Yes' Then 'Marketing'
                             when nvl(press_contact,'No') <> 'No' Then 'Press Contact' 
                             else 'Marketing'
                        end as original_site,
                        'TLH_'  || vid as orig_system_ref,
                        'EUS' as party_type,
                        'PER' as dedupe_type,
                        createdate as usr_created_date,
                        lastmodifieddate as usr_last_visit_date,
                        salutation as usc_contact_title,
                        salutation as orig_title,
                        firstname as usc_contact_firstname,
                        lastname as usc_contact_lastname,
                        country as usc_country,
                        company as usc_org,
                        jobtitle as jobtitle,
                        email as usc_dedupe_email,
                        lifecyclestage as usr_subscriber_code,
                        work_setting as org_type,
                        Profession as desk_type,
                        decode(lower(optoutallemail),'true','N',null) as sales_emails,
                        decode(lower(optoutmktemail),'true','N',null) as marketing_emails,
                        lancet_registration_date as usr_last_visit_date_remembered,
                        lastmodifieddate as run_time
                from    TBL_TEMP_LANCET_HUBSPOT;
                
        END IF;
            
        IF p_query_no = 2 THEN
                
                insert into tbl_temp_interests        
                select * 
                from (
                        select  'TLH_LANCET_EXP_'||VID as orig_system_ref, 
                                'TLH' as orig_system,
                                'TLH_'  || vid as party_ref,
                                createdate as create_date,
                                LASTMODIFIEDDATE as update_date,
                                'EXP' as interest_type,
                                'LANCET Subscription Expiry Date' as interest_value,
                                to_char(LANCET_SUB_EXPIRY_DATE,'DD/MM/YYYY HH24:MI:SS') as interest_value_details,
                                NULL as interest_section,
                                NULL as interest_sub_section,
                                NULL as alert_end_date,
                                NULL as status
                        from   tbl_temp_lancet_hubspot
                        where  LANCET_SUB_EXPIRY_DATE is not null
                        union all
                        select  'TLH_LANDIA_EXP_'||VID as orig_system_ref, 
                                'TLH' as orig_system,
                                'TLH_'  || vid as party_ref,
                                createdate as create_date,
                                LASTMODIFIEDDATE as update_date,
                                'EXP' as interest_type,
                                'LANDIA Subscription Expiry Date' as interest_value,
                                to_char(LANDIA_SUB_EXPIRY_DATE,'DD/MM/YYYY HH24:MI:SS') as interest_value_details,
                                NULL as interest_section,
                                NULL as interest_sub_section,
                                NULL as alert_end_date,
                                NULL as status
                        from   tbl_temp_lancet_hubspot
                        where  LANDIA_SUB_EXPIRY_DATE is not null
                        union all
                        select  'TLH_LANINF_EXP_'||VID as orig_system_ref, 
                                'TLH' as orig_system,
                                'TLH_'  || vid as party_ref,
                                createdate as create_date,
                                LASTMODIFIEDDATE as update_date,
                                'EXP' as interest_type,
                                'LANINF Subscription Expiry Date' as interest_value,
                                to_char(LANINF_SUB_EXPIRY_DATE,'DD/MM/YYYY HH24:MI:SS') as interest_value_details,
                                NULL as interest_section,
                                NULL as interest_sub_section,
                                NULL as alert_end_date,
                                NULL as status
                        from   tbl_temp_lancet_hubspot
                        where  LANINF_SUB_EXPIRY_DATE is not null
                        union all
                        select  'TLH_LANEUR_EXP_'||VID as orig_system_ref, 
                                'TLH' as orig_system,
                                'TLH_'  || vid as party_ref,
                                createdate as create_date,
                                LASTMODIFIEDDATE as update_date,
                                'EXP' as interest_type,
                                'LANEUR Subscription Expiry Date' as interest_value,
                                to_char(LANEUR_SUB_EXPIRY_DATE,'DD/MM/YYYY HH24:MI:SS') as interest_value_details,
                                NULL as interest_section,
                                NULL as interest_sub_section,
                                NULL as alert_end_date,
                                NULL as status
                        from   tbl_temp_lancet_hubspot
                        where  LANEUR_SUB_EXPIRY_DATE is not null
                        union all
                        select  'TLH_LANONC_EXP_'||VID as orig_system_ref, 
                                'TLH' as orig_system,
                                'TLH_'  || vid as party_ref,
                                createdate as create_date,
                                LASTMODIFIEDDATE as update_date,
                                'EXP' as interest_type,
                                'LANONC Subscription Expiry Date' as interest_value,
                                to_char(LANONC_SUB_EXPIRY_DATE,'DD/MM/YYYY HH24:MI:SS') as interest_value_details,
                                NULL as interest_section,
                                NULL as interest_sub_section,
                                NULL as alert_end_date,
                                NULL as status
                        from   tbl_temp_lancet_hubspot
                        where  LANONC_SUB_EXPIRY_DATE is not null
                        union all
                        select  'TLH_LANRES_EXP_'||VID as orig_system_ref, 
                                'TLH' as orig_system,
                                'TLH_'  || vid as party_ref,
                                createdate as create_date,
                                LASTMODIFIEDDATE as update_date,
                                'EXP' as interest_type,
                                'LANRES Subscription Expiry Date' as interest_value,
                                to_char(LANRES_SUB_EXPIRY_DATE,'DD/MM/YYYY HH24:MI:SS') as interest_value_details,
                                NULL as interest_section,
                                NULL as interest_sub_section,
                                NULL as alert_end_date,
                                NULL as status
                        from   tbl_temp_lancet_hubspot
                        where  LANRES_SUB_EXPIRY_DATE is not null
                        union all
                        select  'TLH_LANGAS_EXP_'||VID as orig_system_ref, 
                                'TLH' as orig_system,
                                'TLH_'  || vid as party_ref,
                                createdate as create_date,
                                LASTMODIFIEDDATE as update_date,
                                'EXP' as interest_type,
                                'LANGAS Subscription Expiry Date' as interest_value,
                                to_char(LANGAS_SUB_EXPIRY_DATE,'DD/MM/YYYY HH24:MI:SS') as interest_value_details,
                                NULL as interest_section,
                                NULL as interest_sub_section,
                                NULL as alert_end_date,
                                NULL as status
                        from   tbl_temp_lancet_hubspot
                        where  LANGAS_SUB_EXPIRY_DATE is not null
                        union all
                        select  'TLH_LANPSY_EXP_'||VID as orig_system_ref, 
                                'TLH' as orig_system,
                                'TLH_'  || vid as party_ref,
                                createdate as create_date,
                                LASTMODIFIEDDATE as update_date,
                                'EXP' as interest_type,
                                'LANPSY Subscription Expiry Date' as interest_value,
                                to_char(LANPSY_SUB_EXPIRY_DATE,'DD/MM/YYYY HH24:MI:SS') as interest_value_details,
                                NULL as interest_section,
                                NULL as interest_sub_section,
                                NULL as alert_end_date,
                                NULL as status
                        from   tbl_temp_lancet_hubspot
                        where  LANPSY_SUB_EXPIRY_DATE is not null
                        union all
                        select  'TLH_LANHIV_EXP_'||VID as orig_system_ref, 
                                'TLH' as orig_system,
                                'TLH_'  || vid as party_ref,
                                createdate as create_date,
                                LASTMODIFIEDDATE as update_date,
                                'EXP' as interest_type,
                                'LANHIV Subscription Expiry Date' as interest_value,
                                to_char(LANHIV_SUB_EXPIRY_DATE,'DD/MM/YYYY HH24:MI:SS') as interest_value_details,
                                NULL as interest_section,
                                NULL as interest_sub_section,
                                NULL as alert_end_date,
                                NULL as status
                        from   tbl_temp_lancet_hubspot
                        where  LANHIV_SUB_EXPIRY_DATE is not null
                        union all
                        select  'TLH_LANHAE_EXP_'||VID as orig_system_ref, 
                                'TLH' as orig_system,
                                'TLH_'  || vid as party_ref,
                                createdate as create_date,
                                LASTMODIFIEDDATE as update_date,
                                'EXP' as interest_type,
                                'LANHAE Subscription Expiry Date' as interest_value,
                                to_char(LANHAE_SUB_EXPIRY_DATE,'DD/MM/YYYY HH24:MI:SS') as interest_value_details,
                                NULL as interest_section,
                                NULL as interest_sub_section,
                                NULL as alert_end_date,
                                NULL as status
                        from   tbl_temp_lancet_hubspot
                        where  LANHAE_SUB_EXPIRY_DATE is not null
                        union all
                        select  'TLH_LANCHI_EXP_'||VID as orig_system_ref, 
                                'TLH' as orig_system,
                                'TLH_'  || vid as party_ref,
                                createdate as create_date,
                                LASTMODIFIEDDATE as update_date,
                                'EXP' as interest_type,
                                'LANCHI Subscription Expiry Date' as interest_value,
                                to_char(LANCHI_SUB_EXPIRY_DATE,'DD/MM/YYYY HH24:MI:SS') as interest_value_details,
                                NULL as interest_section,
                                NULL as interest_sub_section,
                                NULL as alert_end_date,
                                NULL as status
                        from   tbl_temp_lancet_hubspot
                        where  LANCHI_SUB_EXPIRY_DATE is not null
                );
                
        END IF;
        

  END IF;

  SAVEPOINT clean_up;
  p_success := 'Y';

/*EXCEPTION

    WHEN OTHERS THEN
     v_message := 'problem running query - ' || v_query_count || '. ' || SQLERRM;
     v_err_code := SQLCODE;
     ROLLBACK TO clean_up;
     p_success := 'N';*/

END run_query;

END pk_pull;
/
