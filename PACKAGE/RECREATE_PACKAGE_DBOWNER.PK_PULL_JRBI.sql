CREATE PACKAGE DBOWNER.PK_PULL_JRBI
  CREATE OR REPLACE EDITIONABLE PACKAGE "DBOWNER"."PK_PULL_JRBI" AS
/*******************************************************************************
Author		: Michael Ranken
Description	: Package to populate Staging Tables
Audit		:
	Version		Date		User		Description
	1		01-JAN-2010	Michael Ranken	Initial version
*******************************************************************************/

	PROCEDURE pull_source(p_source IN VARCHAR2, p_success IN OUT VARCHAR2);
END pk_pull_jrbi;
/
CREATE OR REPLACE EDITIONABLE PACKAGE BODY "DBOWNER"."PK_PULL_JRBI" AS
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

	pk_main.log_time(p_source, 'pk_pull_jrbi', 'pull_source', 'start of procedure', g_pos, true);
	UPDATE dbowner.tbl_sources
	SET pull_start = SYSDATE,
	    pull_end = NULL,
	    pull_status = 'R',
	    pull_error_msg = NULL,
	    related_transfer_session = NULL
	WHERE source = p_source;
	COMMIT;


 IF p_source = 'JRBI' THEN

        SELECT NVL(this_marker_db1,sysdate)
        INTO v_source_time_db1
        FROM dbowner.tbl_sources
        WHERE source = 'JRBI';

    v_total_queries := 9;    

	END IF;

	UPDATE dbowner.tbl_sources SET this_marker_db1 = v_source_time_db1 WHERE source = p_source;
--	UPDATE dbowner.tbl_sources SET this_marker_db2 = v_source_time_db2 WHERE source = p_source;
   	p_success := 'Y';

	WHILE v_query_count < v_total_queries AND p_success = 'Y' LOOP

		v_attempts := 0;
		v_query_count := v_query_count + 1;
		p_success := 'N';

	    pk_main.log_time(p_source, 'pk_pull_jrbi', 'pull_source', p_source || ' query no: ' || v_query_count, v_query_count, FALSE);

		WHILE p_success <> 'Y' AND v_attempts <= 3 LOOP

			run_query(p_source, v_query_count, p_success);
			v_attempts := v_attempts + 1;

		END LOOP;

	END LOOP;

	IF p_success = 'Y' THEN
		UPDATE dbowner.tbl_sources
		SET last_marker_db1 = SYSDATE,
		    last_marker_db2 = SYSDATE,
            this_marker_db1 = SYSDATE,            
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

		pk_main.log_error(p_source, 'pk_pull_jrbi', 'pull_source', v_err_code, v_message, g_pos);

		UPDATE dbowner.tbl_sources
		SET pull_end = SYSDATE,
		pull_status = 'F',
		pull_error_msg = v_message
		WHERE source = p_source;
		COMMIT;
	END IF;

	v_query_count := v_query_count + 1;
	pk_main.log_time(p_source, 'pk_pull_jrbi', 'pull_source', 'end of procedure', v_query_count, true);

EXCEPTION

    WHEN OTHERS THEN
		ROLLBACK;
        p_success := 'N';
		v_message := 'query:' || v_query_count ||'. error:' || sys.dbms_utility.format_error_stack;
        pk_main.log_error(p_source, 'pk_pull_jrbi', 'pull_source', SQLCODE, v_message, g_pos);
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
	IF p_source = 'JRBI' THEN	 

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
				FROM JRBI_USER_DIM@JRBI e , jrbi_source_system_dim@jrbi d , jrbi_article_lifecycle_fact@jrbi f
                where e.SOURCE_SYSTEM_ID = d.SOURCE_SYSTEM_ID
                and   e.user_id = f.author_id
                and  e.rec_update_date > (SELECT max(last_marker_db1) FROM dbowner.tbl_sources WHERE source = 'JRBI') ;

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
				and  b.LATEST_REVISION_IND = 'Y'
                and  c.rec_update_date > (SELECT max(last_marker_db1) FROM dbowner.tbl_sources WHERE source = 'JRBI') ;
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
FROM JRBI_USER_DIM@JRBI e , jrbi_source_system_dim@jrbi d , jrbi_review_lifecycle_fact@jrbi r
where e.SOURCE_SYSTEM_ID = d.SOURCE_SYSTEM_ID
and   e.user_id = r.reviewer_id
and ( e.rec_update_date > (SELECT max(last_marker_db1) FROM dbowner.tbl_sources WHERE source = 'JRBI') or
r.rec_update_date > (SELECT max(last_marker_db1) FROM dbowner.tbl_sources WHERE source = 'JRBI') );
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
FROM JRBI_USER_DIM@JRBI e , jrbi_source_system_dim@jrbi d , jrbi_review_lifecycle_fact@jrbi r
where e.SOURCE_SYSTEM_ID = d.SOURCE_SYSTEM_ID
and   e.user_id = r.editor_id
and ( e.rec_update_date > (SELECT max(last_marker_db1) FROM dbowner.tbl_sources WHERE source = 'JRBI') or
r.rec_update_date > (SELECT max(last_marker_db1) FROM dbowner.tbl_sources WHERE source = 'JRBI') );
COMMIT;
END IF;

		IF p_query_no = 5 THEN

        EXECUTE IMMEDIATE 'ALTER TABLE TBL_TEMP_ITEMS MODIFY PARTITION par_JRBI UNUSABLE LOCAL INDEXES';

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
and iss.active_ind = 1
and ( ar.rec_update_date > (SELECT max(last_marker_db1) FROM dbowner.tbl_sources WHERE source = 'JRBI') or
      iss.rec_update_date > (SELECT max(last_marker_db1) FROM dbowner.tbl_sources WHERE source = 'JRBI') or 
      st.rec_update_date > (SELECT max(last_marker_db1) FROM dbowner.tbl_sources WHERE source = 'JRBI') );
COMMIT;

EXECUTE IMMEDIATE 'ALTER TABLE TBL_TEMP_ITEMS MODIFY PARTITION par_JRBI REBUILD UNUSABLE LOCAL INDEXES';

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
from JRBI_JOURNAL_DIM@JRBI jou ,
JRBI_JOURNAL_PRODUCTION_DIM@jrbi jp
where jou.journal_id = jp.journal_id
and jou.active_ind = 1
and jp.active_ind = 1
and ( jou.rec_update_date > (SELECT max(last_marker_db1) FROM dbowner.tbl_sources WHERE source = 'JRBI') or
      jp.rec_update_date > (SELECT max(last_marker_db1) FROM dbowner.tbl_sources WHERE source = 'JRBI') ) ;
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
FROM JRBI_CONTRIBUTOR_BRIDGE@jrbi b , JRBI_CONTRIBUTOR_DIM@JRBI c 
where b.contributor_id = c.contributor_id (+) 
and b.LATEST_REVISION_IND = 'Y'
and c.corr_auth_ind not in ( 'Y','ON','on' )
and  c.rec_update_date > (SELECT max(last_marker_db1) FROM dbowner.tbl_sources WHERE source = 'JRBI')  ;
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
FROM JRBI_REVIEW_LIFECYCLE_FACT@JRBI rv , JRBI_STATUS_DIM@jrbi st 
where st.status_id = rv.status_id
and rv.REVIEWER_ID >0
and ( rv.rec_update_date > (SELECT max(last_marker_db1) FROM dbowner.tbl_sources WHERE source = 'JRBI') or
      st.rec_update_date > (SELECT max(last_marker_db1) FROM dbowner.tbl_sources WHERE source = 'JRBI') ) ;
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
FROM JRBI_REVIEW_LIFECYCLE_FACT@JRBI rv , JRBI_STATUS_DIM@jrbi st 
where st.status_id = rv.status_id
and rv.EDITOR_ID >0
and ( rv.rec_update_date > (SELECT max(last_marker_db1) FROM dbowner.tbl_sources WHERE source = 'JRBI') or
      st.rec_update_date > (SELECT max(last_marker_db1) FROM dbowner.tbl_sources WHERE source = 'JRBI') );
COMMIT;
END IF;

 END IF;

  SAVEPOINT clean_up;
  p_success := 'Y';


END run_query;

END pk_pull_jrbi;
/
