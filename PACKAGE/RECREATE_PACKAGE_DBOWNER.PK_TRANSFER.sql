CREATE PACKAGE DBOWNER.PK_TRANSFER
  CREATE OR REPLACE EDITIONABLE PACKAGE "DBOWNER"."PK_TRANSFER" AS

TYPE TP_TEMP_PARTY_ROW IS REF CURSOR RETURN dbowner.tbl_temp_parties%ROWTYPE;

PROCEDURE transfer_source(p_source IN VARCHAR2, p_success IN OUT VARCHAR2, p_failing_records IN OUT VARCHAR2);
PROCEDURE transfer_parties(p_source IN VARCHAR2);
PROCEDURE transfer_relationships(p_source IN VARCHAR2);
PROCEDURE transfer_interests(p_source IN VARCHAR2);
PROCEDURE transfer_ips(p_source IN VARCHAR2);
PROCEDURE transfer_items(p_source IN VARCHAR2);
PROCEDURE transfer_downloads(p_source IN VARCHAR2);
PROCEDURE transfer_subscriptions(p_source IN VARCHAR2);
PROCEDURE transfer_item_subjects(p_source IN VARCHAR2);

END pk_transfer;
 
/
CREATE OR REPLACE EDITIONABLE PACKAGE BODY "DBOWNER"."PK_TRANSFER" AS

PROCEDURE prepare_parties(p_source IN VARCHAR2);
PROCEDURE transfer_party(p_row IN dbowner.tbl_temp_parties%ROWTYPE, p_success IN OUT NOCOPY VARCHAR2, p_mode IN OUT VARCHAR2);
PROCEDURE transfer_relationship(p_row IN dbowner.tbl_temp_relationships%ROWTYPE, p_success IN OUT VARCHAR2, p_mode IN OUT VARCHAR2);
PROCEDURE transfer_interest(p_row IN dbowner.tbl_temp_interests%ROWTYPE, p_success IN OUT VARCHAR2, p_mode IN OUT VARCHAR2);
PROCEDURE transfer_ip(p_row IN dbowner.tbl_temp_ips%ROWTYPE, p_success IN OUT VARCHAR2, p_mode IN OUT VARCHAR2);
PROCEDURE transfer_item(p_row IN dbowner.tbl_temp_items%ROWTYPE, p_success IN OUT VARCHAR2, p_mode IN OUT VARCHAR2);
PROCEDURE transfer_download(p_row IN dbowner.tbl_temp_downloads%ROWTYPE, p_success IN OUT VARCHAR2);
PROCEDURE transfer_subscription(p_row IN dbowner.tbl_temp_subscriptions%ROWTYPE, p_success IN OUT VARCHAR2, p_mode IN OUT VARCHAR2);
PROCEDURE transfer_item_subject(p_row IN dbowner.tbl_temp_item_subjects%ROWTYPE, p_success IN OUT VARCHAR2, p_mode IN OUT VARCHAR2);
PROCEDURE update_session_stats(p_table IN VARCHAR2, p_inserts IN NUMBER, p_updates IN NUMBER, p_deletes IN NUMBER);

g_pos	  			NUMBER := 0;
g_warning 			BOOLEAN := FALSE;
g_session_id		NUMBER;
v_error 			VARCHAR2(3000) := NULL;
v_failing_records	VARCHAR2(100) := '';
v_dedupe_running	VARCHAR2(1);
e_dedupe_running	EXCEPTION;

PROCEDURE transfer_source(p_source IN VARCHAR2, p_success IN OUT VARCHAR2, p_failing_records IN OUT VARCHAR2) IS

BEGIN

	pk_main.log_time(p_source, 'pk_transfer', 'transfer_source', 'checking that the dedupe process is not running', g_pos, true);
	SELECT dedupe_running INTO v_dedupe_running FROM dbowner.tbl_dedupe_control;
	IF v_dedupe_running = 'Y' THEN

		RAISE e_dedupe_running;
	END IF;

	pk_main.log_time(p_source, 'pk_transfer', 'transfer_source', 'establishing the transfer_session', g_pos, true);
	SELECT seq_transfer_sessions.NEXTVAL INTO g_session_id FROM dual;
	INSERT INTO dbowner.tbl_transfer_sessions (session_id, session_source, session_start) VALUES (g_session_id, p_source, SYSDATE);
	UPDATE dbowner.tbl_sources SET transfer_start = SYSDATE, transfer_end = NULL, transfer_status = 'R', transfer_error_msg = NULL, related_transfer_session = g_session_id WHERE source = p_source;
	COMMIT;

	EXECUTE IMMEDIATE 'ALTER SESSION SET skip_unusable_indexes = TRUE';

	pk_main.log_time(p_source, 'pk_transfer', 'transfer_source', 'just about to call transfer parties', g_pos, true);
	transfer_parties(p_source);
	pk_main.log_time(p_source, 'pk_transfer', 'transfer_source', 'just about to call transfer relationships', g_pos, true);
	transfer_relationships(p_source);
	IF p_source <> 'DEL' AND p_source <> 'COP' THEN
		pk_main.log_time(p_source, 'pk_transfer', 'transfer_source', 'just about to call transfer interests', g_pos, true);
		transfer_interests(p_source);
	END IF;
	pk_main.log_time(p_source, 'pk_transfer', 'transfer_source', 'just about to call transfer ips', g_pos, true);
	transfer_ips(p_source);
	pk_main.log_time(p_source, 'pk_transfer', 'transfer_source', 'just about to call transfer items', g_pos, true);
	transfer_items(p_source);
	pk_main.log_time(p_source, 'pk_transfer', 'transfer_source', 'just about to call transfer downloads', g_pos, true);
	transfer_downloads(p_source);
	pk_main.log_time(p_source, 'pk_transfer', 'transfer_source', 'just about to call transfer subscriptions', g_pos, true);
	transfer_subscriptions(p_source);
	pk_main.log_time(p_source, 'pk_transfer', 'transfer_source', 'just about to call transfer item subjects', g_pos, true);
	transfer_item_subjects(p_source);
	pk_main.log_time(p_source, 'pk_transfer', 'transfer_source', 'end of procedure', g_pos, true);

	p_success := 'Y';
	p_failing_records := v_failing_records;
	UPDATE dbowner.tbl_transfer_sessions SET session_end = SYSDATE WHERE session_id = g_session_id;
	UPDATE dbowner.tbl_sources SET transfer_end = SYSDATE, transfer_status = 'S' WHERE source = p_source;
	COMMIT;

EXCEPTION

	WHEN e_dedupe_running THEN
        p_success := 'N';
		v_error := 'dedupe process currently running';
		pk_main.log_error(p_source, 'pk_transfer', 'transfer_source', 0, v_error, g_pos);
		UPDATE dbowner.tbl_transfer_sessions SET session_end = SYSDATE WHERE session_id = g_session_id;
		UPDATE dbowner.tbl_sources SET transfer_end = SYSDATE, transfer_status = 'F', transfer_error_msg = v_error WHERE source = p_source;

    WHEN OTHERS THEN
		ROLLBACK;
        p_success := 'N';
		v_error := SQLCODE || ': ' || SQLERRM;
        pk_main.log_error(p_source, 'pk_transfer', 'transfer_source', SQLCODE, 'problem transfering temp tables - ' || SQLERRM, g_pos);
		UPDATE dbowner.tbl_transfer_sessions SET session_end = SYSDATE WHERE session_id = g_session_id;
		UPDATE dbowner.tbl_sources SET transfer_end = SYSDATE, transfer_status = 'F', transfer_error_msg = v_error WHERE source = p_source;

END transfer_source;

PROCEDURE update_session_stats(p_table IN VARCHAR2, p_inserts IN NUMBER, p_updates IN NUMBER, p_deletes IN NUMBER) IS

v_sql VARCHAR2 (1000);

BEGIN

	IF p_table = 'parties' THEN
	   	UPDATE dbowner.tbl_transfer_sessions SET
			parties_last_update = SYSDATE,
			parties_inserts = parties_inserts + p_inserts,
			parties_updates = parties_updates + p_updates,
			parties_deletes = parties_deletes + p_deletes
		WHERE session_id = g_session_id;
	ELSIF p_table = 'relationships' THEN
	   	UPDATE dbowner.tbl_transfer_sessions SET
			relationships_last_update = SYSDATE,
			relationships_inserts = relationships_inserts + p_inserts,
			relationships_updates = relationships_updates + p_updates,
			relationships_deletes = relationships_deletes + p_deletes
		WHERE session_id = g_session_id;
	ELSIF p_table = 'interests' THEN
	   	UPDATE dbowner.tbl_transfer_sessions SET
			interests_last_update = SYSDATE,
			interests_inserts = interests_inserts + p_inserts,
			interests_updates = interests_updates + p_updates,
			interests_deletes = interests_deletes + p_deletes
		WHERE session_id = g_session_id;
	ELSIF p_table = 'ips' THEN
	   	UPDATE dbowner.tbl_transfer_sessions SET
			ips_last_update = SYSDATE,
			ips_inserts = ips_inserts + p_inserts,
			ips_updates = ips_updates + p_updates,
			ips_deletes = ips_deletes + p_deletes
		WHERE session_id = g_session_id;
	ELSIF p_table = 'items' THEN
	   	UPDATE dbowner.tbl_transfer_sessions SET
			items_last_update = SYSDATE,
			items_inserts = items_inserts + p_inserts,
			items_updates = items_updates + p_updates,
			items_deletes = items_deletes + p_deletes
		WHERE session_id = g_session_id;
	ELSIF p_table = 'downloads' THEN
	   	UPDATE dbowner.tbl_transfer_sessions SET
			downloads_last_update = SYSDATE,
			downloads_inserts = downloads_inserts + p_inserts
		WHERE session_id = g_session_id;
	ELSIF p_table = 'subscriptions' THEN
	   	UPDATE dbowner.tbl_transfer_sessions SET
			subscriptions_last_update = SYSDATE,
			subscriptions_inserts = subscriptions_inserts + p_inserts,
			subscriptions_updates = subscriptions_updates + p_updates,
			subscriptions_deletes = subscriptions_deletes + p_deletes
		WHERE session_id = g_session_id;
	ELSIF p_table = 'item_subjects' THEN
	   	UPDATE dbowner.tbl_transfer_sessions SET
			item_subjects_last_update = SYSDATE,
			item_subjects_inserts = item_subjects_inserts + p_inserts,
			item_subjects_updates = item_subjects_updates + p_updates,
			item_subjects_deletes = item_subjects_deletes + p_deletes
		WHERE session_id = g_session_id;
	END IF;

END update_session_stats;

PROCEDURE transfer_parties(p_source IN VARCHAR2) IS

v_success VARCHAR2(1);
v_counter NUMBER(5) := 0;
v_inserts NUMBER := 0;
v_updates NUMBER := 0;
v_deletes NUMBER := 0;
v_fails NUMBER := 0;
v_mode VARCHAR2(1);
v_idxes NUMBER := 0;

CURSOR cur_parties IS
	SELECT * FROM dbowner.tbl_temp_parties
	WHERE orig_system = p_source
	ORDER BY orig_system_ref, run_time ASC;

BEGIN

	SELECT	count(*) INTO v_idxes
	FROM	user_indexes a, user_ind_partitions b
	WHERE	a.index_name = b.index_name
	AND		b.partition_name = 'PAR_' || UPPER(p_source)
	AND		a.table_name = 'TBL_PARTIES';

	COMMIT;

	pk_main.log_time(p_source, 'pk_transfer', 'transfer_parties', 'just about to prepare parties', g_pos, true);
	prepare_parties(p_source);
	pk_main.log_time(p_source, 'pk_transfer', 'transfer_parties', 'just about to truncate tbl_temp_parties_fail', g_pos, true);
	DELETE dbowner.tbl_temp_parties_fail WHERE orig_system = p_source;

	IF v_idxes > 0 THEN

		pk_main.log_time(p_source, 'pk_transfer', 'transfer_parties', 'about to render local indexes unusable', g_pos, true);
		EXECUTE IMMEDIATE 'ALTER TABLE dbowner.tbl_parties MODIFY PARTITION par_' || p_source || ' UNUSABLE LOCAL INDEXES';

	END IF;

	pk_main.log_time(p_source, 'pk_transfer', 'transfer_parties', 'starting transfer', g_pos, true);
   	UPDATE dbowner.tbl_transfer_sessions SET parties_start = SYSDATE WHERE session_id = g_session_id;
	COMMIT;

	FOR rec IN cur_parties LOOP

		SAVEPOINT current_party;
		transfer_party(rec, v_success, v_mode);

		IF v_success = 'Y' THEN

		   v_counter := v_counter + 1;
		   IF v_mode = 'I' THEN
		   	  v_inserts := v_inserts + 1;
		   ELSIF v_mode = 'U' THEN
		   	  v_updates := v_updates + 1;
		   ELSIF v_mode = 'D' THEN
		   	  v_deletes := v_deletes + 1;
		   END IF;

		   IF v_counter = 1000 THEN
		      update_session_stats('parties', v_inserts, v_updates, v_deletes);
			  v_counter := 0;
			  v_inserts := 0;
			  v_updates := 0;
			  v_deletes := 0;
		   	  COMMIT;
		   END IF;

		ELSE

			ROLLBACK TO current_party;
			INSERT INTO dbowner.tbl_temp_parties_fail VALUES (p_source, rec.orig_system_ref);
			v_fails := v_fails + 1;
			COMMIT;

		END IF;

	END LOOP;

    update_session_stats('parties', v_inserts, v_updates, v_deletes);

    IF v_idxes > 0 THEN
		pk_main.log_time(p_source, 'pk_transfer', 'transfer_parties', 'about to rebuild local indexes', g_pos, true);
		EXECUTE IMMEDIATE 'ALTER TABLE dbowner.tbl_parties MODIFY PARTITION par_' || p_source || ' REBUILD UNUSABLE LOCAL INDEXES';
	END IF;

	pk_main.log_time(p_source, 'pk_transfer', 'transfer_parties', 'just about to delete tbl_temp_parties', g_pos, true);
	COMMIT;
	IF v_fails > 0 THEN
	   	DELETE dbowner.tbl_temp_parties a
			WHERE orig_system = p_source
			AND NOT EXISTS (SELECT 'x' FROM dbowner.tbl_temp_parties_fail b WHERE b.orig_system = p_source AND a.orig_system_ref = b.orig_system_ref);
		v_failing_records := v_failing_records || ',parties:' || v_fails;
	ELSE
	   	DELETE dbowner.tbl_temp_parties a
			WHERE orig_system = p_source;
	END IF;
	COMMIT;
	pk_main.log_time(p_source, 'pk_transfer', 'transfer_parties', 'end of procedure', g_pos, true);

END transfer_parties;

PROCEDURE prepare_parties(p_source IN VARCHAR2) IS

BEGIN

	 -- parse out the name field for sis people
   /*Commenting out the update statement of tbl_temp_parties  for Source = 'SIS' for Jira 2261
	IF p_source = 'SIS' THEN
		pk_main.log_time(p_source, 'pk_transfer', 'prepare_parties', 'update sis name info', g_pos, true);
		UPDATE dbowner.tbl_temp_parties SET
			usc_contact_title = fk_render_sis_party(usc_contact_firstname, 'TITLE'),
			orig_title = fk_render_sis_party(usc_contact_firstname, 'TITLE'),
			usc_contact_firstname = fk_render_sis_party(usc_contact_firstname, 'FIRSTNAME'),
			usc_contact_lastname = fk_render_sis_party(usc_contact_firstname, 'LASTNAME')
		WHERE orig_system = 'SIS' AND doctored IS NULL;
	END IF;

	pk_main.log_time(p_source, 'pk_transfer', 'prepare_parties', 'format titles', g_pos, true);
	 -- derive the correct title format
	UPDATE dbowner.tbl_temp_parties a SET a.usc_contact_title =
		(SELECT b.formatted_value FROM dbowner.tbl_titles b WHERE a.usc_contact_title = b.source_value)
	WHERE a.doctored IS NULL
	AND a.orig_system = p_source;*/

	pk_main.log_time(p_source, 'pk_transfer', 'prepare_parties', 'format countries and set doctored = y', g_pos, true);
	 -- derive the correct country name
	UPDATE dbowner.tbl_temp_parties a SET (a.iso_country_code, a.usc_country, a.doctored) =
		(SELECT c.iso_code, c.country_name, 'Y'
		 FROM dbowner.tbl_countries b, dbowner.tbl_iso_countries c
		 WHERE upper(trim(a.usc_country)) = b.source_value
		 AND b.iso_code = c.iso_code)
	WHERE a.doctored IS NULL
	AND a.orig_system = p_source;
	--psloane 26/11/2003 added to tbl_countries and commented out line below
	--AND NOT (a.orig_system = 'STR' AND a.party_type = 'GCN');

    COMMIT;
	pk_main.log_time(p_source, 'pk_transfer', 'prepare_parties', 'end of procedure', g_pos, true);

END prepare_parties;

PROCEDURE transfer_party(p_row IN dbowner.tbl_temp_parties%ROWTYPE, p_success IN OUT NOCOPY VARCHAR2, p_mode IN OUT VARCHAR2) IS

v_party_name VARCHAR2(600);
v_cleaned_email VARCHAR2(256);
v_cleaned_email_ext VARCHAR2(256);
v_cleaned_email_domain VARCHAR2(256);
v_concat_address VARCHAR2(2000);
v_orig_address VARCHAR2(2000);
v_orig_email VARCHAR2(300);
v_orig_country VARCHAR2(300);
v_orig_title VARCHAR2(256);
v_email_clean_status VARCHAR2(1);
v_email_auto_clean_status VARCHAR2(1);
v_address_clean_status VARCHAR2(1);
v_title_clean_status VARCHAR2(1);
v_country_clean_status VARCHAR2(1);
v_orig_mailing_status VARCHAR2(30);

CURSOR cur_existing(p_orig_system_ref IN VARCHAR2) IS
	SELECT orig_address, address_clean_status, orig_email, email_clean_status, orig_country, country_clean_status, orig_title, title_clean_status, mailing_status
	FROM dbowner.tbl_parties WHERE orig_party_ref = p_orig_system_ref;

BEGIN

 	IF p_row.status || 'x' = 'Dx' THEN

	   p_mode := 'D';
	   UPDATE dbowner.tbl_parties SET record_status = 'D', orig_update_date = p_row.run_time, capri_update_date = SYSDATE
	   WHERE orig_party_ref = p_row.orig_system_ref;

	ELSE

        --// GCS - Changing to new function
        -- v_cleaned_email := fk_clean_email(p_row.usc_dedupe_email);
        v_cleaned_email := fk_primary_email(p_row.usc_dedupe_email);

		--v_cleaned_email_domain := substr(v_cleaned_email, instr(v_cleaned_email,'@')+1, instr(v_cleaned_email,'.',instr(v_cleaned_email,'@')+1) - instr(v_cleaned_email,'@')-1);
		--psloane 27/11/2003 adjusted to the line below
		v_cleaned_email_domain := fk_get_email_detail(v_cleaned_email, 'domain');
		--v_cleaned_email_ext := substr(v_cleaned_email, instr(v_cleaned_email, '.', -1, 1) + 1);
		--psloane 27/11/2003 adjusted to the line below
		v_cleaned_email_ext := fk_get_email_detail(v_cleaned_email, 'extension');
		v_email_auto_clean_status := fk_iif(lower(p_row.usc_dedupe_email), '=', v_cleaned_email, 'N', 'A');
		v_party_name := fk_format_party_name(p_row.usc_contact_firstname, p_row.usc_contact_lastname); --same for ORG and PER
		v_concat_address := fk_concat_address(p_row.usc_addr, p_row.usr_address2, p_row.address3, p_row.address4, p_row.usc_city, p_row.usc_state, p_row.usc_zip);

		OPEN cur_existing (p_row.orig_system_ref);
		FETCH cur_existing INTO v_orig_address, v_address_clean_status,
								v_orig_email, v_email_clean_status,
								v_orig_country, v_country_clean_status,
								v_orig_title, v_title_clean_status, v_orig_mailing_status;

		IF (cur_existing%FOUND) THEN
				p_mode := 'U';
				v_address_clean_status := fk_iif(v_orig_address, '=', v_concat_address, v_address_clean_status, 'N');
				v_email_clean_status := fk_iif(v_orig_email, '=', p_row.usc_dedupe_email, v_email_clean_status, v_email_auto_clean_status);
				v_country_clean_status := fk_iif(v_orig_country, '=', p_row.orig_country, v_country_clean_status, 'N');
				v_title_clean_status := fk_iif(v_orig_title, '=', p_row.orig_title, v_title_clean_status, 'N');
		ELSE
				p_mode := 'I';
		END IF;

		CLOSE cur_existing;

		--v_dedupe_no is tested and populated below
		--IF v_cleaned_email <> 'deleted' THEN --if statement to stop deleted people in GCD (together with related de-dupes) being grouped together.
		--   	pk_dedupe.get_party_dedupe_id(p_row.orig_system, p_row.dedupe_type, p_row.orig_system_ref, v_party_name, p_row.usc_dedupe_email, p_mode, v_dedupe_no);
		--END IF;

		IF p_mode = 'I' THEN
		   INSERT INTO dbowner.tbl_parties (party_id,
		   		  	   			    orig_party_ref,
		   		  	   			    orig_system,
									orig_site,
								    orig_create_date,
									orig_update_date,
									capri_create_date,
									capri_update_date,
									record_status,
									dedupe_type,
									dedupe_id,
									party_type,
									party_name,
									title,
									orig_title,
									title_clean_status,
									firstname,
									lastname,
									address_is_primary,
									address_type,
									address1,
									address2,
									address3,
									address4,
									city,
									state,
									post_code,
									orig_address,
									address_clean_status,
									country,
									iso_country_code,
									orig_country,
									country_clean_status,
									region,
									rso_region,
									continent,
									user_department,
									user_organisation,
									phone,
									phone_2,
									phone_3,
									website,
									fax,
									email,
									orig_email,
									email_extension,
									email_domain,
									email_clean_status,
									email_format,
									orig_email_bounces,
									mailing_status,
									sales_emails,
									marketing_emails,
									member_search,
									user_status,
									user_status_date,
									user_referral,
									login,
									password,
									last_visit_date,
									last_visit_date_remembered,
									principal_field,
									url_referral,
									other_referral,
									job_title,
									job_type,
									org_name,
									org_type,
									org_primary_type,
									org_secondary_type,
									org_url,
									org_orig_system_ref,
									org_no_of_users,
									related_sis_id,
									top_level_sis_id,
									subscriber_code,
									total_visits,
									month_visits,
									chunk_size,
									display_search_results,
									display_toc_in_nia,
									access_type,
									sort_preference,
									history_enabled,
									history_expand,
									subject_area_home_page,
									toggle_preference,
									read_regularly,
                  cmx_id,
                  account_loc)
			VALUES (seq_party_id.nextval,
				    p_row.orig_system_ref,
					p_row.orig_system,
					p_row.original_site,
					p_row.usr_created_date,
					p_row.run_time,
					SYSDATE,
					SYSDATE,
					'I',
					p_row.dedupe_type,
					0,
					p_row.party_type,
					v_party_name,
					p_row.usc_contact_title,
					p_row.orig_title,
					'N',
					p_row.usc_contact_firstname,
					p_row.usc_contact_lastname,
					p_row.usc_primary_address,
					p_row.usc_add_type,
					p_row.usc_addr,
					p_row.usr_address2,
					p_row.address3,
					p_row.address4,
					p_row.usc_city,
					p_row.usc_state,
					p_row.usc_zip,
					v_concat_address,
					'N',
					p_row.usc_country,
					p_row.iso_country_code,
					p_row.orig_country,
					'N',
					p_row.region_name,
					p_row.rso_region,
					p_row.con_name,
					p_row.usc_dept,
					p_row.usc_org,
					p_row.usc_phone,
					p_row.phone_2,
					p_row.phone_3,
					p_row.web_site,
					p_row.usc_fax,
					v_cleaned_email,
					p_row.usc_dedupe_email,
					v_cleaned_email_ext,
					v_cleaned_email_domain,
					v_email_auto_clean_status,
					p_row.emailformat,
					p_row.usc_email_bounces,
					p_row.mailing_status,
					p_row.sales_emails,
					p_row.marketing_emails,
					p_row.member_search,
					p_row.usr_status,
					p_row.usr_status_date,
					p_row.usr_ref,
					p_row.usr_login,
					p_row.usr_passwd,
					p_row.usr_last_visit_date,
					p_row.usr_last_visit_date_remembered,
					p_row.usr_principal_field,
					p_row.usr_url_ref,
					p_row.usr_other_ref,
					p_row.jobtitle,
					p_row.desk_type,
					p_row.inst_name,
					p_row.org_type,
					p_row.prime_type_descr,
					p_row.second_type_descr,
					p_row.usc_institute_url,
					p_row.unique_inst_id,
					p_row.inst_no_of_users,
					p_row.related_sis_id,
					p_row.top_level_sis_id,
					p_row.usr_subscriber_code,
					p_row.usi_visits_total,
					p_row.usi_visits_month,
					p_row.chunk_size,
					p_row.display_srch_results,
					p_row.display_toc_in_nia,
					p_row.user_access_type,
					p_row.sort_preference,
					p_row.history_enabled,
					p_row.history_expand,
					p_row.subj_area_home_page,
					p_row.toggle_pref,
					p_row.read_regularly,
          p_row.cmx_id,
          p_row.account_loc);
		ELSE

		   --the following relates to title, email, country and the address fields:
		   --when 'clean status' is null (because the value has not been looked at or because source values have changed)
		   --then the value is replaced, else the original value (which has been checked and possibly cleaned) is kept
       --JIRA 2961 - Domain name updates, added email_domain update below
		   UPDATE dbowner.tbl_parties SET
				orig_site = p_row.original_site,
				orig_update_date = p_row.run_time,
				capri_update_date = SYSDATE,
				record_status = decode(v_cleaned_email||v_party_name, email||party_name, decode(record_status, 'D', 'U', record_status), 'U'), -- wont need to dedupe again if party_name and email haven't changed. chance that just inserted before an update so have left record_status in case. the test for 'D' is incase this record has been reinstanted on the source system for some reason
				party_name = v_party_name,
				title = decode(v_title_clean_status, 'N', p_row.usc_contact_title, title),
				orig_title = p_row.orig_title,
				title_clean_status = v_title_clean_status,
				firstname = p_row.usc_contact_firstname,
				lastname = p_row.usc_contact_lastname,
				address_is_primary = p_row.usc_primary_address,
				address_type = p_row.usc_add_type,
				address1 = decode(v_address_clean_status, 'N', p_row.usc_addr, address1),
				address2 = decode(v_address_clean_status, 'N', p_row.usr_address2, address2),
				address3 = decode(v_address_clean_status, 'N', p_row.address3, address3),
				address4 = decode(v_address_clean_status, 'N', p_row.address4, address4),
				city = decode(v_address_clean_status, 'N', p_row.usc_city, city),
				state = decode(v_address_clean_status, 'N', p_row.usc_state, state),
				post_code = decode(v_address_clean_status, 'N', p_row.usc_zip, post_code),
				orig_address = v_concat_address,
				address_clean_status = v_address_clean_status,
				country = decode(v_country_clean_status, 'N', p_row.usc_country, country),
				iso_country_code = decode(v_country_clean_status, 'N', p_row.iso_country_code, iso_country_code),
				orig_country = p_row.orig_country,
				country_clean_status = v_country_clean_status,
				region = p_row.region_name,
				rso_region = p_row.rso_region,
				continent = p_row.con_name,
				user_department = p_row.usc_dept,
				user_organisation = p_row.usc_org,
				phone = p_row.usc_phone,
        phone_2 = p_row.phone_2,
        phone_3 = p_row.phone_3,
				fax = p_row.usc_fax,
        website = p_row.web_site,
        mailing_status = case when v_cleaned_email <> email and v_email_clean_status = 'N' then '' else mailing_status end,
				email = decode(v_email_clean_status, 'N', v_cleaned_email, email),
        email_domain = decode(v_email_clean_status, 'N', v_cleaned_email_domain, email_domain),
				orig_email = p_row.usc_dedupe_email,
				email_clean_status = v_email_clean_status,
				email_format = p_row.emailformat,
				orig_email_bounces = p_row.usc_email_bounces,
				sales_emails = p_row.sales_emails,
				marketing_emails = p_row.marketing_emails,
				member_search = p_row.member_search,
				user_status = p_row.usr_status,
				user_status_date = p_row.usr_status_date,
				user_referral = p_row.usr_ref,
				login = p_row.usr_login,
				password = p_row.usr_passwd,
				last_visit_date = p_row.usr_last_visit_date,
				last_visit_date_remembered = p_row.usr_last_visit_date_remembered,
				principal_field = p_row.usr_principal_field,
				url_referral = p_row.usr_url_ref,
				other_referral = p_row.usr_other_ref,
				job_title = p_row.jobtitle,
				job_type = p_row.desk_type,
				org_name = p_row.inst_name,
				org_type = p_row.org_type,
				org_primary_type = p_row.prime_type_descr,
				org_secondary_type = p_row.second_type_descr,
				org_url = p_row.usc_institute_url,
				org_orig_system_ref = p_row.unique_inst_id,
				org_no_of_users = p_row.inst_no_of_users,
				related_sis_id = p_row.related_sis_id,
				top_level_sis_id = p_row.top_level_sis_id,
				subscriber_code = p_row.usr_subscriber_code,
				total_visits = p_row.usi_visits_total,
				month_visits = p_row.usi_visits_month,
				chunk_size = p_row.chunk_size,
				display_search_results = p_row.display_srch_results,
				display_toc_in_nia = p_row.display_toc_in_nia,
				access_type = p_row.user_access_type,
				sort_preference = p_row.sort_preference,
				history_enabled = p_row.history_enabled,
				history_expand = p_row.history_expand,
				subject_area_home_page = p_row.subj_area_home_page,
				toggle_preference = p_row.toggle_pref,
				read_regularly = p_row.read_regularly,
        cmx_id = p_row.cmx_id,
        account_loc = p_row.account_loc
		   WHERE orig_party_ref = p_row.orig_system_ref;

		END IF;

	END IF;

	p_success := 'Y';

EXCEPTION

	WHEN OTHERS THEN
   		p_success := 'N';
   		pk_main.log_error(p_row.orig_system, 'pk_transfer', 'transfer_party', SQLCODE, 'party record error (' || p_row.orig_system_ref || '): '  || SQLERRM, g_pos);

END transfer_party;

PROCEDURE transfer_relationships(p_source IN VARCHAR2) IS

v_success VARCHAR2(1);
v_counter NUMBER(5) := 0;
v_inserts NUMBER := 0;
v_updates NUMBER := 0;
v_deletes NUMBER := 0;
v_fails NUMBER := 0;
v_mode VARCHAR2(1);

CURSOR cur_relationships IS
	SELECT * FROM dbowner.tbl_temp_relationships
	WHERE orig_system = p_source;

BEGIN

	EXECUTE IMMEDIATE 'TRUNCATE TABLE dbowner.tbl_temp_relationships_fail';
	pk_main.log_time(p_source, 'pk_transfer', 'transfer_relationships', 'start of transfer', g_pos, true);
   	UPDATE dbowner.tbl_transfer_sessions SET relationships_start = SYSDATE WHERE session_id = g_session_id;
	COMMIT;

	FOR rec IN cur_relationships LOOP

		SAVEPOINT current_relationship;
		transfer_relationship(rec, v_success, v_mode);

		IF v_success = 'Y' THEN

		   v_counter := v_counter + 1;
		   IF v_mode = 'I' THEN
		   	  v_inserts := v_inserts + 1;
		   ELSIF v_mode = 'U' THEN
		   	  v_updates := v_updates + 1;
		   ELSIF v_mode = 'D' THEN
		   	  v_deletes := v_deletes + 1;
		   END IF;

		   IF v_counter = 1000 THEN
		      update_session_stats('relationships', v_inserts, v_updates, v_deletes);
			  v_counter := 0;
			  v_inserts := 0;
			  v_updates := 0;
			  v_deletes := 0;
		   	  COMMIT;
		   END IF;

		ELSE

			ROLLBACK TO current_relationship;
			INSERT INTO dbowner.tbl_temp_relationships_fail VALUES (rec.subject_ref, rec.rel_type, rec.object_ref);
			v_fails := v_fails + 1;
			COMMIT;

		END IF;

	END LOOP;

    update_session_stats('relationships', v_inserts, v_updates, v_deletes);
	COMMIT;
	pk_main.log_time(p_source, 'pk_transfer', 'transfer_relationships', 'just about to delete dbowner.tbl_temp_relationships', g_pos, true);
	IF v_fails > 0 THEN
		DELETE dbowner.tbl_temp_relationships a
			WHERE orig_system = p_source
			AND NOT EXISTS (SELECT 'x' FROM dbowner.tbl_temp_relationships_fail b WHERE a.subject_ref = b.subject_ref AND a.rel_type = b.rel_type AND a.object_ref = b.object_ref);
		v_failing_records := v_failing_records || ',relationships:' || v_fails;
	ELSE
		DELETE dbowner.tbl_temp_relationships a
			WHERE orig_system = p_source;
	NULL;
	END IF;
	COMMIT;
	pk_main.log_time(p_source, 'pk_transfer', 'transfer_relationships', 'end of procedure', g_pos, true);

END transfer_relationships;

PROCEDURE transfer_relationship(p_row IN dbowner.tbl_temp_relationships%ROWTYPE, p_success IN OUT VARCHAR2, p_mode IN OUT VARCHAR2) IS

CURSOR cur_existing(p_subject_ref IN VARCHAR2, p_rel_type IN VARCHAR2, p_object_ref IN VARCHAR2) IS
	SELECT 'x' FROM dbowner.tbl_relationships
	WHERE subject_ref = p_subject_ref
	AND rel_type = p_rel_type
	AND object_ref = p_object_ref;
v_dummy VARCHAR2(1);

BEGIN

 	IF p_row.status || 'x' = 'Dx' THEN

	   p_mode := 'D';
	   UPDATE dbowner.tbl_relationships SET record_status = 'D', capri_update_date = SYSDATE
	   WHERE subject_ref = p_row.subject_ref
	   AND rel_type = p_row.rel_type
	   AND object_ref = p_row.object_ref;

	ELSE

		OPEN cur_existing (p_row.subject_ref, p_row.rel_type, p_row.object_ref);
		FETCH cur_existing INTO v_dummy;
		IF NOT(cur_existing%FOUND) THEN
		   p_mode := 'I';
		   INSERT INTO dbowner.tbl_relationships VALUES
		   		  (seq_relationships_1.NEXTVAL,
				  p_row.orig_system,
				  p_row.subject_ref,
				  p_row.rel_type,
				  p_row.object_ref,
				  SYSDATE,
				  SYSDATE,
				  'A');
		END IF;
		CLOSE cur_existing;

	END IF;

	p_success := 'Y';

EXCEPTION

	WHEN OTHERS THEN
   		p_success := 'N';
   		pk_main.log_error(p_row.orig_system, 'pk_transfer', 'transfer_relationship', SQLCODE, 'relationship record error (subject:' || p_row.subject_ref || ', object:' || p_row.object_ref || '): '  || SQLERRM, g_pos);

END transfer_relationship;

PROCEDURE transfer_interests(p_source IN VARCHAR2) IS

v_success VARCHAR2(1);
v_counter NUMBER(5) := 0;
v_inserts NUMBER := 0;
v_updates NUMBER := 0;
v_deletes NUMBER := 0;
v_fails NUMBER := 0;
v_mode VARCHAR2(1);
v_idxes NUMBER := 0;

CURSOR cur_interests IS
	SELECT * FROM dbowner.tbl_temp_interests
	WHERE orig_system = p_source
	ORDER BY orig_system_ref, update_date ASC;

BEGIN

	SELECT	count(*) INTO v_idxes
	FROM	user_indexes a, user_ind_partitions b
	WHERE	a.index_name = b.index_name
	AND		b.partition_name = 'PAR_' || UPPER(p_source)
	AND		a.table_name = 'TBL_INTERESTS';

	COMMIT;

	pk_main.log_time(p_source, 'pk_transfer', 'transfer_interests', 'about to delete previous fail records', g_pos, true);
	DELETE dbowner.tbl_temp_interests_fail WHERE orig_system = p_source;

	IF v_idxes > 0 THEN
		pk_main.log_time(p_source, 'pk_transfer', 'transfer_interests', 'about to render local indexes unusable', g_pos, true);
		EXECUTE IMMEDIATE 'ALTER TABLE dbowner.tbl_interests MODIFY PARTITION par_' || p_source || ' UNUSABLE LOCAL INDEXES';
	END IF;

	pk_main.log_time(p_source, 'pk_transfer', 'transfer_interests', 'start of transfer', g_pos, true);
	UPDATE dbowner.tbl_transfer_sessions SET interests_start = SYSDATE WHERE session_id = g_session_id;
	COMMIT;

	FOR rec IN cur_interests LOOP

		SAVEPOINT current_interest;
		transfer_interest(rec, v_success, v_mode);

		IF v_success = 'Y' THEN

		   v_counter := v_counter + 1;
		   IF v_mode = 'I' THEN
		   	  v_inserts := v_inserts + 1;
		   ELSIF v_mode = 'U' THEN
		   	  v_updates := v_updates + 1;
		   ELSIF v_mode = 'D' THEN
		   	  v_deletes := v_deletes + 1;
		   END IF;

		   IF v_counter = 50000 THEN
		      update_session_stats('interests', v_inserts, v_updates, v_deletes);
			  v_counter := 0;
			  v_inserts := 0;
			  v_updates := 0;
			  v_deletes := 0;
		   	  COMMIT;
		   END IF;

		ELSE

			ROLLBACK TO current_interest;
			INSERT INTO dbowner.tbl_temp_interests_fail VALUES (p_source, rec.orig_system_ref);
			v_fails := v_fails + 1;
			COMMIT;

		END IF;

	END LOOP;

    update_session_stats('interests', v_inserts, v_updates, v_deletes);
	COMMIT;

	IF v_idxes > 0 THEN
		pk_main.log_time(p_source, 'pk_transfer', 'transfer_interests', 'about to rebuild local indexes', g_pos, true);
		EXECUTE IMMEDIATE 'ALTER TABLE dbowner.tbl_interests MODIFY PARTITION par_' || p_source || ' REBUILD UNUSABLE LOCAL INDEXES';
	END IF;

	pk_main.log_time(p_source, 'pk_transfer', 'transfer_interests', 'just about to delete dbowner.tbl_temp_interests', g_pos, true);
	IF v_fails > 0 THEN
		DELETE dbowner.tbl_temp_interests a
			WHERE orig_system = p_source
			AND NOT EXISTS (SELECT 'x' FROM dbowner.tbl_temp_interests_fail b WHERE b.orig_system = p_source AND a.orig_system_ref = b.orig_system_ref);
		v_failing_records := v_failing_records || ',interests:' || v_fails;
	ELSE
		DELETE dbowner.tbl_temp_interests
			WHERE orig_system = p_source;
	END IF;
	COMMIT;
	pk_main.log_time(p_source, 'pk_transfer', 'transfer_interests', 'end of procedure', g_pos, true);

END transfer_interests;

PROCEDURE transfer_interest(p_row IN dbowner.tbl_temp_interests%ROWTYPE, p_success IN OUT VARCHAR2, p_mode IN OUT VARCHAR2) IS

CURSOR cur_existing(p_orig_system_ref IN VARCHAR2) IS
	SELECT 'x' FROM dbowner.tbl_interests
	WHERE orig_interest_ref = p_orig_system_ref;
v_dummy VARCHAR2(1);

BEGIN

 	IF p_row.status || 'x' = 'Dx' THEN

	   p_mode := 'D';
	   UPDATE dbowner.tbl_interests SET record_status = 'D', orig_update_date = p_row.update_date, capri_update_date = SYSDATE
	   WHERE orig_interest_ref = p_row.orig_system_ref;

	ELSE

		OPEN cur_existing (p_row.orig_system_ref);
		FETCH cur_existing INTO v_dummy;

		IF NOT(cur_existing%FOUND) THEN

		   p_mode := 'I';

		   INSERT INTO dbowner.tbl_interests VALUES
		   		   (p_row.orig_system_ref,
					p_row.orig_system,
					p_row.party_ref,
					p_row.create_date,
					p_row.update_date,
					SYSDATE,
					SYSDATE,
					p_row.interest_type,
					p_row.interest_value,
					p_row.interest_value_details,
					p_row.interest_section,
					p_row.interest_sub_section,
					p_row.alert_end_date,
					'A');

		ELSE

		   p_mode := 'U';

		   UPDATE dbowner.tbl_interests SET
          party_ref = p_row.party_ref,
					orig_update_date = p_row.update_date,
					capri_update_date = SYSDATE,
					interest_type = p_row.interest_type,
					interest_value = p_row.interest_value,
					interest_value_details = p_row.interest_value_details,
					interest_section = p_row.interest_section,
					interest_sub_section = p_row.interest_sub_section,
					alert_end_date = p_row.alert_end_date,
					record_status = 'A'
		   WHERE p_row.orig_system_ref = orig_interest_ref;

		END IF;
		CLOSE cur_existing;

	END IF;

	p_success := 'Y';

EXCEPTION

	WHEN OTHERS THEN
   		p_success := 'N';
   		pk_main.log_error(p_row.orig_system, 'pk_transfer', 'transfer_interest', SQLCODE, 'interest record error (' || p_row.orig_system_ref || '): '  || SQLERRM, g_pos);

END transfer_interest;

PROCEDURE transfer_ips(p_source IN VARCHAR2) IS

v_success VARCHAR2(1);
v_counter NUMBER(5) := 0;
v_inserts NUMBER := 0;
v_updates NUMBER := 0;
v_deletes NUMBER := 0;
v_fails NUMBER := 0;
v_mode VARCHAR2(1);

CURSOR cur_ips IS
	SELECT * FROM dbowner.tbl_temp_ips
	WHERE orig_system = p_source
	ORDER BY orig_system_ref, update_date ASC;

BEGIN

	DELETE dbowner.tbl_temp_ips_fail WHERE orig_system = p_source;
	pk_main.log_time(p_source, 'pk_transfer', 'transfer_ips', 'start of transfer', g_pos, true);
	UPDATE dbowner.tbl_transfer_sessions SET ips_start = SYSDATE WHERE session_id = g_session_id;
	COMMIT;

	FOR rec IN cur_ips LOOP

		SAVEPOINT current_ip;
		transfer_ip(rec, v_success, v_mode);

		IF v_success = 'Y' THEN

		   v_counter := v_counter + 1;
		   IF v_mode = 'I' THEN
		   	  v_inserts := v_inserts + 1;
		   ELSIF v_mode = 'U' THEN
		   	  v_updates := v_updates + 1;
		   ELSIF v_mode = 'D' THEN
		   	  v_deletes := v_deletes + 1;
		   END IF;

		   IF v_counter = 1000 THEN
		      update_session_stats('ips', v_inserts, v_updates, v_deletes);
			  v_counter := 0;
			  v_inserts := 0;
			  v_updates := 0;
			  v_deletes := 0;
		   	  COMMIT;
		   END IF;

		ELSE

			ROLLBACK TO current_ip;
			INSERT INTO dbowner.tbl_temp_ips_fail VALUES (p_source, rec.orig_system_ref);
			v_fails := v_fails + 1;
			COMMIT;

		END IF;

	END LOOP;

    update_session_stats('ips', v_inserts, v_updates, v_deletes);
	COMMIT;
	pk_main.log_time(p_source, 'pk_transfer', 'transfer_ips', 'just about to delete dbowner.tbl_temp_ips', g_pos, true);
	IF v_fails > 0 THEN
		DELETE dbowner.tbl_temp_ips a
			WHERE orig_system = p_source
			AND NOT EXISTS (SELECT 'x' FROM dbowner.tbl_temp_ips_fail b WHERE b.orig_system = p_source AND a.orig_system_ref = b.orig_system_ref);
		v_failing_records := v_failing_records || ',ips:' || v_fails;
	ELSE
		DELETE dbowner.tbl_temp_ips
			WHERE orig_system = p_source;
	END IF;
	COMMIT;
	pk_main.log_time(p_source, 'pk_transfer', 'transfer_ips', 'end of procedure', g_pos, true);

END transfer_ips;

PROCEDURE transfer_ip(p_row IN dbowner.tbl_temp_ips%ROWTYPE, p_success IN OUT VARCHAR2, p_mode IN OUT VARCHAR2) IS

CURSOR cur_existing(p_orig_system_ref IN VARCHAR2) IS
	SELECT 'x' FROM dbowner.tbl_ips
	WHERE orig_ip_ref = p_orig_system_ref;
v_dummy VARCHAR2(1);

BEGIN

 	IF p_row.status || 'x' = 'Dx' THEN

	   p_mode := 'D';
	   UPDATE dbowner.tbl_ips SET record_status = 'D', orig_update_date = p_row.update_date, capri_update_date = SYSDATE
	   WHERE orig_ip_ref = p_row.orig_system_ref;

	ELSE

		OPEN cur_existing (p_row.orig_system_ref);
		FETCH cur_existing INTO v_dummy;
		IF NOT(cur_existing%FOUND) THEN

		   p_mode := 'I';

		   INSERT INTO dbowner.tbl_ips VALUES
		   		   (p_row.orig_system_ref,
				  	p_row.orig_system,
					p_row.party_per_ref,
					p_row.party_org_ref,
					p_row.create_date,
					p_row.update_date,
					SYSDATE,
					SYSDATE,
					p_row.range_start,
					p_row.range_end,
					p_row.number_start,
					p_row.number_end,
					p_row.comp1,
					p_row.comp2,
					p_row.comp3_begin,
					p_row.comp3_end,
					p_row.comp4_begin,
					p_row.comp4_end,
					p_row.exclude,
					'A');

		ELSE

		   p_mode := 'U';

		   UPDATE dbowner.tbl_ips SET
					party_per_ref = p_row.party_per_ref,
					party_org_ref = p_row.party_org_ref,
					orig_update_date = p_row.update_date,
					capri_update_date = SYSDATE,
					range_start = p_row.range_start,
					range_end = p_row.range_end,
					number_start = p_row.number_start,
					number_end = p_row.number_end,
					comp1 = p_row.comp1,
					comp2 = p_row.comp2,
					comp3_begin = p_row.comp3_begin,
					comp3_end = p_row.comp3_end,
					comp4_begin = p_row.comp4_begin,
					comp4_end = p_row.comp4_end,
					exclude = p_row.exclude,
					record_status = 'A'
		   WHERE p_row.orig_system_ref = orig_ip_ref;

		END IF;
		CLOSE cur_existing;

	END IF;

	p_success := 'Y';

EXCEPTION

	WHEN OTHERS THEN
   		p_success := 'N';
   		pk_main.log_error(p_row.orig_system, 'pk_transfer', 'transfer_ip', SQLCODE, 'ip record error (' || p_row.orig_system_ref || '): '  || SQLERRM, g_pos);

END transfer_ip;

PROCEDURE transfer_items(p_source IN VARCHAR2) IS

v_success VARCHAR2(1);
v_counter NUMBER(5) := 0;
v_inserts NUMBER := 0;
v_updates NUMBER := 0;
v_deletes NUMBER := 0;
v_fails NUMBER := 0;
v_mode VARCHAR2(1);
v_dedupe_no NUMBER;
v_idxes NUMBER := 0;

CURSOR cur_items IS
	SELECT * FROM dbowner.tbl_temp_items
	WHERE orig_system = p_source
	ORDER BY orig_system_ref, update_date ASC;

BEGIN

	SELECT	count(*) INTO v_idxes
	FROM	user_indexes a, user_ind_partitions b
	WHERE	a.index_name = b.index_name
	AND		b.partition_name = 'PAR_' || UPPER(p_source)
	AND		a.table_name = 'TBL_ITEMS';

	COMMIT;

	DELETE dbowner.tbl_temp_items_fail WHERE orig_system = p_source;

	IF v_idxes > 0 THEN
		pk_main.log_time(p_source, 'pk_transfer', 'transfer_items', 'about to render local indexes unusable', g_pos, true);
		EXECUTE IMMEDIATE 'ALTER TABLE dbowner.tbl_items MODIFY PARTITION par_' || p_source || ' UNUSABLE LOCAL INDEXES';
	END IF;

	pk_main.log_time(p_source, 'pk_transfer', 'transfer_items', 'start of transfer', g_pos, true);
	UPDATE dbowner.tbl_transfer_sessions SET items_start = SYSDATE WHERE session_id = g_session_id;
	COMMIT;

	FOR rec IN cur_items LOOP

		SAVEPOINT current_item;
		transfer_item(rec, v_success, v_mode);

		IF v_success = 'Y' THEN

		   v_counter := v_counter + 1;
		   IF v_mode = 'I' THEN
		   	  v_inserts := v_inserts + 1;
		   ELSIF v_mode = 'U' THEN
		   	  v_updates := v_updates + 1;
		   ELSIF v_mode = 'D' THEN
		   	  v_deletes := v_deletes + 1;
		   END IF;

		   IF v_counter = 1000 THEN
		      update_session_stats('items', v_inserts, v_updates, v_deletes);
			  v_counter := 0;
			  v_inserts := 0;
			  v_updates := 0;
			  v_deletes := 0;
		   	  COMMIT;
		   END IF;

		ELSE

			ROLLBACK TO current_item;
			INSERT INTO dbowner.tbl_temp_items_fail VALUES (p_source, rec.orig_system_ref);
			v_fails := v_fails + 1;
			COMMIT;

		END IF;

	END LOOP;

    update_session_stats('items', v_inserts, v_updates, v_deletes);
	COMMIT;

	IF v_idxes > 0 THEN
		pk_main.log_time(p_source, 'pk_transfer', 'transfer_items', 'about to rebuild local indexes', g_pos, true);
		EXECUTE IMMEDIATE 'ALTER TABLE dbowner.tbl_items MODIFY PARTITION par_' || p_source || ' REBUILD UNUSABLE LOCAL INDEXES';
	END IF;

	pk_main.log_time(p_source, 'pk_transfer', 'transfer_items', 'just about to delete dbowner.tbl_temp_items', g_pos, true);
	IF v_fails > 0 THEN
		DELETE dbowner.tbl_temp_items a
			WHERE orig_system = p_source
			AND NOT EXISTS (SELECT 'x' FROM dbowner.tbl_temp_items_fail b WHERE b.orig_system = p_source AND a.orig_system_ref = b.orig_system_ref);
		v_failing_records := v_failing_records || ',items:' || v_fails;
	ELSE
		DELETE dbowner.tbl_temp_items
			WHERE orig_system = p_source;
	END IF;
	COMMIT;
	pk_main.log_time(p_source, 'pk_transfer', 'transfer_items', 'end of procedure', g_pos, true);

END transfer_items;

PROCEDURE transfer_item(p_row IN dbowner.tbl_temp_items%ROWTYPE, p_success IN OUT VARCHAR2, p_mode IN OUT VARCHAR2) IS

CURSOR cur_existing(p_orig_system_ref IN VARCHAR2) IS
	SELECT dedupe_id FROM dbowner.tbl_items
	WHERE orig_item_ref = p_orig_system_ref;
v_dedupe_no NUMBER := NULL;
v_value1 VARCHAR2(150) := NULL;
v_value2 VARCHAR2(150) := NULL;

BEGIN

 	IF p_row.status || 'x' = 'Dx' THEN

	   	p_mode := 'D';
	    UPDATE dbowner.tbl_items SET record_status = 'D', orig_update_date = p_row.update_date, capri_update_date = SYSDATE
	    WHERE orig_item_ref = p_row.orig_system_ref;

	ELSE

		OPEN cur_existing (p_row.orig_system_ref);
		FETCH cur_existing INTO v_dedupe_no;

		IF NOT(cur_existing%FOUND) THEN

	   	   p_mode := 'I';

	   	  	v_value1 := p_row.identifier;
		   IF p_row.item_type = 'JOU' THEN
				v_value2 := p_row.code;
		   END IF;

/*		   pk_dedupe.get_item_dedupe_id(p_row.orig_system,
		   								p_row.orig_system_ref,
										p_row.item_type,
										v_value1,
										v_value2,
										v_dedupe_no);*/

		   INSERT INTO dbowner.tbl_items VALUES
		   		   (p_row.orig_system_ref,
					p_row.orig_system,
					p_row.create_date,
					p_row.update_date,
					SYSDATE,
					SYSDATE,
					v_dedupe_no,
					p_row.name,
					p_row.item_type,
					p_row.parent_ref,
					p_row.volume,
					p_row.issue,
					p_row.year,
					p_row.description,
					p_row.identifier,
					p_row.code,
					p_row.authors,
					p_row.show_status,
					p_row.site,
					p_row.publisher,
					p_row.type,
					p_row.sub_type,
					p_row.binding,
					p_row.medium,
					p_row.pmc_code,
					p_row.pmc_descr,
					p_row.pmg_code,
					p_row.pmg_descr,
					p_row.class,
					p_row.imprint,
					p_row.no_of_pages,
					p_row.page_numbers,
					p_row.item_milestone,
					p_row.issue_milestone,
					p_row.issue_date,
					p_row.delivery_date,
					p_row.receive_date,
					p_row.init_pub_date,
					p_row.last_pub_date,
					p_row.added_date,
					'A');

		ELSE

		   p_mode := 'U';

		   UPDATE dbowner.tbl_items SET
					orig_update_date = p_row.update_date,
					capri_update_date = SYSDATE,
					name = decode(p_row.name, NULL, name, p_row.name), -- incase another blank article turns up to overwrite and existing one
					parent_ref = p_row.parent_ref,
					volume = p_row.volume,
					issue = p_row.issue,
					year = p_row.year,
					description = decode(p_row.description, NULL, description, p_row.description),
					identifier = p_row.identifier,
					code = p_row.code,
					authors = p_row.authors,
					show_status = p_row.show_status,
					site = p_row.site,
					publisher = p_row.publisher,
					type = p_row.type,
					sub_type = p_row.sub_type,
					binding = p_row.binding,
					medium = p_row.medium,
					pmc_code = p_row.pmc_code,
					pmc_descr = p_row.pmc_descr,
					pmg_code = p_row.pmg_code,
					pmg_descr = p_row.pmg_descr,
					class = p_row.class,
					imprint = p_row.imprint,
					no_of_pages = p_row.no_of_pages,
					page_numbers = p_row.page_numbers,
					item_milestone = p_row.item_milestone,
					issue_milestone = p_row.issue_milestone,
					issue_date = p_row.issue_date,
					delivery_date = p_row.delivery_date,
					receive_date = p_row.receive_date,
					init_pub_date = p_row.init_pub_date,
					last_pub_date = p_row.last_pub_date,
					added_date = p_row.added_date,
					record_status = 'A'
		   WHERE p_row.orig_system_ref = orig_item_ref;

		END IF;

		CLOSE cur_existing;

	END IF;

	p_success := 'Y';

EXCEPTION

	WHEN OTHERS THEN
   		p_success := 'N';
   		pk_main.log_error(p_row.orig_system, 'pk_transfer', 'transfer_item', SQLCODE, 'item record error (' || p_row.orig_system_ref || '): '  || SQLERRM, g_pos);

END transfer_item;

PROCEDURE transfer_downloads(p_source IN VARCHAR2) IS

v_success VARCHAR2(1);
v_counter NUMBER(5) := 0;
v_fails NUMBER := 0;

CURSOR cur_downloads IS
	SELECT * FROM dbowner.tbl_temp_downloads
	WHERE orig_system = p_source;

BEGIN

	EXECUTE IMMEDIATE 'TRUNCATE TABLE dbowner.tbl_temp_downloads_fail';
	pk_main.log_time(p_source, 'pk_transfer', 'transfer_downloads', 'start of transfer', g_pos, true);
	UPDATE dbowner.tbl_transfer_sessions SET downloads_start = SYSDATE WHERE session_id = g_session_id;
	COMMIT;

	FOR rec IN cur_downloads LOOP

		SAVEPOINT current_download;
		transfer_download(rec, v_success);

		IF v_success = 'Y' THEN

		   v_counter := v_counter + 1;

		   IF v_counter = 1000 THEN
		      update_session_stats('downloads', v_counter, 0, 0);
			  v_counter := 0;
		   	  COMMIT;
		   END IF;

		ELSE

			ROLLBACK TO current_download;
			INSERT INTO dbowner.tbl_temp_downloads_fail VALUES (rec.orig_system_ref);
			v_fails := v_fails + 1;
			COMMIT;

		END IF;

	END LOOP;

    update_session_stats('downloads', v_counter, 0, 0);
	COMMIT;
	pk_main.log_time(p_source, 'pk_transfer', 'transfer_downloads', 'just about to delete dbowner.tbl_temp_downloads', g_pos, true);
	IF v_fails > 0 THEN
		DELETE dbowner.tbl_temp_downloads a
			WHERE orig_system = p_source
			AND NOT EXISTS (SELECT 'x' FROM dbowner.tbl_temp_downloads_fail b WHERE a.orig_system_ref = b.orig_system_ref);
		v_failing_records := v_failing_records || ',downloads:' || v_fails;
	ELSE
		DELETE dbowner.tbl_temp_downloads
			WHERE orig_system = p_source;
	END IF;
	COMMIT;
	pk_main.log_time(p_source, 'pk_transfer', 'transfer_downloads', 'end of transfer', g_pos, true);

END transfer_downloads;

PROCEDURE transfer_download(p_row IN dbowner.tbl_temp_downloads%ROWTYPE, p_success IN OUT VARCHAR2) IS

BEGIN

	INSERT INTO dbowner.tbl_downloads VALUES
	   (p_row.orig_system_ref,
		p_row.orig_system,
		SYSDATE,
		p_row.party_user_ref,
		p_row.party_org_ref,
		p_row.item_article_ref,
		p_row.item_journal_ref,
		p_row.description,
		p_row.section,
		p_row.type,
		p_row.accessed_via,
		p_row.payment_type,
		p_row.source_ip,
		p_row.first_access,
		p_row.site,
		p_row.resource_name,
		p_row.access_price,
		p_row.access_duration,
		p_row.account_change,
		p_row.account_balance,
		p_row.order_id,
		p_row.log_date,
		p_row.end_date,
		'A');

	p_success := 'Y';

EXCEPTION

	WHEN OTHERS THEN
		IF SQLCODE = 'ORA-00001' THEN --already record in db
		   	p_success := 'Y';
		ELSE
			p_success := 'N';
			pk_main.log_error(p_row.orig_system, 'pk_transfer', 'transfer_downloads', SQLCODE, 'download record error (' || p_row.orig_system_ref || '): '  || SQLERRM, g_pos);
		END IF;

END transfer_download;

PROCEDURE transfer_subscriptions(p_source IN VARCHAR2) IS

v_success VARCHAR2(1);
v_counter NUMBER(5) := 0;
v_inserts NUMBER := 0;
v_updates NUMBER := 0;
v_deletes NUMBER := 0;
v_fails NUMBER := 0;
v_mode VARCHAR2(1);
v_dedupe_no NUMBER;
v_idxes NUMBER := 0;

CURSOR cur_subscriptions IS
	SELECT * FROM dbowner.tbl_temp_subscriptions
	WHERE orig_system = p_source
	ORDER BY orig_system_ref, orig_update_date ASC;

BEGIN

	SELECT	count(*) INTO v_idxes
	FROM	user_indexes a, user_ind_partitions b
	WHERE	a.index_name = b.index_name
	AND		b.partition_name = 'PAR_' || UPPER(p_source)
	AND		a.table_name = 'TBL_SUBSCRIPTIONS';

	COMMIT;

	DELETE dbowner.tbl_temp_subscriptions_fail WHERE orig_system = p_source;

	IF v_idxes > 0 THEN
		pk_main.log_time(p_source, 'pk_transfer', 'transfer_subscriptions', 'about to render local indexes unusable', g_pos, true);
		EXECUTE IMMEDIATE 'ALTER TABLE dbowner.tbl_subscriptions MODIFY PARTITION par_' || p_source || ' UNUSABLE LOCAL INDEXES';
	END IF;

	pk_main.log_time(p_source, 'pk_transfer', 'transfer_subscriptions', 'start of transfer', g_pos, true);
	UPDATE tbl_transfer_sessions SET subscriptions_start = SYSDATE WHERE session_id = g_session_id;
	COMMIT;

	FOR rec IN cur_subscriptions LOOP

		SAVEPOINT current_subscription;
		transfer_subscription(rec, v_success, v_mode);

		IF v_success = 'Y' THEN

		   v_counter := v_counter + 1;
		   IF v_mode = 'I' THEN
		   	  v_inserts := v_inserts + 1;
		   ELSIF v_mode = 'U' THEN
		   	  v_updates := v_updates + 1;
		   ELSIF v_mode = 'D' THEN
		   	  v_deletes := v_deletes + 1;
		   END IF;

		   IF v_counter = 1000 THEN
		      update_session_stats('subscriptions', v_inserts, v_updates, v_deletes);
			  v_counter := 0;
			  v_inserts := 0;
			  v_updates := 0;
			  v_deletes := 0;
		   	  COMMIT;
		   END IF;

		ELSE

			ROLLBACK TO current_subscription;
			INSERT INTO dbowner.tbl_temp_subscriptions_fail VALUES (p_source, rec.orig_system_ref);
			v_fails := v_fails + 1;
			COMMIT;

		END IF;

	END LOOP;

    update_session_stats('subscriptions', v_inserts, v_updates, v_deletes);
	COMMIT;

	IF v_idxes > 0 THEN
		pk_main.log_time(p_source, 'pk_transfer', 'transfer_subscriptions', 'about to rebuild local indexes', g_pos, true);
		EXECUTE IMMEDIATE 'ALTER TABLE dbowner.tbl_subscriptions MODIFY PARTITION par_' || p_source || ' REBUILD UNUSABLE LOCAL INDEXES';
	END IF;

	pk_main.log_time(p_source, 'pk_transfer', 'transfer_subscriptions', 'just about to delete dbowner.tbl_temp_subscriptions', g_pos, true);
	IF v_fails > 0 THEN
		DELETE dbowner.tbl_temp_subscriptions a
			WHERE orig_system = p_source
			AND NOT EXISTS (SELECT 'x' FROM dbowner.tbl_temp_subscriptions_fail b WHERE b.orig_system = p_source AND a.orig_system_ref = b.orig_system_ref);
		v_failing_records := v_failing_records || ',subscriptions:' || v_fails;
	ELSE
		DELETE dbowner.tbl_temp_subscriptions
			WHERE orig_system = p_source;
	END IF;
	COMMIT;
	pk_main.log_time(p_source, 'pk_transfer', 'transfer_subscriptions', 'end of procedure', g_pos, true);

END transfer_subscriptions;

PROCEDURE transfer_subscription(p_row IN dbowner.tbl_temp_subscriptions%ROWTYPE, p_success IN OUT VARCHAR2, p_mode IN OUT VARCHAR2) IS

CURSOR cur_existing(p_orig_system_ref IN VARCHAR2) IS
	SELECT 'x' FROM dbowner.tbl_subscriptions
	WHERE orig_subscription_ref = p_orig_system_ref;
v_exists VARCHAR2(1);
v_value1 VARCHAR2(150);
v_value2 VARCHAR2(150);

BEGIN

 	IF p_row.status || 'x' = 'Dx' THEN

	   	p_mode := 'D';
	    UPDATE dbowner.tbl_subscriptions SET record_status = 'D', orig_update_date = p_row.orig_update_date, capri_update_date = SYSDATE
	    WHERE orig_subscription_ref = p_row.orig_system_ref;

	ELSE

		OPEN cur_existing (p_row.orig_system_ref);
		FETCH cur_existing INTO v_exists;

		IF NOT(cur_existing%FOUND) THEN

	   	   p_mode := 'I';
		   INSERT INTO dbowner.tbl_subscriptions VALUES
		   		   (p_row.orig_system_ref,
					p_row.orig_system,
					p_row.orig_create_date,
					p_row.orig_update_date,
					SYSDATE,
					SYSDATE,
					'A',
					p_row.party_ref,
					p_row.item_ref_1,
					p_row.item_ref_2,
					p_row.start_date,
					p_row.end_date,
					p_row.price,
					p_row.sub_status,
					p_row.product_name,
					p_row.product_description,
					p_row.sub_grace,
					p_row.entitlement_type,
					p_row.password_id,
					p_row.password,
					p_row.password_grace,
					p_row.password_status,
					p_row.password_type,
					p_row.claim_limit,
					p_row.claim_count,
					p_row.duration,
					p_row.max_topics,
					p_row.ranking,
					p_row.unlimited,
					p_row.free_alerts,
					p_row.purchase_number,
					p_row.qss_claim_code,
					p_row.claim_code,
					p_row.qss_claim_number,
					p_row.sub_number,
					p_row.claim_start_date,
					p_row.claim_end_date,
					p_row.sub_issue,
					p_row.owner,
					p_row.access_issue,
					p_row.order_id,
					p_row.copy_price_orig,
					p_row.inv_currency,
					p_row.orig_agent,
					p_row.start_vol,
					p_row.end_vol,
					p_row.year,
					p_row.source_code,
					p_row.no_copies,
					p_row.recorded_update_date,
					p_row.credit_card_allowed,
					p_row.tax_code,
					p_row.tax_exempted,
					p_row.start_page,
					p_row.end_page);

		ELSE

		   p_mode := 'U';
		   UPDATE dbowner.tbl_subscriptions SET
					orig_update_date = p_row.orig_update_date,
					capri_update_date = SYSDATE,
					record_status = 'A',
					party_ref = p_row.party_ref,
					item_ref_1 = p_row.item_ref_1,
					item_ref_2 = p_row.item_ref_2,
					start_date = p_row.start_date,
					end_date = p_row.end_date,
					price = p_row.price,
					sub_status = p_row.sub_status,
					product_name = p_row.product_name,
					product_description = p_row.product_description,
					sub_grace = p_row.sub_grace,
					entitlement_type = p_row.entitlement_type,
					password_id = p_row.password_id,
					password = p_row.password,
					password_grace = p_row.password_grace,
					password_status = p_row.password_status,
					password_type = p_row.password_type,
					claim_limit = p_row.claim_limit,
					claim_count = p_row.claim_count,
					duration = p_row.duration,
					max_topics = p_row.max_topics,
					ranking = p_row.ranking,
					unlimited = p_row.unlimited,
					free_alerts = p_row.free_alerts,
					purchase_number = p_row.purchase_number,
					qss_claim_code = p_row.qss_claim_code,
					claim_code = p_row.claim_code,
					qss_claim_number = p_row.qss_claim_number,
					sub_number = p_row.sub_number,
					claim_start_date = p_row.claim_start_date,
					claim_end_date = p_row.claim_end_date,
					sub_issue = p_row.sub_issue,
					owner = p_row.owner,
					access_issue = p_row.access_issue,
					order_id = p_row.order_id,
					copy_price_orig = p_row.copy_price_orig,
					inv_currency = p_row.inv_currency,
					orig_agent = p_row.orig_agent,
					start_vol = p_row.start_vol,
					end_vol = p_row.end_vol,
					year = p_row.year,
					source_code = p_row.source_code,
					no_copies = p_row.no_copies,
					recorded_update_date = p_row.recorded_update_date,
					credit_card_allowed = p_row.credit_card_allowed,
					tax_code = p_row.tax_code,
					tax_exempted = p_row.tax_exempted,
					start_page = p_row.start_page,
					end_page = p_row.end_page
		   WHERE p_row.orig_system_ref = orig_subscription_ref;

		END IF;

		CLOSE cur_existing;

	END IF;

	p_success := 'Y';

EXCEPTION

	WHEN OTHERS THEN
   		p_success := 'N';
   		pk_main.log_error(p_row.orig_system, 'pk_transfer', 'transfer_subscription', SQLCODE, 'subscription record error (' || p_row.orig_system_ref || '): '  || SQLERRM, g_pos);

END transfer_subscription;

PROCEDURE transfer_item_subjects(p_source IN VARCHAR2) IS

v_success VARCHAR2(1);
v_counter NUMBER(5) := 0;
v_inserts NUMBER := 0;
v_updates NUMBER := 0;
v_deletes NUMBER := 0;
v_fails NUMBER := 0;
v_mode VARCHAR2(1);

CURSOR cur_item_subjects IS
	SELECT * FROM dbowner.tbl_temp_item_subjects
	WHERE orig_system = p_source
	ORDER BY orig_system_ref, update_date ASC;

BEGIN

	EXECUTE IMMEDIATE 'TRUNCATE TABLE dbowner.tbl_temp_item_subjects_fail';
	pk_main.log_time(p_source, 'pk_transfer', 'transfer_item_subjects', 'start of transfer', g_pos, true);
	UPDATE dbowner.tbl_transfer_sessions SET item_subjects_start = SYSDATE WHERE session_id = g_session_id;
	COMMIT;

	FOR rec IN cur_item_subjects LOOP

		SAVEPOINT current_item_subject;
		transfer_item_subject(rec, v_success, v_mode);

		IF v_success = 'Y' THEN

		   v_counter := v_counter + 1;
		   IF v_mode = 'I' THEN
		   	  v_inserts := v_inserts + 1;
		   ELSIF v_mode = 'U' THEN
		   	  v_updates := v_updates + 1;
		   ELSIF v_mode = 'D' THEN
		   	  v_deletes := v_deletes + 1;
		   END IF;

		   IF v_counter = 1000 THEN
		      update_session_stats('item_subjects', v_inserts, v_updates, v_deletes);
			  v_counter := 0;
			  v_inserts := 0;
			  v_updates := 0;
			  v_deletes := 0;
		   	  COMMIT;
		   END IF;

		ELSE

			ROLLBACK TO current_item_subject;
			INSERT INTO dbowner.tbl_temp_item_subjects_fail VALUES (rec.orig_system, rec.orig_system_ref);
			v_fails := v_fails + 1;
			COMMIT;

		END IF;

	END LOOP;

    update_session_stats('item_subjects', v_inserts, v_updates, v_deletes);
	COMMIT;
	pk_main.log_time(p_source, 'pk_transfer', 'transfer_item_subjects', 'just about to delete dbowner.tbl_temp_item_subjects', g_pos, true);
	IF v_fails > 0 THEN
		DELETE dbowner.tbl_temp_item_subjects a
			WHERE orig_system = p_source
			AND NOT EXISTS (SELECT 'x' FROM dbowner.tbl_temp_item_subjects_fail b WHERE a.orig_system_ref = b.orig_system_ref);
		v_failing_records := v_failing_records || ',item_subjects:' || v_fails;
	ELSE
		DELETE dbowner.tbl_temp_item_subjects
			WHERE orig_system = p_source;
	END IF;
	COMMIT;
	pk_main.log_time(p_source, 'pk_transfer', 'transfer_item_subjects', 'end of procedure', g_pos, true);

END transfer_item_subjects;

PROCEDURE transfer_item_subject(p_row IN dbowner.tbl_temp_item_subjects%ROWTYPE, p_success IN OUT VARCHAR2, p_mode IN OUT VARCHAR2) IS

CURSOR cur_existing(p_orig_system_ref IN VARCHAR2) IS
	SELECT 'x' FROM dbowner.tbl_item_subjects
	WHERE orig_subject_ref = p_orig_system_ref;
v_dummy VARCHAR2(1);

BEGIN

 	IF p_row.status || 'x' = 'Dx' THEN

	   p_mode := 'D';
	   UPDATE dbowner.tbl_item_subjects SET record_status = 'D', orig_update_date = p_row.update_date, capri_update_date = SYSDATE
	   WHERE orig_subject_ref = p_row.orig_system_ref;

	ELSE

		OPEN cur_existing (p_row.orig_system_ref);
		FETCH cur_existing INTO v_dummy;
		IF NOT(cur_existing%FOUND) THEN

		   p_mode := 'I';
		   INSERT INTO dbowner.tbl_item_subjects VALUES
		   		   (p_row.orig_system_ref,
				   	p_row.item_ref,
				  	p_row.orig_system,
					p_row.create_date,
					p_row.update_date,
					SYSDATE,
					SYSDATE,
					'A',
					p_row.subject_category_code,
					p_row.subject_category,
					p_row.subject_group_code,
					p_row.subject_group,
					p_row.subject_area_code,
					p_row.subject_area,
					p_row.super_area_code,
					p_row.super_area,
					p_row.scaling_fctr);

		ELSE

		   p_mode := 'U';
		   UPDATE dbowner.tbl_item_subjects SET
					item_ref = p_row.item_ref,
					orig_update_date = p_row.update_date,
					capri_update_date = SYSDATE,
					subject_category_code = p_row.subject_category_code,
					subject_category = p_row.subject_category,
					subject_group_code = p_row.subject_group_code,
					subject_group = p_row.subject_group,
					subject_area_code = p_row.subject_area_code,
					subject_area = p_row.subject_area,
					super_area_code = p_row.super_area_code,
					super_area = p_row.super_area,
					scaling_factor = p_row.scaling_fctr,
					record_status = 'A'
		   WHERE p_row.orig_system_ref = orig_subject_ref;

		END IF;
		CLOSE cur_existing;

	END IF;

	p_success := 'Y';

EXCEPTION

	WHEN OTHERS THEN
   		p_success := 'N';
   		pk_main.log_error(p_row.orig_system, 'pk_transfer', 'transfer_item_subject', SQLCODE, 'item subjects record error (' || p_row.orig_system_ref || '): '  || SQLERRM, g_pos);

END transfer_item_subject;


END pk_transfer;
/
