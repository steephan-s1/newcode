CREATE PACKAGE DBOWNER.PK_EMR
  CREATE OR REPLACE EDITIONABLE PACKAGE "DBOWNER"."PK_EMR" AS

PROCEDURE proc_microsite;
PROCEDURE proc_bounce;
PROCEDURE proc_unsubscribe;

END pk_emr;

 
/
CREATE OR REPLACE EDITIONABLE PACKAGE BODY "DBOWNER"."PK_EMR" AS
PROCEDURE proc_microsite IS

string_pos			NUMBER := 1;
alert_value			VARCHAR2(100);
string_length		NUMBER := 0;
v_sender_group		VARCHAR2(100);
v_comm_type_desc	VARCHAR2(100);
v_email				VARCHAR2(100);


CURSOR cur_split_updates IS
	SELECT DISTINCT
		REPLACE(a.alerts_chemistry || a.alerts_earth_environment || a.alerts_engineering || a.alerts_life_sciences || a.alerts_pmca || a.alerts_social_sciences,',,',',') AS alerts,
		a.country,
		a.email,
		a.email_permission,
		a.emr_key,
		a.exclusion,
		a.first_name,
		a.last_name,
		a.last_used_date,
		a.marketing,
		a.offers,
		a.offers3party,
		a.orig_party_ref,
		a.record_creation_date,
		a.sector,
		a.tandc,
		a.microsite
	FROM
		tbl_temp_emr_data a
	WHERE
		a.email LIKE '%@%'
	ORDER BY
		a.email;

BEGIN

	FOR rec IN cur_split_updates LOOP

		SELECT length(rec.alerts) INTO string_length FROM dual;

		WHILE string_pos < string_length LOOP

			-- 1. Split alert values from all alert_* columns
			IF substr(rec.alerts,1,1) = ',' THEN

				SELECT replace(substr(rec.alerts,string_pos,instr(rec.alerts,',',2)),',','') INTO alert_value FROM dual;

			ELSE

				SELECT replace(substr(rec.alerts,string_pos,instr(rec.alerts,',',1)),',','') INTO alert_value FROM dual;

			END IF;

			COMMIT;

			-- 2. Ensure only valid alert_values are entered into tbl_temp_interests
			IF length(alert_value) > 1 THEN

				--SELECT comm_type_description INTO v_comm_type_desc FROM tbl_comm_type_lookup WHERE comm_type_code = substr(alert_value,1,3);
				--SELECT sender_group INTO v_sender_group FROM tbl_comm_type_lookup WHERE comm_type_code = substr(alert_value,1,3);

				COMMIT;

				-- 3. Insert single alert values into tbl_temp_interests
				INSERT INTO tbl_temp_interests
				VALUES ('EMR_' || rec.emr_key || '_' || substr(alert_value,1,3),
						'EMR',
						'EMR_' || rec.emr_key,
						SYSDATE,
						SYSDATE,
						'EMA',
						substr(alert_value,1,3),
						NULL,					--v_comm_type_desc
						'alerts',
						NULL,					--v_sender_group
						NULL,
						NULL);
				COMMIT;

			END IF;

			-- 3. Increment string_pos variable by 3 so that the string cursor is after the alert_value
			IF alert_value = 'CEL' THEN

				string_pos := string_pos + 4;
			ELSE

				string_pos := string_pos + 3;

			END IF;

			SELECT instr(rec.alerts,',',string_pos,1) INTO string_pos FROM dual;

		END LOOP;

		-- 4. Reset Variables ready for next record from cursor cur_split_updates.
		string_pos := 1;
		string_length := 0;

				-- 6. Insert Subscriber values into tbl_temp_parties
				INSERT INTO tbl_temp_parties (orig_system,original_site,orig_system_ref,party_type,usr_created_date,usr_last_visit_date,usc_contact_firstname,usc_contact_lastname,usc_country,
				usc_dedupe_email,org_type,sales_emails,marketing_emails,member_search,usr_subscriber_code,emailformat,mailing_status,usr_status)
				VALUES(	'EMR',
						rec.microsite,
						'EMR_' || rec.emr_key,
						'EUS',
						rec.record_creation_date,
						rec.last_used_date,
						rec.first_name,
						rec.last_name,
						rec.country,
						rec.email,
						rec.sector,
						rec.offers3party,
						rec.marketing,
						rec.offers,
						rec.orig_party_ref,
						CASE rec.email_permission WHEN '1' THEN 'Text/HTML' ELSE '' END,
						CASE rec.email_permission WHEN '3' THEN '702' ELSE '0' END,
						CASE rec.email_permission WHEN '0' THEN 'No Mail' ELSE '' END);

				COMMIT;

	END LOOP;

	-- 5. Delete all entries in tbl_temp_emr_data.
	DELETE FROM tbl_temp_emr_data;

	COMMIT;

	-- 6. Update tbl_temp_interests - interest_value_details and interest_sub_section columns
	UPDATE	tbl_temp_interests a
	SET		(interest_value_details,interest_sub_section) = (SELECT comm_type_description,sender_group
															FROM tbl_microsite_alert_lookup b
														    WHERE a.interest_value = b.comm_type_code)
	WHERE	orig_system = 'EMR'
	AND		interest_value_details IS NULL;

	COMMIT;

	-- 7. Insert any DELETED records
	INSERT INTO tbl_temp_interests

	SELECT
		a.orig_interest_ref,
		a.orig_system,
		a.party_ref,
		a.orig_create_date,
		a.orig_update_date,
		a.interest_type,
		a.interest_value,
		a.interest_value_details,
		a.interest_section,
		a.interest_sub_section,
		a.alert_end_date,
		'D'
	FROM
		tbl_interests a
	WHERE
		a.orig_system = 'EMR'
	AND a.interest_type = 'EMA'
	AND a.party_ref IN (SELECT DISTINCT b.party_ref FROM tbl_temp_interests b WHERE a.party_ref = b.party_ref AND orig_system = 'EMR')
	AND a.orig_interest_ref NOT IN (SELECT orig_system_ref FROM tbl_temp_interests b WHERE b.orig_system = 'EMR' AND a.orig_interest_ref = b.orig_system_ref AND b.party_ref = b.party_ref);

	COMMIT;

EXCEPTION
	WHEN NO_DATA_FOUND THEN
	dbms_output.put_line(alert_value);

END proc_microsite;

PROCEDURE proc_bounce IS

CURSOR cur_bounce_records IS

	SELECT	email_address
	FROM	tbl_temp_emr_bounce;

BEGIN

	FOR rec IN cur_bounce_records LOOP

		UPDATE	tbl_parties
		SET		mailing_status = '702',
				capri_update_date = SYSDATE
		WHERE	email = rec.email_address;

		COMMIT;

	END LOOP;

	DELETE FROM tbl_temp_emr_bounce;

	COMMIT;

END proc_bounce;

PROCEDURE proc_unsubscribe IS

CURSOR cur_unsubscribe_records IS

	SELECT DISTINCT
			email,
			unsubscribe_code,
			email_permission,
			emr_key,
			orig_party_ref,
			booth
	FROM
			tbl_temp_emr_unsubscribe;

BEGIN

	FOR rec IN cur_unsubscribe_records LOOP

		INSERT INTO tbl_optout
		VALUES		(rec.email,
					rec.emr_key,
					rec.unsubscribe_code,
					NULL,
					SYSDATE,
					SYSDATE,
					0,
					rec.orig_party_ref,
					rec.booth);

		COMMIT;

	END LOOP;

	UPDATE	tbl_optout a
	SET		unsubscribe_desc = (SELECT min(comm_type_description)
									FROM tbl_comm_type_lookup b
									WHERE lower(substr(a.unsubscribe_code,1,instr(a.unsubscribe_code,'@',1))) = lower(b.COMM_EMAIL))
	WHERE	unsubscribe_desc IS NULL;

	COMMIT;

	DELETE FROM tbl_temp_emr_unsubscribe;

	COMMIT;

END proc_unsubscribe;

END pk_emr;
/
