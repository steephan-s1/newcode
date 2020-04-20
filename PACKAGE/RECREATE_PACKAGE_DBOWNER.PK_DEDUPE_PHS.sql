CREATE PACKAGE DBOWNER.PK_DEDUPE_PHS
  CREATE OR REPLACE EDITIONABLE PACKAGE "DBOWNER"."PK_DEDUPE_PHS" AS

PROCEDURE dedupe_parties(p_success IN OUT VARCHAR2);

PROCEDURE get_item_dedupe_id(
	p_source			IN VARCHAR2,
	p_orig_system_ref	IN VARCHAR2,
	p_item_type			IN VARCHAR2,
	p_value1			IN VARCHAR2,
	p_value2			IN VARCHAR2,
	p_dedupe_id			IN OUT NUMBER);

PROCEDURE create_dedupe_table;

PROCEDURE apply_match_key(p_key_col IN NUMBER, p_key_code IN VARCHAR2);

END pk_dedupe_phs;
 
/
CREATE OR REPLACE EDITIONABLE PACKAGE BODY "DBOWNER"."PK_DEDUPE_PHS" AS

PROCEDURE assign_party_dedupe_id(
	p_source			IN VARCHAR2,
	p_dedupe_type		IN VARCHAR2,
	p_orig_party_ref	IN VARCHAR2,
	p_name				IN VARCHAR2,
	p_email				IN VARCHAR2,
	p_mode				IN VARCHAR2,
	p_dedupe_id			IN NUMBER);

	e_two_duplicate_keys EXCEPTION;
	e_other_error EXCEPTION;

PROCEDURE dedupe_parties(p_success IN OUT VARCHAR2) IS

CURSOR cur IS
--// GCS Changes: No need of DISTINCT
	-- SELECT DISTINCT orig_system, dedupe_type, orig_party_ref, party_name, email, record_status, dedupe_id, capri_update_date
    SELECT orig_system, dedupe_type, orig_party_ref, party_name, email, record_status, dedupe_id, capri_update_date
	FROM tbl_deduping_phs  d
	WHERE record_status IN ('I','U')
  AND   orig_system = 'PHS'
	ORDER BY capri_update_date;

v_pos 	  	 			NUMBER := 0;
v_transfers 			NUMBER;
e_transfers_running		EXCEPTION;

--// GCS Changes
l_count_updates         NUMBER  := 0;

BEGIN

	pk_main.log_time(NULL, 'pk_dedupe_phs', 'dedupe_parties', 'checking for transfers running', v_pos, TRUE);
	--SELECT count(*) INTO v_transfers FROM tbl_sources WHERE transfer_status = 'R';
  SELECT count(*) INTO v_transfers  FROM tbl_sources s 
    WHERE transfer_status = 'R' 
    AND s.source = 'PHS'
    AND s.source in (
                select ts.session_source 
                from tbl_transfer_sessions ts 
                where ts.session_source = s.source 
                and ts.session_start >= (
                                        select max(ts1.session_start) 
                                        from tbl_transfer_sessions ts1 
                                        where ts.session_source = ts1.session_source
                                        )
                and parties_last_update >= nvl(relationships_start, sysdate+1)
                );
	IF v_transfers > 0 THEN
	   	RAISE e_transfers_running;
	ELSE
		pk_main.log_time(NULL, 'pk_dedupe_phs', 'dedupe_parties', 'locking transfers out using tbl_dedupe_control', v_pos, TRUE);
		--UPDATE tbl_dedupe_control SET dedupe_running = 'Y';
		--COMMIT;
	END IF;


	pk_main.log_time(NULL, 'pk_dedupe_phs', 'dedupe_parties', 'just about to truncate tbl_deduping', v_pos, TRUE);
	EXECUTE IMMEDIATE 'TRUNCATE TABLE tbl_deduping_phs';

	pk_main.log_time(NULL, 'pk_dedupe_phs', 'dedupe_parties', 'just about to insert into tbl_deduping', v_pos, TRUE);
	INSERT /*+ append */ INTO tbl_deduping_phs
        -- GCS Changes: Removing DISTINCT
		-- SELECT DISTINCT orig_system, dedupe_type, orig_party_ref, party_name, email, record_status, dedupe_id, capri_update_date
        SELECT orig_system, dedupe_type, orig_party_ref, party_name, email, record_status, dedupe_id, capri_update_date
		FROM tbl_parties
		WHERE record_status IN ('I','U')
    AND   orig_system = 'PHS';
	COMMIT;


	pk_main.log_time(NULL, 'pk_dedupe_phs', 'dedupe_parties', 'commencing dedupe update', v_pos, TRUE);
	FOR rec IN cur LOOP
		assign_party_dedupe_id (
			rec.orig_system,
			rec.dedupe_type,
			rec.orig_party_ref,
			rec.party_name,
			rec.email,
			rec.record_status,
			rec.dedupe_id);

        --// GCS Changes
		-- COMMIT;
            IF l_count_updates > 10000 THEN
                COMMIT;
                l_count_updates := 0;
                pk_main.log_time(NULL, 'pk_dedupe_phs', 'dedupe_parties', 'Completed COMMITING updates ', v_pos, TRUE);
            ELSE
                l_count_updates := l_count_updates + 1;
            END IF;

	END LOOP;
	pk_main.log_time(NULL, 'pk_dedupe_phs', 'dedupe_parties', 'end of dedupe update', v_pos, TRUE);

	pk_main.log_time(NULL, 'pk_dedupe_phs', 'dedupe_parties', 'unlocking transfers using tbl_dedupe_control', v_pos, TRUE);
	--UPDATE tbl_dedupe_control SET dedupe_running = 'N';
	COMMIT;

	p_success := 'Y';

EXCEPTION

WHEN e_transfers_running THEN

	p_success := 'N';
	pk_main.log_error(NULL, 'pk_dedupe_phs', 'dedupe_parties', 0, 'there are transfer processes already running', v_pos);


WHEN OTHERS THEN

	p_success := 'N';
	pk_main.log_error(NULL, 'pk_dedupe_phs', 'dedupe_parties', SQLCODE, SQLERRM, v_pos);

END dedupe_parties;

PROCEDURE assign_party_dedupe_id(
	p_source			IN VARCHAR2,
	p_dedupe_type		IN VARCHAR2,
	p_orig_party_ref	IN VARCHAR2,
	p_name				IN VARCHAR2,
	p_email				IN VARCHAR2,
	p_mode				IN VARCHAR2,
	p_dedupe_id			IN NUMBER) IS

v_dedupe_id	NUMBER := 0;
v_looped_once BOOLEAN := FALSE;
v_error_message VARCHAR2(1000);
v_pos NUMBER;

CURSOR cur IS
	SELECT DISTINCT dedupe_id FROM tbl_parties
	WHERE party_name = p_name
	AND length(nvl(p_name,'x')) > 1
	AND dedupe_type = p_dedupe_type
	AND dedupe_id <> 0
	AND record_status = 'A'
	AND email = p_email
	AND length(nvl(p_email,'x')) > 1;

BEGIN

	v_pos := 1;
	IF p_dedupe_type = 'ORG' THEN

		v_pos := 2;
		UPDATE tbl_parties SET dedupe_id = seq_org_dedupe.nextval, record_status = 'A' WHERE orig_party_ref = p_orig_party_ref;

	ELSE

		v_pos := 3;
		FOR rec IN cur LOOP
			v_pos := 4;
			IF v_looped_once THEN
		   	   RAISE e_two_duplicate_keys;
			END IF;
			v_dedupe_id := rec.dedupe_id;
			v_looped_once := TRUE;
		END LOOP;

		v_pos := 5;
		IF v_dedupe_id <> 0 THEN
			v_pos := 6;
			IF p_mode = 'U' AND v_dedupe_id <> p_dedupe_id AND p_dedupe_id <> 0 THEN
				v_pos := 7;
				-- The party's old dedupe number is no longer valid as they have joined a new group.
				-- However, any other parties attached to the same old dedupe number need to be updated to the new one as well:
				UPDATE tbl_parties SET dedupe_id = v_dedupe_id, record_status = 'A'
					WHERE dedupe_id = p_dedupe_id
					AND dedupe_type = p_dedupe_type
					AND record_status <> 'D';
			ELSE
				UPDATE tbl_parties SET record_status = 'A', dedupe_id = v_dedupe_id WHERE orig_party_ref = p_orig_party_ref;
			END IF;
		ELSE
			v_pos := 8;
			UPDATE tbl_parties SET dedupe_id = seq_per_dedupe.nextval, record_status = 'A' WHERE orig_party_ref = p_orig_party_ref;
		END IF;

	END IF;

EXCEPTION

WHEN e_two_duplicate_keys THEN

	v_error_message := 'Error (' || p_orig_party_ref || '): Two dedupe ids exist for this match. party_name = ' || p_name || ', email = ' || p_email;
	pk_main.log_error(p_source, 'pk_dedupe_phs', 'assign_party_dedupe_id', 0, v_error_message, v_pos);

WHEN OTHERS THEN

	v_error_message := 'Error (' || p_orig_party_ref || '): ' || SQLERRM;
	pk_main.log_error(p_source, 'pk_dedupe_phs', 'assign_party_dedupe_id', SQLCODE, v_error_message, v_pos);

END assign_party_dedupe_id;


PROCEDURE get_item_dedupe_id(
	p_source			IN VARCHAR2,
	p_orig_system_ref	IN VARCHAR2,
	p_item_type			IN VARCHAR2,
	p_value1			IN VARCHAR2,
	p_value2			IN VARCHAR2,
	p_dedupe_id			IN OUT NUMBER) IS

TYPE cur_type IS REF CURSOR;

cur cur_type;
v_looped_once BOOLEAN := FALSE;
v_error_message VARCHAR2(1000);
v_pos NUMBER;
v_searched BOOLEAN := FALSE;

BEGIN

	v_pos := 1;
	IF p_item_type = 'JOU' THEN
		OPEN cur FOR SELECT DISTINCT dedupe_id FROM tbl_items
			 	 	 WHERE item_type = 'JOU'
					 AND (((p_value1 = code) AND (length(nvl(p_value1,'x')) > 1)) OR ((p_value2 = identifier) AND (length(nvl(p_value2,'x')) > 1)));
		v_searched := TRUE;
	ELSIF p_item_type = 'ART' THEN
		OPEN cur FOR SELECT DISTINCT dedupe_id FROM tbl_items
			 	 	 WHERE item_type = 'ART'
					 AND (p_value1 = identifier) AND (length(nvl(p_value1,'x')) > 1);
		v_searched := TRUE;
	ELSE
		v_searched := FALSE;
	END IF;


	IF v_searched THEN

		LOOP
			FETCH cur INTO p_dedupe_id;
			EXIT WHEN cur%NOTFOUND;
			IF v_looped_once THEN
			   RAISE e_two_duplicate_keys;
			END IF;
			v_looped_once := TRUE;
		END LOOP;

		CLOSE cur;

		v_pos := 2;
		IF p_dedupe_id IS NULL THEN
			v_pos := 3;
			SELECT seq_item_dedupe.nextval INTO p_dedupe_id FROM dual;
		END IF;

	ELSE

		v_pos := 4;
		SELECT seq_item_dedupe.nextval INTO p_dedupe_id FROM dual;

	END IF;

EXCEPTION

WHEN e_two_duplicate_keys THEN

	v_error_message := 'Error (' || p_orig_system_ref || '): More than one dedupe id exists for this match. item_type = ' || p_item_type || ', value = ' || p_value1;
	pk_main.log_error(p_source, 'pk_dedupe_phs', 'get_item_dedupe_id', 0, v_error_message, v_pos);
	RAISE e_two_duplicate_keys;

WHEN OTHERS THEN

	v_error_message := 'Error (' || p_orig_system_ref || '): ' || SQLERRM;
	pk_main.log_error(p_source, 'pk_dedupe_phs', 'get_item_dedupe_id', SQLCODE, v_error_message, v_pos);

END get_item_dedupe_id;

PROCEDURE create_dedupe_table IS

v_pos  NUMBER := 0;

BEGIN

v_pos := 1;
EXECUTE IMMEDIATE ('TRUNCATE TABLE tbl_dedupe');

v_pos := 2;
INSERT INTO tbl_dedupe
		SELECT dedupe_id,
			max(orig_party_ref),
			max(decode(orig_system,'BMN',orig_party_ref, null)),
			max(decode(orig_system,'NS',orig_party_ref, null)),
			max(decode(orig_system,'TLI',orig_party_ref, null)),
			max(decode(orig_system,'SIS',orig_party_ref, null)),
			max(decode(orig_system,'AE',orig_party_ref, null)),
			max(decode(orig_system,'GCD',orig_party_ref, null)),
			max(decode(orig_system,'PTS',orig_party_ref, null)),
			max(decode(orig_system,'SD',orig_party_ref, null)),
			max(decode(orig_system,'EA',orig_party_ref, null)),
			max(decode(orig_system,'STR',orig_party_ref, null)),
			max(decode(orig_system,'ELB',orig_party_ref, null))
		FROM tbl_parties
		GROUP BY dedupe_id;

EXCEPTION

WHEN OTHERS THEN

	pk_main.log_error('', 'pk_dedupe_phs', 'create_dedupe_table', SQLCODE, SQLERRM, v_pos);

END create_dedupe_table;


PROCEDURE apply_match_key(p_key_col IN NUMBER, p_key_code IN VARCHAR2) IS

v_pos  NUMBER := 0;
v_sql VARCHAR2(500);

BEGIN

v_pos := 1;
SELECT key_sql INTO v_sql FROM tbl_match_keys WHERE key_code = p_key_code;

v_pos := 2;
EXECUTE IMMEDIATE ('UPDATE tbl_matches set match_key_' || p_key_col || ' = ' || v_sql);
COMMIT;

EXCEPTION

WHEN OTHERS THEN

	pk_main.log_error('', 'pk_dedupe_phs', 'apply_match_key', SQLCODE, SQLERRM, v_pos);

END apply_match_key;

END pk_dedupe_phs;
/
