CREATE PACKAGE DBOWNER.PK_DEDUPE_BY_SOURCE
  CREATE OR REPLACE EDITIONABLE PACKAGE "DBOWNER"."PK_DEDUPE_BY_SOURCE" AS

PROCEDURE dedupe_parties(p_success IN OUT VARCHAR2, p_source IN VARCHAR2);

PROCEDURE get_item_dedupe_id(
	p_source			IN VARCHAR2,
	p_orig_system_ref	IN VARCHAR2,
	p_item_type			IN VARCHAR2,
	p_value1			IN VARCHAR2,
	p_value2			IN VARCHAR2,
	p_dedupe_id			IN OUT NUMBER);

--PROCEDURE create_dedupe_table;

--PROCEDURE apply_match_key(p_key_col IN NUMBER, p_key_code IN VARCHAR2);

END pk_dedupe_by_source;
 
/
CREATE OR REPLACE EDITIONABLE PACKAGE BODY "DBOWNER"."PK_DEDUPE_BY_SOURCE" AS

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

PROCEDURE dedupe_parties(p_success IN OUT VARCHAR2, p_source IN VARCHAR2) IS

v_pos 	  	 			NUMBER := 0;
v_transfers 			NUMBER;
e_transfers_running		EXCEPTION;

--// GCS Changes
l_count_updates         NUMBER  := 0;

TYPE cur_typ IS REF CURSOR;
c cur_typ;
v_DedupeSQL           VARCHAR2(4000);
rec_orig_system       VARCHAR2(5 CHAR);
rec_dedupe_type       VARCHAR2(3 CHAR);
rec_orig_party_ref    VARCHAR2(265 CHAR);
rec_party_name        VARCHAR2(300 CHAR);
rec_email             VARCHAR2(300 CHAR);
rec_record_status     VARCHAR2(1 CHAR);
rec_dedupe_id         NUMBER;
rec_capri_update_date DATE;

BEGIN

	pk_main.log_time(NULL, 'pk_dedupe_'||p_source, 'dedupe_parties', 'checking for transfers running', v_pos, TRUE);
	--SELECT count(*) INTO v_transfers FROM tbl_sources WHERE transfer_status = 'R';
  SELECT count(*) INTO v_transfers  FROM tbl_sources s 
    WHERE transfer_status = 'R' 
    AND s.source = p_source
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
		pk_main.log_time(NULL, 'pk_dedupe_'||p_source, 'dedupe_parties', 'locking transfers out using tbl_dedupe_control', v_pos, TRUE);
		--UPDATE tbl_dedupe_control SET dedupe_running = 'Y';
		--COMMIT;
	END IF;


	pk_main.log_time(NULL, 'pk_dedupe_'||p_source, 'dedupe_parties', 'just about to truncate tbl_deduping', v_pos, TRUE);
  BEGIN
	     EXECUTE IMMEDIATE 'DROP TABLE tbl_deduping_'||p_source||' PURGE';
  EXCEPTION WHEN OTHERS THEN NULL;
  END;

	pk_main.log_time(NULL, 'pk_dedupe_'||p_source, 'dedupe_parties', 'just about to insert into tbl_deduping', v_pos, TRUE);
  EXECUTE IMMEDIATE 'CREATE TABLE tbl_deduping_'||p_source||'
                    AS
                    SELECT /*+ append */ orig_system, dedupe_type, orig_party_ref, party_name, email, record_status, dedupe_id, capri_update_date
		                FROM tbl_parties
		                WHERE record_status IN (''I'',''U'')
                    AND   orig_system = '''||p_source||'''';
	COMMIT;

  v_DedupeSQL := '  SELECT orig_system, dedupe_type, orig_party_ref, party_name, email, record_status, dedupe_id, capri_update_date
	                  FROM tbl_deduping_'||p_source||'  d
	                  WHERE record_status IN (''I'',''U'')
                    AND   orig_system = '''||p_source||'''
                    ORDER BY capri_update_date';


	pk_main.log_time(NULL, 'pk_dedupe_'||p_source, 'dedupe_parties', 'commencing dedupe update', v_pos, TRUE);
  
  OPEN c FOR v_DedupeSQL;
  LOOP
        FETCH c INTO rec_orig_system,
                     rec_dedupe_type,
                     rec_orig_party_ref,
			               rec_party_name,
			               rec_email,
			               rec_record_status,
			               rec_dedupe_id,
                     rec_capri_update_date;
        EXIT WHEN c%NOTFOUND;


		assign_party_dedupe_id (
			rec_orig_system,
			rec_dedupe_type,
			rec_orig_party_ref,
			rec_party_name,
			rec_email,
			rec_record_status,
			rec_dedupe_id);

        --// GCS Changes
		-- COMMIT;
            IF l_count_updates > 10000 THEN
                COMMIT;
                l_count_updates := 0;
                pk_main.log_time(NULL, 'pk_dedupe_'||p_source, 'dedupe_parties', 'Completed COMMITING updates ', v_pos, TRUE);
            ELSE
                l_count_updates := l_count_updates + 1;
            END IF;

	END LOOP;
  CLOSE c;
  
	pk_main.log_time(NULL, 'pk_dedupe_'||p_source, 'dedupe_parties', 'end of dedupe update', v_pos, TRUE);

	pk_main.log_time(NULL, 'pk_dedupe_'||p_source, 'dedupe_parties', 'unlocking transfers using tbl_dedupe_control', v_pos, TRUE);
	--UPDATE tbl_dedupe_control SET dedupe_running = 'N';
	COMMIT;

	p_success := 'Y';

EXCEPTION

WHEN e_transfers_running THEN

	p_success := 'N';
	pk_main.log_error(NULL, 'pk_dedupe_'||p_source, 'dedupe_parties', 0, 'there are transfer processes already running', v_pos);


WHEN OTHERS THEN

	p_success := 'N';
	pk_main.log_error(NULL, 'pk_dedupe_'||p_source, 'dedupe_parties', SQLCODE, SQLERRM, v_pos);

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
	SELECT /*+ INDEX(TBL_PARTIES IDX_PARTIES_3) */ DISTINCT dedupe_id FROM tbl_parties
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
	pk_main.log_error(p_source, 'pk_dedupe_'||p_source, 'assign_party_dedupe_id', 0, v_error_message, v_pos);

WHEN OTHERS THEN

	v_error_message := 'Error (' || p_orig_party_ref || '): ' || SQLERRM;
	pk_main.log_error(p_source, 'pk_dedupe_'||p_source, 'assign_party_dedupe_id', SQLCODE, v_error_message, v_pos);

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
	pk_main.log_error(p_source, 'pk_dedupe_'||p_source, 'get_item_dedupe_id', 0, v_error_message, v_pos);
	RAISE e_two_duplicate_keys;

WHEN OTHERS THEN

	v_error_message := 'Error (' || p_orig_system_ref || '): ' || SQLERRM;
	pk_main.log_error(p_source, 'pk_dedupe_'||p_source, 'get_item_dedupe_id', SQLCODE, v_error_message, v_pos);

END get_item_dedupe_id;

END pk_dedupe_by_source;
/
