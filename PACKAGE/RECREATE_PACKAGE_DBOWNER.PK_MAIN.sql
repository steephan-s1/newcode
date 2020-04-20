CREATE PACKAGE DBOWNER.PK_MAIN
  CREATE OR REPLACE EDITIONABLE PACKAGE "DBOWNER"."PK_MAIN" AS

PROCEDURE log_time(p_source IN VARCHAR2, p_package IN VARCHAR2, p_module IN VARCHAR2, p_note IN VARCHAR2, p_pos IN OUT NUMBER, p_increment IN BOOLEAN := FALSE);
PROCEDURE log_error(p_source IN VARCHAR2, p_package IN VARCHAR2, p_module IN VARCHAR2, p_code IN VARCHAR2, p_text IN VARCHAR2, p_pos IN NUMBER);

END pk_main;

 
/
CREATE OR REPLACE EDITIONABLE PACKAGE BODY "DBOWNER"."PK_MAIN" AS

PROCEDURE log_time(p_source IN VARCHAR2, p_package IN VARCHAR2, p_module IN VARCHAR2, p_note IN VARCHAR2, p_pos IN OUT NUMBER, p_increment IN BOOLEAN := FALSE) IS

PRAGMA AUTONOMOUS_TRANSACTION;

BEGIN

	UPDATE tbl_logging SET log_time = SYSDATE, note = p_note WHERE package_name = p_package AND module_name = p_module AND position = p_pos AND (source = p_source OR source IS NULL);

	IF SQL%ROWCOUNT = 0 THEN
		INSERT INTO tbl_logging VALUES (p_source, p_package, p_module, p_pos, p_note, SYSDATE);
	END IF;

	COMMIT;

	IF p_increment THEN
		p_pos := p_pos + 1;
	END IF;

END log_time;


PROCEDURE log_error(p_source IN VARCHAR2, p_package IN VARCHAR2, p_module IN VARCHAR2, p_code IN VARCHAR2, p_text IN VARCHAR2, p_pos IN NUMBER) IS

PRAGMA AUTONOMOUS_TRANSACTION;

BEGIN

INSERT INTO tbl_errors (error_id, source, package_name, module_name, position, error_code, error_msg, creation_date, created_by)
	   VALUES (seq_errors_1.NEXTVAL, p_source, p_package, p_module, p_pos, p_code, substr(p_text,1,2000), sysdate, sys_context('userenv','session_user'));

COMMIT;

END log_error;


END pk_main;
/
