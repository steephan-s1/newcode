CREATE PACKAGE DBOWNER.PACK_MONITORING
  CREATE OR REPLACE EDITIONABLE PACKAGE "DBOWNER"."PACK_MONITORING" 
AS

PROCEDURE get_scheduled_items   (p_items OUT SYS_REFCURSOR, p_error_message OUT	VARCHAR2, vMonitorID IN NUMBER DEFAULT 0);
PROCEDURE update_sql_items      (vMonitorID IN NUMBER, p_error_message OUT	VARCHAR2);
PROCEDURE update_www_items      (vMonitorID IN NUMBER, vStatusReturn IN VARCHAR2, vTimeElapsed IN VARCHAR2, vHTTPExt In VARCHAR2, p_error_message OUT	VARCHAR2);
PROCEDURE update_tnsping_items  (vMonitorID IN NUMBER, vStatusReturn IN VARCHAR2, vTimeElapsed IN VARCHAR2, p_error_message OUT	VARCHAR2);
PROCEDURE update_logfile_items  (vMonitorID IN NUMBER, p_log_output IN VARCHAR2, p_error_message OUT	VARCHAR2);
PROCEDURE update_report_items   (vMonitorID IN NUMBER, p_sql IN VARCHAR2, p_error_message OUT	VARCHAR2);
PROCEDURE refresh_page_orders;
PROCEDURE get_tbl_parties_dates;
PROCEDURE generate_alerts       (vMonitorID IN NUMBER);
PROCEDURE clear_alerts          (vMonitorID IN NUMBER);
PROCEDURE update_sent_alerts    (vMonitorID IN NUMBER DEFAULT 0, vEmailSent IN NUMBER DEFAULT 1);
PROCEDURE update_sent_slack_alerts    (vMonitorID IN NUMBER DEFAULT 0, vSlackSent IN NUMBER DEFAULT 1);
PROCEDURE write_audit_log       (vProcedureName IN VARCHAR2, vLogMessage IN VARCHAR2, vMonitorID IN NUMBER DEFAULT 0);
FUNCTION get_http_extension     (vMonitorID IN NUMBER) RETURN VARCHAR2;

END PACK_MONITORING;
/
CREATE OR REPLACE EDITIONABLE PACKAGE BODY "DBOWNER"."PACK_MONITORING" 
AS

vSQLErr        VARCHAR2(4000);
vDateRecorded  DATE;
vMonitorResult VARCHAR2(4000);
vMonitorStatus VARCHAR2(4000);

PROCEDURE get_scheduled_items (p_items OUT	SYS_REFCURSOR, p_error_message OUT	VARCHAR2, vMonitorID IN NUMBER DEFAULT 0)
IS

vProcedureName VARCHAR2(300);

BEGIN

   vProcedureName := 'get_scheduled_items';
   write_audit_log(vProcedureName,'Starting Procedure');
   
   IF vMonitorID = 0 THEN
                     OPEN	p_items FOR   
                        SELECT * 
                        FROM 
                               (
                                SELECT  c.MONITOR_ID,
                                        c.MONITOR_NAME,
                                        c.MONITOR_DESCRIPTION,
                                        c.MONITOR_SECTION,
                                        c.MONITOR_SUB_SECTION,
                                        c.MONITOR_TYPE,
                                        CASE WHEN c.MONITOR_TYPE = 'HTTP' AND c.MONITOR_SOURCE LIKE '%#HTTP_EXT:%' THEN (SELECT pack_monitoring.get_http_extension(c.MONITOR_ID) FROM DUAL) ELSE c.monitor_source end as monitor_source,
                                        c.MONITOR_RESULT_THRESHOLD,
                                        c.MONITOR_ACTIVE,
                                        c.MONITOR_ALERT,
                                        c.MONITOR_ALERT_EMAIL,
                                        c.MONITOR_HTTP_METHOD,
                                        CASE WHEN c.MONITOR_TYPE = 'HTTP' AND c.MONITOR_HTTP_PARAMETERS LIKE '%#HTTP_EXT:%' THEN TO_CLOB((SELECT pack_monitoring.get_http_extension(c.MONITOR_ID) FROM DUAL)) ELSE c.MONITOR_HTTP_PARAMETERS end as MONITOR_HTTP_PARAMETERS,
                                        c.MONITOR_HTTP_CHECKELEMENTS,
                                        s.schedule_minutes,
                                        a.MAX_DATE_RECORDED,
                                        s.schedule_every_execution
                                FROM   tbl_monitor_schedule s,
                                       (select a.monitor_id, max(a.DATE_RECORDED) AS MAX_DATE_RECORDED from tbl_monitor_output_archive a group by a.monitor_id) a,
                                       tbl_monitor_config c
                                WHERE  s.monitor_id = c.monitor_id
                                AND    s.monitor_id = a.monitor_id (+)
                                AND    c.monitor_active = 1    
                                AND    (
                                       schedule_every_execution = 1
                                OR     ((
                                       CASE WHEN trim(to_char(SYSDATE,'DY')) = 'MON' THEN 1 ELSE 2 END = schedule_monday
                                       OR
                                       CASE WHEN trim(to_char(SYSDATE,'DY')) = 'TUE' THEN 1 ELSE 2 END = schedule_tuesday
                                       OR
                                       CASE WHEN trim(to_char(SYSDATE,'DY')) = 'WED' THEN 1 ELSE 2 END = schedule_wednesday
                                       OR
                                       CASE WHEN trim(to_char(SYSDATE,'DY')) = 'THU' THEN 1 ELSE 2 END = schedule_thursday
                                       OR
                                       CASE WHEN trim(to_char(SYSDATE,'DY')) = 'FRI' THEN 1 ELSE 2 END = schedule_friday
                                       OR
                                       CASE WHEN trim(to_char(SYSDATE,'DY')) = 'SAT' THEN 1 ELSE 2 END = schedule_saturday
                                       OR
                                       CASE WHEN trim(to_char(SYSDATE,'DY')) = 'SUN' THEN 1 ELSE 2 END = schedule_sunday
                                       )
                                       AND
                                       (
                                       SYSDATE BETWEEN to_Date(trim(to_char(sysdate,'DD/MM/YYYY')) || ' ' || schedule_start_Time,'DD/MM/YYYY HH24:MI:SS') AND 
                                                       to_Date(trim(to_char(sysdate,'DD/MM/YYYY')) || ' ' || schedule_end_time,'DD/MM/YYYY HH24:MI:SS')
                                       )))
                                )
                        WHERE  (
                               CASE WHEN schedule_every_execution = 1 THEN 
                                         SYSDATE-8 
                                    ELSE nvl(to_date(trim(to_char(MAX_DATE_RECORDED,'DD/MM/YYYY HH24:MI'))||':00','DD/MM/YYYY HH24:MI:SS'),SYSDATE-7) 
                               END
                               ) <= to_date(trim(to_char(SYSDATE,'DD/MM/YYYY HH24:MI'))||':00','DD/MM/YYYY HH24:MI:SS')-nvl(schedule_minutes,1)/(24*60)
                        ORDER BY MONITOR_SECTION, MONITOR_SUB_SECTION, MONITOR_ID;
   ELSE
                     OPEN	p_items FOR   
                        SELECT c.MONITOR_ID,
                               c.MONITOR_NAME,
                               c.MONITOR_DESCRIPTION,
                               c.MONITOR_SECTION,
                               c.MONITOR_SUB_SECTION,
                               c.MONITOR_TYPE,
                               CASE WHEN c.MONITOR_TYPE = 'HTTP' AND c.MONITOR_SOURCE LIKE '%#HTTP_EXT:%' THEN (SELECT pack_monitoring.get_http_extension(c.MONITOR_ID) FROM DUAL) ELSE c.monitor_source end as monitor_source,
                               c.MONITOR_RESULT_THRESHOLD,
                               c.MONITOR_ACTIVE,
                               c.MONITOR_ALERT,
                               c.MONITOR_ALERT_EMAIL,
                               c.MONITOR_HTTP_METHOD,
                               CASE WHEN c.MONITOR_TYPE = 'HTTP' AND c.MONITOR_HTTP_PARAMETERS LIKE '%#HTTP_EXT:%' THEN TO_CLOB((SELECT pack_monitoring.get_http_extension(c.MONITOR_ID) FROM DUAL)) ELSE c.MONITOR_HTTP_PARAMETERS end as MONITOR_HTTP_PARAMETERS,
                               c.MONITOR_HTTP_CHECKELEMENTS, 
                               0 as schedule_minutes, 
                               SYSDATE as MAX_DATE_RECORDED, 
                               0 AS schedule_every_execution
                        FROM 
                               (
                                SELECT c.*
                                FROM   tbl_monitor_config c
                                WHERE  c.monitor_id = vMonitorID
                                ) c;
   END IF;                             
                     
   
                     
   write_audit_log(vProcedureName,'Procedure Completed');
   p_error_message := NULL;
   
EXCEPTION WHEN OTHERS THEN
   vSQLErr := SQLERRM;
   p_error_message := vSQLErr;
   write_audit_log(vProcedureName,'Error: '||vSQLErr);
   
END get_scheduled_items;

PROCEDURE update_sql_items (vMonitorID IN NUMBER, p_error_message OUT	VARCHAR2)
IS

CURSOR CurConfig IS
       SELECT * 
       FROM   tbl_monitor_config
       WHERE  monitor_id = vMonitorID;
       
vSQL VARCHAR2(32000);   
vProcedureName VARCHAR2(300);

BEGIN

   vProcedureName := 'update_sql_items';
   write_audit_log(vProcedureName,'Starting Procedure');
   
   FOR C IN CurConfig LOOP
   
       BEGIN
            EXECUTE IMMEDIATE 'DELETE FROM tbl_monitor_output_temp WHERE monitor_id = '||c.monitor_id;
       EXCEPTION WHEN OTHERS THEN
                 EXECUTE IMMEDIATE 'CREATE TABLE tbl_monitor_output_temp AS SELECT * FROM tbl_monitor_output WHERE ROWNUM < 1';
       END;
       
       vSQL := replace(c.MONITOR_SOURCE,'#MONITOR_THRESHOLD#',c.MONITOR_RESULT_THRESHOLD);
       vSQL := trim(substr(trim(vSQL),7)); 
       vSQL := 'INSERT INTO tbl_monitor_output_temp (monitor_id, date_recorded, monitor_result, monitor_status) SELECT '||c.monitor_id||',sysdate,'||vSQL;
       --write_audit_log(vProcedureName,'SQL: '||vSQL,c.MONITOR_ID);
       EXECUTE IMMEDIATE vSQL;
       
       EXECUTE IMMEDIATE 'DELETE FROM tbl_monitor_output WHERE monitor_id = '||c.monitor_id;
       EXECUTE IMMEDIATE 'INSERT INTO tbl_monitor_output SELECT * FROM tbl_monitor_output_temp WHERE monitor_id = '||c.monitor_id;
       EXECUTE IMMEDIATE 'INSERT INTO tbl_monitor_output_archive SELECT * FROM tbl_monitor_output_temp WHERE monitor_id = '||c.monitor_id;
       EXECUTE IMMEDIATE 'DELETE FROM tbl_monitor_output_temp WHERE monitor_id = '||c.monitor_id;
       COMMIT;
       
       write_audit_log(vProcedureName,'Completed Execution',c.MONITOR_ID);
       
       -- Create or remove alerts as required
       BEGIN 
           SELECT Date_Recorded, monitor_result, monitor_status
           INTO   vDateRecorded, vMonitorResult, vMonitorStatus
           FROM   tbl_monitor_output 
           WHERE  monitor_id = c.monitor_id;
           
           IF upper(vMonitorStatus) NOT IN ('SUCCESS','RUNNING') THEN
              generate_alerts(c.monitor_id);
           ELSE 
              clear_alerts(c.monitor_id);
           END IF;
           
       EXCEPTION WHEN OTHERS THEN
           vSQLErr := SQLERRM;
           write_audit_log(vProcedureName,'Error Raising Alert: '||vSQLErr, c.MONITOR_ID);
           p_error_message := vSQLErr;
       END;
       
   END LOOP;
   
   write_audit_log(vProcedureName,'Procedure Completed');
          
EXCEPTION WHEN OTHERS THEN
   vSQLErr := SQLERRM;
   write_audit_log(vProcedureName,'Error: '||vSQLErr);  
   p_error_message := vSQLErr;
END update_sql_items;

PROCEDURE update_www_items (vMonitorID IN NUMBER, vStatusReturn IN VARCHAR2, vTimeElapsed IN VARCHAR2, vHTTPExt In VARCHAR2, p_error_message OUT	VARCHAR2)
IS

CURSOR CurConfig IS
       SELECT * 
       FROM   tbl_monitor_config
       WHERE  monitor_id = vMonitorID;
       
vSQL VARCHAR2(32000);       
vProcedureName VARCHAR2(300);

BEGIN

   vProcedureName := 'update_www_items';
   write_audit_log(vProcedureName,'Starting Procedure');
   
   FOR C IN CurConfig LOOP
   
       BEGIN
            EXECUTE IMMEDIATE 'DELETE FROM tbl_monitor_output_temp WHERE monitor_id = '||c.monitor_id;
       EXCEPTION WHEN OTHERS THEN
                 EXECUTE IMMEDIATE 'CREATE TABLE tbl_monitor_output_temp AS SELECT * FROM tbl_monitor_output WHERE ROWNUM < 1';
       END;

       
       vSQL := ' ''Return Status = '||vStatusReturn||'<BR>Loaded in '||vTimeElapsed ||'ms'' as OUTPUT,  CASE WHEN '||vTimeElapsed ||' '||c.monitor_result_threshold||' and '''||upper(vStatusReturn)||''' NOT LIKE ''%ERROR%'' THEN ''Success'' ELSE ''Investigate'' END as MONITOR_STATUS FROM DUAL';
       vSQL := 'INSERT INTO tbl_monitor_output_temp (monitor_id, date_recorded, monitor_result, monitor_status) SELECT '||c.monitor_id||',sysdate,'||vSQL;
       write_audit_log(vProcedureName,'SQL: '||vSQL,c.MONITOR_ID);
       EXECUTE IMMEDIATE vSQL;
       
       EXECUTE IMMEDIATE 'DELETE FROM tbl_monitor_output WHERE monitor_id = '||c.monitor_id;
       EXECUTE IMMEDIATE 'INSERT INTO tbl_monitor_output SELECT * FROM tbl_monitor_output_temp WHERE monitor_id = '||c.monitor_id;
       EXECUTE IMMEDIATE 'INSERT INTO tbl_monitor_output_archive SELECT * FROM tbl_monitor_output_temp WHERE monitor_id = '||c.monitor_id;
       EXECUTE IMMEDIATE 'DELETE FROM tbl_monitor_output_temp WHERE monitor_id = '||c.monitor_id;
       COMMIT;
       
       IF vHTTPExt IS NOT NULL THEN
             EXECUTE IMMEDIATE 'DELETE FROM tbl_monitor_http_extensions WHERE MONITOR_ID = '||c.monitor_id;
             EXECUTE IMMEDIATE 'INSERT INTO tbl_monitor_http_extensions VALUES ('||c.monitor_id||','''||vHTTPExt||''')';
             COMMIT;
       END IF;
       
       write_audit_log(vProcedureName,'Completed Execution',c.MONITOR_ID);
       
       -- Create or remove alerts as required
       BEGIN 
           SELECT Date_Recorded, monitor_result, monitor_status
           INTO   vDateRecorded, vMonitorResult, vMonitorStatus
           FROM   tbl_monitor_output 
           WHERE  monitor_id = c.monitor_id;
           
           IF upper(vMonitorStatus) NOT IN ('SUCCESS','RUNNING') THEN
              generate_alerts(c.monitor_id);
           ELSE 
              clear_alerts(c.monitor_id);
           END IF;
           
       EXCEPTION WHEN OTHERS THEN
           vSQLErr := SQLERRM;
           write_audit_log(vProcedureName,'Error Raising Alert: '||vSQLErr, c.MONITOR_ID);
           p_error_message := vSQLErr;
       END;       
   
   END LOOP;
   
   write_audit_log(vProcedureName,'Procedure Completed');
          
EXCEPTION WHEN OTHERS THEN
   vSQLErr := SQLERRM;
   write_audit_log(vProcedureName,'Error: '||vSQLErr);  
   p_error_message := vSQLErr;
END update_www_items;

PROCEDURE update_tnsping_items (vMonitorID IN NUMBER, vStatusReturn IN VARCHAR2, vTimeElapsed IN VARCHAR2, p_error_message OUT	VARCHAR2)
IS

CURSOR CurConfig IS
       SELECT * 
       FROM   tbl_monitor_config
       WHERE  monitor_id = vMonitorID;
       
vSQL VARCHAR2(32000);       
vProcedureName VARCHAR2(300);

BEGIN

   vProcedureName := 'update_tnsping_items';
   write_audit_log(vProcedureName,'Starting Procedure');
   
   FOR C IN CurConfig LOOP
   
       BEGIN
            EXECUTE IMMEDIATE 'DELETE FROM tbl_monitor_output_temp WHERE monitor_id = '||c.monitor_id;
       EXCEPTION WHEN OTHERS THEN
                 EXECUTE IMMEDIATE 'CREATE TABLE tbl_monitor_output_temp AS SELECT * FROM tbl_monitor_output WHERE ROWNUM < 1';
       END;

       
       vSQL := ' ''Return Status = '||upper(vStatusReturn)||'<BR>Loaded in '||vTimeElapsed ||'ms'' as OUTPUT,  CASE WHEN '||vTimeElapsed ||' '||c.monitor_result_threshold||' and '''||upper(vStatusReturn)||''' = ''OK'' THEN ''Success'' ELSE ''Investigate'' END as MONITOR_STATUS FROM DUAL';
       vSQL := 'INSERT INTO tbl_monitor_output_temp (monitor_id, date_recorded, monitor_result, monitor_status) SELECT '||c.monitor_id||',sysdate,'||vSQL;
       write_audit_log(vProcedureName,'SQL: '||vSQL,c.MONITOR_ID);
       EXECUTE IMMEDIATE vSQL;
       
       EXECUTE IMMEDIATE 'DELETE FROM tbl_monitor_output WHERE monitor_id = '||c.monitor_id;
       EXECUTE IMMEDIATE 'INSERT INTO tbl_monitor_output SELECT * FROM tbl_monitor_output_temp WHERE monitor_id = '||c.monitor_id;
       EXECUTE IMMEDIATE 'INSERT INTO tbl_monitor_output_archive SELECT * FROM tbl_monitor_output_temp WHERE monitor_id = '||c.monitor_id;
       EXECUTE IMMEDIATE 'DELETE FROM tbl_monitor_output_temp WHERE monitor_id = '||c.monitor_id;
       COMMIT;
       
       write_audit_log(vProcedureName,'Completed Execution',c.MONITOR_ID);
       
       -- Create or remove alerts as required
       BEGIN 
           SELECT Date_Recorded, monitor_result, monitor_status
           INTO   vDateRecorded, vMonitorResult, vMonitorStatus
           FROM   tbl_monitor_output 
           WHERE  monitor_id = c.monitor_id;
           
           IF upper(vMonitorStatus) NOT IN ('SUCCESS','RUNNING') THEN
              generate_alerts(c.monitor_id);
           ELSE 
              clear_alerts(c.monitor_id);
           END IF;
           
       EXCEPTION WHEN OTHERS THEN
           vSQLErr := SQLERRM;
           write_audit_log(vProcedureName,'Error Raising Alert: '||vSQLErr, c.MONITOR_ID);
           p_error_message := vSQLErr;
       END;       
   
   END LOOP;
   
   write_audit_log(vProcedureName,'Procedure Completed');
          
EXCEPTION WHEN OTHERS THEN
   vSQLErr := SQLERRM;
   write_audit_log(vProcedureName,'Error: '||vSQLErr);  
   p_error_message := vSQLErr;   
END update_tnsping_items;

PROCEDURE update_logfile_items (vMonitorID IN NUMBER, p_log_output IN VARCHAR2, p_error_message OUT	VARCHAR2)
IS

CURSOR CurConfig IS
       SELECT * 
       FROM   tbl_monitor_config
       WHERE  monitor_id = vMonitorID;
       
vSQL VARCHAR2(32000);       
vProcedureName VARCHAR2(300);
vOutput VARCHAR2(4000);

BEGIN

   vProcedureName := 'update_logfile_items';
   write_audit_log(vProcedureName,'Starting Procedure');
   
   FOR C IN CurConfig LOOP
   
       BEGIN
            EXECUTE IMMEDIATE 'DELETE FROM tbl_monitor_output_temp WHERE monitor_id = '||c.monitor_id;
       EXCEPTION WHEN OTHERS THEN
                 EXECUTE IMMEDIATE 'CREATE TABLE tbl_monitor_output_temp AS SELECT * FROM tbl_monitor_output WHERE ROWNUM < 1';
       END;

       vOutput := replace(c.monitor_result_threshold,'#OUTPUT#',''''||replace(nvl(p_log_output,'File Not Found'),CHR(10),'<BR>')||'''');
       
       vSQL := vOutput || ' FROM DUAL';
       vSQL := 'INSERT INTO tbl_monitor_output_temp (monitor_id, date_recorded, monitor_result, monitor_status) SELECT '||c.monitor_id||',sysdate,'||vSQL;
       write_audit_log(vProcedureName,'SQL: '||vSQL,c.MONITOR_ID);
       EXECUTE IMMEDIATE vSQL;
       
       EXECUTE IMMEDIATE 'DELETE FROM tbl_monitor_output WHERE monitor_id = '||c.monitor_id;
       EXECUTE IMMEDIATE 'INSERT INTO tbl_monitor_output SELECT * FROM tbl_monitor_output_temp WHERE monitor_id = '||c.monitor_id;
       EXECUTE IMMEDIATE 'INSERT INTO tbl_monitor_output_archive SELECT * FROM tbl_monitor_output_temp WHERE monitor_id = '||c.monitor_id;
       EXECUTE IMMEDIATE 'DELETE FROM tbl_monitor_output_temp WHERE monitor_id = '||c.monitor_id;
       COMMIT;
             
       write_audit_log(vProcedureName,'Completed Execution',c.MONITOR_ID);
       
       -- Create or remove alerts as required
       BEGIN 
           SELECT Date_Recorded, monitor_result, monitor_status
           INTO   vDateRecorded, vMonitorResult, vMonitorStatus
           FROM   tbl_monitor_output 
           WHERE  monitor_id = c.monitor_id;
           
           IF upper(vMonitorStatus) NOT IN ('SUCCESS','RUNNING') THEN
              generate_alerts(c.monitor_id);
           ELSE 
              clear_alerts(c.monitor_id);
           END IF;
           
       EXCEPTION WHEN OTHERS THEN
           vSQLErr := SQLERRM;
           write_audit_log(vProcedureName,'Error Raising Alert: '||vSQLErr, c.MONITOR_ID);
           p_error_message := vSQLErr;
       END;       
   
   END LOOP;
   
   write_audit_log(vProcedureName,'Procedure Completed');
          
EXCEPTION WHEN OTHERS THEN
   vSQLErr := SQLERRM;
   write_audit_log(vProcedureName,'Error: '||vSQLErr);  
   p_error_message := vSQLErr;
END update_logfile_items;


PROCEDURE update_report_items (vMonitorID IN NUMBER, p_sql IN VARCHAR2, p_error_message OUT	VARCHAR2)
IS

CURSOR CurConfig IS
       SELECT * 
       FROM   tbl_monitor_config
       WHERE  monitor_id = vMonitorID;
       
   
vProcedureName VARCHAR2(300);
vOutput VARCHAR2(4000);

vOutputString CLOB := NULL;
vOutputCLOB CLOB   := NULL;
vPrevOutputString CLOB;

TYPE CurTyp IS REF CURSOR; 
DataCursor   CurTyp;

BEGIN

   vProcedureName := 'update_report_items';
   write_audit_log(vProcedureName,'Starting Procedure');
   
    FOR C IN CurConfig LOOP
         
         dbms_output.ENABLE;
         DBMS_LOB.createtemporary (vOutputString, TRUE);
         DBMS_LOB.createtemporary (vOutputCLOB, TRUE);
         DBMS_LOB.createtemporary (vPrevOutputString, TRUE);
         
    OPEN DataCursor FOR p_sql;
         LOOP
          FETCH DataCursor INTO vOutputString;
                IF nvl(vPrevOutputString,'X') <> vOutputString THEN
                   dbms_lob.append(vOutputCLOB,  vOutputString);
                END IF;
                vPrevOutputString := vOutputString;
          EXIT WHEN DataCursor%NOTFOUND;
     END LOOP;
       
     DELETE FROM tbl_monitor_report_output WHERE monitor_id = vMonitorID;
     INSERT INTO tbl_monitor_report_output (monitor_id, REPORT_OUTPUT) values(vMonitorID, vOutputCLOB);
     COMMIT;
     
     
    EXECUTE IMMEDIATE 'DELETE FROM tbl_monitor_output WHERE monitor_id = '||c.monitor_id;
    EXECUTE IMMEDIATE 'INSERT INTO tbl_monitor_output VALUES ('||c.monitor_id||',sysdate,''Success'',''Success'')';
    EXECUTE IMMEDIATE 'INSERT INTO tbl_monitor_output_archive SELECT * FROM tbl_monitor_output WHERE monitor_id = '||c.monitor_id;
    COMMIT;
     
   END LOOP;
   
   write_audit_log(vProcedureName,'Procedure Completed');
          
EXCEPTION WHEN OTHERS THEN

    EXECUTE IMMEDIATE 'DELETE FROM tbl_monitor_output WHERE monitor_id = '||vMonitorID;
    EXECUTE IMMEDIATE 'INSERT INTO tbl_monitor_output VALUES ('||vMonitorID||',sysdate,''Error Running Report'',''Failure'')';
    EXECUTE IMMEDIATE 'INSERT INTO tbl_monitor_output_archive SELECT * FROM tbl_monitor_output WHERE monitor_id = '||vMonitorID;
    COMMIT;
    
   vSQLErr := SQLERRM;
   write_audit_log(vProcedureName,'Error: '||vSQLErr);  
   p_error_message := vSQLErr;
END update_report_items;


PROCEDURE get_tbl_parties_dates
IS

vProcedureName VARCHAR2(300);

BEGIN

   vProcedureName := 'get_tbl_parties_dates';
   write_audit_log(vProcedureName,'Starting Procedure');

      BEGIN
          EXECUTE IMMEDIATE 'DROP TABLE TBL_MONITOR_TMP_CAPRI_DATES_T PURGE';
      EXCEPTION WHEN OTHERS THEN NULL;
      END;
      
      EXECUTE IMMEDIATE 'CREATE TABLE TBL_MONITOR_TMP_CAPRI_DATES_T
                         AS
                            SELECT ORIG_SYSTEM, 
                                   MAX(CAPRI_UPDATE_DATE) CAPRI_UPDATE_DATE,
                                   MAX(CAPRI_CREATE_DATE) CAPRI_CREATE_DATE,
                                   MAX(ORIG_UPDATE_DATE)  ORIG_UPDATE_DATE,
                                   MAX(ORIG_CREATE_DATE)  ORIG_CREATE_DATE,
                                   RECORD_STATUS
                            FROM   TBL_PARTIES 
                            GROUP BY ORIG_SYSTEM,
                                     RECORD_STATUS';
                                     
      BEGIN
          EXECUTE IMMEDIATE 'DELETE FROM TBL_MONITOR_TMP_CAPRI_DATES';
          EXECUTE IMMEDIATE 'INSERT INTO TBL_MONITOR_TMP_CAPRI_DATES SELECT * FROM TBL_MONITOR_TMP_CAPRI_DATES_T';
      EXCEPTION WHEN OTHERS THEN
          EXECUTE IMMEDIATE 'CREATE TABLE TBL_MONITOR_TMP_CAPRI_DATES AS SELECT * FROM TBL_MONITOR_TMP_CAPRI_DATES_T';
      END;

   COMMIT;
   write_audit_log(vProcedureName,'Procedure Completed');
          
EXCEPTION WHEN OTHERS THEN
   vSQLErr := SQLERRM;
   write_audit_log(vProcedureName,'Error: '||vSQLErr); 
END get_tbl_parties_dates;


PROCEDURE refresh_page_orders
IS

CURSOR CurItems IS 
                SELECT DISTINCT 
                       MONITOR_SECTION, 
                       MONITOR_SUB_SECTION, 
                       MONITOR_NAME
                FROM TBL_MONITOR_CONFIG
                ORDER BY 1,2,3;

vTabChk NUMBER;
vSQL    VARCHAR2(4000);
vProcedureName VARCHAR2(300);

BEGIN 


   vProcedureName := 'refresh_page_orders';
   write_audit_log(vProcedureName,'Starting Procedure');

    BEGIN
        EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM TBL_MONITOR_DISPLAY_ORDER' INTO vTabChk;
    EXCEPTION WHEN OTHERS THEN 
        EXECUTE IMMEDIATE 'CREATE TABLE TBL_MONITOR_DISPLAY_ORDER (monitor_section varchar2(4000), monitor_sub_section varchar2(4000), monitor_name varchar2(4000), display_order number)';
    END;
    
    FOR C IN CurItems LOOP
    
       -- Check the Section exists in the table
       EXECUTE IMMEDIATE ' INSERT INTO TBL_MONITOR_DISPLAY_ORDER (monitor_section, display_order) ' ||
                         ' SELECT '''||replace(c.MONITOR_SECTION,'''','''''')||''', 9999' ||
                         ' FROM DUAL ' ||
                         ' WHERE '''||replace(c.MONITOR_SECTION,'''','''''')||''' NOT IN (SELECT d.MONITOR_SECTION FROM TBL_MONITOR_DISPLAY_ORDER d WHERE '''||replace(c.MONITOR_SECTION,'''','''''')||''' = d.MONITOR_SECTION AND d.MONITOR_SUB_SECTION IS NULL AND d.MONITOR_NAME IS NULL) '||
                         ' AND '''||replace(c.MONITOR_SECTION,'''','''''')||''' IS NOT NULL';
       COMMIT;
       
       -- Check the Sub Section exists in the table
       EXECUTE IMMEDIATE ' INSERT INTO TBL_MONITOR_DISPLAY_ORDER (monitor_section, monitor_sub_section, display_order) ' ||
                         ' SELECT '''||replace(c.MONITOR_SECTION,'''','''''')||''', '''||replace(c.MONITOR_SUB_SECTION,'''','''''')||''', 9999' ||
                         ' FROM DUAL ' ||
                         ' WHERE '''||replace(c.MONITOR_SUB_SECTION,'''','''''')||''' NOT IN (SELECT d.MONITOR_SUB_SECTION FROM TBL_MONITOR_DISPLAY_ORDER d WHERE '''||replace(c.MONITOR_SECTION,'''','''''')||''' = d.MONITOR_SECTION AND '''||replace(c.MONITOR_SUB_SECTION,'''','''''')||''' = d.MONITOR_SUB_SECTION AND d.MONITOR_NAME IS NULL) '||
                         ' AND '''||replace(c.MONITOR_SECTION,'''','''''')||''' IS NOT NULL ' ||
                         ' AND '''||replace(c.MONITOR_SUB_SECTION,'''','''''')||''' IS NOT NULL';
       COMMIT;   
       
       -- Check theMonitor Config exists in the table
       EXECUTE IMMEDIATE ' INSERT INTO TBL_MONITOR_DISPLAY_ORDER (monitor_section, monitor_sub_section, monitor_name, display_order) ' ||
                         ' SELECT '''||replace(c.MONITOR_SECTION,'''','''''')||''', '''||replace(c.MONITOR_SUB_SECTION,'''','''''')||''', '''||replace(c.MONITOR_NAME,'''','''''')||''', 9999' ||
                         ' FROM DUAL ' ||
                         ' WHERE '''||replace(c.MONITOR_NAME,'''','''''')||''' NOT IN (SELECT d.MONITOR_NAME FROM TBL_MONITOR_DISPLAY_ORDER d WHERE '''||replace(c.MONITOR_SECTION,'''','''''')||''' = d.MONITOR_SECTION AND '''||replace(c.MONITOR_SUB_SECTION,'''','''''')||''' = d.MONITOR_SUB_SECTION AND '''||replace(c.MONITOR_NAME,'''','''''')||''' = d.MONITOR_NAME) '||
                         ' AND '''||replace(c.MONITOR_SECTION,'''','''''')||''' IS NOT NULL ' ||
                         ' AND '''||replace(c.MONITOR_NAME,'''','''''')||''' IS NOT NULL ' ||                   
                         ' AND '''||replace(c.MONITOR_SUB_SECTION,'''','''''')||''' IS NOT NULL';           
       COMMIT;      
    
    END LOOP;
    
    EXECUTE IMMEDIATE 'DELETE FROM TBL_MONITOR_DISPLAY_ORDER o WHERE o.MONITOR_NAME IS NOT NULL AND o.MONITOR_NAME NOT IN (SELECT c.MONITOR_NAME FROM TBL_MONITOR_CONFIG c WHERE c.MONITOR_NAME = o.MONITOR_NAME)';
    EXECUTE IMMEDIATE 'DELETE FROM TBL_MONITOR_DISPLAY_ORDER o WHERE o.MONITOR_SUB_SECTION IS NOT NULL AND o.MONITOR_SUB_SECTION NOT IN (SELECT c.MONITOR_SUB_SECTION FROM TBL_MONITOR_CONFIG c WHERE c.MONITOR_SUB_SECTION = o.MONITOR_SUB_SECTION)';
    EXECUTE IMMEDIATE 'DELETE FROM TBL_MONITOR_DISPLAY_ORDER o WHERE o.MONITOR_SECTION IS NOT NULL AND o.MONITOR_SECTION NOT IN (SELECT c.MONITOR_SECTION FROM TBL_MONITOR_CONFIG c WHERE c.MONITOR_SECTION = o.MONITOR_SECTION)';
    EXECUTE IMMEDIATE 'DELETE FROM TBL_MONITOR_DISPLAY_ORDER o WHERE o.MONITOR_NAME IS NOT NULL AND o.MONITOR_NAME NOT IN (SELECT c.MONITOR_NAME FROM TBL_MONITOR_CONFIG c WHERE c.MONITOR_NAME = o.MONITOR_NAME And o.MONITOR_SECTION = c.MONITOR_SECTION and o.MONITOR_SUB_SECTION = c.MONITOR_SUB_SECTION)';
    COMMIT;
    
    write_audit_log(vProcedureName,'Procedure Completed');

EXCEPTION WHEN OTHERS THEN
   vSQLErr := SQLERRM;
   write_audit_log(vProcedureName,'Error: '||vSQLErr); 
END refresh_page_orders;


PROCEDURE generate_alerts (vMonitorID IN NUMBER)
IS

vActiveAlertExists NUMBER := 0;
vProcedureName VARCHAR2(300);

BEGIN

   vProcedureName := 'generate_alerts';
   write_audit_log(vProcedureName,'Starting Procedure');

   BEGIN
       SELECT NVL(COUNT(*),0)
       INTO   vActiveAlertExists
       FROM   tbl_monitor_alerts_active
       WHERE  monitor_id = vMonitorID;
   EXCEPTION WHEN OTHERS THEN NULL;
   END;
   
   IF vActiveAlertExists = 0 THEN
      INSERT INTO tbl_monitor_alerts_active ( monitor_id, 
                                              date_opened,
                                              date_recorded, 
                                              monitor_result, 
                                              monitor_status, 
                                              email_sent, 
                                              email_address, 
                                              email_date, 
                                              acknowledged, 
                                              date_acknowledged,
                                              system_update_date,
                                              user_update_date, 
                                              user_update_username, 
                                              date_resolved, 
                                              notes,
                                              failure_sequence_number,
                                              slack_sent
                                            )
      SELECT c.monitor_id,
             SYSDATE,
             o.date_recorded,
             o.monitor_result,
             o.monitor_status,
             0,
             c.monitor_alert_email,
             null,
             0,
             null,
             sysdate,
             null,
             null,
             null,
             'System Alert Created',
             1,
             0
      FROM   tbl_monitor_config c,
             tbl_monitor_output o
      WHERE  c.monitor_id = vMonitorID
      AND    c.monitor_id = o.monitor_id;
      COMMIT;
      
      INSERT INTO tbl_monitor_alerts_archive
      SELECT * FROM tbl_monitor_alerts_active WHERE monitor_id = vMonitorID;
      COMMIT;
      
      write_audit_log(vProcedureName,'Generated Alert',vMonitorID);
      
   ELSE 
      UPDATE tbl_monitor_alerts_active
      SET    system_update_date = sysdate,
             failure_sequence_number = nvl(failure_sequence_number,0) + 1
      WHERE  monitor_id = vMonitorID;
      COMMIT;
      
      UPDATE tbl_monitor_alerts_archive
      SET    system_update_date = sysdate,
             failure_sequence_number = nvl(failure_sequence_number,0) + 1
      WHERE  monitor_id = vMonitorID
      AND    date_resolved IS NULL;
      COMMIT;     
      
      write_audit_log(vProcedureName,'Updated Alert',vMonitorID);
      
   END IF;
   
    write_audit_log(vProcedureName,'Procedure Completed');   
   
EXCEPTION WHEN OTHERS THEN
   vSQLErr := SQLERRM;
   write_audit_log(vProcedureName,'Error: '||vSQLErr); 
END generate_alerts;   


PROCEDURE clear_alerts (vMonitorID IN NUMBER)
IS

vActiveAlertExists NUMBER := 0;
vCheckEmailSent    NUMBER := 0;
vProcedureName VARCHAR2(300);

BEGIN

   vProcedureName := 'clear_alerts';
   write_audit_log(vProcedureName,'Starting Procedure');
      
      UPDATE tbl_monitor_alerts_archive
      SET    date_resolved = sysdate,
             system_update_date = sysdate
      WHERE  monitor_id = vMonitorID
      AND    date_resolved IS NULL;
      COMMIT;     
      
      BEGIN
          SELECT EMAIL_SENT 
          INTO   vCheckEmailSent
          FROM   tbl_monitor_alerts_active
          WHERE monitor_id = vMonitorID;
      EXCEPTION WHEN OTHERS THEN NULL;
      END;
      
      IF vCheckEmailSent = 0 THEN 
         update_sent_alerts(vMonitorID,0);
      END IF;
      
      
      DELETE FROM tbl_monitor_alerts_active WHERE monitor_id = vMonitorID;
      COMMIT;

    write_audit_log(vProcedureName,'Procedure Completed');
   
EXCEPTION WHEN OTHERS THEN
   vSQLErr := SQLERRM;
   write_audit_log(vProcedureName,'Error: '||vSQLErr); 
END clear_alerts;   
   

PROCEDURE update_sent_alerts (vMonitorID IN NUMBER DEFAULT 0, vEmailSent IN NUMBER DEFAULT 1)
IS

CURSOR CurItems IS 
                SELECT DISTINCT 
                       MONITOR_ID
                FROM TBL_MONITOR_ALERTS_ACTIVE
                WHERE CASE WHEN vMonitorID > 0 THEN MONITOR_ID ELSE 0 END = vMonitorID;

vProcedureName VARCHAR2(300);
BEGIN

      FOR c IN CurItems LOOP
      
            IF vEmailSent = 1 THEN 
                 UPDATE tbl_monitor_alerts_active 
                 SET    email_sent = 1,
                        email_date = sysdate,
                        system_update_date = sysdate
                 WHERE  monitor_id = c.monitor_id
                 AND    email_sent = 0;
                 
                 UPDATE tbl_monitor_alerts_archive
                 SET    email_sent = 1,
                        email_date = sysdate,
                        system_update_date = sysdate
                 WHERE  monitor_id = c.monitor_id
                 AND    nvl(email_sent,0) = 0;
                 COMMIT;
                 
            ELSE 
                 UPDATE tbl_monitor_alerts_active 
                 SET    email_sent = 0,
                        notes = notes || chr(10) || 'Resolved before system could send email',
                        system_update_date = sysdate
                 WHERE  monitor_id = c.monitor_id;
                 
                 UPDATE tbl_monitor_alerts_archive
                 SET    email_sent = 0,
                        notes = notes || chr(10) || 'Resolved before system could send email',
                        system_update_date = sysdate
                 WHERE  monitor_id = c.monitor_id
                 AND    nvl(email_sent,0) = 0;
                 
                 COMMIT;
            END IF;
            
      END LOOP;      
     
END update_sent_alerts;  


PROCEDURE update_sent_slack_alerts (vMonitorID IN NUMBER DEFAULT 0, vSlackSent IN NUMBER DEFAULT 1)
IS

CURSOR CurItems IS 
                SELECT DISTINCT 
                       MONITOR_ID
                FROM TBL_MONITOR_ALERTS_ACTIVE
                WHERE CASE WHEN vMonitorID > 0 THEN MONITOR_ID ELSE 0 END = vMonitorID;

vProcedureName VARCHAR2(300);
BEGIN

      FOR c IN CurItems LOOP
      
            IF vSlackSent = 1 THEN 
                 UPDATE tbl_monitor_alerts_active 
                 SET    slack_sent = 1,
                        slack_date = sysdate,
                        system_update_date = sysdate
                 WHERE  monitor_id = c.monitor_id
                 AND    nvl(slack_sent,0) = 0;
                 
                 UPDATE tbl_monitor_alerts_archive
                 SET    slack_sent = 1,
                        slack_date = sysdate,
                        system_update_date = sysdate
                 WHERE  monitor_id = c.monitor_id
                 AND    nvl(slack_sent,0) = 0;
                 COMMIT;
                 
            ELSE 
                 UPDATE tbl_monitor_alerts_active 
                 SET    slack_sent = 0,
                        notes = notes || chr(10) || 'Resolved before system could send email',
                        system_update_date = sysdate
                 WHERE  monitor_id = c.monitor_id;
                 
                 UPDATE tbl_monitor_alerts_archive
                 SET    slack_sent = 0,
                        notes = notes || chr(10) || 'Resolved before system could send email',
                        system_update_date = sysdate
                 WHERE  monitor_id = c.monitor_id
                 AND    nvl(slack_sent,0) = 0;
                 
                 COMMIT;
            END IF;
            
      END LOOP;      
     
END update_sent_slack_alerts;  

     

PROCEDURE write_audit_log (vProcedureName IN VARCHAR2, vLogMessage IN VARCHAR2, vMonitorID IN NUMBER DEFAULT 0)
IS

BEGIN
     
     INSERT INTO tbl_monitor_audit_log
     VALUES (sysdate,vProcedureName,vLogMessage,vMonitorID);
     COMMIT;

END write_audit_log;


FUNCTION get_http_extension (vMonitorID IN NUMBER)
RETURN VARCHAR2
IS

PRAGMA AUTONOMOUS_TRANSACTION;

vReturn VARCHAR2(30000);
vHTTPParametersC CLOB;
vHTTPParametersV VARCHAR2(32767);

BEGIN

        BEGIN
              select replace(c.monitor_source,'#'||rtrim(substr(ltrim(substr(c.monitor_source,instr(c.monitor_source,'#')),'#'),1,instr(ltrim(substr(c.monitor_source,instr(c.monitor_source,'#')),'#'),'#')),'#')||'#',h.ext_text)
              INTO vReturn
              from tbl_monitor_config c,
                   tbl_monitor_http_extensions h
              where c.monitor_id = vMonitorID
              and   replace(upper(rtrim(substr(ltrim(substr(c.monitor_source,instr(c.monitor_source,'#')),'#'),1,instr(ltrim(substr(c.monitor_source,instr(c.monitor_source,'#')),'#'),'#')),'#')),'HTTP_EXT:') = h.monitor_id;
        EXCEPTION WHEN OTHERS THEN NULL;
        END;
        
        IF vReturn IS NULL THEN
              BEGIN
              
                    SELECT c.MONITOR_HTTP_PARAMETERS
                    INTO   vHTTPParametersC
                    FROM   tbl_monitor_config c
                    WHERE  c.monitor_id = vMonitorID;
                   
                    vHTTPParametersV := dbms_lob.substr(vHTTPParametersC,32767,1);
                    
                    /*BEGIN          
                         EXECUTE IMMEDIATE 'DROP TABLE tbl_monitor_temp_sql PURGE';
                    EXCEPTION WHEN OTHERS THEN NULL;
                    END;
                    
                    EXECUTE IMMEDIATE 'CREATE TABLE tbl_monitor_temp_sql AS SELECT c.MONITOR_HTTP_PARAMETERS, '''||vHTTPParametersV||''' VARCHAR_VAL ' ||
                                      'FROM   tbl_monitor_config c '||
                                      'WHERE  c.monitor_id = '||vMonitorID;
                    */
                    
                    select replace(vHTTPParametersV,'#'||rtrim(substr(ltrim(substr(vHTTPParametersV,instr(vHTTPParametersV,'#')),'#'),1,instr(ltrim(substr(vHTTPParametersV,instr(vHTTPParametersV,'#')),'#'),'#')),'#')||'#',h.ext_text)
                    INTO vReturn
                    from tbl_monitor_config c,
                         tbl_monitor_http_extensions h
                    where c.monitor_id = vMonitorID
                    and   replace(upper(rtrim(substr(ltrim(substr(vHTTPParametersV,instr(vHTTPParametersV,'#')),'#'),1,instr(ltrim(substr(vHTTPParametersV,instr(vHTTPParametersV,'#')),'#'),'#')),'#')),'HTTP_EXT:') = h.monitor_id;
              EXCEPTION WHEN OTHERS THEN NULL;
              END;
        END IF;

RETURN vReturn;

END get_http_extension;

END PACK_MONITORING;
/
