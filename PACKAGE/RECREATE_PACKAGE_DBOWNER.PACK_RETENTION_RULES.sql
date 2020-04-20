CREATE PACKAGE DBOWNER.PACK_RETENTION_RULES
  CREATE OR REPLACE EDITIONABLE PACKAGE "DBOWNER"."PACK_RETENTION_RULES" AS 

  /* TODO enter package declarations (types, exceptions, methods etc) here */ 
  PROCEDURE PROC_RETENTION_RULES(vViewSQL VARCHAR2 DEFAULT 'False', vRunSQL VARCHAR2 DEFAULT 'False', vTruncate VARCHAR2 DEFAULT 'False', vOrigSystem VARCHAR2 DEFAULT 'ALL', vRunOrder VARCHAR2 DEFAULT 'ALL');
  FUNCTION RUN_RETENTION_RULES(vViewSQL VARCHAR2 DEFAULT 'False', vRunSQL VARCHAR2 DEFAULT 'False', vRunOrder IN NUMBER, vOrigSystem IN VARCHAR2, vRetentionAction IN VARCHAR2, vDataTable IN VARCHAR2, vDataRule IN VARCHAR2, vUserKey IN VARCHAR2, vDataType IN VARCHAR2, vRecordStatus IN VARCHAR2) RETURN VARCHAR2;

  FUNCTION FUNC_IS_ENGAGER (vDataTable IN VARCHAR2, vUserKey IN VARCHAR2, vRetentionAction IN VARCHAR2) RETURN VARCHAR2;
  FUNCTION FUNC_GET_EMAIL_DOMAIN_EXT (vEmail IN VARCHAR2) RETURN VARCHAR2;

  PROCEDURE PROC_RUN_DELETES(vOrigSystem IN VARCHAR2, vDataTable IN VARCHAR2, vUserKeyField IN VARCHAR2);
  PROCEDURE PROC_RUN_PSEUDONYMISE(vOrigSystem IN VARCHAR2, vDataTable IN VARCHAR2, vUserKeyField IN VARCHAR2);
  
  PROCEDURE PROC_RUN_NONPROD(vOrigSystem IN VARCHAR2);

END PACK_RETENTION_RULES;
/
CREATE OR REPLACE EDITIONABLE PACKAGE BODY "DBOWNER"."PACK_RETENTION_RULES" AS 

  /* TODO enter package declarations (types, exceptions, methods etc) here */ 

  PROCEDURE PROC_RETENTION_RULES(vViewSQL VARCHAR2 DEFAULT 'False', vRunSQL VARCHAR2 DEFAULT 'False', vTruncate VARCHAR2 DEFAULT 'False', vOrigSystem VARCHAR2 DEFAULT 'ALL', vRunOrder VARCHAR2 DEFAULT 'ALL')
  AS

    CURSOR CurRententionLkup IS
            SELECT ORIGINATING_SOURCE,
                   RUN_ORDER,
                   DATA_TABLE,
                   DATA_TYPE,
                   RECORD_STATUS,
                   DATA_RULE,
                   USER_KEY,
                   RETENTION_ACTION
            FROM   TBL_LKUP_CAPRI_RETENTION a
            WHERE  CASE WHEN vOrigSystem = 'ALL' THEN 'ALL' ELSE vOrigSystem END = 
                   CASE WHEN vOrigSystem = 'ALL' THEN 'ALL' ELSE ORIGINATING_SOURCE END
            AND    CASE WHEN vRunOrder = 'ALL' THEN 'ALL' ELSE vRunOrder END = 
                   CASE WHEN vRunOrder = 'ALL' THEN 'ALL' ELSE trim(to_char(run_order)) END
            ORDER BY 1,2;

     type vTableArray IS VARRAY(5) OF VARCHAR2(100);
     vTablesArray vTableArray;
     vTablesTotal NUMBER;
     vDataTable VARCHAR2(100);
     vUserKey   VARCHAR2(100);
     vResult    VARCHAR2(32767);

BEGIN

    IF upper(vTruncate) = 'TRUE' THEN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE TBL_MDT_CAPRI_RETENTION';
    END IF;


    FOR C IN CurRententionLkup LOOP

        vTablesArray := vTableArray('TBL_PARTIES','TBL_INTERESTS','TBL_SUBSCRIPTIONS','TBL_DOWNLOADS','TBL_IPS'); 

        IF c.DATA_TABLE = '<<ALL>>' THEN

          vTablesTotal := vTablesArray.count; 
          FOR i in 1 .. vTablesTotal LOOP 
                vDataTable := vTablesArray(i);

                IF vDataTable = 'TBL_SUBSCRIPTIONS' OR vDataTable = 'TBL_INTERESTS' THEN
                   vUserKey := 'PARTY_REF';
                ELSIF vDataTable = 'TBL_PARTIES' THEN
                   vUserKey := 'ORIG_PARTY_REF';                              
                ELSIF vDataTable = 'TBL_DOWNLOADS' THEN
                   vUserKey := 'PARTY_USER_REF';
                ELSIF vDataTable = 'TBL_IPS' THEN
                   vUserKey := 'PARTY_PER_REF';               
                END IF;

                vResult := RUN_RETENTION_RULES(vViewSQL, vRunSQL, c.RUN_ORDER, C.ORIGINATING_SOURCE, C.RETENTION_ACTION, vDataTable, C.DATA_RULE, vUserKey, c.DATA_TYPE, c.RECORD_STATUS); 

                IF upper(vViewSQL) = 'TRUE' THEN
                    dbms_output.put_line(vResult);
                END IF;

          END LOOP;

        ELSE
            vDataTable := C.DATA_TABLE;
            vUserKey := C.USER_KEY;

            vResult := RUN_RETENTION_RULES(vViewSQL, vRunSQL, c.RUN_ORDER, C.ORIGINATING_SOURCE, C.RETENTION_ACTION, vDataTable, C.DATA_RULE, vUserKey, c.DATA_TYPE, c.RECORD_STATUS); 

            IF upper(vViewSQL) = 'TRUE' THEN
                 dbms_output.put_line(vResult);
            END IF;            

        END IF;

     END LOOP;

  END PROC_RETENTION_RULES;


  FUNCTION RUN_RETENTION_RULES(vViewSQL VARCHAR2 DEFAULT 'False', vRunSQL VARCHAR2 DEFAULT 'False', vRunOrder IN NUMBER, vOrigSystem IN VARCHAR2, vRetentionAction IN VARCHAR2, vDataTable IN VARCHAR2, vDataRule IN VARCHAR2, vUserKey IN VARCHAR2, vDataType IN VARCHAR2, vRecordStatus IN VARCHAR2)
  RETURN VARCHAR2
  AS

     vSQL VARCHAR2(32767);
     vWhere VARCHAR2(32767);
     vRuleAdded NUMBER := 0;
     vReturn VARCHAR2(32767);

  BEGIN

         vRuleAdded := 0;

         vSQL := 'INSERT INTO TBL_MDT_CAPRI_RETENTION ';
         vSQL := vSQL || 'SELECT '|| vRunOrder || ',''' || vOrigSystem || ''',''' || vRetentionAction || ''', ''' || vDataTable || ''',';
         vSQL := vSQL || vUserKey || ', ROWID, CASE WHEN '''||upper(vRetentionAction)||''' = ''DELETE'' THEN NULL ELSE PACK_RETENTION_RULES.FUNC_IS_ENGAGER('''||vDataTable||''','||vUserKey||','''||vRetentionAction||''') END FROM ' || vDataTable;

         vWhere := ' WHERE ORIG_SYSTEM = ''' || vOrigSystem || '''';

         IF vDataType IS NOT NULL AND vDataType <> '<<ALL>>' THEN 
              vWhere := vWhere || ' AND ' || vDataType;
         END IF;

         IF vRecordStatus IS NOT NULL AND vRecordStatus <> '<<ALL>>' THEN 
             vWhere := vWhere || ' AND ' || vRecordStatus;
         END IF;

         IF vDataTable = 'TBL_ITEMS' AND vDataRule LIKE '%#MAX(ORIG_UPDATE_DATE)#%' THEN 
             vWhere := vWhere || ' AND ' || replace(vDataRule,'#MAX(ORIG_UPDATE_DATE)#', ' AUTHORS IN (SELECT USER_FIELD FROM VW_ITEMS_OPR_MAX_OUD z WHERE '|| vUserKey ||' = z.USER_FIELD AND z.ORIG_SYSTEM = ''' || vOrigSystem || ''' AND z.MAX_DATE ') || ')';   
             vRuleAdded := 1;
         END IF;   

         IF vDataTable = 'TBL_INTERESTS' AND vDataRule LIKE '%#MAX(ORIG_UPDATE_DATE)#%' THEN 
             vWhere := vWhere || ' AND ' || replace(vDataRule,'#MAX(ORIG_UPDATE_DATE)#', ' PARTY_REF IN (SELECT USER_FIELD FROM VW_INTERESTS_OPR_MAX_OUD z WHERE '|| vUserKey ||' = z.USER_FIELD AND z.ORIG_SYSTEM = ''' || vOrigSystem || ''' AND z.MAX_DATE ') || ')';   
             vRuleAdded := 1;
         END IF;       

         IF vDataRule LIKE '%#MAX_INTERESTS(ORIG_UPDATE_DATE)#%' THEN 
             vWhere := vWhere || ' AND ' || replace(vDataRule,'#MAX_INTERESTS(ORIG_UPDATE_DATE)#', ' PARTY_REF IN (SELECT USER_FIELD FROM VW_INTERESTS_OPR_MAX_OUD z WHERE '|| vUserKey ||' = z.USER_FIELD AND z.ORIG_SYSTEM = ''' || vOrigSystem || ''' AND z.MAX_DATE ') || ')';   
             vRuleAdded := 1;
         END IF;            

         IF vDataTable = 'TBL_INTERESTS' AND vDataRule LIKE '%#MAX(ORIG_CREATE_DATE)#%' THEN 
             vWhere := vWhere || ' AND ' || replace(vDataRule,'#MAX(ORIG_CREATE_DATE)#', ' PARTY_REF IN (SELECT USER_FIELD FROM VW_INTERESTS_OPR_MAX_OCD z WHERE '|| vUserKey ||' = z.USER_FIELD AND z.ORIG_SYSTEM = ''' || vOrigSystem || ''' AND z.MAX_DATE ') || ')';   
             vRuleAdded := 1;
         END IF;              

         IF vDataTable = 'TBL_INTERESTS' AND vDataRule LIKE '%#MAX(ALERT_END_DATE)#%' THEN 
             vWhere := vWhere || ' AND ' || replace(vDataRule,'#MAX(ALERT_END_DATE)#', ' PARTY_REF IN (SELECT USER_FIELD FROM VW_INTERESTS_OPR_MAX_AED z WHERE '|| vUserKey ||' = z.USER_FIELD AND z.ORIG_SYSTEM = ''' || vOrigSystem || ''' AND z.MAX_DATE ') || ')';   
             vRuleAdded := 1;
         END IF;               

         IF vDataTable = 'TBL_SUBSCRIPTIONS' AND vDataRule LIKE '%#MAX(ORIG_UPDATE_DATE)#%' THEN 
             vWhere := vWhere || ' AND ' || replace(vDataRule,'#MAX(ORIG_UPDATE_DATE)#', ' PARTY_REF IN (SELECT USER_FIELD FROM VW_SUBSCRIPTIONS_OPR_MAX_OUD z WHERE '|| vUserKey ||' = z.USER_FIELD AND z.ORIG_SYSTEM = ''' || vOrigSystem || ''' AND z.MAX_DATE ') || ')';   
             vRuleAdded := 1;
         END IF;    

         IF vDataRule LIKE '%#MAX_SUBSCRIPTIONS(ORIG_UPDATE_DATE)#%' THEN 
             vWhere := vWhere || ' AND ' || replace(vDataRule,'#MAX_SUBSCRIPTIONS(ORIG_UPDATE_DATE)#', ' PARTY_REF IN (SELECT USER_FIELD FROM VW_SUBSCRIPTIONS_OPR_MAX_OUD z WHERE '|| vUserKey ||' = z.USER_FIELD AND z.ORIG_SYSTEM = ''' || vOrigSystem || ''' AND z.MAX_DATE ') || ')';   
             vRuleAdded := 1;
         END IF;                     

         IF vDataTable = 'TBL_SUBSCRIPTIONS' AND vDataRule LIKE '%#MAX(ORIG_CREATE_DATE)#%' THEN 
             vWhere := vWhere || ' AND ' || replace(vDataRule,'#MAX(ORIG_CREATE_DATE)#', ' PARTY_REF IN (SELECT USER_FIELD FROM VW_SUBSCRIPTIONS_OPR_MAX_OCD z WHERE '|| vUserKey ||' = z.USER_FIELD AND z.ORIG_SYSTEM = ''' || vOrigSystem || ''' AND z.MAX_DATE ') || ')';   
             vRuleAdded := 1;
         END IF;        

         IF vDataRule LIKE '%#MAX_SUBSCRIPTIONS(ORIG_CREATE_DATE)#%' THEN 
             vWhere := vWhere || ' AND ' || replace(vDataRule,'#MAX_SUBSCRIPTIONS(ORIG_CREATE_DATE)#', ' PARTY_REF IN (SELECT USER_FIELD FROM VW_SUBSCRIPTIONS_OPR_MAX_OCD z WHERE '|| vUserKey ||' = z.USER_FIELD AND z.ORIG_SYSTEM = ''' || vOrigSystem || ''' AND z.MAX_DATE ') || ')';   
             vRuleAdded := 1;
         END IF;                 

         IF vDataTable = 'TBL_SUBSCRIPTIONS' AND vDataRule LIKE '%#MAX(END_DATE)#%' THEN 
             vWhere := vWhere || ' AND ' || replace(vDataRule,'#MAX(END_DATE)#', ' PARTY_REF IN (SELECT USER_FIELD FROM VW_SUBSCRIPTIONS_OPR_MAX_ED z WHERE '|| vUserKey ||' = z.USER_FIELD AND z.ORIG_SYSTEM = ''' || vOrigSystem || ''' AND z.MAX_DATE ') || ')';   
             vRuleAdded := 1;
         END IF;   

         IF vDataRule LIKE '%#MAX_SUBSCRIPTIONS(END_DATE)#%' THEN 
             vWhere := vWhere || ' AND ' || replace(vDataRule,'#MAX_SUBSCRIPTIONS(END_DATE)#', ' PARTY_REF IN (SELECT USER_FIELD FROM VW_SUBSCRIPTIONS_OPR_MAX_ED z WHERE PARTY_REF = z.USER_FIELD AND z.ORIG_SYSTEM = ''' || vOrigSystem || ''' AND z.MAX_DATE ') || ')';   
             vRuleAdded := 1;
         END IF;   

         IF vDataTable = 'TBL_PARTIES' AND vDataRule LIKE '%#MAX(ORIG_CREATE_DATE)#%' THEN 
             vWhere := vWhere || ' AND ' || replace(vDataRule,'#MAX(ORIG_CREATE_DATE)#', ' ORIG_CREATE_DATE ') ;   
             vRuleAdded := 1;
         END IF;            

         IF vDataTable = 'TBL_PARTIES' AND vDataRule LIKE '%#MAX(ORIG_UPDATE_DATE)#%' THEN 
             vWhere := vWhere || ' AND ' || replace(vDataRule,'#MAX(ORIG_UPDATE_DATE)#', ' ORIG_UPDATE_DATE ') ;   
             vRuleAdded := 1;
         END IF;                     


         IF vRuleAdded = 0 AND vDataRule <> '<<ALL>>' THEN 
            vWhere := vWhere || ' AND ' || vDataRule;
         END IF;

         vWhere := replace(vWhere,'<<USER_KEY>>',vUserKey);
         vWhere := replace(replace(vWhere,'<<RETENTION_ID=','(SELECT TABLE_KEY_VALUE FROM TBL_MDT_CAPRI_RETENTION WHERE RUN_ORDER='),'>>',')');

         IF upper(vViewSQL) = 'TRUE' THEN
            vReturn := vSQL || chr(10) || vWhere;
         END IF;

         IF upper(vRunSQL) = 'TRUE' THEN
            EXECUTE IMMEDIATE vSQL||vWhere;
            vReturn := 'Executed Successful';
            COMMIT;
         END IF;

  RETURN vReturn;
  END RUN_RETENTION_RULES;


    FUNCTION FUNC_GET_EMAIL_DOMAIN_EXT (vEmail IN VARCHAR2)
    RETURN VARCHAR2
    AS

    vExt VARCHAR2(4000);
    vTmp VARCHAR2(4000);

    BEGIN

        BEGIN
            SELECT EMAIL_DOMAIN_EXT
            INTO   vExt
            FROM   TBL_LKUP_DOMAIN_EXTENSIONS
            WHERE  LOWER(TRIM(vEmail)) LIKE '%'||EMAIL_DOMAIN_EXT;
        EXCEPTION WHEN OTHERS THEN 
                   vTmp := substr(vEmail, instr(vEmail,'@')+1);
                   vExt := '.'||substr(vTmp, instr(vTmp,'.',-1)+1);
        END;

    RETURN vExt;
    END FUNC_GET_EMAIL_DOMAIN_EXT;


    FUNCTION FUNC_IS_ENGAGER (vDataTable IN VARCHAR2, vUserKey IN VARCHAR2, vRetentionAction IN VARCHAR2)
    RETURN VARCHAR2
    AS

    vReturnString VARCHAR2(100);
    vCount        NUMBER := 0;

    BEGIN

        IF upper(vRetentionAction) <> 'DELETE' THEN

                SELECT COUNT(*)
                INTO   vReturnString
                FROM   TBL_PARTIES p,
                       TBL_CAPRI_ENGAGERS e
                WHERE  p.orig_party_ref = vUserKey
                and    p.email = e.email;

        ELSE
            vReturnString := '0';
        END IF;

        RETURN vReturnString;

    END FUNC_IS_ENGAGER;



    PROCEDURE PROC_RUN_DELETES(vOrigSystem IN VARCHAR2, vDataTable IN VARCHAR2, vUserKeyField IN VARCHAR2)
    AS

    vTotalRecords     NUMBER := 0;
    vTableRecords     NUMBER := 0;
    vRetentionRecords NUMBER := 0;
    vIndexesDropped   NUMBER := 0;

    BEGIN

        INSERT INTO tbl_retention_process_log
        VALUES (sysdate,'Started delete for '||vOrigSystem||' against table '||vDataTable);
        COMMIT;

        BEGIN 
             EXECUTE IMMEDIATE 'ALTER TABLE dbowner.'||vDataTable||' MODIFY PARTITION par_' || vOrigSystem || ' UNUSABLE LOCAL INDEXES';
             vIndexesDropped := 1;
        EXCEPTION WHEN OTHERS THEN
             vIndexesDropped := 0;
        END;

        INSERT INTO tbl_retention_process_log
        VALUES (sysdate,'Partition indexes dropped for '||vOrigSystem||' against table '||vDataTable);
        COMMIT;

        EXECUTE IMMEDIATE 'SELECT nvl(COUNT(*),0) FROM '||vDataTable||' WHERE ORIG_SYSTEM = '''||vOrigSystem||'''' INTO vTableRecords;
        EXECUTE IMMEDIATE 'SELECT nvl(COUNT(*),0) FROM TBL_MDT_CAPRI_RETENTION WHERE ORIGINATING_SOURCE = '''||vOrigSystem||''' AND upper(RETENTION_ACTION) = ''DELETE'' AND DATA_TABLE = '''||vDataTable||'''' INTO vRetentionRecords;
        
        IF vTableRecords = vRetentionRecords AND vIndexesDropped = 1 THEN
            EXECUTE IMMEDIATE 'ALTER TABLE '||vDataTable||' TRUNCATE PARTITION par_' || vOrigSystem;
            vTotalRecords := vTableRecords;
        ELSE 
            EXECUTE IMMEDIATE 'DELETE FROM '||vDataTable||' WHERE ORIG_SYSTEM = '''||vOrigSystem||''' AND ROWID IN (SELECT TABLE_ROWID FROM TBL_MDT_CAPRI_RETENTION WHERE ORIGINATING_SOURCE = '''||vOrigSystem||''' AND upper(RETENTION_ACTION) = ''DELETE'' AND DATA_TABLE = '''||vDataTable||''')';
            vTotalRecords := SQL%ROWCOUNT;
            COMMIT;
        END IF;

        INSERT INTO tbl_retention_process_log
        VALUES (sysdate,'Completed delete for '||vOrigSystem||' against table '||vDataTable||' with '||vTotalRecords||' records deleted');
        COMMIT;       

        IF vIndexesDropped = 1 THEN 
            EXECUTE IMMEDIATE 'ALTER TABLE dbowner.'||vDataTable||' MODIFY PARTITION par_' || vOrigSystem || ' REBUILD UNUSABLE LOCAL INDEXES';
        END IF;    

        INSERT INTO tbl_retention_process_log
        VALUES (sysdate,'Partition indexes created for '||vOrigSystem||' against table '||vDataTable);
        COMMIT;        


    END PROC_RUN_DELETES;


    PROCEDURE PROC_RUN_PSEUDONYMISE(vOrigSystem IN VARCHAR2, vDataTable IN VARCHAR2, vUserKeyField IN VARCHAR2)
    AS

    vTotalRecords     NUMBER := 0;
    vTableRecords     NUMBER := 0;
    vRetentionRecords NUMBER := 0;
    vIndexesDropped   NUMBER := 0;

    BEGIN

        INSERT INTO tbl_retention_process_log
        VALUES (sysdate,'Started pseudonymise for '||vOrigSystem||' against table '||vDataTable);
        COMMIT;

        BEGIN 
             EXECUTE IMMEDIATE 'ALTER TABLE dbowner.'||vDataTable||' MODIFY PARTITION par_' || vOrigSystem || ' UNUSABLE LOCAL INDEXES';
             vIndexesDropped := 1;
        EXCEPTION WHEN OTHERS THEN
             vIndexesDropped := 0;
        END;

        INSERT INTO tbl_retention_process_log
        VALUES (sysdate,'Partition indexes dropped for '||vOrigSystem||' against table '||vDataTable);
        COMMIT;
       
                UPDATE tbl_parties
                set    ADDRESS1 = decode(ADDRESS1,null,null,PARTY_ID),
                       ADDRESS2 = decode(ADDRESS2,null,null,PARTY_ID),
                       ADDRESS3 = decode(ADDRESS3,null,null,PARTY_ID),
                       ADDRESS4 = decode(ADDRESS4,null,null,PARTY_ID),
                       POST_CODE = case when POST_CODE is not null then (case 
                                                                            when POST_CODE like '% %' then trim(substr(POST_CODE,1,instr(POST_CODE,' ')-1))
                                                                            when POST_CODE like '%-%' then trim(substr(POST_CODE,1,instr(POST_CODE,'-')-1))
                                                                         else 'XXX' end
                                                                         ) else null end,
                       ORIG_ADDRESS = decode(ADDRESS1,null,null,PARTY_ID)||'~'||
                                      decode(ADDRESS2,null,null,PARTY_ID)||'~'||
                                      decode(ADDRESS3,null,null,PARTY_ID)||'~'||
                                      decode(ADDRESS4,null,null,PARTY_ID)||'~'||
                                      CITY||'~'||
                                      STATE||'~'||
                                      case when POST_CODE is not null then (case 
                                                                            when POST_CODE like '% %' then trim(substr(POST_CODE,1,instr(POST_CODE,' ')-1))
                                                                            when POST_CODE like '%-%' then trim(substr(POST_CODE,1,instr(POST_CODE,'-')-1))
                                                                            else 'XXX' end
                                                                            ) else null end,
                       FIRSTNAME = decode(FIRSTNAME,null,null,DEDUPE_ID),
                       LASTNAME  = decode(LASTNAME,null,null,DEDUPE_ID),
                       PARTY_NAME = decode(FIRSTNAME,null,null,DEDUPE_ID)||' '||
                                    decode(LASTNAME,null,null,DEDUPE_ID),
                       PHONE = decode(PHONE,null,null,PARTY_ID),
                       PHONE_2 = decode(PHONE_2,null,null,PARTY_ID),
                       PHONE_3 = decode(PHONE_3,null,null,PARTY_ID),
                       WEBSITE = decode(WEBSITE,null,null,PARTY_ID),
                       FAX = decode(FAX,null,null,PARTY_ID),
                       EMAIL = decode(EMAIL,null,null,DEDUPE_ID||'@'||DEDUPE_ID||PACK_RETENTION_RULES.FUNC_GET_EMAIL_DOMAIN_EXT(EMAIL)),
                       ORIG_EMAIL = decode(ORIG_EMAIL,null,null,DEDUPE_ID||'@'||DEDUPE_ID||PACK_RETENTION_RULES.FUNC_GET_EMAIL_DOMAIN_EXT(EMAIL)),       
                       EMAIL_DOMAIN = decode(EMAIL_DOMAIN,null,null,DEDUPE_ID||PACK_RETENTION_RULES.FUNC_GET_EMAIL_DOMAIN_EXT(EMAIL)),              
                       LOGIN = decode(LOGIN,null,null,PARTY_ID)                       
                WHERE orig_system = vOrigSystem
                AND   ROWID IN (SELECT TABLE_ROWID FROM TBL_MDT_CAPRI_RETENTION WHERE ORIGINATING_SOURCE = vOrigSystem AND upper(RETENTION_ACTION) = 'PSEUDONYMISE' AND upper(DATA_TABLE) = 'TBL_PARTIES');       
               
        	vTotalRecords := SQL%ROWCOUNT;
            COMMIT;


        INSERT INTO tbl_retention_process_log
        VALUES (sysdate,'Completed pseudonymise for '||vOrigSystem||' against table '||vDataTable||' with '||vTotalRecords||' records deleted');
        COMMIT;       

        IF vIndexesDropped = 1 THEN 
            EXECUTE IMMEDIATE 'ALTER TABLE dbowner.'||vDataTable||' MODIFY PARTITION par_' || vOrigSystem || ' REBUILD UNUSABLE LOCAL INDEXES';
        END IF;    

        INSERT INTO tbl_retention_process_log
        VALUES (sysdate,'Partition indexes created for '||vOrigSystem||' against table '||vDataTable);
        COMMIT;        


    END PROC_RUN_PSEUDONYMISE;


--
-- Procedure for non-prod data
--
    PROCEDURE PROC_RUN_NONPROD(vOrigSystem IN VARCHAR2)
    AS

    vTotalRecords     NUMBER := 0;
    vTableRecords     NUMBER := 0;
    vRetentionRecords NUMBER := 0;
    vIndexesDropped   NUMBER := 0;

    BEGIN

        INSERT INTO tbl_retention_process_log
        VALUES (sysdate,'Started pseudonymise (NONPROD) for '||vOrigSystem||' against table TBL_PARTIES');
        COMMIT;

        BEGIN 
             EXECUTE IMMEDIATE 'ALTER TABLE dbowner.TBL_PARTIES MODIFY PARTITION par_' || vOrigSystem || ' UNUSABLE LOCAL INDEXES';
             vIndexesDropped := 1;
        EXCEPTION WHEN OTHERS THEN
             vIndexesDropped := 0;
        END;

        INSERT INTO tbl_retention_process_log
        VALUES (sysdate,'Partition indexes dropped for '||vOrigSystem||' against table TBL_PARTIES');
        COMMIT;
       
                UPDATE tbl_parties
                set    ADDRESS1 = decode(ADDRESS1,null,null,PARTY_ID),
                       ADDRESS2 = decode(ADDRESS2,null,null,PARTY_ID),
                       ADDRESS3 = decode(ADDRESS3,null,null,PARTY_ID),
                       ADDRESS4 = decode(ADDRESS4,null,null,PARTY_ID),
                       POST_CODE = case when POST_CODE is not null then (case 
                                                                            when POST_CODE like '% %' then trim(substr(POST_CODE,1,instr(POST_CODE,' ')-1))
                                                                            when POST_CODE like '%-%' then trim(substr(POST_CODE,1,instr(POST_CODE,'-')-1))
                                                                         else 'XXX' end
                                                                         ) else null end,
                       ORIG_ADDRESS = decode(ADDRESS1,null,null,PARTY_ID)||'~'||
                                      decode(ADDRESS2,null,null,PARTY_ID)||'~'||
                                      decode(ADDRESS3,null,null,PARTY_ID)||'~'||
                                      decode(ADDRESS4,null,null,PARTY_ID)||'~'||
                                      CITY||'~'||
                                      STATE||'~'||
                                      case when POST_CODE is not null then (case 
                                                                            when POST_CODE like '% %' then trim(substr(POST_CODE,1,instr(POST_CODE,' ')-1))
                                                                            when POST_CODE like '%-%' then trim(substr(POST_CODE,1,instr(POST_CODE,'-')-1))
                                                                            else 'XXX' end
                                                                            ) else null end,
                       FIRSTNAME = decode(FIRSTNAME,null,null,DEDUPE_ID),
                       LASTNAME  = decode(LASTNAME,null,null,DEDUPE_ID),
                       PARTY_NAME = decode(FIRSTNAME,null,null,DEDUPE_ID)||' '||
                                    decode(LASTNAME,null,null,DEDUPE_ID),
                       PHONE = decode(PHONE,null,null,PARTY_ID),
                       PHONE_2 = decode(PHONE_2,null,null,PARTY_ID),
                       PHONE_3 = decode(PHONE_3,null,null,PARTY_ID),
                       WEBSITE = decode(WEBSITE,null,null,PARTY_ID),
                       FAX = decode(FAX,null,null,PARTY_ID),
                       EMAIL = decode(EMAIL,null,null,DEDUPE_ID||'@'||DEDUPE_ID||PACK_RETENTION_RULES.FUNC_GET_EMAIL_DOMAIN_EXT(EMAIL)),
                       ORIG_EMAIL = decode(ORIG_EMAIL,null,null,DEDUPE_ID||'@'||DEDUPE_ID||PACK_RETENTION_RULES.FUNC_GET_EMAIL_DOMAIN_EXT(EMAIL)),       
                       EMAIL_DOMAIN = decode(EMAIL_DOMAIN,null,null,DEDUPE_ID||PACK_RETENTION_RULES.FUNC_GET_EMAIL_DOMAIN_EXT(EMAIL)),              
                       LOGIN = decode(LOGIN,null,null,PARTY_ID)                       
                WHERE orig_system = vOrigSystem
                AND   nvl(email,'X') not like dedupe_id||'@'||dedupe_id||'%'
                AND   nvl(party_name,'X') not like dedupe_id||'%';
               
        	vTotalRecords := SQL%ROWCOUNT;
            COMMIT;


        INSERT INTO tbl_retention_process_log
        VALUES (sysdate,'Completed pseudonymise (NONPROD) for '||vOrigSystem||' against table TBL_PARTIES with '||vTotalRecords||' records deleted');
        COMMIT;       

        IF vIndexesDropped = 1 THEN 
            EXECUTE IMMEDIATE 'ALTER TABLE dbowner.TBL_PARTIES MODIFY PARTITION par_' || vOrigSystem || ' REBUILD UNUSABLE LOCAL INDEXES';
        END IF;    

        INSERT INTO tbl_retention_process_log
        VALUES (sysdate,'Partition indexes created for '||vOrigSystem||' against table TBL_PARTIES');
        COMMIT;        


    END PROC_RUN_NONPROD;


END PACK_RETENTION_RULES;
/
