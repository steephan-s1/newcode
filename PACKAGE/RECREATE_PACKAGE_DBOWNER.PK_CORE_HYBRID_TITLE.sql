CREATE PACKAGE DBOWNER.PK_CORE_HYBRID_TITLE
  CREATE OR REPLACE EDITIONABLE PACKAGE "DBOWNER"."PK_CORE_HYBRID_TITLE" 
AS

PROCEDURE CreateDataCoreTitle;
PROCEDURE CreateDataHybridTitle;



END PK_CORE_Hybrid_TITLE;
/
CREATE OR REPLACE EDITIONABLE PACKAGE BODY "DBOWNER"."PK_CORE_HYBRID_TITLE" 
AS

PROCEDURE CreateDataHybridTitle
IS
Cursor c1 is 
select ES_Subject_Codes , REGEXP_COUNT(ES_Subject_Codes,';') new from TBL_TEMP_HybridTitle;
v_code VARCHAR2(11);
l_cnt NUMBER;
BEGIN 

--processLog('processENEWSCoreTitle','Information','Started Procedure'); 

EXECUTE IMMEDIATE 'TRUNCATE TABLE TBL_TEMP_HYBRIDTITLE_NEW';

FOR i in c1 LOOP

   if  i.new = 0 THEN
   dbms_output.put_line ('Inside 0');
       INSERT INTO TBL_TEMP_HybridTitle_new 
       SELECT * from TBL_TEMP_HybridTitle where ES_Subject_Codes =  i.ES_Subject_Codes;
    END IF;

    IF i.new = 1 THEN
     dbms_output.put_line ('Inside 1');
    dbms_output.put_line ( i.ES_Subject_Codes);
          v_code := substr(i.ES_Subject_Codes,4,3);
          dbms_output.put_line ( v_code);
          INSERT INTO TBL_TEMP_HybridTitle_new
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_HybridTitle where ES_Subject_Codes =  i.ES_Subject_Codes;

            v_code := substr(i.ES_Subject_Codes,17,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_HybridTitle_new
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_HybridTitle where ES_Subject_Codes =  i.ES_Subject_Codes;
        END IF;  

        IF i.new = 2 THEN
        dbms_output.put_line ('Inside 2');
         dbms_output.put_line ( i.ES_Subject_Codes);
          v_code := substr(i.ES_Subject_Codes,4,3);
          dbms_output.put_line ( v_code);
          INSERT INTO TBL_TEMP_HybridTitle_new
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_HybridTitle where ES_Subject_Codes =  i.ES_Subject_Codes;

            v_code := substr(i.ES_Subject_Codes,17,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_HybridTitle_new
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_HybridTitle where ES_Subject_Codes =  i.ES_Subject_Codes;

            v_code := substr(i.ES_Subject_Codes,30,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_HybridTitle_new
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_HybridTitle where ES_Subject_Codes =  i.ES_Subject_Codes;

        END IF; 

        dbms_output.put_line (i.new);
         IF i.new = 3  THEN
         dbms_output.put_line ('Inside 3');
          dbms_output.put_line ( i.ES_Subject_Codes);
          v_code := substr(i.ES_Subject_Codes,4,3);
          dbms_output.put_line ( v_code);
          INSERT INTO TBL_TEMP_HybridTitle_new
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_HybridTitle where ES_Subject_Codes =  i.ES_Subject_Codes;

            v_code := substr(i.ES_Subject_Codes,17,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_HybridTitle_new
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_HybridTitle where ES_Subject_Codes =  i.ES_Subject_Codes;


            v_code := substr(i.ES_Subject_Codes,30,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_HybridTitle_new
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_HybridTitle where ES_Subject_Codes =  i.ES_Subject_Codes;

             v_code := substr(i.ES_Subject_Codes,43,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_HybridTitle_new
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_HybridTitle where ES_Subject_Codes =  i.ES_Subject_Codes;

         END IF;

        dbms_output.put_line( i.new);

          IF i.new = 4  THEN
          dbms_output.put_line ('Inside 4');
          dbms_output.put_line ( i.ES_Subject_Codes);
          v_code := substr(i.ES_Subject_Codes,4,3);
          dbms_output.put_line ( v_code);
          INSERT INTO TBL_TEMP_HybridTitle_new
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_HybridTitle where ES_Subject_Codes =  i.ES_Subject_Codes;

            v_code := substr(i.ES_Subject_Codes,17,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_HybridTitle_new
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_HybridTitle where ES_Subject_Codes =  i.ES_Subject_Codes;


            v_code := substr(i.ES_Subject_Codes,30,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_HybridTitle_new
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_HybridTitle where ES_Subject_Codes =  i.ES_Subject_Codes;

             v_code := substr(i.ES_Subject_Codes,43,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_HybridTitle_new
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_HybridTitle where ES_Subject_Codes =  i.ES_Subject_Codes;

                v_code := substr(i.ES_Subject_Codes,56,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_HybridTitle_new
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_HybridTitle where ES_Subject_Codes =  i.ES_Subject_Codes;


          END IF;

          ----

          IF i.new = 5  THEN
          dbms_output.put_line ('Inside 5');
          dbms_output.put_line ( i.ES_Subject_Codes);
          v_code := substr(i.ES_Subject_Codes,4,3);
          dbms_output.put_line ( v_code);
          INSERT INTO TBL_TEMP_HybridTitle_new
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_HybridTitle where ES_Subject_Codes =  i.ES_Subject_Codes;

            v_code := substr(i.ES_Subject_Codes,17,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_HybridTitle_new
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_HybridTitle where ES_Subject_Codes =  i.ES_Subject_Codes;


            v_code := substr(i.ES_Subject_Codes,30,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_HybridTitle_new
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_HybridTitle where ES_Subject_Codes =  i.ES_Subject_Codes;

             v_code := substr(i.ES_Subject_Codes,43,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_HybridTitle_new
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_HybridTitle where ES_Subject_Codes =  i.ES_Subject_Codes;

                v_code := substr(i.ES_Subject_Codes,56,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_HybridTitle_new
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_HybridTitle where ES_Subject_Codes =  i.ES_Subject_Codes;

               v_code := substr(i.ES_Subject_Codes,69,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_HybridTitle_new
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_HybridTitle where ES_Subject_Codes =  i.ES_Subject_Codes;
          END IF;
          ----6-----------
           IF i.new = 6  THEN
           dbms_output.put_line ('Inside 6');
          dbms_output.put_line ( i.ES_Subject_Codes);
          v_code := substr(i.ES_Subject_Codes,4,3);
          dbms_output.put_line ( v_code);
          INSERT INTO TBL_TEMP_HybridTitle_new
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_HybridTitle where ES_Subject_Codes =  i.ES_Subject_Codes;

            v_code := substr(i.ES_Subject_Codes,17,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_HybridTitle_new
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_HybridTitle where ES_Subject_Codes =  i.ES_Subject_Codes;


            v_code := substr(i.ES_Subject_Codes,30,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_HybridTitle_new
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_HybridTitle where ES_Subject_Codes =  i.ES_Subject_Codes;

             v_code := substr(i.ES_Subject_Codes,43,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_HybridTitle_new
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_HybridTitle where ES_Subject_Codes =  i.ES_Subject_Codes;

                v_code := substr(i.ES_Subject_Codes,56,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_HybridTitle_new
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_HybridTitle where ES_Subject_Codes =  i.ES_Subject_Codes;

               v_code := substr(i.ES_Subject_Codes,69,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_HybridTitle_new
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_HybridTitle where ES_Subject_Codes =  i.ES_Subject_Codes;

             v_code := substr(i.ES_Subject_Codes,82,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_HybridTitle_new
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_HybridTitle where ES_Subject_Codes =  i.ES_Subject_Codes;

          END IF;

          ----8---
           IF i.new = 8  THEN
           dbms_output.put_line ('Inside 8');
          dbms_output.put_line ( i.ES_Subject_Codes);
          v_code := substr(i.ES_Subject_Codes,4,3);
          dbms_output.put_line ( v_code);
          INSERT INTO TBL_TEMP_HybridTitle_new
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_HybridTitle where ES_Subject_Codes =  i.ES_Subject_Codes;

            v_code := substr(i.ES_Subject_Codes,17,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_HybridTitle_new
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_HybridTitle where ES_Subject_Codes =  i.ES_Subject_Codes;


            v_code := substr(i.ES_Subject_Codes,30,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_HybridTitle_new
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_HybridTitle where ES_Subject_Codes =  i.ES_Subject_Codes;

             v_code := substr(i.ES_Subject_Codes,43,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_HybridTitle_new
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_HybridTitle where ES_Subject_Codes =  i.ES_Subject_Codes;

                v_code := substr(i.ES_Subject_Codes,56,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_HybridTitle_new
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_HybridTitle where ES_Subject_Codes =  i.ES_Subject_Codes;

               v_code := substr(i.ES_Subject_Codes,69,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_HybridTitle_new
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_HybridTitle where ES_Subject_Codes =  i.ES_Subject_Codes;

             v_code := substr(i.ES_Subject_Codes,82,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_HybridTitle_new
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_HybridTitle where ES_Subject_Codes =  i.ES_Subject_Codes;


              v_code := substr(i.ES_Subject_Codes,109,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_HybridTitle_new
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_HybridTitle where ES_Subject_Codes =  i.ES_Subject_Codes;


          END IF;
END LOOP;
commit;
EXECUTE IMMEDIATE 'TRUNCATE TABLE TBL_TEMP_HYBRIDTITLE';
  INSERT INTO TBL_TEMP_HYBRIDTITLE select * from TBL_TEMP_HYBRIDTITLE_NEW;
commit;
DELETE FROM 
    TBL_TEMP_HYBRIDTITLE A
 WHERE 
   a.rowid > 
    ANY (
      SELECT 
        B.rowid
     FROM 
        TBL_TEMP_HYBRIDTITLE B
     WHERE 
        A.ES_SUBJECT_CODE_LEVEL3 = B.ES_SUBJECT_CODE_LEVEL3
    AND A.Isbn13OrImpressionId = B.Isbn13OrImpressionId
            ); 
    commit;
commit;
END CreateDataHybridTitle ;
PROCEDURE CreateDataCoreTitle
IS
Cursor c1 is 
select ES_Subject_Codes , REGEXP_COUNT(ES_Subject_Codes,';') new from TBL_TEMP_CoreTitle;
v_code VARCHAR2(11);
l_cnt NUMBER;
BEGIN
--processLog('processENEWSCoreTitle','Information','Started Procedure'); 



EXECUTE IMMEDIATE 'TRUNCATE TABLE TBL_TEMP_CORETITLE_NEW ';

FOR i in c1 LOOP
   if  i.new = 0 THEN
       INSERT INTO TBL_TEMP_CoreTitle_NEW 
       SELECT * from TBL_TEMP_CoreTitle where ES_Subject_Codes =  i.ES_Subject_Codes;
    END IF;

    IF i.new = 1 THEN
    dbms_output.put_line ( i.ES_Subject_Codes);
          v_code := substr(i.ES_Subject_Codes,4,3);
          dbms_output.put_line ( v_code);
          INSERT INTO TBL_TEMP_CoreTitle_NEW
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_CORETITLE where ES_Subject_Codes =  i.ES_Subject_Codes;

            v_code := substr(i.ES_Subject_Codes,17,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_CoreTitle_NEW
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_CORETITLE where ES_Subject_Codes =  i.ES_Subject_Codes;
        END IF;  

        IF i.new = 2 THEN
         dbms_output.put_line ( i.ES_Subject_Codes);
          v_code := substr(i.ES_Subject_Codes,4,3);
          dbms_output.put_line ( v_code);
          INSERT INTO TBL_TEMP_CoreTitle_NEW
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_CORETITLE where ES_Subject_Codes =  i.ES_Subject_Codes;

            v_code := substr(i.ES_Subject_Codes,17,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_CoreTitle_NEW
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_CORETITLE where ES_Subject_Codes =  i.ES_Subject_Codes;


            v_code := substr(i.ES_Subject_Codes,30,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_CoreTitle_NEW
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_CORETITLE where ES_Subject_Codes =  i.ES_Subject_Codes;

        END IF;  
         IF i.new = 3  THEN
          dbms_output.put_line ( i.ES_Subject_Codes);
          v_code := substr(i.ES_Subject_Codes,4,3);
          dbms_output.put_line ( v_code);
          INSERT INTO TBL_TEMP_CoreTitle_NEW
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_CORETITLE where ES_Subject_Codes =  i.ES_Subject_Codes;

            v_code := substr(i.ES_Subject_Codes,17,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_CoreTitle_NEW
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_CORETITLE where ES_Subject_Codes =  i.ES_Subject_Codes;


            v_code := substr(i.ES_Subject_Codes,30,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_CoreTitle_NEW
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_CORETITLE where ES_Subject_Codes =  i.ES_Subject_Codes;

             v_code := substr(i.ES_Subject_Codes,43,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_CoreTitle_NEW
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_CORETITLE where ES_Subject_Codes =  i.ES_Subject_Codes;

         END IF;
          IF i.new = 4  THEN
          dbms_output.put_line ( i.ES_Subject_Codes);
          v_code := substr(i.ES_Subject_Codes,4,3);
          dbms_output.put_line ( v_code);
          INSERT INTO TBL_TEMP_CoreTitle_NEW
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_CORETITLE where ES_Subject_Codes =  i.ES_Subject_Codes;

            v_code := substr(i.ES_Subject_Codes,17,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_CoreTitle_NEW
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_CORETITLE where ES_Subject_Codes =  i.ES_Subject_Codes;


            v_code := substr(i.ES_Subject_Codes,30,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_CoreTitle_NEW
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_CORETITLE where ES_Subject_Codes =  i.ES_Subject_Codes;

             v_code := substr(i.ES_Subject_Codes,43,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_CoreTitle_NEW
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_CORETITLE where ES_Subject_Codes =  i.ES_Subject_Codes;

                v_code := substr(i.ES_Subject_Codes,56,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_CoreTitle_NEW
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_CORETITLE where ES_Subject_Codes =  i.ES_Subject_Codes;


          END IF;

          ----

          IF i.new = 5  THEN
          dbms_output.put_line ( i.ES_Subject_Codes);
          v_code := substr(i.ES_Subject_Codes,4,3);
          dbms_output.put_line ( v_code);
          INSERT INTO TBL_TEMP_CoreTitle_NEW
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_CORETITLE where ES_Subject_Codes =  i.ES_Subject_Codes;

            v_code := substr(i.ES_Subject_Codes,17,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_CoreTitle_NEW
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_CORETITLE where ES_Subject_Codes =  i.ES_Subject_Codes;


            v_code := substr(i.ES_Subject_Codes,30,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_CoreTitle_NEW
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_CORETITLE where ES_Subject_Codes =  i.ES_Subject_Codes;

             v_code := substr(i.ES_Subject_Codes,43,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_CoreTitle_NEW
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_CORETITLE where ES_Subject_Codes =  i.ES_Subject_Codes;

                v_code := substr(i.ES_Subject_Codes,56,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_CoreTitle_NEW
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_CORETITLE where ES_Subject_Codes =  i.ES_Subject_Codes;

               v_code := substr(i.ES_Subject_Codes,69,3);
            dbms_output.put_line (v_code);
            INSERT INTO TBL_TEMP_CoreTitle_NEW
          SELECT 
           Tease ,
            TITLE,
            EDITION_NUMBER,
            SUBTITLE,
            Isbn13OrImpressionId ,
            AUTHOR_NAMES,--Need to work on this
             PRICE_USD ,
             PRICE_EUR,
            PRICE_GBP,
            PAGES_NUMBER,
            COPYRIGHT_YEAR,
            General_Description,
            KEY_FEATURES,
            DELTA_US_STATUS,
            v_code,
            ES_SUBJECT_CODE_LEVEL2,
            ES_SUBJECT_CODE_LEVEL1,
            ES_Subject_Codes,
            PMG,
            PMC
            from TBL_TEMP_CORETITLE where ES_Subject_Codes =  i.ES_Subject_Codes;
          END IF;
END LOOP;
COMMIT;

EXECUTE IMMEDIATE 'TRUNCATE   TABLE TBL_TEMP_CORETITLE';
  INSERT INTO TBL_TEMP_CORETITLE select * from TBL_TEMP_CORETITLE_NEW;
  commit;
  DELETE FROM 
    TBL_TEMP_CoreTitle A
 WHERE 
   a.rowid > 
    ANY (
      SELECT 
        B.rowid
     FROM 
        TBL_TEMP_CoreTitle B
     WHERE 
        A.ES_SUBJECT_CODE_LEVEL3 = B.ES_SUBJECT_CODE_LEVEL3
    AND A.Isbn13OrImpressionId = B.Isbn13OrImpressionId
            ); 
    commit;
commit;
END CreateDataCoreTitle;

END  PK_CORE_Hybrid_TITLE;
/
