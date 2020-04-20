CREATE PACKAGE DBOWNER.PK_PULL_PPM
  CREATE OR REPLACE EDITIONABLE PACKAGE "DBOWNER"."PK_PULL_PPM" AS
/*******************************************************************************
Author		: Chandrashekar Ganesh
Description	: Package to populate Staging Tables
Audit		:
	Version		Date		User		Description
	1		29-SEP-2015	Ganesh Shekar	Initial version
*******************************************************************************/

	PROCEDURE pull_source(p_source IN VARCHAR2, p_success IN OUT VARCHAR2);

	FUNCTION getNthOccurance(p_source_str		VARCHAR2
				,p_occurance		INTEGER)
		RETURN VARCHAR2;
END pk_pull_ppm;
 
/
CREATE OR REPLACE EDITIONABLE PACKAGE BODY "DBOWNER"."PK_PULL_PPM" AS
/*******************************************************************************
Author		: Chandrashekar Ganesh
Description	: Package to populate Staging Tables
Audit		:
	Version		Date		User		Description
	1		29-SEP-2015	Ganesh Shekar	Initial version
*******************************************************************************/

	g_pos			NUMBER := 0;
	v_message		VARCHAR2(3000) := '';
	v_err_code		NUMBER;
	v_query_count		NUMBER := 0;


	--// Create Parties from Authors
	PROCEDURE pull_parties(	 p_source	IN	VARCHAR2
				,p_success	IN OUT	VARCHAR2) IS
	BEGIN
		g_pos	:= 1;
		pk_main.log_time(p_source, 'PK_PULL_PPM', 'pull_parties', 'Creating Authors', g_pos, true);

		INSERT
		INTO	tbl_temp_parties
			(status
			,orig_system
			,orig_system_ref
			,party_type
			,dedupe_type
			,usr_created_date
			,usr_ref
			,usr_url_ref
			,usc_contact_title
			,usc_contact_firstname
			,usc_contact_lastname
			,usc_addr
			,usr_address2
			,usc_city
			,usc_state
			,usc_zip
			,usc_country
			,usc_dedupe_email
			,usc_institute_url
			,unique_inst_id
			,inst_name
			,run_time)
		SELECT	 'A'		status
			,'PPM'		orig_system
			,'PPM' || '_' || ID	orig_system_ref
			,'AUT'		party_type
			,'PER'		dedupe_type
			,source	usr_created_date
			,NOTES		usr_ref
			,MIDDLE_NAME	usr_url_ref
			,TITLE		usc_contact_title
			,FIRST_NAME	usc_contact_firstname
			,SURNAME	usc_contact_lastname
			,ADDR1		usc_addr
			,ADDR2		usr_address2
			,CITY		usc_city
			,STATE		usc_state
			,ZIP		usc_zip
			,COUNTRY	usc_country
			,EMAIL		usc_dedupe_email
			,SUBSTR(AWARDS,1,2000)		usc_institute_url
			,ORCID		unique_inst_id
			,SUBSTR(AFFILIATION,1,255)	inst_name
			,sysdate	run_time
		FROM	tbl_temp_ppm_authors;

		--// Saving Changes
		COMMIT;
		g_pos	:= 2;
		pk_main.log_time(p_source, 'PK_PULL_PPM', 'pull_parties', 'Completed Creating Authors', g_pos, true);

		--// Mark Comletion as SUCCESS
		p_success	:= 'Y';

	EXCEPTION
		WHEN OTHERS THEN
			pk_main.log_error(p_source, 'PK_PULL_PPM', 'pull_parties', SQLCODE, SQLERRM, 999);
			p_success	:= 'N';
			ROLLBACK;
			RAISE_APPLICATION_ERROR(-20001, 'Failed to Load pull_parties ');
	END pull_parties;


	--// Create Interests from Books
	PROCEDURE interest_books(p_source	IN	VARCHAR2
				,p_success	IN OUT	VARCHAR2) IS
		lPersons	INTEGER;
		lSQL		VARCHAR2(4000);
	BEGIN
		g_pos	:= 1;
		pk_main.log_time(p_source, 'PK_PULL_PPM', 'interest_books', 'Start Creating Books for Authors', g_pos, true);
		p_success	:= 'N';

		--// Create Books for the rest of Books
		lPersons	:= 1;

		WHILE lPersons <= 50 LOOP
			lSQL	:= 'INSERT
					INTO	tbl_temp_interests
						(orig_system_ref
						,orig_system
						,party_ref
						,create_date
						,update_date
						,interest_type
						,interest_value
						,interest_value_details
						,interest_section
						,interest_sub_section
						,alert_end_date
						,status) ';

			g_pos	:= lPersons;
			IF lPersons = 1 THEN
				pk_main.log_time(p_source, 'PK_PULL_PPM', 'interest_books', 'Creating Books for Primary Authors '||lPersons, g_pos, true);
			ELSE
				pk_main.log_time(p_source, 'PK_PULL_PPM', 'interest_books', 'Creating Books for Corresponding Authors '||lPersons, g_pos, true);
			END IF;

			IF lPersons = 1 THEN
				lSQL:=	lSQL ||
					'SELECT	/*+ APPEND */
						 ''PPM_BOK_'' || Person_'||TRIM(TO_CHAR(lPersons)) ||'|| ''_'' || Isbn13OrImpressionId
						,''PPM''
						,''PPM_'' || PERSON_'||TRIM(TO_CHAR(lPersons)) ||'
						,source
						,sysdate
						,''BOK''
						,''PPM_BOK_'' || ProjectNo
						,''Primary Author''
						,''PPM_BOK_'' || Isbn13OrImpressionId
						,ROLE_'||TRIM(TO_CHAR(lPersons)) ||'
						,NULL
						,''I''
					FROM	tbl_temp_ppm_products a
					WHERE	NOT Person_'||TRIM(TO_CHAR(lPersons)) ||' IS NULL
					AND NOT EXISTS
						(SELECT	1
						 FROM	tbl_temp_interests b
						 WHERE	b.orig_system		= ''PPM''
						 AND	b.orig_system_ref	= ''PPM_BOK_'' || a.Person_'||TRIM(TO_CHAR(lPersons)) ||'|| ''_'' || a.Isbn13OrImpressionId)';
			ELSE
				lSQL:=	lSQL ||
					'SELECT	/*+ APPEND */
						 ''PPM_BOK_'' || Person_'||TRIM(TO_CHAR(lPersons)) ||'|| ''_'' || Isbn13OrImpressionId
						,''PPM''
						,''PPM_'' || PERSON_'||TRIM(TO_CHAR(lPersons)) ||'
						,source
						,sysdate
						,''BOK''
						,''PPM_BOK_'' || ProjectNo
						,''Corresponding Author''
						,''PPM_BOK_'' || Isbn13OrImpressionId
						,ROLE_'||TRIM(TO_CHAR(lPersons)) ||'
						,NULL
						,''I''
					FROM	tbl_temp_ppm_products a
					WHERE	NOT Person_'||TRIM(TO_CHAR(lPersons)) ||'	IS NULL
					AND NOT EXISTS
						(SELECT	1
						 FROM	tbl_temp_interests b
						 WHERE	b.orig_system		= ''PPM''
						 AND	b.orig_system_ref	= ''PPM_BOK_'' || a.Person_'||TRIM(TO_CHAR(lPersons)) ||'|| ''_'' || a.Isbn13OrImpressionId)';
			END IF;

			--DBMS_OUTPUT.PUT_LINE(lSQL);

			EXECUTE IMMEDIATE
				lSQL;

			lPersons	:= lPersons + 1;

			--// Save Changes
			COMMIT;
		END LOOP;

		--// Mark Comletion as SUCCESS
		g_pos	:= 100;
		pk_main.log_time(p_source, 'PK_PULL_PPM', 'interest_books', 'Completed Creating Books for Authors', g_pos, true);
		p_success	:= 'Y';
	EXCEPTION
		WHEN OTHERS THEN
			pk_main.log_error(p_source, 'PK_PULL_PPM', 'interest_books', SQLCODE, SQLERRM, 999);
			p_success	:= 'N';
			ROLLBACK;
			RAISE_APPLICATION_ERROR(-20001, 'Failed to Load interest_books ');
	END interest_books;


	--// Create Interests from Publications
	PROCEDURE interest_publisher_edition(p_source	IN	VARCHAR2
					,p_success	IN OUT	VARCHAR2) IS
	BEGIN
		g_pos	:= 1;
		pk_main.log_time(p_source, 'PK_PULL_PPM', 'interest_publisher_edition', 'Start Creating Publisher Edition', g_pos, true);
		p_success	:= 'N';

		--// Create Publications
			INSERT
			INTO	tbl_temp_interests
				(orig_system_ref
				,orig_system
				,party_ref
				,create_date
				,update_date
				,interest_type
				,interest_value
				,interest_value_details
				,interest_sub_section
				,alert_end_date
				,status)
			SELECT	 'PPM_PUB_' || ISBN13ORIMPRESSIONID || '_' || PUBLISHER_EDITION	orig_system_ref
				,'PPM'					orig_system
				,'PPM_' || PUBLISHER_EDITION		party_ref
				,source				create_date
				,SYSDATE				update_date
				,'LNK'					interest_type
				,'PPM_BOK_' || ISBN13ORIMPRESSIONID	interest_value
				,'Publisher'				interest_value_details
				,NULL					interest_sub_section
				,NULL					alert_end_date
				,'I'					status
			FROM	tbl_temp_ppm_products
			WHERE	NOT publisher_edition	IS NULL;

		--// Save Changes
		COMMIT;

		--// Mark Comletion as SUCCESS
		g_pos	:= 100;
		pk_main.log_time(p_source, 'PK_PULL_PPM', 'interest_publisher_edition', 'Completed Creating Publisher Edition', g_pos, true);
		p_success	:= 'Y';
	EXCEPTION
		WHEN OTHERS THEN
			pk_main.log_error(p_source, 'PK_PULL_PPM', 'interest_publisher_edition', SQLCODE, SQLERRM, 999);
			p_success	:= 'N';
			ROLLBACK;
			RAISE_APPLICATION_ERROR(-20001, 'Failed to Load interest_publisher_edition ');
	END interest_publisher_edition;



	--// Create Interests from Aquisitions Editor
	PROCEDURE interest_acquisitions_editor(p_source	IN	VARCHAR2
					,p_success	IN OUT	VARCHAR2) IS
	BEGIN
		g_pos	:= 1;
		pk_main.log_time(p_source, 'PK_PULL_PPM', 'interest_acquisitions_editor', 'Start Creating Aquisitions Editor', g_pos, true);
		p_success	:= 'N';

		--// Create Publications
			INSERT
			INTO	tbl_temp_interests
				(orig_system_ref
				,orig_system
				,party_ref
				,create_date
				,update_date
				,interest_type
				,interest_value
				,interest_value_details
				,interest_sub_section
				,alert_end_date
				,status)
			SELECT	 'PPM_ACQ_' || ISBN13ORIMPRESSIONID || '_' || acquisitions_editor	orig_system_ref
				,'PPM'				orig_system
				,'PPM_' || acquisitions_editor	party_ref
				,source			create_date
				,sysdate			update_date
				,'LNK'				interest_type
				,'PPM_BOK_' || ISBN13ORIMPRESSIONID	interest_value
				,'Acquisition Editor'		interest_value_details
				,NULL					interest_sub_section
				,NULL					alert_end_date
				,'I'					status
			FROM	tbl_temp_ppm_products
			WHERE	NOT acquisitions_editor	IS NULL;

		--// Save Changes
		COMMIT;

		--// Mark Comletion as SUCCESS
		g_pos	:= 100;
		pk_main.log_time(p_source, 'PK_PULL_PPM', 'interest_acquisitions_editor', 'Completed Creating Aquisitions Editor', g_pos, true);
		p_success	:= 'Y';
	EXCEPTION
		WHEN OTHERS THEN
			pk_main.log_error(p_source, 'PK_PULL_PPM', 'interest_acquisitions_editor', SQLCODE, SQLERRM, 999);
			p_success	:= 'N';
			ROLLBACK;
			RAISE_APPLICATION_ERROR(-20001, 'Failed to Load interest_acquisitions_editor ');
	END interest_acquisitions_editor;



	--// Create Interests from Development Manager
	PROCEDURE interest_development_manager(p_source	IN	VARCHAR2
					,p_success	IN OUT	VARCHAR2) IS
	BEGIN
		g_pos	:= 1;
		pk_main.log_time(p_source, 'PK_PULL_PPM', 'interest_development_manager', 'Start Creating Development Manager', g_pos, true);
		p_success	:= 'N';

		--// Create Publications
			INSERT
			INTO	tbl_temp_interests
				(orig_system_ref
				,orig_system
				,party_ref
				,create_date
				,update_date
				,interest_type
				,interest_value
				,interest_value_details
				,interest_sub_section
				,alert_end_date
				,status)
			SELECT	 'PPM_DEV_' || ISBN13ORIMPRESSIONID || '_' || DEVELOPMENT_MANAGER	orig_system_ref
				,'PPM'				orig_system
				,'PPM_' || DEVELOPMENT_MANAGER	party_ref
				,source			create_date
				,sysdate			update_date
				,'LNK'				interest_type
				,'PPM_BOK_' || ISBN13ORIMPRESSIONID	interest_value
				,'Development Manager'		interest_value_details
				,NULL					interest_sub_section
				,NULL					alert_end_date
				,'I'					status
			FROM	tbl_temp_ppm_products
			WHERE	NOT development_manager	IS NULL;

		--// Save Changes
		COMMIT;

		--// Mark Comletion as SUCCESS
		g_pos	:= 100;
		pk_main.log_time(p_source, 'PK_PULL_PPM', 'interest_development_manager', 'Completed Creating Development Manager', g_pos, true);
		p_success	:= 'Y';
	EXCEPTION
		WHEN OTHERS THEN
			pk_main.log_error(p_source, 'PK_PULL_PPM', 'interest_development_manager', SQLCODE, SQLERRM, 999);
			p_success	:= 'N';
			ROLLBACK;
			RAISE_APPLICATION_ERROR(-20001, 'Failed to Load interest_development_manager ');
	END interest_development_manager;



	--// Create Interests from Project Manager
	PROCEDURE interest_project_manager(	 p_source	IN	VARCHAR2
					,p_success	IN OUT	VARCHAR2) IS
	BEGIN
		g_pos	:= 1;
		pk_main.log_time(p_source, 'PK_PULL_PPM', 'interest_project_manager', 'Start Creating Project Manager', g_pos, true);
		p_success	:= 'N';

		--// Create Publications
			INSERT
			INTO	tbl_temp_interests
				(orig_system_ref
				,orig_system
				,party_ref
				,create_date
				,update_date
				,interest_type
				,interest_value
				,interest_value_details
				,interest_sub_section
				,alert_end_date
				,status)
			SELECT	 'PPM_PRJ_' || ISBN13ORIMPRESSIONID || '_' || PROJECT_MANAGER          orig_system_ref
				,'PPM'				orig_system
				,'PPM_' || PROJECT_MANAGER	party_ref
				,source			create_date
				,sysdate			update_date
				,'LNK'				interest_type
				,'PPM_BOK_' || ISBN13ORIMPRESSIONID	interest_value
				,'Project Manager'		interest_value_details
				,NULL					interest_sub_section
				,NULL					alert_end_date
				,'I'					status
			FROM	tbl_temp_ppm_products
			WHERE	NOT project_manager	IS NULL;

		--// Save Changes
		COMMIT;

		--// Mark Comletion as SUCCESS
		g_pos	:= 100;
		pk_main.log_time(p_source, 'PK_PULL_PPM', 'interest_project_manager', 'Completed Creating Project Editor', g_pos, true);
		p_success	:= 'Y';
	EXCEPTION
		WHEN OTHERS THEN
			pk_main.log_error(p_source, 'PK_PULL_PPM', 'interest_project_manager', SQLCODE, SQLERRM, 999);
			p_success	:= 'N';
			ROLLBACK;
			RAISE_APPLICATION_ERROR(-20001, 'Failed to Load interest_project_manager ');
	END interest_project_manager;



	--// Create Interests from Marketing Manager
	PROCEDURE interest_marketing_manager(p_source	IN	VARCHAR2
					,p_success	IN OUT	VARCHAR2) IS
	BEGIN
		g_pos	:= 1;
		pk_main.log_time(p_source, 'PK_PULL_PPM', 'interest_marketing_manager', 'Start Creating Marketing Manager', g_pos, true);
		p_success	:= 'N';

		--// Create Publications
			INSERT
			INTO	tbl_temp_interests
				(orig_system_ref
				,orig_system
				,party_ref
				,create_date
				,update_date
				,interest_type
				,interest_value
				,interest_value_details
				,interest_sub_section
				,alert_end_date
				,status)
			SELECT	 'PPM_MKT_' || ISBN13ORIMPRESSIONID || '_' || MARKETING_MANAGER	orig_system_ref
				,'PPM'				orig_system
				,'PPM_' || MARKETING_MANAGER	party_ref
				,source			create_date
				,sysdate			update_date
				,'LNK'				interest_type
				,'PPM_BOK_' || ISBN13ORIMPRESSIONID	interest_value
				,'Marketing Manager'			interest_value_details
				,NULL					interest_sub_section
				,NULL					alert_end_date
				,'I'					status
			FROM	tbl_temp_ppm_products
			WHERE	NOT marketing_manager	IS NULL;

		--// Save Changes
		COMMIT;

		--// Mark Comletion as SUCCESS
		g_pos	:= 100;
		pk_main.log_time(p_source, 'PK_PULL_PPM', 'interest_marketing_manager', 'Completed Creating Marketing Editor', g_pos, true);
		p_success	:= 'Y';
	EXCEPTION
		WHEN OTHERS THEN
			pk_main.log_error(p_source, 'PK_PULL_PPM', 'interest_marketing_manager', SQLCODE, SQLERRM, 999);
			p_success	:= 'N';
			ROLLBACK;
			RAISE_APPLICATION_ERROR(-20001, 'Failed to Load interest_marketing_manager ');
	END interest_marketing_manager;


--// Populate Items
	--// Create Interests from Marketing Manager
	PROCEDURE item_books	(p_source	IN	VARCHAR2
				,p_success	IN OUT	VARCHAR2) IS
	BEGIN
		g_pos	:= 1;
		pk_main.log_time(p_source, 'PK_PULL_PPM', 'item_books', 'Start Creating Book Items', g_pos, true);
		p_success	:= 'N';

		--// Create Publications
			INSERT
			INTO	tbl_temp_items
				(orig_system_ref
				,orig_system
				,create_date
				,update_date
				,name
				,item_type
				,parent_ref
				,volume
				,issue
				,year
				,description
				,identifier
				,code
				,authors
				,show_status
				,site
				,publisher
				,type
				,sub_type
				,binding
				,medium
				,pmc_code
				,pmg_code
				,class
				,imprint
				,page_numbers
				,item_milestone
				,issue_milestone
				,issue_date
				,delivery_date
				,init_pub_date
				,last_pub_date
				,added_date
				,status)
			SELECT	 'PPM_BOK_' || ISBN13ORIMPRESSIONID	orig_system_ref
				,'PPM'					orig_system
				,source				create_date
				,sysdate				update_date
				,TITLE					name
				,'BOK'					item_type
				,PREVIOUS_EDITION_ISBN			parent_ref
				,VOLUME_NUMBER				volume
				,EDITION_NUMBER				issue
				,SUBSTR(E_ISBN13,1,10)			year
				,SUBTITLE				description
				,ISBN13ORIMPRESSIONID			identifier
				,COPS_PRODUCT_TYPE			code
				,NEXT_EDITION_ISBN			authors
				,PREVIOUS_EDITION_EISBN			show_status
				,ONLINE_VIA_SD				site
				,Delta_UK_Discount_Code			publisher
				,VERSION_TYPE				type
				,DELTA_US_STATUS ||
					'#' ||
					DELTA_UK_STATUS			sub_type
				,BINDING				binding
				,Emea_Trade_Cat_Year ||
					'#' ||
					Emea_Trade_Cat_Month		medium
				,PMC					pmc_code
				,PMG					pmg_code
				,SUBSTR(SERIES_NAME,1,30)		class
				,EXPORTTOOBS				imprint
				,EXPORTTOWEB				page_numbers
				,DELIVERY_STATUS			item_milestone
				,SUBSTR(MARKET_RESTRICTIONS,1,45)	issue_milestone
				,TO_DATE(ATI_DATE,'DD-MON-YYYY')	issue_date
				,TO_DATE(ATI_MAIL_DATE,'DD-MON-YYYY')	delivery_date
				,TO_DATE(PUBDATELE,'YYYYMMDD')		init_pub_date
				,TO_DATE(PUBDATEUS,'YYYYMMDD')		last_pub_date
				,TO_DATE(PUBDATEEMEA,'YYYYMMDD')	added_date
				,'I'					status
			FROM	tbl_temp_ppm_products;

		--// Save Changes
		COMMIT;

		--// Mark Comletion as SUCCESS
		g_pos	:= 100;
		pk_main.log_time(p_source, 'PK_PULL_PPM', 'item_books', 'Completed Creating Book Items', g_pos, true);
		p_success	:= 'Y';
	EXCEPTION
		WHEN OTHERS THEN
			pk_main.log_error(p_source, 'PK_PULL_PPM', 'item_books', SQLCODE, SQLERRM, 999);
			p_success	:= 'N';
			ROLLBACK;
			RAISE_APPLICATION_ERROR(-20001, 'Failed to Load Items ');
	END item_books;

--MARKAU 3738 - Added new procedure for making a full compare for PPM Feed
PROCEDURE ppm_deletes(	 p_source	IN	VARCHAR2
				,p_success	IN OUT	VARCHAR2) IS
	BEGIN
		g_pos	:= 1;
		pk_main.log_time(p_source, 'PK_PULL_PPM', 'PPM_DELETES', 'PARTIES DELETES', g_pos, true);

		--INSERT INTO TBL_TEMP_PARTIES FOR DELETING PPM PARTY RECORDS NOT PRESENT IN THE FULL FEED
      insert into dbowner.tbl_temp_parties (status, orig_system, orig_system_ref, party_type, run_time)
        select distinct 'D', orig_system, orig_party_ref, party_type, sysdate
        from dbowner.tbl_parties
        where orig_system = 'PPM' AND party_type = 'AUT'
        and orig_party_ref in
        (select orig_party_ref from dbowner.tbl_parties where orig_system = 'PPM'
         minus
         select orig_system_ref from dbowner.tbl_temp_parties where orig_system = 'PPM');

		--Saving Changes
		COMMIT;
		
		g_pos	:= 2;
		pk_main.log_time(p_source, 'PK_PULL_PPM', 'PPM_DELETES', 'INTERESTS DELETES', g_pos, true);
		
		--INSERT INTO TBL_TEMP_INTERESTS FOR DELETING PPM INTEREST RECORDS NOT PRESENT IN THE FULL FEED
	  insert into dbowner.tbl_temp_interests (orig_system_ref, orig_system, update_date, interest_type, status)
        select distinct orig_interest_ref, orig_system, sysdate, interest_type, 'D'
        from dbowner.tbl_interests
        where orig_system = 'PPM'
        and orig_interest_ref in
        (select orig_interest_ref from dbowner.tbl_interests where orig_system = 'PPM'
         minus
         select orig_system_ref from dbowner.tbl_temp_interests where orig_system = 'PPM');
		 
		--Saving Changes
		COMMIT;
		 
		g_pos	:= 3;
		pk_main.log_time(p_source, 'PK_PULL_PPM', 'PPM_DELETES', 'ITEM DELETES', g_pos, true);
		
		--INSERT INTO TBL_TEMP_ITEMS FOR DELETING PPM ITEM RECORDS NOT PRESENT IN THE FULL FEED
      insert into dbowner.tbl_temp_items (orig_system_ref, orig_system, update_date, item_type, status)
        select distinct orig_item_ref, orig_system, sysdate, item_type, 'D'
        from dbowner.tbl_items
        where orig_system = 'PPM'
        and orig_item_ref in
        (select orig_item_ref from dbowner.tbl_items where orig_system = 'PPM'
         minus
         select orig_system_ref from dbowner.tbl_temp_items where orig_system = 'PPM');
		 
		--Saving Changes
		COMMIT;
		 
		g_pos	:= 4;
		pk_main.log_time(p_source, 'PK_PULL_PPM', 'PPM_DELETES', 'ITEM SUBJECT DELETES', g_pos, true);
		
		--INSERT INTO TBL_TEMP_ITEM_SUBJECTS FOR DELETING PPM ITEM SUBJECT RECORDS NOT PRESENT IN THE FULL FEED       
      insert into dbowner.tbl_temp_item_subjects (status, orig_system, orig_system_ref, item_ref, update_date)
        select distinct 'D', orig_system, orig_subject_ref, item_ref, sysdate
        from dbowner.tbl_item_subjects
        where orig_system = 'PPM'
        and orig_subject_ref in
        (select orig_subject_ref from dbowner.tbl_item_subjects where orig_system = 'PPM'
         minus
         select orig_system_ref from dbowner.tbl_temp_item_subjects where orig_system = 'PPM');
		 
		--Saving Changes
		COMMIT;
		
		--// Mark Comletion as SUCCESS
		p_success	:= 'Y';

	EXCEPTION
		WHEN OTHERS THEN
			pk_main.log_error(p_source, 'PK_PULL_PPM', 'PPM_DELETES', SQLCODE, SQLERRM, 999);
			p_success	:= 'N';
			ROLLBACK;
			RAISE_APPLICATION_ERROR(-20001, 'Failed to Load ppm_deletes ');
	END ppm_deletes;

	FUNCTION getNthOccurance(p_source_str		VARCHAR2
				,p_occurance		INTEGER)
		RETURN VARCHAR2 IS

		lReturnString		VARCHAR2(100);
	BEGIN
		SELECT	TRIM(value_str)
		INTO	lReturnString
		FROM	(SELECT	 level	lvl
				,regexp_substr(p_source_str,'[^;]+', 1, level)	value_str
			 FROM	DUAL
				CONNECT BY
					regexp_substr(p_source_str, '[^;]+', 1, level) IS NOT NULL
			)
		WHERE	lvl	= p_occurance;

		RETURN lReturnString;
	END getNthOccurance;



--// Populate Item_Subjects
	--// Create Interests from Marketing Manager
	PROCEDURE item_subjects_books	(p_source	IN	VARCHAR2
					,p_success	IN OUT	VARCHAR2) IS
	BEGIN
		g_pos	:= 1;
		pk_main.log_time(p_source, 'PK_PULL_PPM', 'item_subjects_books', 'Start Creating Book Items', g_pos, true);
		p_success	:= 'N';

		--// Create Publications
			INSERT
			INTO	tbl_temp_item_subjects
				(status
				,orig_system
				,orig_system_ref
				,item_ref
				,create_date
				,update_date
				,subject_category_code
				,subject_group_code
				,subject_area_code
				,super_area_code)
			SELECT	/*+ APPEND */
				 'I'		status
				,'PPM'		orig_system
				,'PPM_PRO_' || ISBN13ORIMPRESSIONID || '_'
					|| getNthOccurance(es_subject_codes, 1)	orig_system_ref
				,'PPM_BOK_' || ISBN13ORIMPRESSIONID	item_ref
				,source		create_date
				,sysdate		update_date
				,substr(getNthOccurance(es_subject_codes, 1)
					,7,5)	subject_category_code
				,substr(getNthOccurance(es_subject_codes, 1)
					,4,3)	subject_group_code
				,substr(getNthOccurance(es_subject_codes, 1)
					,1,3)	subject_area_code
				,substr(getNthOccurance(es_subject_codes, 1)
					,1,1)	super_area_code
			FROM	tbl_temp_ppm_products
			WHERE	NOT getNthOccurance(es_subject_codes, 1) IS NULL
			UNION
			SELECT	 'I'		status
				,'PPM'		orig_system
				,'PPM_PRO_' || ISBN13ORIMPRESSIONID || '_'
					|| getNthOccurance(es_subject_codes, 2)	orig_system_ref
				,'PPM_BOK_' || ISBN13ORIMPRESSIONID	item_ref
				,source		create_date
				,sysdate		update_date
				,substr(getNthOccurance(es_subject_codes, 2)
					,7,5)	subject_category_code
				,substr(getNthOccurance(es_subject_codes, 2)
					,4,3)	subject_group_code
				,substr(getNthOccurance(es_subject_codes, 2)
					,1,3)	subject_area_code
				,substr(getNthOccurance(es_subject_codes, 2)
					,1,1)	super_area_code
			FROM	tbl_temp_ppm_products
			WHERE	NOT getNthOccurance(es_subject_codes, 2) IS NULL
			UNION
			SELECT	 'I'		status
				,'PPM'		orig_system
				,'PPM_PRO_' || ISBN13ORIMPRESSIONID || '_'
					|| getNthOccurance(es_subject_codes, 3)	orig_system_ref
				,'PPM_BOK_' || ISBN13ORIMPRESSIONID	item_ref
				,source		create_date
				,sysdate		update_date
				,substr(getNthOccurance(es_subject_codes, 3)
					,7,5)	subject_category_code
				,substr(getNthOccurance(es_subject_codes, 3)
					,4,3)	subject_group_code
				,substr(getNthOccurance(es_subject_codes, 3)
					,1,3)	subject_area_code
				,substr(getNthOccurance(es_subject_codes, 3)
					,1,1)	super_area_code
			FROM	tbl_temp_ppm_products
			WHERE	NOT getNthOccurance(es_subject_codes, 3) IS NULL;

		--// Save Changes
		COMMIT;

		--// Mark Comletion as SUCCESS
		g_pos	:= 100;
		pk_main.log_time(p_source, 'PK_PULL_PPM', 'item_subjects_books', 'Completed Creating Book Items', g_pos, true);
		p_success	:= 'Y';
	EXCEPTION
		WHEN OTHERS THEN
			pk_main.log_error(p_source, 'PK_PULL_PPM', 'item_subjects_books', SQLCODE, SQLERRM, 999);
			p_success	:= 'N';
			ROLLBACK;
			RAISE_APPLICATION_ERROR(-20001, 'Failed to Load Item Subjects ');
	END item_subjects_books;

--// Get Current Jobs Status
	FUNCTION get_pull_trns_status(p_source	IN	VARCHAR2)
		RETURN VARCHAR2
	IS
		l_pull_status		tbl_sources.pull_status%TYPE;
		l_transfer_status	tbl_sources.transfer_status%TYPE;

		l_return_status		tbl_sources.pull_status%TYPE;
		l_status_message		VARCHAR2(100);
	BEGIN
		SELECT	 CASE
				WHEN pull_status = 'S' AND transfer_status = 'S' THEN 'S'
				ELSE 'R'
			 END	return_status
			,CASE
				WHEN pull_status = 'S' AND transfer_status = 'S' THEN NULL
				WHEN pull_status = 'R' THEN 'PULL is Currently running'
				WHEN transfer_status = 'R' THEN 'TRANSFER is Currently running'
				ELSE 'JOB Status Unknown. Investigate'
			 END	job_status
		INTO	 l_return_status
			,l_status_message
		FROM	dbowner.tbl_sources
		WHERE source	= p_source;

		g_pos	:= 1;
		pk_main.log_time(p_source, 'PK_PULL_PPM', 'get_pull_trns_status', l_status_message, g_pos, true);

		IF NOT (l_return_status = 'S' OR l_return_status = 'R') THEN
			--// Log Unknow Status ERROR
			pk_main.log_error(p_source, 'PK_PULL_PPM', 'get_pull_trns_status', SQLCODE, l_status_message, 999);
		END IF;

		RETURN l_return_status;

	EXCEPTION
		WHEN OTHERS THEN
			pk_main.log_error(p_source, 'PK_PULL_PPM', 'get_pull_trns_status', SQLCODE, SQLERRM, 999);
			ROLLBACK;
	END get_pull_trns_status;


--// Main
	PROCEDURE pull_source(	 p_source	IN	VARCHAR2
				,p_success	IN OUT	VARCHAR2)
	IS
		v_source_time_db1	DATE := NULL;
		v_source_time_db2	DATE := NULL;
		v_total_queries		NUMBER;
		v_attempts		NUMBER;

		v_pull_status		tbl_sources.pull_status%TYPE;
		v_transfer_status	tbl_sources.transfer_status%TYPE;
	BEGIN
		pk_main.log_time(p_source, 'pk_pull_ppm', 'pull_source', 'Start', g_pos, true);
		p_success	:= 'N';

		UPDATE	dbowner.tbl_sources
		SET	 pull_start			= SYSDATE
			,pull_end			= NULL
			,pull_status			= 'R'
			,pull_error_msg 		= NULL
			,related_transfer_session	= NULL
		WHERE source				= p_source;

		--// Clean Temp Tables
		EXECUTE IMMEDIATE 'ALTER TABLE tbl_temp_parties TRUNCATE PARTITION par_ppm';
		EXECUTE IMMEDIATE 'ALTER TABLE tbl_temp_interests TRUNCATE PARTITION par_ppm';
		EXECUTE IMMEDIATE 'ALTER TABLE tbl_temp_items TRUNCATE PARTITION par_ppm';
		EXECUTE IMMEDIATE 'ALTER TABLE tbl_temp_item_subjects TRUNCATE PARTITION par_ppm';

		--// Perform Load
		pull_parties(p_source, p_success);
		interest_books(p_source, p_success);
		interest_publisher_edition(p_source, p_success);
		interest_acquisitions_editor(p_source, p_success);
		interest_development_manager(p_source, p_success);
		interest_project_manager(p_source, p_success);
		interest_marketing_manager(p_source, p_success);
		item_books(p_source, p_success);
		item_subjects_books(p_source, p_success);
    ----MARKAU 3738 - Calling the new procedure ppm_deletes for full compare
    ppm_deletes(p_source, p_success);

		--// Correct Country in Tbl_Temp_Parties
		FOR rec0 IN	(SELECT  rowid
					,iso_country_code
					,usc_country
				 FROM	dbowner.tbl_temp_parties
				 WHERE	orig_system	= 'PPM') LOOP
			FOR rec1 IN	(SELECT	 c.iso_code
						,c.country_name
					 FROM	 dbowner.tbl_countries		b
					 	,dbowner.tbl_iso_countries	c
					 WHERE	(upper(trim(rec0.usc_country)) = b.source_value OR
					 	 rec0.usc_country	= b.iso_code
					 	)
					 AND	b.iso_code	= c.iso_code) LOOP
				UPDATE	dbowner.tbl_temp_parties
				SET	 iso_country_code	= rec1.iso_code
					,usc_country		= rec1.country_name
					,doctored		= 'Y'
				WHERE	rowid			= rec0.rowid;
			END LOOP;
		END LOOP;

	/*	UPDATE	dbowner.tbl_temp_parties a
		SET	(a.iso_country_code, a.usc_country, a.doctored) =
				(SELECT	DISTINCT c.iso_code, c.country_name, 'Y'
				 FROM	dbowner.tbl_countries b, dbowner.tbl_iso_countries c
				 WHERE	a.usc_country	= b.iso_code
				 AND	b.iso_code	= c.iso_code
				)
		WHERE	a.orig_system	= p_source;
	*/

		--// Update End Time on Successful Completion
		IF p_success = 'Y' THEN
			UPDATE	dbowner.tbl_sources
			SET	 last_marker_db1= this_marker_db1
				,pull_end	= SYSDATE
				,pull_status	= 'S'
				,pull_error_msg	= NULL
			WHERE	source = p_source;
		ELSE
			ROLLBACK;
			pk_main.log_error(p_source, 'PK_PULL_PPM', 'pull_source', SQLCODE, 'FAILED To Complete Load', 999);

			UPDATE	dbowner.tbl_sources
			SET	 pull_end 	= SYSDATE
				,pull_status	= 'F'
				,pull_error_msg	= 'FAILED To Complete Load'
			WHERE	source = p_source;
		END IF;

		COMMIT;

	EXCEPTION
		WHEN OTHERS THEN
			pk_main.log_error(p_source, 'PK_PULL_PPM', 'pull_source', SQLCODE, SQLERRM, 999);
			p_success	:= 'N';
			ROLLBACK;
	END pull_source;

END pk_pull_ppm;
/
