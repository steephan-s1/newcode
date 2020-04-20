CREATE PACKAGE DBOWNER.PACK_APP_MONITORING
  CREATE OR REPLACE EDITIONABLE PACKAGE "DBOWNER"."PACK_APP_MONITORING" AS
/*******************************************************************************
Author		: Ganesh Shekar
Description	: Package to support Application Monitoring web application
Audit		:
	Version		Date	User			Description
		1	21-AUG-2015	Ganesh Shekar	Initial version
*******************************************************************************/

	PROCEDURE	get_admin_users
				(p_admin_users		OUT	SYS_REFCURSOR
				,p_error_message	OUT	VARCHAR2);

	PROCEDURE	merge_application
				(p_app_ref		IN	tbl_rmon_apps.app_ref%TYPE
				,p_owner_email		IN	tbl_rmon_apps.owner_email%TYPE
				,p_description		IN	tbl_rmon_apps.description%TYPE
				,p_send_notification	IN	tbl_rmon_apps.send_notification%TYPE
				,p_active_flag		IN	tbl_rmon_apps.active_flag%TYPE
				,p_error_message	OUT	VARCHAR2);

	PROCEDURE	merge_app_items
				(p_app_item_id		IN	tbl_rmon_app_items.app_item_id%TYPE
				,p_app_ref		IN	tbl_rmon_app_items.app_ref%TYPE
				,p_item_desc		IN	tbl_rmon_app_items.item_desc%TYPE
				,p_item_value		IN	tbl_rmon_app_items.item_value%TYPE
				,p_item_units		IN	tbl_rmon_app_items.item_units%TYPE
				,p_item_type		IN	tbl_rmon_app_items.item_type%TYPE
			--	,p_item_sub_type	IN	tbl_rmon_app_items.item_sub_type%TYPE
				,p_alert_notification	IN	tbl_rmon_app_items.alert_notification%TYPE
				,p_alert_max_value	IN	tbl_rmon_app_items.alert_max_value%TYPE
				,p_run_frequency_in_min	IN	tbl_rmon_app_items.run_frequency_in_min%TYPE
			--	,p_credentials		IN	tbl_rmon_app_items.credentials%TYPE
				,p_status		IN	tbl_rmon_app_items.status%TYPE
				,p_active_flag		IN	tbl_rmon_app_items.active_flag%TYPE
				,p_error_message	OUT	VARCHAR2);

	PROCEDURE	get_applications
				(p_active_flag		IN	tbl_rmon_apps.active_flag%TYPE	DEFAULT	NULL
				,p_applications		OUT	SYS_REFCURSOR
				,p_error_message	OUT	VARCHAR2);

	PROCEDURE	get_app_items
				(p_app_ref		IN	tbl_rmon_apps.app_ref%TYPE
				,p_item_types		OUT	SYS_REFCURSOR
				,p_app_items		OUT	SYS_REFCURSOR
				,p_error_message	OUT	VARCHAR2);

	PROCEDURE	get_app_items_list
				(p_app_ref		IN	tbl_rmon_apps.app_ref%TYPE
				,p_error_message	OUT	VARCHAR2
				,p_app_items		OUT	SYS_REFCURSOR);

	PROCEDURE	upd_app_item_response
				(p_app_ref		IN	tbl_rmon_apps.app_ref%TYPE
				,p_app_item_id		IN	tbl_rmon_app_items.app_item_id%TYPE
				,p_item_value		IN	tbl_rmon_app_items.item_value%TYPE
				,p_error_message	OUT	VARCHAR2);

	PROCEDURE	upd_app_status
				(p_app_ref		IN	tbl_rmon_apps.app_ref%TYPE
				,p_error_message	OUT	VARCHAR2);

	PROCEDURE	upd_app_item_status
				(p_app_ref		IN	tbl_rmon_apps.app_ref%TYPE
				,p_error_message	OUT	VARCHAR2
				,p_alert_app_items	OUT	SYS_REFCURSOR);

	PROCEDURE	confirm_alert
				(p_app_ref		IN	tbl_rmon_apps.app_ref%TYPE
				,p_app_item_id		IN	tbl_rmon_app_items.app_item_id%TYPE
				,p_error_message	OUT	VARCHAR2);

	PROCEDURE	issue_resolved
				(p_app_ref		IN	tbl_rmon_apps.app_ref%TYPE
				,p_app_item_id		IN	tbl_rmon_app_items.app_item_id%TYPE
				,p_error_message	OUT	VARCHAR2);

	FUNCTION	get_last_party_date
				(p_source		IN	tbl_sources.source%TYPE)
		RETURN VARCHAR2;

	PROCEDURE	updDataTransStatus
				(p_app_ref		IN	tbl_rmon_apps.app_ref%TYPE
				,p_error_message	OUT	VARCHAR2);

	PROCEDURE	updDataTransStatus
				(p_app_ref		IN	tbl_rmon_apps.app_ref%TYPE
				,p_source		IN	tbl_sources.source%TYPE
				,p_error_message	OUT	VARCHAR2);

	PROCEDURE	get_ADOC_stats
				(p_adocc_webApp_status	OUT	SYS_REFCURSOR
				,p_adocc_webApp_agg	OUT	SYS_REFCURSOR
				,p_adocc_wrkFlow_status	OUT	SYS_REFCURSOR
				,p_adocc_wrkFlow_agg	OUT	SYS_REFCURSOR
				,p_error_message	OUT	VARCHAR2);
END pack_app_monitoring;
 
/
CREATE OR REPLACE EDITIONABLE PACKAGE BODY "DBOWNER"."PACK_APP_MONITORING" AS
/*******************************************************************************
Author		: Ganesh Shekar
Description	: Package to support Application Monitoring web application
Audit		:
	Version		Date	User			Description
		1	21-AUG-2015	Ganesh Shekar	Initial version
*******************************************************************************/

	PROCEDURE	get_admin_users
				(p_admin_users		OUT	SYS_REFCURSOR
				,p_error_message	OUT	VARCHAR2)
	IS
	BEGIN
		OPEN p_admin_users FOR
			SELECT	*
			FROM	tbl_rmon_admin_users;

		p_error_message	:= SQLERRM;
	EXCEPTION
		WHEN OTHERS THEN
			p_error_message	:= SQLERRM;
			ROLLBACK;
	END get_admin_users;


	PROCEDURE	merge_application
				(p_app_ref		IN	tbl_rmon_apps.app_ref%TYPE
				,p_owner_email		IN	tbl_rmon_apps.owner_email%TYPE
				,p_description		IN	tbl_rmon_apps.description%TYPE
				,p_send_notification	IN	tbl_rmon_apps.send_notification%TYPE
				,p_active_flag		IN	tbl_rmon_apps.active_flag%TYPE
				,p_error_message	OUT	VARCHAR2)
	IS
	BEGIN
		MERGE
		INTO	tbl_rmon_apps	t
		USING	(select	 p_app_ref		app_ref
				,p_owner_email		owner_email
				,p_description		description
				,p_send_notification	send_notification
				,p_active_flag		active_flag
			 FROM	DUAL)	s
		ON	(t.app_ref	= s.app_ref)
		WHEN MATCHED THEN
			UPDATE
			SET	 t.owner_email		= s.owner_email
				,t.description		= s.description
				,t.send_notification	= s.send_notification
				,t.active_flag		= s.active_flag
		WHEN NOT MATCHED THEN
			INSERT	(app_ref
				,owner_email
				,description
				,last_checked
				,status
				,send_notification
				,active_flag
				,create_date)
			VALUES	(s.app_ref
				,s.owner_email
				,s.description
				,SYSDATE
				,'0'
				,s.send_notification
				,s.active_flag
				,SYSDATE);

		--// Save Changes
		COMMIT;
		p_error_message	:= SQLERRM;
	EXCEPTION
		WHEN OTHERS THEN
			p_error_message	:= SQLERRM;
			ROLLBACK;
	END merge_application;

	PROCEDURE	merge_app_items
				(p_app_item_id		IN	tbl_rmon_app_items.app_item_id%TYPE
				,p_app_ref		IN	tbl_rmon_app_items.app_ref%TYPE
				,p_item_desc		IN	tbl_rmon_app_items.item_desc%TYPE
				,p_item_value		IN	tbl_rmon_app_items.item_value%TYPE
				,p_item_units		IN	tbl_rmon_app_items.item_units%TYPE
				,p_item_type		IN	tbl_rmon_app_items.item_type%TYPE
			--	,p_item_sub_type	IN	tbl_rmon_app_items.item_sub_type%TYPE
				,p_alert_notification	IN	tbl_rmon_app_items.alert_notification%TYPE
				,p_alert_max_value	IN	tbl_rmon_app_items.alert_max_value%TYPE
				,p_run_frequency_in_min	IN	tbl_rmon_app_items.run_frequency_in_min%TYPE
			--	,p_credentials		IN	tbl_rmon_app_items.credentials%TYPE
				,p_status		IN	tbl_rmon_app_items.status%TYPE
				,p_active_flag		IN	tbl_rmon_app_items.active_flag%TYPE
				,p_error_message	OUT	VARCHAR2)
	IS
	BEGIN
		MERGE
		INTO	tbl_rmon_app_items	t
		USING	(select	 p_app_item_id		app_item_id
				,p_app_ref		app_ref
				,p_item_desc		item_desc
				,p_item_value		item_value
				,p_item_units		item_units
				,p_item_type		item_type
			--	,p_item_sub_type	item_sub_type
				,p_alert_notification	alert_notification
				,p_alert_max_value	alert_max_value
				,p_run_frequency_in_min	run_frequency_in_min
			--	,p_credentials		credentials
				,p_status		status
				,p_active_flag		active_flag
			 FROM	DUAL)	s
		ON	(t.app_ref	= s.app_ref	AND
			 t.app_item_id	= s.app_item_id)
		WHEN MATCHED THEN
			UPDATE
			SET	 t.item_desc		= s.item_desc
				,t.item_value		= s.item_value
				,t.item_units		= s.item_units
				,t.item_type		= s.item_type
			--	,t.item_sub_type	= s.item_sub_type
				,t.alert_notification	= s.alert_notification
				,t.alert_max_value	= s.alert_max_value
				,t.run_frequency_in_min	= s.run_frequency_in_min
			--	,t.credentials		= s.credentials
				,t.status		= s.status
				,t.active_flag		= s.active_flag
				,t.update_date		= SYSDATE
		WHEN NOT MATCHED THEN
			INSERT	(app_ref
				,app_item_id
				,item_desc
				,item_value
				,item_units
				,item_type
				,item_sub_type
				,alert_notification
				,alert_max_value
				,run_frequency_in_min
			--	,credentials
				,status
				,active_flag
				,create_date)
			VALUES	(s.app_ref
				,seq_app_item.NEXTVAL
				,s.item_desc
				,s.item_value
				,s.item_units
				,s.item_type
				,s.item_type		--// same as p_item_type
				,s.alert_notification
				,s.alert_max_value
				,s.run_frequency_in_min
			--	,s.credentials
				,s.status
				,s.active_flag
				,SYSDATE);

		--// Save Changes
		COMMIT;
		p_error_message	:= SQLERRM;

	EXCEPTION
		WHEN OTHERS THEN
			-- p_error_message	:= p_app_item_id || ':' || p_alert_max_value || ':' || p_run_frequency_in_min;
			p_error_message	:= SQLERRM;
			ROLLBACK;
	END merge_app_items;


	PROCEDURE	get_applications
				(p_active_flag		IN	tbl_rmon_apps.active_flag%TYPE	DEFAULT	NULL
				,p_applications		OUT	SYS_REFCURSOR
				,p_error_message	OUT	VARCHAR2)
	IS
	BEGIN
		OPEN	p_applications FOR
			SELECT	 app_ref
				,description
				,TO_CHAR(last_checked, 'DD-MON-YYYY HH24:MI')	last_checked
				,status
				,comments
				,owner_email
				,send_notification
				,active_flag
			FROM	tbl_rmon_apps
			WHERE	active_flag	= NVL(p_active_flag, active_flag);

		p_error_message	:= SQLERRM;
	EXCEPTION
		WHEN OTHERS THEN
			p_error_message	:= SQLERRM;
			ROLLBACK;
	END get_applications;


	PROCEDURE	get_app_items
				(p_app_ref		IN	tbl_rmon_apps.app_ref%TYPE
				,p_item_types		OUT	SYS_REFCURSOR
				,p_app_items		OUT	SYS_REFCURSOR
				,p_error_message	OUT	VARCHAR2)
	IS
	BEGIN
		--pk_main.log_error('WWW', 'pack_app_monitoring', 'get_app_items', SQLCODE, p_app_ref, 1);

		OPEN	p_item_types FOR
			SELECT	DISTINCT ait.item_type
					,ity.item_type_desc
					,ity.item_type_order
			FROM	 tbl_rmon_app_items	ait
				,tbl_rmon_item_types	ity
			WHERE	ait.app_ref		= p_app_ref
			AND	ait.item_type		IN ('WWW', 'DB', 'DTF')
			AND NOT	(ait.item_value		= '0'    AND
				 ait.alert_max_value	= 0)
			AND	ity.item_type		= ait.item_type
			ORDER BY
				ity.item_type_order;

		OPEN	p_app_items FOR
			SELECT	 ait.app_item_id
				,ait.item_desc
				,ait.item_value||' '||ait.item_units	item_value_units
				,CASE
					WHEN status = 0 OR (status is null and alert_notification='N') THEN
						'#C2F0C2'
					ELSE	'#FF8080'
				 END	bgcolor
				,CASE
					WHEN status = 0 OR (status is null and alert_notification='N') THEN
						'<font bgcolor="#rrggbb">Service Available</font>'
					ELSE	'<font bgcolor="red">Service Unavailable</font>'
				 END	app_item_status
				,CASE
					WHEN status = 0 OR (status is null and alert_notification='N') THEN
						'<font bgcolor="#rrggbb">Service Avaliable</font>'
					ELSE	'<font bgcolor="red">Service Unavailable</font>'
				 END	app_item_status
				,'Actual = '||ait.item_value||' '||ait.item_units ||' Threshold = '||alert_max_value
				 	app_item_tooltip
				,ait.item_value
				,ait.item_units
				,ait.alert_max_value
				,ait.item_type
				,ait.alert_notification
				,ait.run_frequency_in_min
				,ait.credentials
				,ait.status
				,CASE
					WHEN ait.status = 0 AND InStr(ait.item_desc,'In Progress') > 0 THEN 'In Progress'
					WHEN ait.status = 0 THEN 'Successful'
					WHEN ait.status = 1 THEN 'Failed'
					WHEN ait.status = 2 THEN 'Alert Confirmed'
					ELSE 'UnKnown'
				 END	status_desc
				,ity.item_type_desc
				,ait.item_sub_type
				,ait.status
				,ait.active_flag
				,TO_CHAR(ait.update_date, 'DD-MON-YYYY HH24:MI:SS')	update_date
			FROM	 tbl_rmon_app_items	ait
				,tbl_rmon_item_types	ity
			WHERE	ait.app_ref		= p_app_ref
			AND	ait.item_type		IN ('WWW', 'DB', 'DTF')
			AND NOT (ait.item_value		= '0'	AND
				 ait.alert_max_value	= 0)
			AND	ity.item_type		= ait.item_type
			AND	ait.active_flag		= 'Y'
			ORDER BY
				ity.item_type_order;

		p_error_message	:= SQLERRM;
	EXCEPTION
		WHEN OTHERS THEN
			p_error_message	:= SQLERRM;
			ROLLBACK;
	END get_app_items;


	PROCEDURE	get_app_items_list
				(p_app_ref		IN	tbl_rmon_apps.app_ref%TYPE
				,p_error_message	OUT	VARCHAR2
				,p_app_items		OUT	SYS_REFCURSOR)
	IS
	BEGIN
		OPEN p_app_items FOR
			SELECT	 app_ref
				,app_item_id
				,item_type
				,item_desc
				,replace(substr(item_desc, (instr(item_desc, '(') + 1), 200), ')')	app_item_url
				,credentials
				,alert_max_value
			FROM	tbl_rmon_app_items
			WHERE	app_ref		= p_app_ref
			AND	(NVL(update_date,TRUNC(SYSDATE)) + ((run_frequency_in_min - 0.1)/1440)) <= SYSDATE
			AND	active_flag	= 'Y'
			ORDER BY app_ref
				,app_item_id;

		p_error_message	:= SQLERRM;
	EXCEPTION
		WHEN OTHERS THEN
			p_error_message	:= SQLERRM;
			ROLLBACK;
	END get_app_items_list;


	PROCEDURE	upd_app_item_response
				(p_app_ref		IN	tbl_rmon_apps.app_ref%TYPE
				,p_app_item_id		IN	tbl_rmon_app_items.app_item_id%TYPE
				,p_item_value		IN	tbl_rmon_app_items.item_value%TYPE
				,p_error_message	OUT	VARCHAR2)
	IS
	BEGIN
		UPDATE	tbl_rmon_app_items
		SET	 item_value	= p_item_value
			,update_date	= SYSDATE
		WHERE	app_ref		= p_app_ref
		AND	app_item_id	= p_app_item_id;

		p_error_message	:= SQLERRM;
	EXCEPTION
		WHEN OTHERS THEN
			p_error_message	:= SQLERRM;
			ROLLBACK;
	END upd_app_item_response;


	PROCEDURE	upd_app_status
				(p_app_ref		IN	tbl_rmon_apps.app_ref%TYPE
				,p_error_message	OUT	VARCHAR2)
	IS
	BEGIN
		UPDATE	tbl_rmon_apps
		SET	 status		=
				(SELECT	DECODE(status_failure
						,0	,DECODE(status_investigate
								,0	,0
								,1)
						,1)
				 FROM	(SELECT	 SUM(DECODE(status, 0, 1, 0))	status_success
						,SUM(DECODE(status, 1, 1, 0))	status_failure
						,SUM(DECODE(status, 2, 1, 0))	status_investigate
					 FROM   tbl_rmon_app_items
					 WHERE	app_ref		= p_app_ref
					 AND	item_type	IN ('WWW', 'DB')
					 AND	active_flag	= 'Y'
					)
				)
			,comments	=
				(SELECT	DECODE(status_failure
						,0	,DECODE(status_investigate
								,0	,NULL
								,'Pending Investigation')
						,'Pending Investigation')
				 FROM	(SELECT	 SUM(DECODE(status, 0, 1, 0))	status_success
						,SUM(DECODE(status, 1, 1, 0))	status_failure
						,SUM(DECODE(status, 2, 1, 0))	status_investigate
					 FROM   tbl_rmon_app_items
					 WHERE	app_ref		= p_app_ref
					 AND	item_type	IN ('WWW', 'DB')
					 AND	active_flag	= 'Y'
					)
				)
			,last_checked	= SYSDATE
		WHERE	app_ref		= p_app_ref;

		p_error_message	:= SQLERRM;
	EXCEPTION
		WHEN OTHERS THEN
			p_error_message	:= SQLERRM;
			ROLLBACK;
	END upd_app_status;



	PROCEDURE	upd_app_item_status
				(p_app_ref		IN	tbl_rmon_apps.app_ref%TYPE
				,p_error_message	OUT	VARCHAR2
				,p_alert_app_items	OUT	SYS_REFCURSOR)
	IS
	BEGIN
		--// Update Application Item Status
		UPDATE	tbl_rmon_app_items
		SET	status	= CASE
					WHEN item_value = 'TRUE' THEN 0
					WHEN item_value = 'FALSE' THEN 1
					WHEN TO_NUMBER(item_value) < alert_max_value THEN 0
					WHEN TO_NUMBER(item_value) = -1 THEN 1
					WHEN item_value >= alert_max_value THEN 1
					ELSE 2
				  END
		WHERE	app_ref			= p_app_ref
		AND	alert_notification	= 'Y'
		AND	(update_date +(2/1440))	> SYSDATE;

	--	AND	(item_value = 'TRUE'	AND status = 1	OR
	--		 item_value = 'FALSE'	AND status = 0	OR
	--		 TO_NUMBER(item_value) < alert_max_value	AND status = 1	OR
	--		 TO_NUMBER(item_value) = -1	AND status = 0	OR
	--		 item_value >= alert_max_value	AND status = 0);

		IF SQL%ROWCOUNT > 0 THEN
			--// Update Application Item Status
			UPDATE	tbl_rmon_apps
			SET	 status		=
					(SELECT	DECODE(status_failure
							,0	,DECODE(status_investigate
									,0	,0
									,1)
							,1)
					 FROM	(SELECT	 SUM(DECODE(status, 0, 1, 0))	status_success
							,SUM(DECODE(status, 1, 1, 0))	status_failure
							,SUM(DECODE(status, 2, 1, 0))	status_investigate
						 FROM   tbl_rmon_app_items
						 WHERE	app_ref		= p_app_ref
						 AND	item_type	IN ('WWW', 'DB')
						 AND	active_flag	= 'Y'
						)
					)
				,comments	=
					(SELECT	DECODE(status_failure
							,0	,DECODE(status_investigate
									,0	,NULL
								--	,0	,DECODE(p_app_ref, 'ADOC', 'Experiencing Intermittent Network failures', NULL)
									,'Pending Investigation')
							,'Pending Investigation')
					 FROM	(SELECT	 SUM(DECODE(status, 0, 1, 0))	status_success
							,SUM(DECODE(status, 1, 1, 0))	status_failure
							,SUM(DECODE(status, 2, 1, 0))	status_investigate
						 FROM   tbl_rmon_app_items
						 WHERE	app_ref		= p_app_ref
						 AND	item_type	IN ('WWW', 'DB', 'DTF')
						 AND	active_flag	= 'Y'
						)
					)
				,last_checked	= SYSDATE
			WHERE	app_ref		= p_app_ref;
		END IF;

		OPEN p_alert_app_items FOR
			SELECT	 app_ref
				,item_desc
				,CASE
					WHEN item_desc LIKE '%Investigate%' THEN
						'Investigate'
					WHEN item_desc LIKE '%Being investigated%' THEN
						'Being investigated'
					ELSE 'Others'
				 END feed_status
				,item_type
				,item_value||' '||item_units	response_time
				,alert_max_value
				,TO_CHAR(update_date, 'DD-MON-YYYY HH24:MI:SS')	update_date_time
				,status
				,CASE
					WHEN status = 1 THEN 'Failed'
					ELSE 'Investigate'
				 END status_description
			FROM	tbl_rmon_app_items
			WHERE	app_ref		= p_app_ref
			AND	item_type	IN ('WWW', 'DB', 'DTF')
			AND	active_flag	= 'Y'
			AND	((NOT status	= 0)			OR
				-- item_desc	LIKE '%In Progress%'	OR
				 item_desc	LIKE '%Investigate%'	OR
				 item_desc	LIKE '%Being investigated%')
			AND	(NVL(update_date,TRUNC(SYSDATE)) + ((run_frequency_in_min - 0.1)/1440)) <= SYSDATE
			ORDER BY
				 status
				,app_item_id;

		COMMIT;

		p_error_message	:= SQLERRM;
	EXCEPTION
		WHEN OTHERS THEN
			p_error_message	:= SQLERRM;
			ROLLBACK;
	END upd_app_item_status;


	PROCEDURE	confirm_alert
				(p_app_ref		IN	tbl_rmon_apps.app_ref%TYPE
				,p_app_item_id		IN	tbl_rmon_app_items.app_item_id%TYPE
				,p_error_message	OUT	VARCHAR2)
	IS
	BEGIN
		UPDATE	tbl_rmon_app_items
		SET	 status		= 2
			,update_date	= SYSDATE
		WHERE	app_item_id	= p_app_item_id
		AND	app_ref		= p_app_ref;

		UPDATE	tbl_rmon_apps
		SET	 comments	= 'Under Investigation'
		WHERE	app_ref		= p_app_ref;

		p_error_message	:= SQLERRM;
	EXCEPTION
		WHEN OTHERS THEN
			p_error_message	:= SQLERRM;
			ROLLBACK;
	END confirm_alert;


	PROCEDURE	issue_resolved
				(p_app_ref		IN	tbl_rmon_apps.app_ref%TYPE
				,p_app_item_id		IN	tbl_rmon_app_items.app_item_id%TYPE
				,p_error_message	OUT	VARCHAR2)
	IS
	BEGIN
		UPDATE	tbl_rmon_app_items
		SET	 status		= '0'
			,update_date	= SYSDATE
		WHERE	app_item_id	= p_app_item_id
		AND	app_ref		= p_app_ref;

		UPDATE	tbl_rmon_apps
		SET	 comments	= NULL
			,status		= 0
		WHERE	app_ref		= p_app_ref;

		p_error_message	:= SQLERRM;
	EXCEPTION
		WHEN OTHERS THEN
			p_error_message	:= SQLERRM;
			ROLLBACK;
	END issue_resolved;


	FUNCTION	get_last_party_date
				(p_source		IN	tbl_sources.source%TYPE)
		RETURN VARCHAR2
	IS
		CURSOR	c_last_party
		IS
		SELECT	/*+ PARALLEL(pty,4) */
			 max(capri_create_date) create_date
			,max(capri_update_date) update_date
		FROM	tbl_parties	pt
		WHERE	orig_system 	= p_source;

		lReturn		VARCHAR2(30)	:= NULL;
	BEGIN
		FOR rec IN c_last_party LOOP
			lReturn:= TO_CHAR(GREATEST(rec.create_date, rec.update_date), 'DD-MON-YYYY HH24:MI');
		END LOOP;

		DBMS_OUTPUT.PUT_LINE('Returning: '||lReturn);
		RETURN (lReturn);
	EXCEPTION
		WHEN OTHERS THEN
		DBMS_OUTPUT.PUT_LINE(SQLERRM);
			RETURN NULL;
	END get_last_party_date;


	PROCEDURE	updDataTransStatus
				(p_app_ref		IN	tbl_rmon_apps.app_ref%TYPE
				,p_error_message	OUT	VARCHAR2)
	IS
	BEGIN
		MERGE
		INTO	tbl_rmon_app_items	t
		USING	(SELECT	 source
				,description
				,CASE
					WHEN load_status ||pull_status ||transfer_status LIKE '%E%' THEN '1'	--  'Failure Reported'
					WHEN load_status ||pull_status ||transfer_status LIKE '%F%' THEN '1'	--  'Failure Reported'
					WHEN load_status ||pull_status ||transfer_status LIKE '%R%' THEN '0'	--  'In Progress'
					WHEN load_status ||pull_status ||transfer_status = 'SSS' THEN '0'	--  'Successful'
				 END	Source_Status
				/*
				,CASE
					WHEN load_status = 'E' THEN '1'	-- 'Failure Reported'
					WHEN load_status = 'F' THEN '1'	-- 'Failure Reported'
					WHEN load_status = 'R' THEN '2'	-- 'In Progress'
					WHEN load_status = 'S' THEN
						CASE
							WHEN pull_status = 'E' THEN '1'	-- 'Failure Reported'
							WHEN pull_status = 'F' THEN '1'	-- 'Failure Reported'
							WHEN pull_status = 'R' THEN '2'	-- 'In Progress'
							WHEN pull_status = 'S' THEN
								CASE
									WHEN transfer_status = 'E' THEN '1'	-- 'Failure Reported'
									WHEN transfer_status = 'F' THEN '1'	-- 'Failure Reported'
									WHEN transfer_status = 'R' THEN '0'	-- 'In Progress'
									WHEN transfer_status = 'S' THEN '0'	-- 'Successful'
								END
						END
				 END	Source_Status
				,CASE
					WHEN trunc((transfer_end - this_marker_db1)) <= 144 THEN
						'Buss Date: '|| TO_CHAR(this_marker_db1) ||';  Last Party Update: '|| pack_app_monitoring.get_last_party_date(source)
					ELSE
						'Buss Date: '|| TO_CHAR(transfer_end) ||';  Last Party Update: '|| pack_app_monitoring.get_last_party_date(source)
				 END ||
				*/
				,'Transfer Date: '|| TO_CHAR(transfer_end,'DD-MON-YYYY') ||';  Last Party Update: '|| pack_app_monitoring.get_last_party_date(source)
				 ||
				 CASE
					WHEN load_status ||pull_status ||transfer_status = 'SSS' AND
							trunc((transfer_end - this_marker_db1)) <= 144 THEN
						''
					WHEN load_status ||pull_status ||transfer_status = 'SSS' AND
							trunc((transfer_end - this_marker_db1)) > 144 THEN
						'  (Investigate)'
					WHEN load_status ||pull_status ||transfer_status LIKE '%R%' THEN
						'  (In Progress)'
					ELSE
						'  (Being investigated)'
				 END	status_desc
			 FROM	tbl_sources
			 WHERE	source	IN ('AE2', 'COM', 'CRM', 'CRS', 'DEL', 'DOM', 'EES', 'ELB', 'EST', 'JCI', 'NLN', 'PPV', 'PTS', 'SD', 'SIS', 'TEC')
			)	s
		ON	(t.app_ref	= p_app_ref		AND
			 t.item_type	= 'DTF'			AND
			 t.credentials	= s.source)
		WHEN MATCHED THEN
			UPDATE
			SET	 t.item_desc	= '<pre>' ||
							CASE
								WHEN s.source = s.description THEN ''
								ELSE s.source ||': '
							END ||
							s.description	||
							'<br> - '	||
							s.status_desc
				,t.status	= s.Source_Status
				,t.update_date	= SYSDATE
		WHEN NOT MATCHED THEN
			INSERT	(app_ref
				,item_desc
				,item_value
				,item_type
				,item_sub_type
				,alert_notification
				,alert_max_value
				,run_frequency_in_min
				,create_date
				,update_date
				,app_item_id
				,credentials
				,status
				,active_flag)
			VALUES	(p_app_ref
				,RPAD(s.description,35)	||
					'(' ||s.source ||') - '	||
					s.status_desc
				,'TRUE'
				,'DTF'
				,'DB'
				,'Y'
				,0
				,240
				,SYSDATE
				,SYSDATE
				,seq_app_item.NEXTVAL
				,s.source
				,s.Source_Status
				,'Y');

	EXCEPTION
		WHEN OTHERS THEN
			p_error_message	:= SQLERRM;
			ROLLBACK;
	END updDataTransStatus;


	PROCEDURE	updDataTransStatus
				(p_app_ref		IN	tbl_rmon_apps.app_ref%TYPE
				,p_source		IN	tbl_sources.source%TYPE
				,p_error_message	OUT	VARCHAR2)
	IS
	BEGIN
		MERGE
		INTO	tbl_rmon_app_items	t
		USING	(SELECT	 source
				,description
				,CASE
					WHEN load_status ||pull_status ||transfer_status LIKE '%E%' THEN '1'	--  'Failure Reported'
					WHEN load_status ||pull_status ||transfer_status LIKE '%F%' THEN '1'	--  'Failure Reported'
					WHEN load_status ||pull_status ||transfer_status LIKE '%R%' THEN '0'	--  'In Progress'
					WHEN load_status ||pull_status ||transfer_status = 'SSS' THEN '0'	--  'Successful'
				 END	Source_Status
				/*
				,CASE
					WHEN load_status = 'E' THEN '1'	-- 'Failure Reported'
					WHEN load_status = 'F' THEN '1'	-- 'Failure Reported'
					WHEN load_status = 'R' THEN '2'	-- 'In Progress'
					WHEN load_status = 'S' THEN
						CASE
							WHEN pull_status = 'E' THEN '1'	-- 'Failure Reported'
							WHEN pull_status = 'F' THEN '1'	-- 'Failure Reported'
							WHEN pull_status = 'R' THEN '2'	-- 'In Progress'
							WHEN pull_status = 'S' THEN
								CASE
									WHEN transfer_status = 'E' THEN '1'	-- 'Failure Reported'
									WHEN transfer_status = 'F' THEN '1'	-- 'Failure Reported'
									WHEN transfer_status = 'R' THEN '0'	-- 'In Progress'
									WHEN transfer_status = 'S' THEN '0'	-- 'Successful'
								END
						END
				 END	Source_Status
				,CASE
					WHEN trunc((transfer_end - this_marker_db1)) <= 144 THEN
						'Buss Date: '|| TO_CHAR(this_marker_db1) ||';  Last Party Update: '|| pack_app_monitoring.get_last_party_date(source)
					ELSE
						'Buss Date: '|| TO_CHAR(transfer_end) ||';  Last Party Update: '|| pack_app_monitoring.get_last_party_date(source)
				 END ||
				*/
				,'Transfer Date: '|| TO_CHAR(transfer_end,'DD-MON-YYYY') ||';  Last Party Update: '|| pack_app_monitoring.get_last_party_date(source)
				 ||
				 CASE
					WHEN load_status ||pull_status ||transfer_status = 'SSS' AND
							trunc((transfer_end - this_marker_db1)) <= 144 THEN
						''
					WHEN load_status ||pull_status ||transfer_status = 'SSS' AND
							trunc((transfer_end - this_marker_db1)) > 144 THEN
						'  (Investigate)'
					WHEN load_status ||pull_status ||transfer_status LIKE '%R%' THEN
						'  (In Progress)'
					ELSE
						'  (Being investigated)'
				 END	status_desc
			 FROM	tbl_sources
			 WHERE	source	= p_source
			)	s
		ON	(t.app_ref	= p_app_ref		AND
			 t.item_type	= 'DTF'			AND
			 t.credentials	= s.source)
		WHEN MATCHED THEN
			UPDATE
			SET	 t.item_desc	= '<pre>' ||
							CASE
								WHEN s.source = s.description THEN ''
								ELSE s.source ||': '
							END ||
							s.description	||
							'<br> - '	||
							s.status_desc
				,t.status	= s.Source_Status
				,t.update_date	= SYSDATE
		WHEN NOT MATCHED THEN
			INSERT	(app_ref
				,item_desc
				,item_value
				,item_type
				,item_sub_type
				,alert_notification
				,alert_max_value
				,run_frequency_in_min
				,create_date
				,update_date
				,app_item_id
				,credentials
				,status
				,active_flag)
			VALUES	(p_app_ref
				,RPAD(s.description,35)	||
					'(' ||s.source ||') - '	||
					s.status_desc
				,'TRUE'
				,'DTF'
				,'DB'
				,'Y'
				,0
				,240
				,SYSDATE
				,SYSDATE
				,seq_app_item.NEXTVAL
				,s.source
				,s.Source_Status
				,'Y');

		COMMIT;

	EXCEPTION
		WHEN OTHERS THEN
			p_error_message	:= SQLERRM;
			ROLLBACK;
	END updDataTransStatus;



	PROCEDURE	get_ADOC_stats
				(p_adocc_webApp_status	OUT	SYS_REFCURSOR
				,p_adocc_webApp_agg	OUT	SYS_REFCURSOR
				,p_adocc_wrkFlow_status	OUT	SYS_REFCURSOR
				,p_adocc_wrkFlow_agg	OUT	SYS_REFCURSOR
				,p_error_message	OUT	VARCHAR2)
	IS
	BEGIN
		OPEN p_adocc_webApp_status FOR
			SELECT	*
			FROM	tbl_ac_webApp_status;

		OPEN p_adocc_webApp_agg FOR
			SELECT	*
			FROM	tbl_ac_webApp_agg;

		OPEN p_adocc_wrkFlow_status FOR
			SELECT	*
			FROM	tbl_ac_workFlow_status;

		OPEN p_adocc_wrkFlow_agg FOR
			SELECT	*
			FROM	tbl_ac_workFlow_agg;

		p_error_message	:= SQLERRM;
	EXCEPTION
		WHEN OTHERS THEN
			p_error_message	:= SQLERRM;
			ROLLBACK;
	END get_ADOC_stats;

END pack_app_monitoring;
/
