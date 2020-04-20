CREATE PACKAGE DBOWNER.PACK_HUBSPOT_INTEG
  CREATE OR REPLACE EDITIONABLE PACKAGE "DBOWNER"."PACK_HUBSPOT_INTEG" AS
/*******************************************************************************
Author		: Ganesh Shekar
Description	: Package to support Integration of HubSpot into CAPRI
Audit		:
	Version		Date	User			Description
		1	09-DEC-2015	Ganesh Shekar	Initial version
*******************************************************************************/

	PROCEDURE	get_cell_contacts_01
				(p_orig_upd_create_date		IN	VARCHAR2
				,p_cell_contacts		OUT	SYS_REFCURSOR
				,p_error_message		OUT	VARCHAR2);
END pack_hubspot_integ;
 
/
CREATE OR REPLACE EDITIONABLE PACKAGE BODY "DBOWNER"."PACK_HUBSPOT_INTEG" AS
/*******************************************************************************
Author		: Ganesh Shekar
Description	: Package to support Application Monitoring web application
Audit		:
	Version		Date	User			Description
		1	09-DEC-2015	Ganesh Shekar	Initial version
*******************************************************************************/

	PROCEDURE	get_cell_contacts_01
				(p_orig_upd_create_date		IN	VARCHAR2
				,p_cell_contacts		OUT	SYS_REFCURSOR
				,p_error_message		OUT	VARCHAR2)
	IS
		l_orig_upd_create_date		DATE;
	BEGIN
		--l_orig_upd_create_date	:= TO_DATE(p_orig_upd_create_date, 'DD-MON-YYYY');
		l_orig_upd_create_date	:= TRUNC(SYSDATE) - TO_NUMBER(p_orig_upd_create_date, '9999');

		OPEN p_cell_contacts FOR
			SELECT	 pty.orig_title			title
				,pty.firstname			first_name
				,pty.lastname			last_name
				,pty.email			email
				,pty.country			country
				,pty.iso_country_code		iso_country_code
				,pty.job_title			job_title
				,itr.interest_sub_section	field_of_research
				,pty.org_type			organisation_type
				,pty.org_name			Institution
				,pty.orig_party_ref		capri_ref
			FROM	 tbl_parties	pty
				,tbl_interests	itr
			WHERE	pty.orig_system			= 'AE2'
			AND	pty.orig_site			= 'Cell Press'
			AND	pty.dedupe_type			= 'PER'
			AND	(pty.orig_update_date	> l_orig_upd_create_date	OR
				 pty.orig_create_date	> l_orig_upd_create_date)
			AND	pty.record_status		IN ('A', 'I', 'U')
			AND	itr.party_ref		(+)	= pty.org_orig_system_ref
			AND	itr.orig_system		(+)	= pty.orig_system
			AND	itr.interest_value	(+)	= 'profile-cell-specialties'
			AND	exists
				(SELECT	1
				 FROM	tbl_interests	min
				 WHERE	min.party_ref			= pty.org_orig_system_ref
				 AND	min.orig_system			= pty.orig_system
				 AND	min.interest_type		=  'MIN'
				 AND	min.interest_value_details	= 'cell'
				);

		p_error_message	:= SQLERRM;
	EXCEPTION
		WHEN OTHERS THEN
			p_error_message	:= SQLERRM;
			ROLLBACK;
	END get_cell_contacts_01;


END pack_hubspot_integ;
/
