CREATE PACKAGE DBOWNER.PK_CLEAN
  CREATE OR REPLACE EDITIONABLE PACKAGE "DBOWNER"."PK_CLEAN" AS

PROCEDURE upload_cleaned_emails;
PROCEDURE upload_cleaned_countries;

END pk_clean;

 
/
CREATE OR REPLACE EDITIONABLE PACKAGE BODY "DBOWNER"."PK_CLEAN" AS

PROCEDURE upload_cleaned_emails IS

CURSOR c_clean_emails IS
	SELECT * FROM tbl_cleaned_emails;

BEGIN

	FOR c_rec IN c_clean_emails LOOP

		UPDATE tbl_parties SET
			   email = decode(c_rec.email_clean_status,'U', email, c_rec.email),
			   email_clean_status = c_rec.email_clean_status,
			   record_status = decode(c_rec.email_clean_status,'M', 'U', record_status)
		WHERE orig_party_ref = c_rec.orig_party_ref;

		COMMIT;

	END LOOP;

	DELETE tbl_cleaned_emails;

	COMMIT;


END upload_cleaned_emails;

PROCEDURE upload_cleaned_countries IS

CURSOR c_clean_countries IS
	SELECT * FROM tbl_cleaned_countries a, tbl_iso_countries b
	WHERE a.iso_country = b.iso_code;

BEGIN

	FOR c_rec IN c_clean_countries LOOP

		UPDATE tbl_parties SET
			   country = decode(c_rec.country_clean_status,'U', country, c_rec.country_name),
			   iso_country_code = decode(c_rec.country_clean_status,'U', iso_country_code, c_rec.iso_code),
			   country_clean_status = decode(c_rec.country_clean_status,'M','U', record_status)
		WHERE orig_party_ref = c_rec.orig_party_ref;

	END LOOP;

	--DELETE tbl_cleaned_countries;

	COMMIT;


END upload_cleaned_countries;

END pk_clean;
/
