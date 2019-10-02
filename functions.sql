CREATE OR REPLACE FUNCTION timeslice_transaction_jensen(val INT)
RETURNS TABLE(
	customer_id INT,
	customer_name TEXT,
	property_number INT,
	Vs INT,
	Ve INT,
	Op TEXT
)
LANGUAGE 'plpgsql'
AS $$
DECLARE
	rec1 RECORD;
	rec2 RECORD;
BEGIN
	
	/* Do a filtering*/
	/* Excluded: */
	/* 1) Tuple with Op D but T < val */
	/* 2) Tuple with Op I but T > val */
	EXECUTE format(
		'CREATE TEMP TABLE tmp ON COMMIT DROP AS
		SELECT * FROM property_ownership WHERE ("Op" = ''I'' AND "T" <= %s) OR ("Op" = ''D'' AND "T" >= %s) ', val, val);
	
	/* SEARCH RESULT */
	EXECUTE 'CREATE TEMP TABLE result( customer_id INT, customer_name TEXT, property_number INT, Vs INT, Ve INT, Op TEXT) ON COMMIT DROP';
	FOR rec1 IN SELECT * FROM tmp WHERE "Op" = 'I' LOOP

		FOR rec2 IN SELECT * FROM tmp WHERE "Op" = 'D' LOOP
			IF (rec1.customer_id = rec2.customer_id) AND (rec1.property_number = rec2.property_number) AND (rec1."Vs" = rec2."Vs") AND
			   (rec1."Ve" = rec2."Ve") AND (rec1."T" <= rec2."T") THEN

			   EXECUTE format(
			   	'INSERT INTO result VALUES (%s, ''%s'', %s, %s, %s, ''%s'')',
			   	rec1.customer_id, rec1.customer_name, rec1.property_number, rec1."Vs", rec1."Ve", rec1."Op");

			   EXECUTE format(
			   	'INSERT INTO result VALUES (%s, ''%s'', %s, %s, %s, ''%s'')',
			   	rec2.customer_id, rec2.customer_name, rec2.property_number, rec2."Vs", rec2."Ve", rec2."Op");

			END IF;

		END LOOP;

	END LOOP;

	RETURN QUERY SELECT * FROM result;
END;
$$;

CREATE OR REPLACE FUNCTION timeslice_valid_jensen(val INT)
RETURNS TABLE(
	customer_id INT,
	customer_name TEXT,
	property_number INT,
	T INT,
	Op TEXT
)
LANGUAGE 'plpgsql'
AS $$
BEGIN
	RETURN QUERY SELECT property_ownership.customer_id, 
						property_ownership.customer_name, 
						property_ownership.property_number, 
						property_ownership."T", 
						property_ownership."Op"
				 FROM property_ownership
				 WHERE "Vs" <= val AND "Ve" >= val;
END;
$$;