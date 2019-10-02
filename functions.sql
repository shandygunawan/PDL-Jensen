CREATE OR REPLACE FUNCTION set_difference_jensen(table1 TEXT, table2 TEXT)
RETURNS TABLE(
	customer_id INT,
	customer_name TEXT,
	property_number INT,
	Vs INT,
	Ve INT,
	T INT,
	Op TEXT
)
LANGUAGE 'plpgsql'
AS $$
DECLARE
	rec1 RECORD;
	/* rec2 RECORD; */
	is_exist BOOLEAN;
BEGIN
	EXECUTE 'CREATE TEMP TABLE result( customer_id INT, customer_name TEXT, property_number INT, Vs INT, Ve INT, T INT, Op TEXT) ON COMMIT DROP';

	FOR rec1 IN SELECT * FROM table1 LOOP
		
		/* Check if rec1 exist in table2 (Vs, Ve, Op are same) */
		rec2 := (SELECT * FROM table2 
				 WHERE (rec1.customer_id = table2.customer_id) 
				   		AND (rec1.property_number = table2.property_number)
				   		AND (rec1."Vs" = table2."Vs") AND (rec1."Ve" = table2."Ve") 
				   		AND (rec1."Op" = table2."Op"))

		/* Insert record to result tablefrom table1 if it doesn't exist in table2  */
		IF rec2 IS NULL THEN
			EXECUTE format(
			   	'INSERT INTO result VALUES (%s, ''%s'', %s, %s, %s, %s, ''%s'')',
			   	rec1.customer_id, rec1.customer_name, rec1.property_number, rec1."Vs", rec1."Ve", rec1."T", rec1."Op");
		ELSE
			/* IF the records is same but the T is different */
			/* IF rec1.T > rec2.T THEN don't need to put it to result table */
			/* IF rec1.T < rec2.T THEN insert both of them to result table but change rec2.Op to 'D' */ 
			IF (rec1."T" <= rec2."T") THEN
				EXECUTE format(
			   	'INSERT INTO result VALUES (%s, ''%s'', %s, %s, %s, %s, ''%s'')',
			   	rec1.customer_id, rec1.customer_name, rec1.property_number, rec1."Vs", rec1."Ve", rec1."T", rec1."Op");

			   	EXECUTE format(
			   	'INSERT INTO result VALUES (%s, ''%s'', %s, %s, %s, %s, ''%s'')',
			   	rec2.customer_id, rec2.customer_name, rec2.property_number, rec2."Vs", rec2."Ve", rec2."T", 'D');
			END IF;
		END IF;
	END LOOP;

	RETURN QUERY SELECT * FROM result;
END;
$$;

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