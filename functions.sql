CREATE OR REPLACE FUNCTION set_difference_jensen(table1_name TEXT, table2_name TEXT)
RETURNS TABLE(
	customer_id INT,
	customer_name TEXT,
	property_number INT,
	vs INT,
	ve INT,
	t INT,
	op TEXT
)
LANGUAGE 'plpgsql'
AS $$
DECLARE
	rec1 RECORD;
	rec2 RECORD;
BEGIN
	EXECUTE 'CREATE TEMP TABLE result( customer_id INT, customer_name TEXT, property_number INT, Vs INT, Ve INT, T INT, Op TEXT) ON COMMIT DROP';

	EXECUTE format('CREATE TEMP TABLE table1 ON COMMIT DROP AS SELECT * FROM %s', table1_name);
	EXECUTE format('CREATE TEMP TABLE table2 ON COMMIT DROP AS SELECT * FROM %s', table2_name);

	FOR rec1 IN SELECT * FROM table1 LOOP
		
		/* Check if rec1 exist in table2 */
		SELECT * INTO rec2 FROM table2
				 WHERE (rec1.customer_id = table2.customer_id) 
				   		AND (rec1.property_number = table2.property_number)
				   		AND (rec1.vs = table2.vs) AND (rec1.ve = table2.ve) 
				   		AND (rec1.op = table2.op) LIMIT 1;

		/* Insert record to result tablefrom table1 if it doesn't exist in table2  */
		IF rec2 IS NULL THEN
			EXECUTE format(
			   	'INSERT INTO result VALUES (%s, ''%s'', %s, %s, %s, %s, ''%s'')',
			   	rec1.customer_id, rec1.customer_name, rec1.property_number, rec1.vs, rec1.ve, rec1.t, rec1.op);
		ELSE
			/* IF the records is same (Op is I) but the T is different */
			/* IF rec1.T > rec2.T THEN don't need to put it to result table */
			/* IF rec1.T < rec2.T THEN insert both of them to result table but change rec2.Op to 'D' */ 
			IF (rec1.op = 'I') AND (rec1.t < rec2.t) THEN

				/* Insert rec1 */
				EXECUTE format(
			   	'INSERT INTO result VALUES (%s, ''%s'', %s, %s, %s, %s, ''%s'')',
			   	rec1.customer_id, rec1.customer_name, rec1.property_number, rec1.vs, rec1.ve, rec1.t, rec1.op);

				/* Insert rec2 but change op to 'D' */
			   	EXECUTE format(
			   	'INSERT INTO result VALUES (%s, ''%s'', %s, %s, %s, %s, ''%s'')',
			   	rec2.customer_id, rec2.customer_name, rec2.property_number, rec2.vs, rec2.ve, rec2.t, 'D');
			END IF;
		END IF;
	END LOOP;

	RETURN QUERY SELECT * FROM result;
END;
$$;

CREATE OR REPLACE FUNCTION join_jensen(table1_name TEXT, table2_name TEXT)
RETURNS TABLE(
	customer_id INT,
	customer_name TEXT,
	property_number INT,
	conditon TEXT,
	vs INT,
	ve INT,
	t INT,
	op TEXT
)
LANGUAGE 'plpgsql'
AS $$
DECLARE
	rec1 RECORD;
	rec2 RECORD;
	vs_new INT;
	ve_new INT;
	t_new INT;
BEGIN
	EXECUTE 'CREATE TEMP TABLE result 
		(customer_id INT, customer_name TEXT, property_number INT, condition TEXT, vs INT, ve INT, t INT, op TEXT) ON COMMIT DROP';

	/* Store the two table to a new table to be iterated */
	EXECUTE format('CREATE TEMP TABLE table1 ON COMMIT DROP AS SELECT * FROM %s', table1_name);
	EXECUTE format('CREATE TEMP TABLE table2 ON COMMIT DROP AS SELECT * FROM %s', table2_name);

	FOR rec1 IN SELECT * FROM table1 LOOP
		FOR rec2 IN SELECT * FROM table2 LOOP
			-- Join if attribute join (property_number is same)
			IF rec1.property_number = rec2.property_number THEN
				
				-- Join conditions inside if
				IF (rec1.t > rec2.t) OR ( (rec1.t = rec2.t) AND (rec1.op = 'I') ) THEN
					
					/* get the highest vs */
					IF rec1.vs >= rec2.vs THEN
						vs_new = rec1.vs;
					ELSE
						vs_new = rec2.vs;
					END IF;

					/* get the lowest ve */
					IF rec1.ve <= rec2.ve THEN
						ve_new = rec1.ve;
					ELSE
						ve_new = rec2.ve;
					END IF;

					/* Get the highest t */
					IF rec1.t >= rec2.t THEN
						t_new = rec1.t;
					ELSE
						t_new = rec2.t;
					END IF;

					EXECUTE format(
						'INSERT INTO result VALUES (%s, ''%s'', %s, ''%s'', %s, %s, %s, ''%s'')', 
						rec1.customer_id, rec1.customer_name, rec1.property_number, rec2.condition, vs_new, ve_new, t_new, rec1.op);
				END IF;
			END IF;
		END LOOP;
	END LOOP;

	RETURN QUERY SELECT * FROM result;
END;
$$;

CREATE OR REPLACE FUNCTION timeslice_transaction_jensen(val INT)
RETURNS TABLE(
	customer_id INT,
	customer_name TEXT,
	property_number INT,
	vs INT,
	ve INT,
	op TEXT
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
		SELECT * FROM property_ownership WHERE (op = ''I'' AND t <= %s) OR (op = ''D'' AND t >= %s) ', val, val);
	
	/* SEARCH RESULT */
	EXECUTE 'CREATE TEMP TABLE result( customer_id INT, customer_name TEXT, property_number INT, vs INT, ve INT, op TEXT) ON COMMIT DROP';
	FOR rec1 IN SELECT * FROM tmp WHERE tmp.op = 'I' LOOP
		FOR rec2 IN SELECT * FROM tmp WHERE tmp.op = 'D' LOOP
			IF (rec1.customer_id = rec2.customer_id) AND (rec1.property_number = rec2.property_number) AND (rec1.vs = rec2.vs) AND
			   (rec1.ve = rec2.ve) AND (rec1.t <= rec2.t) THEN

			   EXECUTE format(
			   	'INSERT INTO result VALUES (%s, ''%s'', %s, %s, %s, ''%s'')',
			   	rec1.customer_id, rec1.customer_name, rec1.property_number, rec1.vs, rec1.ve, rec1.op);

			   EXECUTE format(
			   	'INSERT INTO result VALUES (%s, ''%s'', %s, %s, %s, ''%s'')',
			   	rec2.customer_id, rec2.customer_name, rec2.property_number, rec2.vs, rec2.ve, rec2.op);

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
	t INT,
	op TEXT
)
LANGUAGE 'plpgsql'
AS $$
BEGIN
	RETURN QUERY SELECT property_ownership.customer_id, 
						property_ownership.customer_name, 
						property_ownership.property_number, 
						property_ownership.t, 
						property_ownership.op
				 FROM property_ownership
				 WHERE vs <= val AND ve >= val;
END;
$$;