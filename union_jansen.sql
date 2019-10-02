CREATE OR REPLACE FUNCTION union_jansen(_tblname1 text, _tblname2 text)
	RETURNS TABLE (customer_id int, customer_name text, property_number int, vs int, ve int, t int, op text) AS $$
	
	BEGIN
		EXECUTE format('CREATE TEMP TABLE table1 ON COMMIT DROP AS SELECT * FROM %s', _tblname1);
		EXECUTE format('CREATE TEMP TABLE table2 ON COMMIT DROP AS SELECT * FROM %s', _tblname2);
		RETURN QUERY SELECT * FROM table1 UNION SELECT * FROM table2;
	END;
$$ language plpgsql;