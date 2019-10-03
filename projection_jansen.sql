CREATE OR REPLACE FUNCTION projection_jensen(_cols text)
	RETURNS SETOF record AS $$
 	BEGIN
 		RETURN QUERY EXECUTE 'SELECT ' || _cols || ', "vs", "ve", "t", "op" FROM property_ownership';
	END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION projection_jansen_refcursor(query_name refcursor, _cols text)
	RETURNS refcursor AS $$
 	BEGIN
 		OPEN query_name FOR EXECUTE 'SELECT ' || _cols || ', "vs", "ve", "t", "op" FROM property_ownership';
		return query_name;
	END;
$$ LANGUAGE plpgsql;

-- SELECT * FROM projection_jansen('customer_id') AS f(customer_id integer, "vs" integer, "ve" integer, "t" integer, "op" "char");

-- SELECT projection_jansen_refcursor('ids', 'customer_id');
-- FETCH ALL IN ids