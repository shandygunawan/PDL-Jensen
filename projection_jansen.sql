CREATE OR REPLACE FUNCTION projection_jansen(_cols text)
	RETURNS SETOF record AS $$
 	BEGIN
 		RETURN QUERY EXECUTE 'SELECT ' || _cols || ', "Vs", "Ve", "T", "Op" FROM property_ownership';
	END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION projection_jansen_refcursor(query_name refcursor, _cols text)
	RETURNS refcursor AS $$
 	BEGIN
 		OPEN query_name FOR EXECUTE 'SELECT ' || _cols || ', "Vs", "Ve", "T", "Op" FROM property_ownership';
		return query_name;
	END;
$$ LANGUAGE plpgsql;

-- SELECT * FROM projection_jansen('customer_id') AS f(customer_id integer, "Vs" integer, "Ve" integer, "T" integer, "Op" "char");

-- SELECT projection_jansen_refcursor('ids', 'customer_id');
-- FETCH ALL IN ids