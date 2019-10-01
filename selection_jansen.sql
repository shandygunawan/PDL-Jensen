CREATE OR REPLACE FUNCTION selection_jansen(cond text)
	RETURNS SETOF property_ownership AS $$
	
	BEGIN
		RETURN QUERY EXECUTE 'SELECT * FROM property_ownership WHERE ' || cond;
	END;

$$ LANGUAGE PLPGSQL;