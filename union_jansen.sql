CREATE OR REPLACE FUNCTION union_jansen(_tbl1 anyelement, _tbl2 anyelement)
	RETURNS SETOF anyelement AS $$
	
	BEGIN
		RETURN QUERY SELECT * FROM table1 UNION SELECT * FROM table2;
	END;
$$ language plpgsql;