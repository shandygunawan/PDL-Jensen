-- ALLEN 1: Check if rec1 precedes rec2
CREATE OR REPLACE FUNCTION allen_relationship_precedes(t1_start INT, t1_end INT, t2_start INT, t2_end INT)
RETURNS BOOLEAN
LANGUAGE 'plpgsql'
AS $$
BEGIN
	RETURN (t1_end < t2_start);	
END;
$$;

-- ALLEN 2: Check if rec1 meets rec2
CREATE OR REPLACE FUNCTION allen_relationship_meets(t1_start INT, t1_end INT, t2_start INT, t2_end INT)
RETURNS BOOLEAN
LANGUAGE 'plpgsql'
AS $$
BEGIN
	RETURN (t1_end = t2_start);	
END;
$$;

-- ALLEN 3: Check if rec1 overlaps rec2
CREATE OR REPLACE FUNCTION allen_relationship_overlaps(t1_start INT, t1_end INT, t2_start INT, t2_end INT)
RETURNS BOOLEAN
LANGUAGE 'plpgsql'
AS $$
BEGIN
	RETURN ((t1_start < t2_start) AND (t1_end > t2_start) AND (t1_end < t2_end));	
END;
$$;

-- ALLEN 4: Check if rec1 Finished by rec2
CREATE OR REPLACE FUNCTION allen_relationship_finished_by(t1_start INT, t1_end INT, t2_start INT, t2_end INT)
RETURNS BOOLEAN
LANGUAGE 'plpgsql'
AS $$
BEGIN
	RETURN ((t1_start < t2_start) AND (t1_end = t2_end));	
END;
$$;

-- ALLEN 5: CHECK if rec1 contains rec2
CREATE OR REPLACE FUNCTION allen_relationship_contains(t1_start INT, t1_end INT, t2_start INT, t2_end INT)
RETURNS BOOLEAN
LANGUAGE 'plpgsql'
AS $$
BEGIN
	RETURN ((t1_start < t2_start) AND (t1_end > t2_end));	
END;
$$;

-- ALLEN 6: Check if rec1 starts with rec2
CREATE OR REPLACE FUNCTION allen_relationship_starts(t1_start INT, t1_end INT, t2_start INT, t2_end INT)
RETURNS BOOLEAN
LANGUAGE 'plpgsql'
AS $$
BEGIN
	RETURN ((t1_start = t2_start) AND (t1_end < t2_end));
END;
$$;

-- ALLEN 7: Check if rec1 equals rec2
CREATE OR REPLACE FUNCTION allen_relationship_equals(t1_start INT, t1_end INT, t2_start INT, t2_end INT)
RETURNS BOOLEAN
LANGUAGE 'plpgsql'
AS $$
BEGIN
	RETURN ((t1_start = t2_start) AND (t1_end = t2_end));
END;
$$;

-- ALLEN 8: Check if rec1 started by rec2
CREATE OR REPLACE FUNCTION allen_relationship_started_by(t1_start INT, t1_end INT, t2_start INT, t2_end INT)
RETURNS BOOLEAN
LANGUAGE 'plpgsql'
AS $$
BEGIN
	RETURN ((t1_start = t2_start) AND (t1_end > t2_end));
END;
$$;

-- ALLEN 9: Check if rec1 during rec2
CREATE OR REPLACE FUNCTION allen_relationship_during(t1_start INT, t1_end INT, t2_start INT, t2_end INT)
RETURNS BOOLEAN
LANGUAGE 'plpgsql'
AS $$
BEGIN
	RETURN ((t1_start > t2_start) AND (t1_end < t2_end));
END;
$$;

-- ALLEN 10: check if rec1 finishes during with rec2
CREATE OR REPLACE FUNCTION allen_relationship_finishes(t1_start INT, t1_end INT, t2_start INT, t2_end INT)
RETURNS BOOLEAN
LANGUAGE 'plpgsql'
AS $$
BEGIN
	RETURN ((t1_start > t2_start) AND (t1_end = t2_end));
END;
$$;

-- ALLEN 11: check if rec1 overlapped by rec2
CREATE OR REPLACE FUNCTION allen_relationship_overlapped_by(t1_start INT, t1_end INT, t2_start INT, t2_end INT)
RETURNS BOOLEAN
LANGUAGE 'plpgsql'
AS $$
BEGIN
	RETURN ((t1_start > t2_start) AND (t1_start < t2_end) AND (t1_end > t2_end));
END;
$$;

-- ALLEN 12: check if rec1 met by rec2
CREATE OR REPLACE FUNCTION allen_relationship_met(t1_start INT, t1_end INT, t2_start INT, t2_end INT)
RETURNS BOOLEAN
LANGUAGE 'plpgsql'
AS $$
BEGIN
	RETURN (t1_start = t2_end);
END;
$$;

-- ALLEN 13: check if rec1 preceded by rec2
CREATE OR REPLACE FUNCTION allen_relationship_preceded_by(t1_start INT, t1_end INT, t2_start INT, t2_end INT)
RETURNS BOOLEAN
LANGUAGE 'plpgsql'
AS $$
BEGIN
	RETURN (t1_start > t2_end);
END;
$$;

CREATE OR REPLACE FUNCTION allen_relationship_predicates(relationship TEXT, t1_start INT, t1_end INT, t2_start INT, t2_end INT)
RETURNS BOOLEAN
LANGUAGE 'plpgsql'
AS $$
BEGIN
	-- ALLEN's RELATIONSHIPS : https://www.ics.uci.edu/~alspaugh/cls/shr/allen.html
	CASE relationship
		WHEN 'precedes' THEN RETURN allen_relationship_precedes(t1_start, t1_end, t2_start, t2_end);
		WHEN 'meets' THEN RETURN allen_relationship_meets(t1_start, t1_end, t2_start, t2_end);
		WHEN 'overlaps' THEN RETURN allen_relationship_overlaps(t1_start, t1_end, t2_start, t2_end);
		WHEN 'finished by' THEN RETURN allen_relationship_finished_by(t1_start, t1_end, t2_start, t2_end);
		WHEN 'contains' THEN RETURN allen_relationship_contains(t1_start, t1_end, t2_start, t2_end);
		WHEN 'starts' THEN RETURN allen_relationship_starts(t1_start, t1_end, t2_start, t2_end);
		WHEN 'equals' THEN RETURN allen_relationship_equals(t1_start, t1_end, t2_start, t2_end);
		WHEN 'started by' THEN RETURN allen_relationship_started_by(t1_start, t1_end, t2_start, t2_end);
		WHEN 'during' THEN RETURN allen_relationship_during(t1_start, t1_end, t2_start, t2_end);
		WHEN 'finishes' THEN RETURN allen_relationship_finishes(t1_start, t1_end, t2_start, t2_end);
		WHEN 'overlapped by' THEN RETURN allen_relationship_overlapped_by(t1_start, t1_end, t2_start, t2_end);
		WHEN 'met' THEN RETURN allen_relationship_met(t1_start, t1_end, t2_start, t2_end);
		WHEN 'preceded by' THEN RETURN allen_relationship_preceded_by(t1_start, t1_end, t2_start, t2_end);
		ELSE RETURN FALSE;
	END CASE;
END;
$$;

CREATE OR REPLACE FUNCTION allen_relationship_jensen(table_name TEXT, time_dimension TEXT, rec1 RECORD, rec2 RECORD)
RETURNS TEXT
LANGUAGE 'plpgsql'
AS $$
DECLARE
	t1_start INT;
	t1_end INT;
	t2_start INT;
	t2_end INT;
BEGIN
	-- FOR time_dimension == valid
	IF time_dimension = 'valid' THEN

		RETURN allen_relationship(rec1.vs, rec1.ve, rec2.vs, rec2.ve);

	ELSIF time_dimension = 'transaction' THEN
		EXECUTE format('CREATE TEMP TABLE tmp ON COMMIT DROP AS SELECT * FROM %s', table_name);

		-- FOR time_dimension == transaction
		-- Search for the r1's pair
		IF rec1.op = 'I' THEN
			SELECT tmp.t INTO t1_end
			FROM tmp 
			WHERE rec1.customer_id = tmp.customer_id AND rec1.property_number = tmp.property_number AND
				  rec1.vs = tmp.vs AND rec1.ve = tmp.ve AND rec1.t <= tmp.t AND tmp.t = 'D'
			LIMIT 1;
			t1_start = rec1.t;
		ELSE
			SELECT tmp.t INTO t1_start 
			FROM tmp 
			WHERE rec1.customer_id = tmp.customer_id AND rec1.property_number = tmp.property_number AND
				  rec1.vs = tmp.vs AND rec1.ve = tmp.ve AND rec1.t >= tmp.t AND tmp.t = 'I'
			LIMIT 1;
			t1_end = rec1.t;
		END IF;

		-- Search for the r2's pair
		IF rec2.op = 'I' THEN
			SELECT * INTO t2_end
			FROM tmp 
			WHERE rec2.customer_id = tmp.customer_id AND rec2.property_number = tmp.property_number AND
				  rec2.vs = tmp.vs AND rec2.ve = tmp.ve AND rec2.t <= tmp.t AND tmp.t = 'D'
			LIMIT 1; 
			t2_start = rec2.t;
		ELSE
			SELECT * INTO t2_start
			FROM tmp 
			WHERE rec2.customer_id = tmp.customer_id AND rec2.property_number = tmp.property_number AND
				  rec2.vs = tmp.vs AND rec2.ve = tmp.ve AND rec2.t >= tmp.t AND tmp.t = 'I'
			LIMIT 1; 
			t2_end = rec2.t;
		END IF;

		RETURN allen_relationship(t1_start, t1_end, t2_start, t2_end);
	END IF;

	RETURN 'Time dimension is not valid!';
END;
$$;