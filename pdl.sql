PGDMP         7            	    w            pdl    11.4    11.4 %    #           0    0    ENCODING    ENCODING        SET client_encoding = 'UTF8';
                       false            $           0    0 
   STDSTRINGS 
   STDSTRINGS     (   SET standard_conforming_strings = 'on';
                       false            %           0    0 
   SEARCHPATH 
   SEARCHPATH     8   SELECT pg_catalog.set_config('search_path', '', false);
                       false            &           1262    25329    pdl    DATABASE     �   CREATE DATABASE pdl WITH TEMPLATE = template0 ENCODING = 'UTF8' LC_COLLATE = 'English_United States.1252' LC_CTYPE = 'English_United States.1252';
    DROP DATABASE pdl;
             postgres    false            �            1255    25637 ?   allen_relationship_contains(integer, integer, integer, integer)    FUNCTION     �   CREATE FUNCTION public.allen_relationship_contains(t1_start integer, t1_end integer, t2_start integer, t2_end integer) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
BEGIN
RETURN ((t1_start < t2_start) AND (t1_end > t2_end));
END;
$$;
 v   DROP FUNCTION public.allen_relationship_contains(t1_start integer, t1_end integer, t2_start integer, t2_end integer);
       public       postgres    false            �            1255    25641 =   allen_relationship_during(integer, integer, integer, integer)    FUNCTION     �   CREATE FUNCTION public.allen_relationship_during(t1_start integer, t1_end integer, t2_start integer, t2_end integer) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
BEGIN
RETURN ((t1_start > t2_start) AND (t1_end < t2_end));
END;
$$;
 t   DROP FUNCTION public.allen_relationship_during(t1_start integer, t1_end integer, t2_start integer, t2_end integer);
       public       postgres    false            �            1255    25639 =   allen_relationship_equals(integer, integer, integer, integer)    FUNCTION     �   CREATE FUNCTION public.allen_relationship_equals(t1_start integer, t1_end integer, t2_start integer, t2_end integer) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
BEGIN
RETURN ((t1_start = t2_start) AND (t1_end = t2_end));
END;
$$;
 t   DROP FUNCTION public.allen_relationship_equals(t1_start integer, t1_end integer, t2_start integer, t2_end integer);
       public       postgres    false            �            1255    25636 B   allen_relationship_finished_by(integer, integer, integer, integer)    FUNCTION     �   CREATE FUNCTION public.allen_relationship_finished_by(t1_start integer, t1_end integer, t2_start integer, t2_end integer) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
BEGIN
RETURN ((t1_start < t2_start) AND (t1_end = t2_end));
END;
$$;
 y   DROP FUNCTION public.allen_relationship_finished_by(t1_start integer, t1_end integer, t2_start integer, t2_end integer);
       public       postgres    false            �            1255    25642 ?   allen_relationship_finishes(integer, integer, integer, integer)    FUNCTION     �   CREATE FUNCTION public.allen_relationship_finishes(t1_start integer, t1_end integer, t2_start integer, t2_end integer) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
BEGIN
RETURN ((t1_start > t2_start) AND (t1_end = t2_end));
END;
$$;
 v   DROP FUNCTION public.allen_relationship_finishes(t1_start integer, t1_end integer, t2_start integer, t2_end integer);
       public       postgres    false            �            1255    25668 5   allen_relationship_jensen(text, text, record, record)    FUNCTION     �  CREATE FUNCTION public.allen_relationship_jensen(table_name text, time_dimension text, rec1 record, rec2 record) RETURNS text
    LANGUAGE plpgsql
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
 p   DROP FUNCTION public.allen_relationship_jensen(table_name text, time_dimension text, rec1 record, rec2 record);
       public       postgres    false            �            1255    25634 <   allen_relationship_meets(integer, integer, integer, integer)    FUNCTION     �   CREATE FUNCTION public.allen_relationship_meets(t1_start integer, t1_end integer, t2_start integer, t2_end integer) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
BEGIN
RETURN (t1_end = t2_start);
END;
$$;
 s   DROP FUNCTION public.allen_relationship_meets(t1_start integer, t1_end integer, t2_start integer, t2_end integer);
       public       postgres    false            �            1255    25644 :   allen_relationship_met(integer, integer, integer, integer)    FUNCTION     �   CREATE FUNCTION public.allen_relationship_met(t1_start integer, t1_end integer, t2_start integer, t2_end integer) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
BEGIN
RETURN (t1_start = t2_end);
END;
$$;
 q   DROP FUNCTION public.allen_relationship_met(t1_start integer, t1_end integer, t2_start integer, t2_end integer);
       public       postgres    false            �            1255    25643 D   allen_relationship_overlapped_by(integer, integer, integer, integer)    FUNCTION       CREATE FUNCTION public.allen_relationship_overlapped_by(t1_start integer, t1_end integer, t2_start integer, t2_end integer) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
BEGIN
RETURN ((t1_start > t2_start) AND (t1_start < t2_end) AND (t1_end > t2_end));
END;
$$;
 {   DROP FUNCTION public.allen_relationship_overlapped_by(t1_start integer, t1_end integer, t2_start integer, t2_end integer);
       public       postgres    false            �            1255    25635 ?   allen_relationship_overlaps(integer, integer, integer, integer)    FUNCTION       CREATE FUNCTION public.allen_relationship_overlaps(t1_start integer, t1_end integer, t2_start integer, t2_end integer) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
BEGIN
RETURN ((t1_start < t2_start) AND (t1_end > t2_start) AND (t1_end < t2_end));
END;
$$;
 v   DROP FUNCTION public.allen_relationship_overlaps(t1_start integer, t1_end integer, t2_start integer, t2_end integer);
       public       postgres    false            �            1255    25645 B   allen_relationship_preceded_by(integer, integer, integer, integer)    FUNCTION     �   CREATE FUNCTION public.allen_relationship_preceded_by(t1_start integer, t1_end integer, t2_start integer, t2_end integer) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
BEGIN
RETURN (t1_start > t2_end);
END;
$$;
 y   DROP FUNCTION public.allen_relationship_preceded_by(t1_start integer, t1_end integer, t2_start integer, t2_end integer);
       public       postgres    false            �            1255    25633 ?   allen_relationship_precedes(integer, integer, integer, integer)    FUNCTION     �   CREATE FUNCTION public.allen_relationship_precedes(t1_start integer, t1_end integer, t2_start integer, t2_end integer) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
BEGIN
RETURN (t1_end < t2_start);
END;
$$;
 v   DROP FUNCTION public.allen_relationship_precedes(t1_start integer, t1_end integer, t2_start integer, t2_end integer);
       public       postgres    false            �            1255    25646 G   allen_relationship_predicates(text, integer, integer, integer, integer)    FUNCTION     �  CREATE FUNCTION public.allen_relationship_predicates(relationship text, t1_start integer, t1_end integer, t2_start integer, t2_end integer) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
BEGIN
-- ALLEN's RELATIONSHIP : https://www.ics.uci.edu/~alspaugh/cls/shr/allen.html
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
 �   DROP FUNCTION public.allen_relationship_predicates(relationship text, t1_start integer, t1_end integer, t2_start integer, t2_end integer);
       public       postgres    false            �            1255    25640 A   allen_relationship_started_by(integer, integer, integer, integer)    FUNCTION     �   CREATE FUNCTION public.allen_relationship_started_by(t1_start integer, t1_end integer, t2_start integer, t2_end integer) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
BEGIN
RETURN ((t1_start = t2_start) AND (t1_end > t2_end));
END;
$$;
 x   DROP FUNCTION public.allen_relationship_started_by(t1_start integer, t1_end integer, t2_start integer, t2_end integer);
       public       postgres    false            �            1255    25638 =   allen_relationship_starts(integer, integer, integer, integer)    FUNCTION     �   CREATE FUNCTION public.allen_relationship_starts(t1_start integer, t1_end integer, t2_start integer, t2_end integer) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
BEGIN
RETURN ((t1_start = t2_start) AND (t1_end < t2_end));
END;
$$;
 t   DROP FUNCTION public.allen_relationship_starts(t1_start integer, t1_end integer, t2_start integer, t2_end integer);
       public       postgres    false            �            1255    25762    coalesce_jensen(text)    FUNCTION     �  CREATE FUNCTION public.coalesce_jensen(_tbl text) RETURNS TABLE(customer_id integer, customer_name text, property_number integer, vs integer, ve integer, t integer, op text)
    LANGUAGE plpgsql
    AS $$
BEGIN
RETURN QUERY EXECUTE 'select distinct fi.customer_id, fi.customer_name, fi.property_number, fi.vs, la.ve, fi.t, fi.op
from ' || _tbl || ' fi, ' || _tbl || ' la
where fi.vs < la.ve
and la.customer_id = fi.customer_id AND la.customer_name = fi.customer_name AND la.property_number = fi.property_number
and not exists (
select *
 from ' || _tbl || ' mi
 where mi.customer_id = fi.customer_id AND mi.customer_name = fi.customer_name AND mi.property_number = fi.property_number
 and fi.vs < mi.vs and mi.vs < la.ve
 and not exists (
 select *
 from ' || _tbl || ' a1
 where a1.customer_id = fi.customer_id AND a1.customer_name = fi.customer_name AND a1.property_number = fi.property_number
 and a1.vs < mi.vs and mi.vs <= a1.ve))
 and not exists (
 select *
 from ' || _tbl || ' a2
 where a2.customer_id = fi.customer_id AND a2.customer_name = fi.customer_name AND a2.property_number = fi.property_number
 and (a2.vs < fi.vs and fi.vs <= a2.ve or a2.vs <= la.ve and la.ve < a2.ve))
order by fi.vs';
END;
$$;
 1   DROP FUNCTION public.coalesce_jensen(_tbl text);
       public       postgres    false            �            1255    25663 3   delete_jensen(character varying, character varying) 	   PROCEDURE     s  CREATE PROCEDURE public.delete_jensen(t_name character varying, t_data character varying)
    LANGUAGE plpgsql
    AS $$DECLARE t_column_info VARCHAR[];
DECLARE t_column_data VARCHAR[];
DECLARE temp_query VARCHAR;
DECLARE	i INT;

BEGIN
	IF ((SELECT COUNT(*) FROM information_schema.tables WHERE TABLE_NAME=t_name) > 0) THEN
		BEGIN
			t_column_info := (SELECT ARRAY_AGG(COLUMN_NAME) AS column_name FROM (SELECT COLUMN_NAME FROM information_schema.columns WHERE TABLE_NAME=t_name) AS temp_table);
			t_column_data := (SELECT * FROM string_to_array(t_data,';'));
			
			IF ((array_length(t_column_info, 1) - 1) = array_length(t_column_data, 1)) THEN
				i := 1;
				
				temp_query := (format('CREATE TEMP TABLE t_latest_records AS (SELECT * FROM %s WHERE ', t_name));
				LOOP
					EXIT WHEN i = (array_length(t_column_info, 1) - 3);
					temp_query := temp_query || format('%s=%s', t_column_info[i], REPLACE(t_column_data[i], '"', ''''));
					
					IF (((array_length(t_column_info, 1) - 3) - i) > 1) THEN
						temp_query := temp_query || ' AND ';
					END IF;
					
					i := i + 1;
				END LOOP;
				temp_query := temp_query || ' ORDER BY T DESC, Op)';
				
				EXECUTE temp_query;
				
				IF ((SELECT COUNT(*) FROM t_latest_records LIMIT 1) = 0) THEN
					DROP TABLE t_latest_records;
					RAISE EXCEPTION 'No previous record exist.';
				ELSIF ((SELECT Op FROM t_latest_records LIMIT 1) = 'I') THEN
					DROP TABLE t_latest_records;
					EXECUTE format('INSERT INTO %s VALUES (%s, ''D'');', t_name, REPLACE(REPLACE(t_data, ';', ', '), '"', ''''));
				ELSIF ((SELECT Op FROM t_latest_records LIMIT 1) = 'D') THEN
					DROP TABLE t_latest_records;
					RAISE EXCEPTION 'The latest record of data has bounded transaction time.';
				END IF;
			ELSE
				RAISE EXCEPTION 'Inserted tuple does not have the same column count.';
			END IF;
		END;
	ELSE
		RAISE EXCEPTION 'No table exists.';
	END IF;
END$$;
 Y   DROP PROCEDURE public.delete_jensen(t_name character varying, t_data character varying);
       public       postgres    false            �            1255    25661 3   insert_jensen(character varying, character varying) 	   PROCEDURE     �  CREATE PROCEDURE public.insert_jensen(t_name character varying, t_data character varying)
    LANGUAGE plpgsql
    AS $$DECLARE t_column_info VARCHAR[];
DECLARE t_column_data VARCHAR[];
DECLARE temp_query VARCHAR;
DECLARE	i INT;

BEGIN
	IF ((SELECT COUNT(*) FROM information_schema.tables WHERE TABLE_NAME=t_name) > 0) THEN
		BEGIN
			t_column_info := (SELECT ARRAY_AGG(COLUMN_NAME) AS column_name FROM (SELECT COLUMN_NAME FROM information_schema.columns WHERE TABLE_NAME=t_name) AS temp_table);
			t_column_data := (SELECT * FROM string_to_array(t_data,';'));
			
			IF ((array_length(t_column_info, 1) - 1) = array_length(t_column_data, 1)) THEN
				i := 1;
				
				temp_query := (format('CREATE TEMP TABLE t_latest_records AS (SELECT * FROM %s WHERE ', t_name));
				LOOP
					EXIT WHEN i = (array_length(t_column_info, 1) - 3);
					temp_query := temp_query || format('%s=%s', t_column_info[i], REPLACE(t_column_data[i], '"', ''''));
					
					IF (((array_length(t_column_info, 1) - 3) - i) > 1) THEN
						temp_query := temp_query || ' AND ';
					END IF;
					
					i := i + 1;
				END LOOP;
				temp_query := temp_query || ' ORDER BY T DESC, Op)';
				
				EXECUTE temp_query;
				
				IF ((SELECT op FROM t_latest_records LIMIT 1) = 'I') THEN
					DROP TABLE t_latest_records;
					RAISE EXCEPTION 'The latest record of data still has unbounded transaction time.';
				ELSE
					EXECUTE format('INSERT INTO %s VALUES (%s, ''I'');', t_name, REPLACE(REPLACE(t_data, ';', ', '), '"', ''''));
					DROP TABLE t_latest_records;
				END IF;
			ELSE
				RAISE EXCEPTION 'Inserted tuple does not have the same column count.';
			END IF;
		END;
	ELSE
		RAISE EXCEPTION 'No table exists.';
	END IF;
END$$;
 Y   DROP PROCEDURE public.insert_jensen(t_name character varying, t_data character varying);
       public       postgres    false            �            1255    25563    join_jensen(text, text)    FUNCTION     *  CREATE FUNCTION public.join_jensen(table1_name text, table2_name text) RETURNS TABLE(customer_id integer, customer_name text, property_number integer, conditon text, vs integer, ve integer, t integer, op text)
    LANGUAGE plpgsql
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
 F   DROP FUNCTION public.join_jensen(table1_name text, table2_name text);
       public       postgres    false            �            1255    25665 ,   projection_jansen_refcursor(refcursor, text)    FUNCTION       CREATE FUNCTION public.projection_jansen_refcursor(query_name refcursor, _cols text) RETURNS refcursor
    LANGUAGE plpgsql
    AS $$
 BEGIN
 OPEN query_name FOR EXECUTE 'SELECT ' || _cols || ', "vs", "ve", "t", "op" FROM property_ownership';
return query_name;
END;
$$;
 T   DROP FUNCTION public.projection_jansen_refcursor(query_name refcursor, _cols text);
       public       postgres    false            �            1255    25664    projection_jensen(text)    FUNCTION     �   CREATE FUNCTION public.projection_jensen(_cols text) RETURNS SETOF record
    LANGUAGE plpgsql
    AS $$
 BEGIN
 RETURN QUERY EXECUTE 'SELECT ' || _cols || ', "vs", "ve", "t", "op" FROM property_ownership';
END;
$$;
 4   DROP FUNCTION public.projection_jensen(_cols text);
       public       postgres    false            �            1259    25333    property_ownership    TABLE     �   CREATE TABLE public.property_ownership (
    customer_id integer NOT NULL,
    customer_name text NOT NULL,
    property_number integer NOT NULL,
    vs integer NOT NULL,
    ve integer NOT NULL,
    t integer NOT NULL,
    op text NOT NULL
);
 &   DROP TABLE public.property_ownership;
       public         postgres    false            �            1255    25666    selection_jensen(text)    FUNCTION     �   CREATE FUNCTION public.selection_jensen(cond text) RETURNS SETOF public.property_ownership
    LANGUAGE plpgsql
    AS $$

BEGIN
RETURN QUERY EXECUTE 'SELECT * FROM property_ownership WHERE ' || cond;
END;

$$;
 2   DROP FUNCTION public.selection_jensen(cond text);
       public       postgres    false    200            �            1255    25381 !   set_difference_jensen(text, text)    FUNCTION     Q  CREATE FUNCTION public.set_difference_jensen(table1_name text, table2_name text) RETURNS TABLE(customer_id integer, customer_name text, property_number integer, vs integer, ve integer, t integer, op text)
    LANGUAGE plpgsql
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
/* IF the records is same but the T is different */
/* IF rec1.T > rec2.T THEN don't need to put it to result table */
/* IF rec1.T < rec2.T THEN insert both of them to result table but change rec2.Op to 'D' */ 
IF (rec1.t < rec2.t) THEN
EXECUTE format(
   'INSERT INTO result VALUES (%s, ''%s'', %s, %s, %s, %s, ''%s'')',
   rec1.customer_id, rec1.customer_name, rec1.property_number, rec1.vs, rec1.ve, rec1.t, rec1.op);

   EXECUTE format(
   'INSERT INTO result VALUES (%s, ''%s'', %s, %s, %s, %s, ''%s'')',
   rec2.customer_id, rec2.customer_name, rec2.property_number, rec2.vs, rec2.ve, rec2.t, 'D');
END IF;
END IF;
END LOOP;

RETURN QUERY SELECT * FROM result;
END;
$$;
 P   DROP FUNCTION public.set_difference_jensen(table1_name text, table2_name text);
       public       postgres    false            �            1255    25340 %   timeslice_transaction_jensen(integer)    FUNCTION     i  CREATE FUNCTION public.timeslice_transaction_jensen(val integer) RETURNS TABLE(customer_id integer, customer_name text, property_number integer, vs integer, ve integer, op text)
    LANGUAGE plpgsql
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
 @   DROP FUNCTION public.timeslice_transaction_jensen(val integer);
       public       postgres    false            �            1255    25614    timeslice_valid_jensen(integer)    FUNCTION     �  CREATE FUNCTION public.timeslice_valid_jensen(val integer) RETURNS TABLE(customer_id integer, customer_name text, property_number integer, t integer, op text)
    LANGUAGE plpgsql
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
 :   DROP FUNCTION public.timeslice_valid_jensen(val integer);
       public       postgres    false            �            1255    25667    union_jensen(text, text)    FUNCTION     �  CREATE FUNCTION public.union_jensen(_tblname1 text, _tblname2 text) RETURNS TABLE(customer_id integer, customer_name text, property_number integer, vs integer, ve integer, t integer, op text)
    LANGUAGE plpgsql
    AS $$

BEGIN
EXECUTE format('CREATE TEMP TABLE table1 ON COMMIT DROP AS SELECT * FROM %s', _tblname1);
EXECUTE format('CREATE TEMP TABLE table2 ON COMMIT DROP AS SELECT * FROM %s', _tblname2);
RETURN QUERY SELECT * FROM table1 UNION SELECT * FROM table2;
END;
$$;
 C   DROP FUNCTION public.union_jensen(_tblname1 text, _tblname2 text);
       public       postgres    false            �            1255    25662 3   update_jensen(character varying, character varying) 	   PROCEDURE     V	  CREATE PROCEDURE public.update_jensen(t_name character varying, t_data character varying)
    LANGUAGE plpgsql
    AS $$DECLARE t_column_info VARCHAR[];
DECLARE t_column_data VARCHAR[];
DECLARE temp_parameter VARCHAR;
DECLARE temp_query VARCHAR;
DECLARE temp_record VARCHAR;
DECLARE	i INT;

BEGIN
	IF ((SELECT COUNT(*) FROM information_schema.tables WHERE TABLE_NAME=t_name) > 0) THEN
		BEGIN
			t_column_info := (SELECT ARRAY_AGG(COLUMN_NAME) AS column_name FROM (SELECT COLUMN_NAME FROM information_schema.columns WHERE TABLE_NAME=t_name) AS temp_table);
			t_column_data := (SELECT * FROM string_to_array(t_data,';'));
			
			IF ((array_length(t_column_info, 1) - 1) = array_length(t_column_data, 1)) THEN
				i := 1;
				
				temp_query := (format('CREATE TEMP TABLE t_latest_records AS (SELECT * FROM %s WHERE ', t_name));
				LOOP
					EXIT WHEN i = (array_length(t_column_info, 1) - 3);
					temp_query := temp_query || format('%s=%s', t_column_info[i], REPLACE(t_column_data[i], '"', ''''));
					
					IF (((array_length(t_column_info, 1) - 3) - i) > 1) THEN
						temp_query := temp_query || ' AND ';
					END IF;
					
					i := i + 1;
				END LOOP;
				temp_query := temp_query || ' ORDER BY T DESC, Op)';
				
				EXECUTE temp_query;
				
				IF ((SELECT COUNT(*) FROM t_latest_records LIMIT 1) = 0) THEN
					DROP TABLE t_latest_records;
					RAISE EXCEPTION 'No previous record exist.';
				ELSIF ((SELECT Op FROM t_latest_records LIMIT 1) = 'I') THEN
					UPDATE t_latest_records SET Op='D';
					
					i := 1;
					temp_parameter := '';
					LOOP
						EXIT WHEN i = (array_length(t_column_info, 1));
						EXECUTE format('SELECT "%s" FROM t_latest_records LIMIT 1', t_column_info[i]) INTO temp_record;
						temp_parameter := temp_parameter || format('''%s''', temp_record);
						i := i + 1;
						
						IF (i != array_length(t_column_info, 1)) THEN
							temp_parameter := temp_parameter || ';';
						END IF;
					END LOOP;
					
					DROP TABLE t_latest_records;
					CALL delete_jansen(t_name, temp_parameter);
					CALL insert_jansen(t_name, t_data);
				ELSIF ((SELECT Op FROM t_latest_records LIMIT 1) = 'D') THEN
					DROP TABLE t_latest_records;
					CALL insert_jansen(t_name, t_data);
				END IF;
			ELSE
				RAISE EXCEPTION 'Inserted tuple does not have the same column count.';
			END IF;
		END;
	ELSE
		RAISE EXCEPTION 'No table exists.';
	END IF;
END$$;
 Y   DROP PROCEDURE public.update_jensen(t_name character varying, t_data character varying);
       public       postgres    false            �            1259    25425    property_condition    TABLE     �   CREATE TABLE public.property_condition (
    property_number integer,
    condition text,
    vs integer,
    ve integer,
    t integer,
    op text
);
 &   DROP TABLE public.property_condition;
       public         postgres    false            �            1259    25382    property_ownership_2    TABLE     �   CREATE TABLE public.property_ownership_2 (
    customer_id integer,
    customer_name text,
    property_number integer,
    vs integer,
    ve integer,
    t integer,
    op text
);
 (   DROP TABLE public.property_ownership_2;
       public         postgres    false                       0    25425    property_condition 
   TABLE DATA               W   COPY public.property_condition (property_number, condition, vs, ve, t, op) FROM stdin;
    public       postgres    false    202   It                 0    25333    property_ownership 
   TABLE DATA               h   COPY public.property_ownership (customer_id, customer_name, property_number, vs, ve, t, op) FROM stdin;
    public       postgres    false    200   �t                 0    25382    property_ownership_2 
   TABLE DATA               j   COPY public.property_ownership_2 (customer_id, customer_name, property_number, vs, ve, t, op) FROM stdin;
    public       postgres    false    201   3u           L   x�37�4�K-�Tp��O�4�4�44���2����\CNCS�����%�b���Ly`ifj��&s�Mm����� ��+�         ~   x�341�t-KT��L�)N��47�4�44���e�W�)�N�& yO.#s΀Ԓ�"$yS�	T ����,�ە@7�r�1T�v��.ݦPi��9��㍠�x4�	1z\\\ �N��         5   x�341�t-KT��L�)N��47�4�44���eH�
CNC3�|� pC     