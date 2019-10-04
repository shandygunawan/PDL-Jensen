--
-- PostgreSQL database dump
--

-- Dumped from database version 11.5
-- Dumped by pg_dump version 11.5

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: delete_jansen(character varying, character varying); Type: PROCEDURE; Schema: public; Owner: postgres
--

CREATE PROCEDURE public.delete_jansen(t_name character varying, t_data character varying)
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


ALTER PROCEDURE public.delete_jansen(t_name character varying, t_data character varying) OWNER TO postgres;

--
-- Name: insert_jansen(character varying, character varying); Type: PROCEDURE; Schema: public; Owner: postgres
--

CREATE PROCEDURE public.insert_jansen(t_name character varying, t_data character varying)
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


ALTER PROCEDURE public.insert_jansen(t_name character varying, t_data character varying) OWNER TO postgres;

--
-- Name: set_difference_jensen(text, text); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.set_difference_jensen(table1_name text, table2_name text) RETURNS TABLE(customer_id integer, customer_name text, property_number integer, vs integer, ve integer, t integer, op text)
    LANGUAGE plpgsql
    AS $$
DECLARE
rec1 RECORD;
rec2 RECORD;
BEGIN
EXECUTE 'CREATE TEMP TABLE result( customer_id INT, customer_name TEXT, property_number INT, "Vs" INT, "Ve" INT, "T" INT, "Op" TEXT) ON COMMIT DROP';

EXECUTE format('CREATE TEMP TABLE table1 ON COMMIT DROP AS SELECT * FROM %s', table1_name);
EXECUTE format('CREATE TEMP TABLE table2 ON COMMIT DROP AS SELECT * FROM %s', table2_name);

FOR rec1 IN SELECT * FROM table1 LOOP

/* Check if rec1 exist in table2 */
SELECT * INTO rec2 FROM table2
 WHERE (rec1.customer_id = table2.customer_id) 
   AND (rec1.property_number = table2.property_number)
   AND (rec1."Vs" = table2."Vs") AND (rec1."Ve" = table2."Ve") 
   AND (rec1."Op" = table2."Op") LIMIT 1;

/* Insert record to result tablefrom table1 if it doesn't exist in table2  */
IF rec2 IS NULL THEN
EXECUTE format(
   'INSERT INTO result VALUES (%s, ''%s'', %s, %s, %s, %s, ''%s'')',
   rec1.customer_id, rec1.customer_name, rec1.property_number, rec1."Vs", rec1."Ve", rec1."T", rec1."Op");
ELSE
/* IF the records is same but the T is different */
/* IF rec1.T > rec2.T THEN don't need to put it to result table */
/* IF rec1.T < rec2.T THEN insert both of them to result table but change rec2.Op to 'D' */ 
IF (rec1."T" < rec2."T") THEN
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


ALTER FUNCTION public.set_difference_jensen(table1_name text, table2_name text) OWNER TO postgres;

--
-- Name: test(anyelement, anyelement); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.test(_tbl1 anyelement, _tbl2 anyelement) RETURNS void
    LANGUAGE plpgsql
    AS $$BEGIN
	RAISE '%', format('%s\n%s', _tbl1, _tbl2);
END;$$;


ALTER FUNCTION public.test(_tbl1 anyelement, _tbl2 anyelement) OWNER TO postgres;

--
-- Name: timeslice_transaction_jensen(integer); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.timeslice_transaction_jensen(val integer) RETURNS TABLE(customer_id integer, customer_name text, property_number integer, vs integer, ve integer, op text)
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


ALTER FUNCTION public.timeslice_transaction_jensen(val integer) OWNER TO postgres;

--
-- Name: timeslice_valid_jensen(integer); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.timeslice_valid_jensen(val integer) RETURNS TABLE(customer_id integer, customer_name text, property_number integer, t integer, op text)
    LANGUAGE plpgsql
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


ALTER FUNCTION public.timeslice_valid_jensen(val integer) OWNER TO postgres;

--
-- Name: update_jansen(character varying, character varying); Type: PROCEDURE; Schema: public; Owner: postgres
--

CREATE PROCEDURE public.update_jansen(t_name character varying, t_data character varying)
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


ALTER PROCEDURE public.update_jansen(t_name character varying, t_data character varying) OWNER TO postgres;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: property_ownership; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.property_ownership (
    customer_id integer NOT NULL,
    customer_name text NOT NULL,
    property_number integer NOT NULL,
    vs integer NOT NULL,
    ve integer NOT NULL,
    t integer NOT NULL,
    op text NOT NULL
);


ALTER TABLE public.property_ownership OWNER TO postgres;

--
-- Data for Name: property_ownership; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.property_ownership (customer_id, customer_name, property_number, vs, ve, t, op) FROM stdin;
145	Eva Nielsen	7797	10	9999	10	I
145	Eva Nielsen	7797	10	9999	15	D
145	Eva Nielsen	7797	10	14	15	I
827	Peter Olsen	7797	15	9999	15	I
827	Peter Olsen	7797	15	9999	20	D
827	Peter Olsen	7797	15	19	20	I
145	Eva Nielsen	7797	10	14	23	D
145	Eva Nielsen	7797	3	14	23	I
145	Eva Nielsen	7797	3	14	26	D
145	Eva Nielsen	7797	5	14	26	I
827	Peter Olsen	7797	15	19	28	D
827	Peter Olsen	7797	12	19	28	I
\.


--
-- PostgreSQL database dump complete
--

