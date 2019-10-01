--
-- PostgreSQL database dump
--

-- Dumped from database version 11.4
-- Dumped by pg_dump version 11.4

-- Started on 2019-10-02 00:57:50

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
-- TOC entry 212 (class 1255 OID 24840)
-- Name: test_jensen(integer); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.test_jensen(val integer) RETURNS TABLE(customer_id integer, customer_name text, property_number integer, vs integer, ve integer, op text)
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
SELECT * FROM property_ownership WHERE ("Op" = ''I'' AND "T" <= %s) OR ("Op" = ''D'' AND "T" > %s) ', val, val);

/* SEARCH RESULT */
EXECUTE 'CREATE TEMP TABLE result( customer_id INT, customer_name TEXT, property_number INT, Vs INT, Ve INT, Op TEXT) ON COMMIT DROP';
FOR rec1 IN SELECT * FROM tmp LOOP
FOR rec2 IN SELECT * FROM tmp LOOP
IF (rec1.customer_id = rec2.customer_id) AND (rec1.property_number = rec2.property_number) AND (rec1."Vs" = rec2."Vs") AND
   (rec1."Ve" = rec2."Ve") AND (rec1."T" <= rec2."T") AND rec1."Op" = 'I' AND rec2."Op" = 'D' THEN

   EXECUTE format(
   'INSERT INTO result VALUES (%s, ''%s'', %s, %s, ''%s'')',
   rec1.customer_id, rec1.customer_name, rec1.property_number, rec1."Vs", rec1."Ve", rec1."Op"::TEXT);

   EXECUTE format(
   'INSERT INTO result VALUES (%s, ''%s'', %s, %s, ''%s'')',
   rec2.customer_id, rec2.customer_name, rec2.property_number, rec2."Vs", rec2."Ve", rec2."Op"::TEXT);

END IF;

END LOOP;

END LOOP;

RETURN QUERY SELECT * FROM result;
END;
$$;


ALTER FUNCTION public.test_jensen(val integer) OWNER TO postgres;

--
-- TOC entry 211 (class 1255 OID 24617)
-- Name: valid_timeslice_jensen(integer); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.valid_timeslice_jensen(val integer) RETURNS integer
    LANGUAGE plpgsql
    AS $$DECLARE
	rec record;
	ins_numbers INT;
	del_numbers INT;
BEGIN
	ins_numbers = 0;
	del_numbers = 0;
	
	FOR rec in SELECT * FROM property_ownership
	LOOP
		IF rec."Op" = 'I' and rec."T" <= val THEN
			ins_numbers = ins_numbers + 1;
		ELSIF rec."Op" = 'D' and rec."T" > val THEN
			del_numbers = del_numbers + 1;
		END IF;
	END LOOP;
	
	/*
	ins_numbers := ( SELECT count(*) 
					FROM property_ownership 
					WHERE "Op" = 'I' and "T" <= val);
	*/

	RETURN del_numbers;

END;$$;


ALTER FUNCTION public.valid_timeslice_jensen(val integer) OWNER TO postgres;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- TOC entry 198 (class 1259 OID 24602)
-- Name: property_ownership; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.property_ownership (
    customer_id integer NOT NULL,
    customer_name text NOT NULL,
    property_number integer NOT NULL,
    "Vs" integer NOT NULL,
    "Ve" integer NOT NULL,
    "T" integer NOT NULL,
    "Op" text NOT NULL
);


ALTER TABLE public.property_ownership OWNER TO postgres;

--
-- TOC entry 2809 (class 0 OID 24602)
-- Dependencies: 198
-- Data for Name: property_ownership; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.property_ownership (customer_id, customer_name, property_number, "Vs", "Ve", "T", "Op") FROM stdin;
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


-- Completed on 2019-10-02 00:57:50

--
-- PostgreSQL database dump complete
--

