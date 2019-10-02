PGDMP          0            	    w            pdl    11.4    11.4 	    �
           0    0    ENCODING    ENCODING        SET client_encoding = 'UTF8';
                       false            �
           0    0 
   STDSTRINGS 
   STDSTRINGS     (   SET standard_conforming_strings = 'on';
                       false            �
           0    0 
   SEARCHPATH 
   SEARCHPATH     8   SELECT pg_catalog.set_config('search_path', '', false);
                       false                        1262    24892    pdl    DATABASE     �   CREATE DATABASE pdl WITH TEMPLATE = template0 ENCODING = 'UTF8' LC_COLLATE = 'English_United States.1252' LC_CTYPE = 'English_United States.1252';
    DROP DATABASE pdl;
             postgres    false            �            1255    25061 !   set_difference_jensen(text, text)    FUNCTION       CREATE FUNCTION public.set_difference_jensen(table1_name text, table2_name text) RETURNS TABLE(customer_id integer, customer_name text, property_number integer, vs integer, ve integer, t integer, op text)
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
 P   DROP FUNCTION public.set_difference_jensen(table1_name text, table2_name text);
       public       postgres    false            �            1255    24933 %   timeslice_transaction_jensen(integer)    FUNCTION     �  CREATE FUNCTION public.timeslice_transaction_jensen(val integer) RETURNS TABLE(customer_id integer, customer_name text, property_number integer, vs integer, ve integer, op text)
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
 @   DROP FUNCTION public.timeslice_transaction_jensen(val integer);
       public       postgres    false            �            1255    24930    timeslice_valid_jensen(integer)    FUNCTION     �  CREATE FUNCTION public.timeslice_valid_jensen(val integer) RETURNS TABLE(customer_id integer, customer_name text, property_number integer, t integer, op text)
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
 :   DROP FUNCTION public.timeslice_valid_jensen(val integer);
       public       postgres    false            �            1259    24893    property_ownership    TABLE     �   CREATE TABLE public.property_ownership (
    customer_id integer NOT NULL,
    customer_name text NOT NULL,
    property_number integer NOT NULL,
    vs integer NOT NULL,
    ve integer NOT NULL,
    t integer NOT NULL,
    op text NOT NULL
);
 &   DROP TABLE public.property_ownership;
       public         postgres    false            �
          0    24893    property_ownership 
   TABLE DATA               h   COPY public.property_ownership (customer_id, customer_name, property_number, vs, ve, t, op) FROM stdin;
    public       postgres    false    198   "       �
   x   x�341�t-KT��L�)N��47�4�44���e�W�)�N�& yO.#s΀Ԓ�"$yS�	T ����,�ە@7�r�1T�v��.ݦPi��9��㍠�\1z\\\ �,YP     