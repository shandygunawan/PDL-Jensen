-- PROCEDURE: public.insert_jansen(character varying, character varying)
-- DROP PROCEDURE public.insert_jansen(character varying, character varying);
CREATE OR REPLACE PROCEDURE public.insert_jansen(
	t_name character varying,
	t_data character varying)
LANGUAGE 'plpgsql'

AS $BODY$
DECLARE t_column_info VARCHAR[];
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
				temp_query := temp_query || ' ORDER BY "T" DESC, "Op")';
				
				EXECUTE temp_query;
				
				IF ((SELECT "Op" FROM t_latest_records LIMIT 1) = 'I') THEN
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
END
$BODY$;


-- PROCEDURE: public.delete_jansen(character varying, character varying)
-- DROP PROCEDURE public.delete_jansen(character varying, character varying);
CREATE OR REPLACE PROCEDURE public.delete_jansen(
	t_name character varying,
	t_data character varying)
LANGUAGE 'plpgsql'

AS $BODY$
DECLARE t_column_info VARCHAR[];
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
				temp_query := temp_query || ' ORDER BY "T" DESC, "Op")';
				
				EXECUTE temp_query;
				
				IF ((SELECT COUNT(*) FROM t_latest_records LIMIT 1) = 0) THEN
					DROP TABLE t_latest_records;
					RAISE EXCEPTION 'No previous record exist.';
				ELSIF ((SELECT "Op" FROM t_latest_records LIMIT 1) = 'I') THEN
					DROP TABLE t_latest_records;
					EXECUTE format('INSERT INTO %s VALUES (%s, ''D'');', t_name, REPLACE(REPLACE(t_data, ';', ', '), '"', ''''));
				ELSIF ((SELECT "Op" FROM t_latest_records LIMIT 1) = 'D') THEN
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
END
$BODY$;


-- PROCEDURE: public.update_jansen(character varying, character varying)

-- DROP PROCEDURE public.update_jansen(character varying, character varying);

CREATE OR REPLACE PROCEDURE public.update_jansen(
	t_name character varying,
	t_data character varying)
LANGUAGE 'plpgsql'

AS $BODY$
DECLARE t_column_info VARCHAR[];
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
				temp_query := temp_query || ' ORDER BY "T" DESC, "Op")';
				
				EXECUTE temp_query;
				
				IF ((SELECT COUNT(*) FROM t_latest_records LIMIT 1) = 0) THEN
					DROP TABLE t_latest_records;
					RAISE EXCEPTION 'No previous record exist.';
				ELSIF ((SELECT "Op" FROM t_latest_records LIMIT 1) = 'I') THEN
					UPDATE t_latest_records SET "Op"='D';
					
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
				ELSIF ((SELECT "Op" FROM t_latest_records LIMIT 1) = 'D') THEN
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
END
$BODY$;