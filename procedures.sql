-- PROCEDURE: public.insert_jansen(character varying, character varying, "char")
-- DROP PROCEDURE public.insert_jansen(character varying, character varying, "char");
CREATE OR REPLACE PROCEDURE public.insert_jansen(
	t_name character varying,
	t_data character varying,
	flag "char" DEFAULT NULL::"char")
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
			
			IF (array_length(t_column_info, 1) = array_length(t_column_data, 1)) THEN
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
				temp_query := temp_query || ' ORDER BY "T" DESC)';
				
				EXECUTE temp_query;
				
				IF ((SELECT "Op" FROM t_latest_records LIMIT 1) = 'I') THEN
					DROP TABLE t_latest_records;
					RAISE 'The latest record of data still has unbounded transaction time.';
				ELSE
					EXECUTE format('INSERT INTO %s VALUES (%s);', t_name, REPLACE(REPLACE(t_data, ';', ', '), '"', ''''));
					DROP TABLE t_latest_records;
				END IF;
			ELSE
				RAISE 'Inserted tuple does not have the same column count.';
			END IF;
		END;
	ELSE
		RAISE 'No table exists.';
	END IF;
END
$BODY$;
