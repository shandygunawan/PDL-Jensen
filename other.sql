CREATE TABLE property_ownership_2(
	customer_id INT,
	customer_name TEXT,
	property_number INT,
	vs INT,
	ve INT,
	t INT,
	op TEXT
);
INSERT INTO property_ownership_2 VALUES (145, 'Eva Nielsen', 7797, 10, 9999, 10, 'I');
INSERT INTO property_ownership_2 VALUES (145, 'Eva Nielsen', 7797, 10, 14, 16, 'I');
select * from set_difference_jensen('property_ownership', 'property_ownership_2');


CREATE TABLE property_condition(
	property_number INT,
	condition TEXT,
	vs INT,
	ve INT,
	t INT,
	op TEXT
);
INSERT INTO property_condition VALUES (7797, 'Very Good', 5, 7, 12, 'I');
INSERT INTO property_condition VALUES (7797, 'Very Good', 5, 7, 13, 'D');
INSERT INTO property_condition VALUES (7797, 'Good', 12, 14, 15, 'I');
INSERT INTO property_condition VALUES (7797, 'Quiet Good', 12, 14, 17, 'D');
INSERT INTO property_condition VALUES (2109, 'Very Good', 14, 15, 18, 'I');
SELECT * FROM property_ownership JOIN property_condition 
ON (property_ownership.property_number = property_condition.property_number) AND (property_ownership.T > property_condition.T);
select * from join_jensen('property_ownership', 'property_condition');

-- TRYING TO IMPLEMENT GENERAL JOIN (but failed)
-- Get all the columns' name and datatype from both table
EXECUTE format('CREATE TEMP TABLE table1_columns ON COMMIT DROP 
	AS SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ''$s''', table1_name)
EXECUTE format('CREATE TEMP TABLE table2_columns ON COMMIT DROP 
	AS SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ''$s''', table2_name)

-- Get the datatype of the join column
SELECT data_type into join_datatype FROM table1_columns WHERE column_name = join_name;

-- Create table with the join column
EXECUTE format('CREATE TEMP TABLE result ( %s %s ) ON COMMIT DROP', join_name, join_datatype);

-- Insert all columns from table1 except temporals into result columns
FOR rec1 IN SELECT * FROM table1_columns LOOP
	IF (rec1.column_name != 'vs') AND (rec1.column_name != 've') 
		AND (rec1.column_name != 't') AND (rec1.column_name != 'op') THEN
		EXECUTE format('ALTER TABLE result ADD COLUMN %s %s', rec1.column_name, rec1.data_type);
	END IF;
END LOOP;

-- Insert all columns from table2 except temporals and join attribute into result columns
FOR rec1 IN SELECT * FROM table1_columns LOOP
	IF (rec1.column_name != join_name) AND (rec1.column_name != 'vs') AND (rec1.column_name != 've') 
		AND (rec1.column_name != 't') AND (rec1.column_name != 'op') THEN
		EXECUTE format('ALTER TABLE result ADD COLUMN %s %s', rec1.column_name, rec1.data_type);
	END IF;
END LOOP;

-- Add all temporal columns
EXECUTE 'ALTER TABLE result ADD COLUMN vs INT';
EXECUTE 'ALTER TABLE result ADD COLUMN ve INT';
EXECUTE 'ALTER TABLE result ADD COLUMN t INT';
EXECUTE 'ALTER TABLE result ADD COLUMN op TEXT';

select * from property_ownership where vs = 10 and ve = 14 and t = 15 LIMIT 1;
select * from property_ownership where vs = 15 and ve = 19 and t = 20 LIMIT 1;
select * from allen_relationship_jensen('property_ownership', 'other', ex1, ex2);
select * from allen_relationship_jensen('property_ownership', 'other', (select * from property_ownership where vs = 10 and ve = 14 and t = 15 LIMIT 1), (select * from property_ownership where vs = 15 and ve = 19 and t = 20 LIMIT 1));
