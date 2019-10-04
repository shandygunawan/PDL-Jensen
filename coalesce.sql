-- select distinct fi.customer_id, fi.customer_name, fi.property_number, fi.vs, la.ve, fi.t, fi.op
-- from property_ownership fi, property_ownership la
-- where fi.vs < la.ve
-- and la.customer_id = fi.customer_id AND la.customer_name = fi.customer_name AND la.property_number = fi.property_number
-- and not exists (
-- 	select *
--  	from property_ownership mi
--  	where mi.customer_id = fi.customer_id AND mi.customer_name = fi.customer_name AND mi.property_number = fi.property_number
--  	and fi.vs < mi.vs and mi.vs < la.ve
--  	and not exists (
--  		select *
--  		from property_ownership a1
--  		where a1.customer_id = fi.customer_id AND a1.customer_name = fi.customer_name AND a1.property_number = fi.property_number
--  		and a1.vs < mi.vs and mi.vs <= a1.ve))
--  	and not exists (
--  		select *
--  		from property_ownership a2
--  		where a2.customer_id = fi.customer_id AND a2.customer_name = fi.customer_name AND a2.property_number = fi.property_number
--  		and (a2.vs < fi.vs and fi.vs <= a2.ve or a2.vs <= la.ve and la.ve < a2.ve))
-- order by fi.vs

CREATE OR REPLACE FUNCTION coalesce_jensen(_tbl text)
RETURNS TABLE(
	customer_id INT,
	customer_name TEXT,
	property_number INT,
	vs INT,
	ve INT,
	t INT,
	op TEXT
) AS $$
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
$$ LANGUAGE 'plpgsql';

