-- https://docs.aws.amazon.com/redshift/latest/dg/JSON_EXTRACT_ARRAY_ELEMENT_TEXT.html
DROP FUNCTION IF EXISTS json_extract_array_element_text(text, int);
CREATE OR REPLACE FUNCTION json_extract_array_element_text(json_array text, array_index int)
RETURNS text immutable
STRICT
LANGUAGE plpythonu
AS $$
	import json
	if json_array == '':
		return None
	items = json.loads(json_array)
	if 0 <= array_index and array_index < len(items):
		return json.dumps(items[array_index]).strip('"')
	else:
		return None
$$;


-- https://docs.aws.amazon.com/redshift/latest/dg/JSON_EXTRACT_PATH_TEXT.html
-- This isn't needed as postgres include json_extract_path_text a function that does something similar.
-- See https://www.postgresql.org/docs/10/functions-json.html
DROP FUNCTION IF EXISTS json_extract_path_text(text, character[]);
CREATE OR REPLACE FUNCTION json_extract_path_text(json_string text, VARIADIC path_elems character[])
RETURNS text immutable
STRICT
LANGUAGE plpythonu
AS $$
  import json
  result = json.loads(json_string)
  for path_elem in path_elems:
      if path_elem not in result: return ""
      result = result[path_elem]
  return json.dumps(result).strip('"')
$$;


-- https://docs.aws.amazon.com/redshift/latest/dg/JSON_ARRAY_LENGTH.html
-- DROP FUNCTION IF EXISTS json_array_length;
-- CREATE OR REPLACE FUNCTION json_array_length(json_array text) RETURNS int immutable as $$
-- import json
-- return len(json.loads(json_array))
-- $$ LANGUAGE plpythonu;

-- decode()
CREATE FUNCTION decode(expression int, search int, result int, "default" int) RETURNS int immutable as $$
return result if expression == search else default
$$ LANGUAGE plpythonu;

-- median()
CREATE FUNCTION _final_median(numeric[]) RETURNS numeric immutable as $$
	SELECT AVG(val) FROM (
		SELECT val FROM unnest($1) val
		ORDER BY 1
		LIMIT 2 - MOD(array_upper($1, 1), 2)
		OFFSET CEIL(array_upper($1, 1) / 2.0) - 1
	) sub;
$$ LANGUAGE sql;

CREATE AGGREGATE median(numeric) (
	SFUNC=array_append,
	STYPE=numeric[],
	FINALFUNC=_final_median,
	INITCOND='{}'
);

-- https://docs.aws.amazon.com/redshift/latest/dg/REGEXP_SUBSTR.html
CREATE OR REPLACE FUNCTION REGEXP_SUBSTR(source_string varchar, pattern varchar)
RETURNS varchar
LANGUAGE plpgsql
AS $$
	DECLARE
		first_match varchar;
	BEGIN
		first_match := (
			REGEXP_MATCH(
				source_string,
				REGEXP_REPLACE(pattern, '\\+', '\', 'g')
			)
		)[1];
		IF first_match IS NULL THEN
			RETURN '';
		END IF;
		RETURN first_match;
	END;
$$;





-- Drops dependencies first
DROP AGGREGATE IF EXISTS LISTAGG(text, text);
DROP FUNCTION IF EXISTS LISTAGG_FINAL(LISTAGG_TYPE, text, text);
DROP FUNCTION IF EXISTS LISTAGG_SFUNC(LISTAGG_TYPE, text, text);
DROP TYPE IF EXISTS LISTAGG_TYPE;

-- https://www.postgresql.org/docs/11/sql-createtype.html
CREATE TYPE LISTAGG_TYPE AS (
    a TEXT[],
    delim TEXT
);

CREATE OR REPLACE FUNCTION LISTAGG_SFUNC(LISTAGG_TYPE, text, text)
RETURNS LISTAGG_TYPE
LANGUAGE 'plpgsql'
AS $$
	DECLARE
		appended LISTAGG_TYPE;
	BEGIN
		appended.a := array_append($1.a, $2::text);
		appended.delim := $3;
		RETURN appended;
	END;
$$;

CREATE OR REPLACE FUNCTION LISTAGG_FINAL(LISTAGG_TYPE, text, text)
RETURNS TEXT
LANGUAGE 'plpgsql'
AS $$
	BEGIN
--		RAISE EXCEPTION 'BAD: %|%|%',$1,$2,$3;
		RETURN ARRAY_TO_STRING($1.a,$1.delim);
	END;
$$;

-- https://www.postgresql.org/docs/11/sql-createaggregate.html
-- https://www.postgresql.org/docs/11/xaggr.html
-- https://stackoverflow.com/q/67159133/326979
-- https://stackoverflow.com/q/46411209/326979
-- https://stackoverflow.com/a/48190288/326979
CREATE AGGREGATE LISTAGG(text, text) (
	sfunc = LISTAGG_SFUNC,
  stype = LISTAGG_TYPE,
  initcond = '({},",")',
  finalfunc = LISTAGG_FINAL,
	FINALFUNC_EXTRA
);


-- NOW() is deprecated in Redshift
-- https://docs.aws.amazon.com/redshift/latest/dg/Date_functions_header.html#date-functions-deprecated
CREATE OR REPLACE FUNCTION getdate() RETURNS TIMESTAMP immutable as $$
    SELECT (NOW() AT TIME ZONE 'UTC')::timestamp(0)
    $$ LANGUAGE sql;