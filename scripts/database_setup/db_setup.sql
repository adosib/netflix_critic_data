CREATE TYPE content_type AS ENUM (
    'movie',
    'series'
);

CREATE TABLE public.titles (
    id serial4 NOT NULL,
    netflix_id integer NULL,
    title varchar(256) NULL,
    content_type public.content_type NULL,
    release_year int4 NULL,
    runtime int4 NULL,
    metadata jsonb NULL,
    CONSTRAINT netflix_id UNIQUE (netflix_id),
    CONSTRAINT titles_pkey PRIMARY KEY (id)
);



CREATE TABLE public.availability (
    id serial PRIMARY KEY,
    netflix_id integer REFERENCES titles (netflix_id),
    redirected_netflix_id integer,
    country char(2),
    available boolean,
    titlepage_reachable boolean,
    checked_at timestamp,
    CONSTRAINT unique_country_and_netflix_id UNIQUE (country, netflix_id)
);



CREATE TABLE public.ratings (
    id serial PRIMARY KEY,
    netflix_id integer REFERENCES titles (netflix_id),
    vendor varchar(32),
    url text,
    rating smallint,
    ratings_count integer,
    checked_at timestamp,
    CONSTRAINT unique_vendor_and_netflix_id UNIQUE (vendor, netflix_id)
);


-- WARNING: I loaded movies first, then ran the below to set content_type = 'movie',
UPDATE
    titles
SET
    content_type = 'movie'
WHERE
    content_type IS null;

-- then I loaded series and ran the block below
UPDATE
    titles
SET
    content_type = 'series'
WHERE
    content_type IS null;


/*
    Create audit table for CDC
*/

CREATE TABLE public.audit (
    audit_id serial PRIMARY KEY,
    table_name varchar(32) NOT NULL,
    table_oid oid NOT NULL,
    trigger varchar(32) NOT NULL,
    operation text NOT NULL,
    changed_at timestamp DEFAULT CURRENT_TIMESTAMP,
    record_id int NOT NULL,
    field text NOT NULL,
    old_value text,
    new_value text
);

CREATE OR REPLACE FUNCTION UDF_TRIGGER_AUDIT()
RETURNS trigger AS $$
DECLARE
    col TEXT;
    old_val TEXT;
    new_val TEXT;
BEGIN
    IF (TG_OP = 'UPDATE') THEN
        -- Loop through each column of the table
        FOR col IN 
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = TG_TABLE_NAME AND column_name NOT IN ('id') -- could dynamically exclude PK here
        LOOP
            -- Get old and new values for the column
            EXECUTE format('SELECT $1.%I', col) INTO old_val USING OLD;
            EXECUTE format('SELECT $1.%I', col) INTO new_val USING NEW;
            
            -- Log only if the field was changed
            IF old_val IS DISTINCT FROM new_val THEN
                INSERT INTO public.audit (table_name, table_oid, trigger, operation, changed_at, record_id, field, old_value, new_value)
                VALUES (TG_TABLE_NAME, TG_RELID, TG_NAME, TG_OP, NOW(), NEW.id, col, old_val, new_val);
            END IF;
        END LOOP;

    ELSIF (TG_OP = 'INSERT') THEN
        -- Log all fields for an insert
        FOR col IN 
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = TG_TABLE_NAME AND column_name NOT IN ('id')
        LOOP
            EXECUTE format('SELECT $1.%I', col) INTO new_val USING NEW;
            INSERT INTO public.audit (table_name, table_oid, trigger, operation, changed_at, record_id, field, old_value, new_value)
            VALUES (TG_TABLE_NAME, TG_RELID, TG_NAME, TG_OP, NOW(), NEW.id, col, NULL, new_val);
        END LOOP;

    ELSIF (TG_OP = 'DELETE') THEN
        -- Log all fields for a delete
        FOR col IN 
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = TG_TABLE_NAME AND column_name NOT IN ('id')
        LOOP
            EXECUTE format('SELECT $1.%I', col) INTO old_val USING OLD;
            INSERT INTO public.audit (table_name, table_oid, trigger, operation, changed_at, record_id, field, old_value, new_value)
            VALUES (TG_TABLE_NAME, TG_RELID, TG_NAME, TG_OP, NOW(), OLD.id, col, old_val, NULL);
        END LOOP;
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE TRIGGER availability_audit_trigger
AFTER INSERT OR UPDATE OR DELETE ON public.availability
FOR EACH ROW
EXECUTE FUNCTION UDF_TRIGGER_AUDIT();

CREATE OR REPLACE TRIGGER titles_audit_trigger
AFTER INSERT OR UPDATE OR DELETE ON public.titles
FOR EACH ROW
EXECUTE FUNCTION UDF_TRIGGER_AUDIT();

CREATE OR REPLACE TRIGGER ratings_audit_trigger
AFTER INSERT OR UPDATE OR DELETE ON public.ratings
FOR EACH ROW
EXECUTE FUNCTION UDF_TRIGGER_AUDIT();


/*
    Ancillary functions for QA/analysis/whatever
*/
CREATE OR REPLACE FUNCTION JSON_EXTRACT_ELEMENT_FROM_METADATA(
    metadata jsonb,
    element_type text
) RETURNS jsonb AS $$
DECLARE
    element JSONB;
BEGIN
    -- Iterate through the JSONB array
    FOR element IN
        SELECT value
        FROM jsonb_array_elements(metadata)
    LOOP
        -- Check if the "type" field matches the supplied element_type
        IF element->>'type' = element_type THEN
            RETURN element;
        END IF;
    END LOOP;

    -- Return NULL if no matching element is found
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
