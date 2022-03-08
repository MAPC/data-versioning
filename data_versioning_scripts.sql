-- Table: public.business_data_changes

-- DROP TABLE IF EXISTS public.business_data_changes;

CREATE TABLE IF NOT EXISTS public.business_data_changes
(
    id serial NOT NULL ,
    tstamp timestamp without time zone DEFAULT now(),
    operation text,
    who text DEFAULT CURRENT_USER,
    new_val json,
    old_val json
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.business_data_changes
    OWNER to postgres;

-- FUNCTION: public.change_trigger()

-- DROP FUNCTION IF EXISTS public.change_trigger();

CREATE OR REPLACE FUNCTION mapc.change_trigger()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF SECURITY DEFINER
AS $BODY$
  BEGIN
    IF TG_OP = 'INSERT'
    THEN
      INSERT INTO public.business_data_changes (operation, new_val)
        VALUES (TG_OP, row_to_json(NEW));
      RETURN NEW;
    ELSIF TG_OP = 'UPDATE'
    THEN
      INSERT INTO public.business_data_changes (operation, new_val, old_val)
        VALUES (TG_OP, row_to_json(NEW), row_to_json(OLD));
      RETURN NEW;
    ELSIF TG_OP = 'DELETE'
    THEN
      INSERT INTO public.business_data_changes (operation, old_val)
        VALUES (TG_OP, row_to_json(OLD));
      RETURN OLD;
    END IF;
  END;
 
$BODY$;

ALTER FUNCTION mapc.change_trigger()
    OWNER TO postgres;


-- Trigger: data_change_log

-- DROP TRIGGER IF EXISTS data_change_log ON public.business_data_ma_2021;

CREATE TRIGGER data_change_log
    BEFORE INSERT OR DELETE OR UPDATE 
    ON mapc.dataaxle_mapc_2021_reduced_columns
    FOR EACH ROW
    EXECUTE FUNCTION mapc.change_trigger();

