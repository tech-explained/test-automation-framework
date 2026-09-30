-- 006_silver_merge.sql
-- SCD Type 4 merge: applies one bronze file batch to silver.
-- Runs inside a single transaction (one function call = atomic).
--
-- Change detection is hash-based: record_hash = sha256 over the canonical
-- business fields. Identical data reprocessed => no new versions, no history
-- rows: the merge is naturally idempotent.
--
-- Out-of-order protection: a file whose as_of_date is older than a worker's
-- current as_of_date never regresses silver (row counted as skipped_stale).

-- Safe parsers: bad values become NULL instead of aborting the batch.
CREATE OR REPLACE FUNCTION silver.safe_date(p_text TEXT)
RETURNS DATE LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    IF p_text IS NULL OR btrim(p_text) = '' THEN RETURN NULL; END IF;
    RETURN btrim(p_text)::DATE;
EXCEPTION WHEN OTHERS THEN RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION silver.safe_numeric(p_text TEXT)
RETURNS NUMERIC(14,2) LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    IF p_text IS NULL OR btrim(p_text) = '' THEN RETURN NULL; END IF;
    RETURN btrim(p_text)::NUMERIC(14,2);
EXCEPTION WHEN OTHERS THEN RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION silver.apply_bronze_batch(p_file_id UUID)
RETURNS TABLE(workers_upserted INTEGER, history_rows_added INTEGER, workers_skipped_stale INTEGER)
LANGUAGE plpgsql AS $$
DECLARE
    v_as_of      DATE;
    v_file_name  TEXT;
    v_ins        INTEGER := 0;
    v_upd        INTEGER := 0;
    v_history    INTEGER := 0;
    v_stale      INTEGER := 0;
    v_dups       INTEGER := 0;
    v_staged     INTEGER := 0;
BEGIN
    SELECT f.as_of_date, f.file_name
      INTO v_as_of, v_file_name
      FROM ops.file_ingestions f
     WHERE f.file_id = p_file_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'ops.file_ingestions has no row for file_id %', p_file_id;
    END IF;

    -- Duplicate worker rows inside the file (for the audit trail; dedupe keeps LAST line).
    SELECT COALESCE(SUM(c - 1), 0) INTO v_dups
    FROM (
        SELECT COUNT(*) AS c
        FROM bronze.raw_worker_events e
        WHERE e.file_id = p_file_id
        GROUP BY btrim(e.worker_json ->> 'Worker_ID')
        HAVING COUNT(*) > 1
    ) t;

    DROP TABLE IF EXISTS _staged_workers;
    CREATE TEMP TABLE _staged_workers ON COMMIT DROP AS
    WITH ranked AS (
        SELECT
            e.line_no,
            e.worker_json,
            btrim(e.worker_json ->> 'Worker_ID') AS worker_id,
            ROW_NUMBER() OVER (
                PARTITION BY btrim(e.worker_json ->> 'Worker_ID')
                ORDER BY e.line_no DESC
            ) AS rn
        FROM bronze.raw_worker_events e
        WHERE e.file_id = p_file_id
          AND btrim(e.worker_json ->> 'Worker_ID') <> ''
    ),
    deduped AS (
        SELECT line_no, worker_json, worker_id
        FROM ranked WHERE rn = 1
    ),
    parsed AS (
        SELECT
            d.worker_id,
            NULLIF(btrim(d.worker_json ->> 'Employee_ID'), '')       AS employee_id,
            NULLIF(btrim(d.worker_json ->> 'First_Name'), '')        AS first_name,
            NULLIF(btrim(d.worker_json ->> 'Last_Name'), '')         AS last_name,
            NULLIF(btrim(d.worker_json ->> 'Preferred_Name'), '')    AS preferred_name,
            NULLIF(btrim(d.worker_json ->> 'Email'), '')             AS email,
            silver.safe_date(d.worker_json ->> 'Hire_Date')          AS hire_date,
            silver.safe_date(d.worker_json ->> 'Termination_Date')   AS termination_date,
            NULLIF(btrim(d.worker_json ->> 'Worker_Status'), '')     AS worker_status,
            NULLIF(btrim(d.worker_json ->> 'Job_Profile'), '')       AS job_profile,
            NULLIF(btrim(d.worker_json ->> 'Job_Family'), '')        AS job_family,
            NULLIF(btrim(d.worker_json ->> 'Department'), '')        AS department,
            NULLIF(btrim(d.worker_json ->> 'Department_ID'), '')     AS department_id,
            NULLIF(btrim(d.worker_json ->> 'Location'), '')          AS location,
            NULLIF(btrim(d.worker_json ->> 'Country'), '')           AS country,
            NULLIF(btrim(d.worker_json ->> 'Manager_Worker_ID'), '') AS manager_worker_id,
            NULLIF(btrim(d.worker_json ->> 'Cost_Center'), '')       AS cost_center,
            NULLIF(btrim(d.worker_json ->> 'Employment_Type'), '')   AS employment_type,
            NULLIF(btrim(d.worker_json ->> 'Worker_Type'), '')       AS worker_type,
            NULLIF(btrim(d.worker_json ->> 'Time_Type'), '')         AS time_type,
            NULLIF(btrim(d.worker_json ->> 'Compensation_Grade'), '') AS compensation_grade,
            silver.safe_numeric(d.worker_json ->> 'Annual_Salary')   AS annual_salary,
            NULLIF(btrim(d.worker_json ->> 'Currency'), '')          AS currency,
            (d.worker_json - ARRAY[
                'Worker_ID','Employee_ID','First_Name','Last_Name','Preferred_Name',
                'Email','Hire_Date','Termination_Date','Worker_Status','Job_Profile',
                'Job_Family','Department','Department_ID','Location','Country',
                'Manager_Worker_ID','Cost_Center','Employment_Type','Worker_Type',
                'Time_Type','Compensation_Grade','Annual_Salary','Currency'
            ]) AS attributes,
            d.line_no AS source_line_no,
            v_as_of   AS as_of_date
        FROM deduped d
    )
    SELECT
        p.*,
        encode(digest(concat_ws(chr(31),
            coalesce(p.worker_id,'<null>'),
            coalesce(p.employee_id,'<null>'),
            coalesce(p.first_name,'<null>'),
            coalesce(p.last_name,'<null>'),
            coalesce(p.preferred_name,'<null>'),
            coalesce(p.email,'<null>'),
            coalesce(p.hire_date::text,'<null>'),
            coalesce(p.termination_date::text,'<null>'),
            coalesce(p.worker_status,'<null>'),
            coalesce(p.job_profile,'<null>'),
            coalesce(p.job_family,'<null>'),
            coalesce(p.department,'<null>'),
            coalesce(p.department_id,'<null>'),
            coalesce(p.location,'<null>'),
            coalesce(p.country,'<null>'),
            coalesce(p.manager_worker_id,'<null>'),
            coalesce(p.cost_center,'<null>'),
            coalesce(p.employment_type,'<null>'),
            coalesce(p.worker_type,'<null>'),
            coalesce(p.time_type,'<null>'),
            coalesce(p.compensation_grade,'<null>'),
            coalesce(p.annual_salary::text,'<null>'),
            coalesce(p.currency,'<null>'),
            coalesce(p.attributes::text,'<null>')
        ), 'sha256'), 'hex') AS record_hash
    FROM parsed p;

    SELECT COUNT(*) INTO v_staged FROM _staged_workers;

    -- 1. Stale guard: never regress a worker with an older-dated file.
    SELECT COUNT(*) INTO v_stale
    FROM _staged_workers s
    JOIN silver.workers_current c USING (worker_id)
    WHERE s.as_of_date < c.as_of_date;

    -- 2. Advance last_seen for every non-stale staged worker (even hash-equal no-ops).
    UPDATE silver.workers_current c
    SET last_seen_as_of_date = s.as_of_date,
        updated_at = now()
    FROM _staged_workers s
    WHERE c.worker_id = s.worker_id
      AND s.as_of_date >= c.as_of_date
      AND s.as_of_date > c.last_seen_as_of_date;

    -- 3. Archive superseded versions into the history table (SCD Type 4).
    WITH changed AS (
        SELECT s.*
        FROM _staged_workers s
        JOIN silver.workers_current c USING (worker_id)
        WHERE s.as_of_date >= c.as_of_date
          AND s.record_hash IS DISTINCT FROM c.record_hash
    ),
    archived AS (
        INSERT INTO silver.workers_history (
            worker_id, employee_id, first_name, last_name, preferred_name, email,
            hire_date, termination_date, worker_status, job_profile, job_family,
            department, department_id, location, country, manager_worker_id,
            cost_center, employment_type, worker_type, time_type,
            compensation_grade, annual_salary, currency, attributes,
            record_hash, source_file_id, source_line_no,
            as_of_date, last_seen_as_of_date, valid_from, valid_to, version,
            superseded_by_file_id
        )
        SELECT
            c.worker_id, c.employee_id, c.first_name, c.last_name, c.preferred_name, c.email,
            c.hire_date, c.termination_date, c.worker_status, c.job_profile, c.job_family,
            c.department, c.department_id, c.location, c.country, c.manager_worker_id,
            c.cost_center, c.employment_type, c.worker_type, c.time_type,
            c.compensation_grade, c.annual_salary, c.currency, c.attributes,
            c.record_hash, c.source_file_id, c.source_line_no,
            c.as_of_date, c.last_seen_as_of_date, c.valid_from, now(), c.version,
            p_file_id
        FROM silver.workers_current c
        JOIN changed ch ON ch.worker_id = c.worker_id
        RETURNING 1
    )
    SELECT COUNT(*) INTO v_history FROM archived;

    -- 4. Apply the new versions to the current table.
    WITH changed AS (
        SELECT s.*
        FROM _staged_workers s
        JOIN silver.workers_current c USING (worker_id)
        WHERE s.as_of_date >= c.as_of_date
          AND s.record_hash IS DISTINCT FROM c.record_hash
    )
    UPDATE silver.workers_current c
    SET employee_id = ch.employee_id,
        first_name = ch.first_name,
        last_name = ch.last_name,
        preferred_name = ch.preferred_name,
        email = ch.email,
        hire_date = ch.hire_date,
        termination_date = ch.termination_date,
        worker_status = ch.worker_status,
        job_profile = ch.job_profile,
        job_family = ch.job_family,
        department = ch.department,
        department_id = ch.department_id,
        location = ch.location,
        country = ch.country,
        manager_worker_id = ch.manager_worker_id,
        cost_center = ch.cost_center,
        employment_type = ch.employment_type,
        worker_type = ch.worker_type,
        time_type = ch.time_type,
        compensation_grade = ch.compensation_grade,
        annual_salary = ch.annual_salary,
        currency = ch.currency,
        attributes = ch.attributes,
        record_hash = ch.record_hash,
        source_file_id = p_file_id,
        source_line_no = ch.source_line_no,
        as_of_date = ch.as_of_date,
        last_seen_as_of_date = ch.as_of_date,
        valid_from = now(),
        version = c.version + 1,
        updated_at = now()
    FROM changed ch
    WHERE c.worker_id = ch.worker_id;
    GET DIAGNOSTICS v_upd = ROW_COUNT;

    -- 5. Insert brand-new workers.
    INSERT INTO silver.workers_current (
        worker_id, employee_id, first_name, last_name, preferred_name, email,
        hire_date, termination_date, worker_status, job_profile, job_family,
        department, department_id, location, country, manager_worker_id,
        cost_center, employment_type, worker_type, time_type,
        compensation_grade, annual_salary, currency, attributes,
        record_hash, source_file_id, source_line_no,
        as_of_date, last_seen_as_of_date, valid_from, version, updated_at
    )
    SELECT
        s.worker_id, s.employee_id, s.first_name, s.last_name, s.preferred_name, s.email,
        s.hire_date, s.termination_date, s.worker_status, s.job_profile, s.job_family,
        s.department, s.department_id, s.location, s.country, s.manager_worker_id,
        s.cost_center, s.employment_type, s.worker_type, s.time_type,
        s.compensation_grade, s.annual_salary, s.currency, s.attributes,
        s.record_hash, p_file_id, s.source_line_no,
        s.as_of_date, s.as_of_date, now(), 1, now()
    FROM _staged_workers s
    WHERE NOT EXISTS (
        SELECT 1 FROM silver.workers_current c WHERE c.worker_id = s.worker_id
    );
    GET DIAGNOSTICS v_ins = ROW_COUNT;

    INSERT INTO ops.audit_log (actor, action, entity, entity_id, details)
    VALUES ('silver.merge', 'silver.merge.completed', 'file', p_file_id::text,
            jsonb_build_object(
                'file_name', v_file_name,
                'staged_workers', v_staged,
                'duplicates_in_file', v_dups,
                'workers_inserted', v_ins,
                'workers_updated', v_upd,
                'history_rows_added', v_history,
                'workers_skipped_stale', v_stale
            ));

    workers_upserted := v_ins + v_upd;
    history_rows_added := v_history;
    workers_skipped_stale := v_stale;
    RETURN NEXT;
END $$;

-- Non-blocking data-quality checks over one bronze file. Findings land in
-- ops.file_ingestions.dq_warnings; they never fail the load.
CREATE OR REPLACE FUNCTION ops.compute_dq_warnings(p_file_id UUID)
RETURNS JSONB LANGUAGE plpgsql AS $$
DECLARE
    v_result JSONB;
BEGIN
    WITH stats AS (
        SELECT
            COUNT(*) AS n,
            COUNT(*) FILTER (WHERE NULLIF(btrim(worker_json ->> 'Email'), '') IS NULL) AS null_email,
            COUNT(*) FILTER (WHERE NULLIF(btrim(worker_json ->> 'Department'), '') IS NULL) AS null_department,
            COUNT(*) FILTER (WHERE NULLIF(btrim(worker_json ->> 'Hire_Date'), '') IS NOT NULL
                             AND silver.safe_date(worker_json ->> 'Hire_Date') IS NULL) AS bad_hire_date,
            COUNT(*) FILTER (WHERE NULLIF(btrim(worker_json ->> 'Annual_Salary'), '') IS NOT NULL
                             AND silver.safe_numeric(worker_json ->> 'Annual_Salary') IS NULL) AS bad_salary,
            COUNT(*) FILTER (WHERE NULLIF(btrim(worker_json ->> 'Worker_Status'), '') IS NULL) AS null_status
        FROM bronze.raw_worker_events
        WHERE file_id = p_file_id
    ),
    dups AS (
        SELECT COUNT(*) AS dup_extra_rows FROM (
            SELECT 1
            FROM bronze.raw_worker_events
            WHERE file_id = p_file_id
            GROUP BY btrim(worker_json ->> 'Worker_ID')
            HAVING COUNT(*) > 1
        ) t
    )
    SELECT COALESCE(jsonb_agg(f ORDER BY f ->> 'check'), '[]'::jsonb) INTO v_result
    FROM (
        SELECT jsonb_build_object('check','empty_file','severity','info',
                                  'message','file contained zero loadable lines') AS f
        FROM stats WHERE n = 0
        UNION ALL
        SELECT jsonb_build_object('check','null_email_ratio','severity','warning',
                                  'ratio', ROUND(null_email::numeric / NULLIF(n,0), 4),
                                  'message', null_email || ' of ' || n || ' workers missing email')
        FROM stats WHERE n > 0 AND null_email::float / n >= 0.2
        UNION ALL
        SELECT jsonb_build_object('check','null_department_ratio','severity','warning',
                                  'ratio', ROUND(null_department::numeric / NULLIF(n,0), 4),
                                  'message', null_department || ' of ' || n || ' workers missing department')
        FROM stats WHERE n > 0 AND null_department::float / n >= 0.2
        UNION ALL
        SELECT jsonb_build_object('check','invalid_hire_date','severity','warning',
                                  'count', bad_hire_date,
                                  'message', bad_hire_date || ' workers with unparseable Hire_Date (loaded as NULL)')
        FROM stats WHERE bad_hire_date > 0
        UNION ALL
        SELECT jsonb_build_object('check','invalid_annual_salary','severity','warning',
                                  'count', bad_salary,
                                  'message', bad_salary || ' workers with unparseable Annual_Salary (loaded as NULL)')
        FROM stats WHERE bad_salary > 0
        UNION ALL
        SELECT jsonb_build_object('check','null_worker_status','severity','warning',
                                  'count', null_status,
                                  'message', null_status || ' workers missing Worker_Status')
        FROM stats WHERE null_status > 0
        UNION ALL
        SELECT jsonb_build_object('check','duplicate_workers_in_file','severity','info',
                                  'count', dup_extra_rows,
                                  'message', dup_extra_rows || ' duplicate worker row(s); last line wins')
        FROM dups WHERE dup_extra_rows > 0
    ) findings(f);

    RETURN v_result;
END $$;
