-- 0.1 Ensure pgcrypto is available for gen_random_uuid, if you use it
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- 0.2 Unified function to update 'updated_at' column
CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- 1.1 Create the table if it doesn't exist
CREATE TABLE IF NOT EXISTS patient_allergies (
    id SERIAL PRIMARY KEY,
    start_date DATE NOT NULL,
    stop_date DATE,
    patient_id UUID NOT NULL,
    encounter_id UUID NOT NULL,
    code VARCHAR(20) NOT NULL,
    coding_system VARCHAR(50) NOT NULL,
    description TEXT NOT NULL,
    type VARCHAR(20) NOT NULL CHECK (type IN ('allergy', 'intolerance')),
    category VARCHAR(20) NOT NULL CHECK (category IN ('environment', 'medication', 'food')),
    reaction1_code VARCHAR(20),
    reaction1_description TEXT,
    reaction1_severity VARCHAR(10) CHECK (reaction1_severity IN ('MILD', 'MODERATE', 'SEVERE')),
    reaction2_code VARCHAR(20),
    reaction2_description TEXT,
    reaction2_severity VARCHAR(10) CHECK (reaction2_severity IN ('MILD', 'MODERATE', 'SEVERE')),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT valid_date_range CHECK (stop_date IS NULL OR stop_date >= start_date),
    CONSTRAINT reaction1_completeness CHECK (
        (reaction1_code IS NULL AND reaction1_description IS NULL AND reaction1_severity IS NULL)
        OR
        (reaction1_code IS NOT NULL AND reaction1_description IS NOT NULL AND reaction1_severity IS NOT NULL)
    ),
    CONSTRAINT reaction2_completeness CHECK (
        (reaction2_code IS NULL AND reaction2_description IS NULL AND reaction2_severity IS NULL)
        OR
        (reaction2_code IS NOT NULL AND reaction2_description IS NOT NULL AND reaction2_severity IS NOT NULL)
    )
);

-- 1.2 Indexes
DO $$
BEGIN
    IF NOT EXISTS (
       SELECT 1 FROM pg_class c 
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relname = 'idx_patient_allergies_patient'
         AND n.nspname = current_schema()
    ) THEN
       CREATE INDEX idx_patient_allergies_patient 
         ON patient_allergies(patient_id);
    END IF;
    
    IF NOT EXISTS (
       SELECT 1 FROM pg_class c 
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relname = 'idx_patient_allergies_dates'
         AND n.nspname = current_schema()
    ) THEN
       CREATE INDEX idx_patient_allergies_dates 
         ON patient_allergies(start_date, stop_date);
    END IF;
END;
$$;

-- 1.3 Trigger to update 'updated_at'
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger 
        WHERE tgname = 'update_patient_allergies_updated_at'
          AND tgrelid = 'patient_allergies'::regclass
    ) THEN
        CREATE TRIGGER update_patient_allergies_updated_at
            BEFORE UPDATE ON patient_allergies
            FOR EACH ROW
            EXECUTE FUNCTION update_timestamp();
    END IF;
END;
$$;
CREATE TABLE IF NOT EXISTS careplans (
    id UUID PRIMARY KEY,
    start_date DATE NOT NULL,
    stop_date DATE,
    patient_id UUID NOT NULL,
    encounter_id UUID NOT NULL,
    code VARCHAR(20) NOT NULL,
    description TEXT NOT NULL,
    reason_code VARCHAR(20),
    reason_description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT valid_date_range CHECK (stop_date IS NULL OR stop_date >= start_date),
    CONSTRAINT reason_completeness CHECK (
        (reason_code IS NULL AND reason_description IS NULL) OR
        (reason_code IS NOT NULL AND reason_description IS NOT NULL)
    )
);

-- Indexes
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c 
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = 'idx_careplans_patient'
          AND n.nspname = current_schema()
    ) THEN
        CREATE INDEX idx_careplans_patient ON careplans(patient_id);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_class c 
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = 'idx_careplans_encounter'
          AND n.nspname = current_schema()
    ) THEN
        CREATE INDEX idx_careplans_encounter ON careplans(encounter_id);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_class c 
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = 'idx_careplans_dates'
          AND n.nspname = current_schema()
    ) THEN
        CREATE INDEX idx_careplans_dates ON careplans(start_date, stop_date);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_class c 
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = 'idx_careplans_code'
          AND n.nspname = current_schema()
    ) THEN
        CREATE INDEX idx_careplans_code ON careplans(code);
    END IF;
END;
$$;

-- Trigger
DO $$
BEGIN
    IF NOT EXISTS (
       SELECT 1 FROM pg_trigger 
       WHERE tgname = 'update_careplans_updated_at'
         AND tgrelid = 'careplans'::regclass
    ) THEN
        CREATE TRIGGER update_careplans_updated_at
            BEFORE UPDATE ON careplans
            FOR EACH ROW
            EXECUTE FUNCTION update_timestamp();
    END IF;
END;
$$;
CREATE TABLE IF NOT EXISTS conditions (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    start_date DATE NOT NULL,
    stop_date DATE,
    patient_id UUID NOT NULL,
    encounter_id UUID NOT NULL,
    system VARCHAR(50) NOT NULL,
    code VARCHAR(20) NOT NULL,
    description TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT valid_date_range CHECK (stop_date IS NULL OR stop_date >= start_date),
    CONSTRAINT valid_coding_system CHECK (system IN ('SNOMED-CT', 'ICD-10', 'ICD-9-CM')),
    CONSTRAINT unique_patient_encounter_condition UNIQUE (patient_id, encounter_id, code)
);

-- Indexes
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = 'idx_conditions_patient'
          AND n.nspname = current_schema()
    ) THEN
        CREATE INDEX idx_conditions_patient ON conditions(patient_id);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = 'idx_conditions_encounter'
          AND n.nspname = current_schema()
    ) THEN
        CREATE INDEX idx_conditions_encounter ON conditions(encounter_id);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = 'idx_conditions_dates'
          AND n.nspname = current_schema()
    ) THEN
        CREATE INDEX idx_conditions_dates ON conditions(start_date, stop_date);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = 'idx_conditions_code'
          AND n.nspname = current_schema()
    ) THEN
        CREATE INDEX idx_conditions_code ON conditions(code);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = 'idx_conditions_system_code'
          AND n.nspname = current_schema()
    ) THEN
        CREATE INDEX idx_conditions_system_code ON conditions(system, code);
    END IF;
END;
$$;

-- Trigger
DO $$
BEGIN
    IF NOT EXISTS (
       SELECT 1 FROM pg_trigger 
       WHERE tgname = 'update_conditions_updated_at'
         AND tgrelid = 'conditions'::regclass
    ) THEN
        CREATE TRIGGER update_conditions_updated_at
            BEFORE UPDATE ON conditions
            FOR EACH ROW
            EXECUTE FUNCTION update_timestamp();
    END IF;
END;
$$;
CREATE TABLE IF NOT EXISTS devices (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    start_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    stop_timestamp TIMESTAMP WITH TIME ZONE,
    patient_id UUID NOT NULL,
    encounter_id UUID NOT NULL,
    code VARCHAR(20) NOT NULL,
    description TEXT NOT NULL,
    udi TEXT NOT NULL,
    udi_di VARCHAR(50),
    udi_manufacture_date DATE,
    udi_expiration_date DATE,
    udi_lot_number VARCHAR(50),
    udi_serial_number VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT valid_timestamp_range CHECK (
        stop_timestamp IS NULL OR stop_timestamp >= start_timestamp
    ),
    CONSTRAINT valid_udi CHECK (
        -- You might need double-escaping if parentheses are required
        udi ~ '^\(01\)[0-9]+\(11\)[0-9]+\(17\)[0-9]+\(10\)[0-9]+\(21\)[0-9]+'
    )
);

-- Parsing function for UDI
CREATE OR REPLACE FUNCTION parse_udi()
RETURNS TRIGGER AS $$
DECLARE
    di_match TEXT;
    mfg_date_match TEXT;
    exp_date_match TEXT;
    lot_match TEXT;
    serial_match TEXT;
BEGIN
    -- Extract Device Identifier (01)
    di_match := substring(NEW.udi from '\(01\)([0-9]+)');
    IF di_match IS NOT NULL THEN
        NEW.udi_di := di_match;
    END IF;

    -- Extract and parse manufacture date (11)
    mfg_date_match := substring(NEW.udi from '\(11\)([0-9]{6})');
    IF mfg_date_match IS NOT NULL THEN
        NEW.udi_manufacture_date := to_date(mfg_date_match, 'YYMMDD');
    END IF;

    -- Extract and parse expiration date (17)
    exp_date_match := substring(NEW.udi from '\(17\)([0-9]{6})');
    IF exp_date_match IS NOT NULL THEN
        NEW.udi_expiration_date := to_date(exp_date_match, 'YYMMDD');
    END IF;

    -- Extract lot number (10)
    lot_match := substring(NEW.udi from '\(10\)([0-9]+)');
    IF lot_match IS NOT NULL THEN
        NEW.udi_lot_number := lot_match;
    END IF;

    -- Extract serial number (21)
    serial_match := substring(NEW.udi from '\(21\)([0-9]+)');
    IF serial_match IS NOT NULL THEN
        NEW.udi_serial_number := serial_match;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to parse UDI before insert/update
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'parse_udi_components'
          AND tgrelid = 'devices'::regclass
    ) THEN
        CREATE TRIGGER parse_udi_components
            BEFORE INSERT OR UPDATE ON devices
            FOR EACH ROW
            EXECUTE FUNCTION parse_udi();
    END IF;
END;
$$;

-- Indexes
DO $$
BEGIN
    IF NOT EXISTS (
       SELECT 1 FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relname = 'idx_devices_patient'
         AND n.nspname = current_schema()
    ) THEN
       CREATE INDEX idx_devices_patient ON devices(patient_id);
    END IF;
    
    IF NOT EXISTS (
       SELECT 1 FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relname = 'idx_devices_encounter'
         AND n.nspname = current_schema()
    ) THEN
       CREATE INDEX idx_devices_encounter ON devices(encounter_id);
    END IF;

    IF NOT EXISTS (
       SELECT 1 FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relname = 'idx_devices_timestamps'
         AND n.nspname = current_schema()
    ) THEN
       CREATE INDEX idx_devices_timestamps
         ON devices(start_timestamp, stop_timestamp);
    END IF;

    IF NOT EXISTS (
       SELECT 1 FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relname = 'idx_devices_code'
         AND n.nspname = current_schema()
    ) THEN
       CREATE INDEX idx_devices_code ON devices(code);
    END IF;

    IF NOT EXISTS (
       SELECT 1 FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relname = 'idx_devices_udi_di'
         AND n.nspname = current_schema()
    ) THEN
       CREATE INDEX idx_devices_udi_di ON devices(udi_di);
    END IF;

    IF NOT EXISTS (
       SELECT 1 FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relname = 'idx_devices_lot'
         AND n.nspname = current_schema()
    ) THEN
       CREATE INDEX idx_devices_lot ON devices(udi_lot_number);
    END IF;

    IF NOT EXISTS (
       SELECT 1 FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relname = 'idx_devices_serial'
         AND n.nspname = current_schema()
    ) THEN
       CREATE INDEX idx_devices_serial ON devices(udi_serial_number);
    END IF;
END;
$$;

-- Trigger for updated_at
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'update_devices_updated_at'
          AND tgrelid = 'devices'::regclass
    ) THEN
        CREATE TRIGGER update_devices_updated_at
            BEFORE UPDATE ON devices
            FOR EACH ROW
            EXECUTE FUNCTION update_timestamp();
    END IF;
END;
$$;
CREATE TABLE IF NOT EXISTS medications (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    start_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    stop_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    patient_id UUID NOT NULL,
    payer_id UUID NOT NULL,
    encounter_id UUID NOT NULL,
    code VARCHAR(20) NOT NULL,
    description TEXT NOT NULL,
    base_cost DECIMAL(10,2) NOT NULL,
    payer_coverage DECIMAL(10,2) NOT NULL,
    dispenses INTEGER NOT NULL,
    total_cost DECIMAL(10,2) NOT NULL,
    reason_code VARCHAR(20),
    reason_description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT valid_timestamp_range CHECK (stop_timestamp >= start_timestamp),
    CONSTRAINT valid_costs CHECK (
        base_cost >= 0 AND 
        payer_coverage >= 0 AND 
        total_cost >= 0 AND
        dispenses > 0
    ),
    CONSTRAINT total_cost_check CHECK (
        round(total_cost::numeric, 2) = round((base_cost * dispenses)::numeric, 2)
    ),
    CONSTRAINT reason_completeness CHECK (
        (reason_code IS NULL AND reason_description IS NULL) OR
        (reason_code IS NOT NULL AND reason_description IS NOT NULL)
    )
);

-- Indexes
DO $$
BEGIN
    IF NOT EXISTS (
       SELECT 1 FROM pg_class c 
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relname = 'idx_medications_patient'
         AND n.nspname = current_schema()
    ) THEN
       CREATE INDEX idx_medications_patient ON medications(patient_id);
    END IF;

    IF NOT EXISTS (
       SELECT 1 FROM pg_class c 
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relname = 'idx_medications_payer'
         AND n.nspname = current_schema()
    ) THEN
       CREATE INDEX idx_medications_payer ON medications(payer_id);
    END IF;

    IF NOT EXISTS (
       SELECT 1 FROM pg_class c 
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relname = 'idx_medications_encounter'
         AND n.nspname = current_schema()
    ) THEN
       CREATE INDEX idx_medications_encounter ON medications(encounter_id);
    END IF;

    IF NOT EXISTS (
       SELECT 1 FROM pg_class c 
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relname = 'idx_medications_code'
         AND n.nspname = current_schema()
    ) THEN
       CREATE INDEX idx_medications_code ON medications(code);
    END IF;

    IF NOT EXISTS (
       SELECT 1 FROM pg_class c 
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relname = 'idx_medications_dates'
         AND n.nspname = current_schema()
    ) THEN
       CREATE INDEX idx_medications_dates 
         ON medications(start_timestamp, stop_timestamp);
    END IF;
    
    IF NOT EXISTS (
       SELECT 1 FROM pg_class c 
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relname = 'idx_medications_reason'
         AND n.nspname = current_schema()
    ) THEN
       CREATE INDEX idx_medications_reason ON medications(reason_code);
    END IF;

    -- For time-range queries: gist index
    IF NOT EXISTS (
       SELECT 1 FROM pg_class c 
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relname = 'idx_medications_timerange'
         AND n.nspname = current_schema()
    ) THEN
       CREATE INDEX idx_medications_timerange 
         ON medications USING gist (tstzrange(start_timestamp, stop_timestamp));
    END IF;
END;
$$;

-- Trigger
DO $$
BEGIN
    IF NOT EXISTS (
       SELECT 1 FROM pg_trigger 
       WHERE tgname = 'update_medications_updated_at'
         AND tgrelid = 'medications'::regclass
    ) THEN
        CREATE TRIGGER update_medications_updated_at
            BEFORE UPDATE ON medications
            FOR EACH ROW
            EXECUTE FUNCTION update_timestamp();
    END IF;
END;
$$;

-- Example views (cannot do IF NOT EXISTS on a materialized view)
-- If the view name already exists, remove or rename it manually, or skip.
CREATE OR REPLACE VIEW active_medications AS
SELECT *
FROM medications
WHERE stop_timestamp > CURRENT_TIMESTAMP;

-- Recreate the materialized view
DROP MATERIALIZED VIEW IF EXISTS medication_monthly_costs;
CREATE MATERIALIZED VIEW medication_monthly_costs AS
SELECT 
    date_trunc('month', start_timestamp) AS month,
    patient_id,
    payer_id,
    SUM(total_cost) as total_cost,
    SUM(payer_coverage) as total_coverage,
    SUM(total_cost - payer_coverage) as patient_responsibility,
    COUNT(*) as prescription_count
FROM medications
GROUP BY 
    date_trunc('month', start_timestamp),
    patient_id,
    payer_id
WITH DATA;

CREATE INDEX IF NOT EXISTS idx_med_monthly_costs_month 
  ON medication_monthly_costs(month);
CREATE INDEX IF NOT EXISTS idx_med_monthly_costs_patient 
  ON medication_monthly_costs(patient_id);
CREATE INDEX IF NOT EXISTS idx_med_monthly_costs_payer 
  ON medication_monthly_costs(payer_id);
CREATE TABLE IF NOT EXISTS patient_expenses (
    patient_id UUID NOT NULL,
    year_date TIMESTAMP WITH TIME ZONE NOT NULL,
    payer_id UUID NOT NULL,
    healthcare_expenses DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    insurance_costs DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    covered_costs DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    total_patient_responsibility DECIMAL(12,2) 
        GENERATED ALWAYS AS (healthcare_expenses + insurance_costs - covered_costs) STORED,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (patient_id, year_date),
    CONSTRAINT non_negative_amounts CHECK (
        healthcare_expenses >= 0 AND
        insurance_costs >= 0 AND
        covered_costs >= 0
    )
);

-- Indexes
DO $$
BEGIN
    IF NOT EXISTS (
       SELECT 1 FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relname = 'idx_patient_expenses_payer'
         AND n.nspname = current_schema()
    ) THEN
       CREATE INDEX idx_patient_expenses_payer ON patient_expenses(payer_id);
    END IF;

    IF NOT EXISTS (
       SELECT 1 FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relname = 'idx_patient_expenses_year'
         AND n.nspname = current_schema()
    ) THEN
       CREATE INDEX idx_patient_expenses_year ON patient_expenses(year_date);
    END IF;

    IF NOT EXISTS (
       SELECT 1 FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relname = 'idx_patient_expenses_with_costs'
         AND n.nspname = current_schema()
    ) THEN
       CREATE INDEX idx_patient_expenses_with_costs 
         ON patient_expenses(patient_id, year_date)
         WHERE healthcare_expenses > 0 OR covered_costs > 0;
    END IF;
END;
$$;

-- Trigger
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'update_patient_expenses_updated_at'
          AND tgrelid = 'patient_expenses'::regclass
    ) THEN
        CREATE TRIGGER update_patient_expenses_updated_at
            BEFORE UPDATE ON patient_expenses
            FOR EACH ROW
            EXECUTE FUNCTION update_timestamp();
    END IF;
END;
$$;

-- Example materialized view
DROP MATERIALIZED VIEW IF EXISTS patient_annual_expense_summary;
CREATE MATERIALIZED VIEW patient_annual_expense_summary AS
SELECT 
    patient_id,
    date_trunc('year', year_date) AS year,
    payer_id,
    SUM(healthcare_expenses) as total_healthcare_expenses,
    SUM(insurance_costs) as total_insurance_costs,
    SUM(covered_costs) as total_covered_costs,
    SUM(healthcare_expenses + insurance_costs - covered_costs) as total_patient_responsibility
FROM patient_expenses
GROUP BY 
    patient_id,
    date_trunc('year', year_date),
    payer_id
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_patient_annual_summary_pk 
  ON patient_annual_expense_summary(patient_id, year);
CREATE TABLE IF NOT EXISTS encounters (
    id UUID PRIMARY KEY,
    start_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    stop_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    patient_id UUID NOT NULL,
    organization_id UUID NOT NULL,
    provider_id UUID NOT NULL,
    payer_id UUID NOT NULL,
    encounter_class VARCHAR(50) NOT NULL,
    code VARCHAR(20) NOT NULL,
    description TEXT NOT NULL,
    base_encounter_cost DECIMAL(10,2) NOT NULL,
    total_claim_cost DECIMAL(10,2) NOT NULL,
    payer_coverage DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    reason_code VARCHAR(20),
    reason_description TEXT,
    patient_responsibility DECIMAL(10,2) 
        GENERATED ALWAYS AS (total_claim_cost - payer_coverage) STORED,
    encounter_duration INTERVAL
        GENERATED ALWAYS AS (stop_timestamp - start_timestamp) STORED,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT valid_timestamp_range CHECK (stop_timestamp >= start_timestamp),
    CONSTRAINT valid_costs CHECK (
        base_encounter_cost >= 0 AND 
        total_claim_cost >= 0 AND 
        payer_coverage >= 0 AND
        total_claim_cost >= base_encounter_cost AND
        payer_coverage <= total_claim_cost
    ),
    CONSTRAINT valid_encounter_class CHECK (
        encounter_class IN ('wellness', 'ambulatory', 'outpatient', 'emergency', 'urgent')
    ),
    CONSTRAINT reason_completeness CHECK (
        (reason_code IS NULL AND reason_description IS NULL) OR 
        (reason_code IS NOT NULL AND reason_description IS NOT NULL)
    )
);

-- Indexes
DO $$
BEGIN
    IF NOT EXISTS (
       SELECT 1 FROM pg_class c 
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relname = 'idx_encounters_patient'
         AND n.nspname = current_schema()
    ) THEN
       CREATE INDEX idx_encounters_patient ON encounters(patient_id);
    END IF;

    IF NOT EXISTS (
       SELECT 1 FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relname = 'idx_encounters_organization'
         AND n.nspname = current_schema()
    ) THEN
       CREATE INDEX idx_encounters_organization ON encounters(organization_id);
    END IF;

    IF NOT EXISTS (
       SELECT 1 FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relname = 'idx_encounters_provider'
         AND n.nspname = current_schema()
    ) THEN
       CREATE INDEX idx_encounters_provider ON encounters(provider_id);
    END IF;

    IF NOT EXISTS (
       SELECT 1 FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relname = 'idx_encounters_payer'
         AND n.nspname = current_schema()
    ) THEN
       CREATE INDEX idx_encounters_payer ON encounters(payer_id);
    END IF;

    IF NOT EXISTS (
       SELECT 1 FROM pg_class c 
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relname = 'idx_encounters_dates'
         AND n.nspname = current_schema()
    ) THEN
       CREATE INDEX idx_encounters_dates 
         ON encounters(start_timestamp, stop_timestamp);
    END IF;

    IF NOT EXISTS (
       SELECT 1 FROM pg_class c 
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relname = 'idx_encounters_class'
         AND n.nspname = current_schema()
    ) THEN
       CREATE INDEX idx_encounters_class ON encounters(encounter_class);
    END IF;

    IF NOT EXISTS (
       SELECT 1 FROM pg_class c 
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relname = 'idx_encounters_code'
         AND n.nspname = current_schema()
    ) THEN
       CREATE INDEX idx_encounters_code ON encounters(code);
    END IF;

    IF NOT EXISTS (
       SELECT 1 FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relname = 'idx_encounters_timerange'
         AND n.nspname = current_schema()
    ) THEN
       CREATE INDEX idx_encounters_timerange
         ON encounters USING gist (tstzrange(start_timestamp, stop_timestamp));
    END IF;
END;
$$;

-- Trigger
DO $$
BEGIN
    IF NOT EXISTS (
       SELECT 1 FROM pg_trigger 
       WHERE tgname = 'update_encounters_updated_at'
         AND tgrelid = 'encounters'::regclass
    ) THEN
        CREATE TRIGGER update_encounters_updated_at
            BEFORE UPDATE ON encounters
            FOR EACH ROW
            EXECUTE FUNCTION update_timestamp();
    END IF;
END;
$$;

-- Views (DROP first if they already exist, or rename them)
CREATE OR REPLACE VIEW active_encounters AS
SELECT *
FROM encounters
WHERE start_timestamp <= CURRENT_TIMESTAMP
  AND stop_timestamp >= CURRENT_TIMESTAMP;

DROP MATERIALIZED VIEW IF EXISTS monthly_encounter_stats;
CREATE MATERIALIZED VIEW monthly_encounter_stats AS
SELECT 
    date_trunc('month', start_timestamp) AS month,
    organization_id,
    encounter_class,
    COUNT(*) as encounter_count,
    AVG(stop_timestamp - start_timestamp) as avg_duration,
    SUM(total_claim_cost) as total_claims,
    SUM(payer_coverage) as total_coverage,
    SUM(total_claim_cost - payer_coverage) as total_patient_responsibility
FROM encounters
GROUP BY 
    date_trunc('month', start_timestamp),
    organization_id,
    encounter_class
WITH DATA;

CREATE INDEX IF NOT EXISTS idx_monthly_stats_month 
  ON monthly_encounter_stats(month);
CREATE INDEX IF NOT EXISTS idx_monthly_stats_org 
  ON monthly_encounter_stats(organization_id);
CREATE INDEX IF NOT EXISTS idx_monthly_stats_class 
  ON monthly_encounter_stats(encounter_class);
CREATE TABLE IF NOT EXISTS claims_transactions (
    id UUID PRIMARY KEY,
    claim_id UUID NOT NULL,
    charge_id INTEGER NOT NULL,
    patient_id UUID NOT NULL,
    place_of_service_id UUID,
    appointment_id UUID,
    patient_insurance_id UUID,
    fee_schedule_id INTEGER,
    provider_id UUID,
    supervising_provider_id UUID,
    department_id INTEGER,
    type VARCHAR(20) NOT NULL CHECK (
        type IN ('CHARGE', 'PAYMENT', 'ADJUSTMENT', 'TRANSFERIN', 'TRANSFEROUT')
    ),
    amount DECIMAL(10,2),
    payment_method VARCHAR(20),
    from_date TIMESTAMP WITH TIME ZONE NOT NULL,
    to_date TIMESTAMP WITH TIME ZONE,
    procedure_code VARCHAR(20),
    modifier1 VARCHAR(20),
    modifier2 VARCHAR(20),
    diagnosis_ref1 INTEGER,
    diagnosis_ref2 INTEGER,
    diagnosis_ref3 INTEGER,
    diagnosis_ref4 INTEGER,
    units INTEGER,
    unit_amount DECIMAL(10,2),
    transfer_out_id INTEGER,
    transfer_type CHAR(1),
    notes TEXT,
    line_note TEXT,
    payments DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    adjustments DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    transfers DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    outstanding DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT valid_date_range CHECK (
        to_date IS NULL OR to_date >= from_date
    ),
    CONSTRAINT valid_amounts CHECK (
        payments >= 0 AND
        adjustments >= 0 AND
        transfers >= 0 AND
        outstanding >= 0
    ),
    CONSTRAINT payment_method_check CHECK (
        (type != 'PAYMENT' AND payment_method IS NULL)
        OR
        (type = 'PAYMENT' AND payment_method IN ('CASH', 'CHECK', 'CC'))
    ),
    CONSTRAINT amount_requirements CHECK (
        (type IN ('CHARGE', 'TRANSFERIN') AND amount IS NOT NULL)
        OR
        (type = 'PAYMENT' AND amount IS NULL)
        OR
        (type = 'TRANSFEROUT' AND amount IS NULL)
    ),
    CONSTRAINT units_check CHECK (units > 0)
);

-- Indexes
DO $$
BEGIN
    IF NOT EXISTS (
       SELECT 1 FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relname = 'idx_claims_trans_claim'
         AND n.nspname = current_schema()
    ) THEN
       CREATE INDEX idx_claims_trans_claim ON claims_transactions(claim_id);
    END IF;

    IF NOT EXISTS (
       SELECT 1 FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relname = 'idx_claims_trans_patient'
         AND n.nspname = current_schema()
    ) THEN
       CREATE INDEX idx_claims_trans_patient ON claims_transactions(patient_id);
    END IF;

    IF NOT EXISTS (
       SELECT 1 FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relname = 'idx_claims_trans_appointment'
         AND n.nspname = current_schema()
    ) THEN
       CREATE INDEX idx_claims_trans_appointment ON claims_transactions(appointment_id);
    END IF;

    IF NOT EXISTS (
       SELECT 1 FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relname = 'idx_claims_trans_provider'
         AND n.nspname = current_schema()
    ) THEN
       CREATE INDEX idx_claims_trans_provider ON claims_transactions(provider_id);
    END IF;

    IF NOT EXISTS (
       SELECT 1 FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relname = 'idx_claims_trans_dates'
         AND n.nspname = current_schema()
    ) THEN
       CREATE INDEX idx_claims_trans_dates
         ON claims_transactions(from_date, to_date);
    END IF;

    IF NOT EXISTS (
       SELECT 1 FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relname = 'idx_claims_trans_type'
         AND n.nspname = current_schema()
    ) THEN
       CREATE INDEX idx_claims_trans_type ON claims_transactions(type);
    END IF;

    IF NOT EXISTS (
       SELECT 1 FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relname = 'idx_claims_trans_procedure'
         AND n.nspname = current_schema()
    ) THEN
       CREATE INDEX idx_claims_trans_procedure ON claims_transactions(procedure_code);
    END IF;

    -- Partial indexes
    IF NOT EXISTS (
       SELECT 1 FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relname = 'idx_claims_trans_payments'
         AND n.nspname = current_schema()
    ) THEN
       CREATE INDEX idx_claims_trans_payments
         ON claims_transactions(claim_id, amount, payment_method) 
         WHERE type = 'PAYMENT';
    END IF;

    IF NOT EXISTS (
       SELECT 1 FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relname = 'idx_claims_trans_charges'
         AND n.nspname = current_schema()
    ) THEN
       CREATE INDEX idx_claims_trans_charges
         ON claims_transactions(claim_id, amount)
         WHERE type = 'CHARGE';
    END IF;
END;
$$;

-- Trigger
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'update_claims_transactions_updated_at'
          AND tgrelid = 'claims_transactions'::regclass
    ) THEN
        CREATE TRIGGER update_claims_transactions_updated_at
            BEFORE UPDATE ON claims_transactions
            FOR EACH ROW
            EXECUTE FUNCTION update_timestamp();
    END IF;
END;
$$;

-- Views
CREATE OR REPLACE VIEW claim_balances AS
SELECT 
    claim_id,
    SUM(CASE WHEN type = 'CHARGE' THEN amount ELSE 0 END) as total_charges,
    SUM(payments) as total_payments,
    SUM(adjustments) as total_adjustments,
    SUM(transfers) as total_transfers,
    SUM(outstanding) as total_outstanding
FROM claims_transactions
GROUP BY claim_id;

DROP MATERIALIZED VIEW IF EXISTS daily_transaction_summary;
CREATE MATERIALIZED VIEW daily_transaction_summary AS
SELECT 
    date_trunc('day', from_date) AS transaction_date,
    type,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    SUM(payments) as total_payments,
    SUM(adjustments) as total_adjustments,
    SUM(transfers) as total_transfers,
    SUM(outstanding) as total_outstanding
FROM claims_transactions
GROUP BY 
    date_trunc('day', from_date),
    type
WITH DATA;

CREATE INDEX IF NOT EXISTS idx_daily_summary_date 
  ON daily_transaction_summary(transaction_date);
CREATE TABLE IF NOT EXISTS claims (
    id UUID PRIMARY KEY,
    patient_id UUID NOT NULL,
    provider_id UUID NOT NULL,
    supervising_provider_id UUID,
    referring_provider_id UUID,
    primary_patient_insurance_id UUID,
    secondary_patient_insurance_id UUID,
    department_id INTEGER NOT NULL,
    patient_department_id INTEGER NOT NULL,
    diagnosis1 VARCHAR(20),
    diagnosis2 VARCHAR(20),
    diagnosis3 VARCHAR(20),
    diagnosis4 VARCHAR(20),
    diagnosis5 VARCHAR(20),
    diagnosis6 VARCHAR(20),
    diagnosis7 VARCHAR(20),
    diagnosis8 VARCHAR(20),
    appointment_id UUID,
    current_illness_date TIMESTAMP WITH TIME ZONE,
    service_date TIMESTAMP WITH TIME ZONE NOT NULL,
    status1 VARCHAR(20) NOT NULL,
    status2 VARCHAR(20) NOT NULL,
    status_p VARCHAR(20) NOT NULL,
    outstanding1 DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    outstanding2 DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    outstanding_p DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    last_billed_date1 TIMESTAMP WITH TIME ZONE,
    last_billed_date2 TIMESTAMP WITH TIME ZONE,
    last_billed_date_p TIMESTAMP WITH TIME ZONE,
    healthcare_claim_type_id1 INTEGER,
    healthcare_claim_type_id2 INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT valid_status CHECK (
        status1 IN ('OPEN', 'CLOSED', 'PENDING') AND
        status2 IN ('OPEN', 'CLOSED', 'PENDING') AND
        status_p IN ('OPEN', 'CLOSED', 'PENDING')
    ),
    CONSTRAINT valid_amounts CHECK (
        outstanding1 >= 0 AND
        outstanding2 >= 0 AND
        outstanding_p >= 0
    ),
    CONSTRAINT valid_dates CHECK (
        service_date >= current_illness_date OR current_illness_date IS NULL
    ),
    CONSTRAINT matching_departments CHECK (
        department_id = patient_department_id
    )
);

-- Indexes
DO $$
BEGIN
    IF NOT EXISTS (
      SELECT 1 FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE c.relname = 'idx_claims_patient'
        AND n.nspname = current_schema()
    ) THEN
      CREATE INDEX idx_claims_patient ON claims(patient_id);
    END IF;

    IF NOT EXISTS (
      SELECT 1 FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE c.relname = 'idx_claims_provider'
        AND n.nspname = current_schema()
    ) THEN
      CREATE INDEX idx_claims_provider ON claims(provider_id);
    END IF;

    IF NOT EXISTS (
      SELECT 1 FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE c.relname = 'idx_claims_appointment'
        AND n.nspname = current_schema()
    ) THEN
      CREATE INDEX idx_claims_appointment ON claims(appointment_id);
    END IF;

    IF NOT EXISTS (
      SELECT 1 FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE c.relname = 'idx_claims_service_date'
        AND n.nspname = current_schema()
    ) THEN
      CREATE INDEX idx_claims_service_date ON claims(service_date);
    END IF;

    IF NOT EXISTS (
      SELECT 1 FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE c.relname = 'idx_claims_insurance'
        AND n.nspname = current_schema()
    ) THEN
      CREATE INDEX idx_claims_insurance
        ON claims(primary_patient_insurance_id, secondary_patient_insurance_id);
    END IF;

    IF NOT EXISTS (
      SELECT 1 FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE c.relname = 'idx_claims_open'
        AND n.nspname = current_schema()
    ) THEN
      CREATE INDEX idx_claims_open ON claims(patient_id, service_date)
      WHERE status1 = 'OPEN' OR status2 = 'OPEN' OR status_p = 'OPEN';
    END IF;

    IF NOT EXISTS (
      SELECT 1 FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE c.relname = 'idx_claims_outstanding'
        AND n.nspname = current_schema()
    ) THEN
      CREATE INDEX idx_claims_outstanding ON claims(patient_id)
      WHERE outstanding1 > 0 OR outstanding2 > 0 OR outstanding_p > 0;
    END IF;
END;
$$;

-- Trigger
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'update_claims_updated_at'
          AND tgrelid = 'claims'::regclass
    ) THEN
        CREATE TRIGGER update_claims_updated_at
            BEFORE UPDATE ON claims
            FOR EACH ROW
            EXECUTE FUNCTION update_timestamp();
    END IF;
END;
$$;

-- Views
CREATE OR REPLACE VIEW active_claims AS
SELECT *
FROM claims
WHERE status1 = 'OPEN' 
   OR status2 = 'OPEN' 
   OR status_p = 'OPEN'
   OR outstanding1 > 0 
   OR outstanding2 > 0 
   OR outstanding_p > 0;

DROP MATERIALIZED VIEW IF EXISTS patient_claims_summary;
CREATE MATERIALIZED VIEW patient_claims_summary AS
SELECT 
    patient_id,
    COUNT(*) as total_claims,
    SUM(outstanding1 + outstanding2 + outstanding_p) as total_outstanding,
    MAX(service_date) as last_service_date,
    MAX(GREATEST(last_billed_date1, last_billed_date2, last_billed_date_p)) as last_billed_date
FROM claims
GROUP BY patient_id
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_patient_claims_summary 
  ON patient_claims_summary(patient_id);
CREATE TABLE IF NOT EXISTS imaging_studies (
    id UUID PRIMARY KEY,
    study_date TIMESTAMP WITH TIME ZONE NOT NULL,
    patient_id UUID NOT NULL,
    encounter_id UUID NOT NULL,
    series_uid VARCHAR(64) NOT NULL,
    instance_uid VARCHAR(64) NOT NULL,
    bodysite_code VARCHAR(20) NOT NULL,
    bodysite_description TEXT NOT NULL,
    modality_code VARCHAR(10) NOT NULL,
    modality_description TEXT NOT NULL,
    sop_code VARCHAR(64) NOT NULL,
    sop_description TEXT NOT NULL,
    procedure_code VARCHAR(20) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT valid_dicom_uids CHECK (
        series_uid ~ '^[0-9.]+$' AND
        instance_uid ~ '^[0-9.]+$' AND
        sop_code ~ '^[0-9.]+$'
    ),
    CONSTRAINT valid_modality CHECK (
        modality_code IN ('DX', 'CT', 'MR', 'US', 'NM', 'PT', 'XA', 'RF', 'MG')
    )
);

-- Indexes
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c 
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = 'idx_imaging_patient'
          AND n.nspname = current_schema()
    ) THEN
        CREATE INDEX idx_imaging_patient ON imaging_studies(patient_id);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = 'idx_imaging_encounter'
          AND n.nspname = current_schema()
    ) THEN
        CREATE INDEX idx_imaging_encounter ON imaging_studies(encounter_id);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = 'idx_imaging_date'
          AND n.nspname = current_schema()
    ) THEN
        CREATE INDEX idx_imaging_date ON imaging_studies(study_date);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = 'idx_imaging_series'
          AND n.nspname = current_schema()
    ) THEN
        CREATE INDEX idx_imaging_series ON imaging_studies(series_uid);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = 'idx_imaging_instance'
          AND n.nspname = current_schema()
    ) THEN
        CREATE INDEX idx_imaging_instance ON imaging_studies(instance_uid);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = 'idx_imaging_procedure'
          AND n.nspname = current_schema()
    ) THEN
        CREATE INDEX idx_imaging_procedure ON imaging_studies(procedure_code);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = 'idx_imaging_bodysite'
          AND n.nspname = current_schema()
    ) THEN
        CREATE INDEX idx_imaging_bodysite ON imaging_studies(bodysite_code);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = 'idx_imaging_dx'
          AND n.nspname = current_schema()
    ) THEN
        CREATE INDEX idx_imaging_dx
          ON imaging_studies(study_date, patient_id)
          WHERE modality_code = 'DX';
    END IF;
END;
$$;

-- Trigger
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'update_imaging_studies_updated_at'
          AND tgrelid = 'imaging_studies'::regclass
    ) THEN
        CREATE TRIGGER update_imaging_studies_updated_at
            BEFORE UPDATE ON imaging_studies
            FOR EACH ROW
            EXECUTE FUNCTION update_timestamp();
    END IF;
END;
$$;

-- Views
CREATE OR REPLACE VIEW patient_imaging_history AS
SELECT 
    patient_id,
    study_date,
    modality_code,
    bodysite_code,
    bodysite_description,
    procedure_code,
    encounter_id
FROM imaging_studies
ORDER BY patient_id, study_date DESC;

DROP MATERIALIZED VIEW IF EXISTS imaging_study_stats;
CREATE MATERIALIZED VIEW imaging_study_stats AS
SELECT 
    date_trunc('month', study_date) AS month,
    modality_code,
    bodysite_code,
    COUNT(*) as study_count
FROM imaging_studies
GROUP BY 
    date_trunc('month', study_date),
    modality_code,
    bodysite_code
WITH DATA;

CREATE INDEX IF NOT EXISTS idx_imaging_stats_month 
  ON imaging_study_stats(month);
CREATE INDEX IF NOT EXISTS idx_imaging_stats_modality 
  ON imaging_study_stats(modality_code);
CREATE INDEX IF NOT EXISTS idx_imaging_stats_bodysite 
  ON imaging_study_stats(bodysite_code);
CREATE TABLE IF NOT EXISTS immunizations (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    date TIMESTAMP WITH TIME ZONE NOT NULL,
    patient_id UUID NOT NULL,
    encounter_id UUID NOT NULL,
    code VARCHAR(10) NOT NULL,
    description TEXT NOT NULL,
    base_cost DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT valid_cost CHECK (base_cost >= 0)
);

-- Indexes
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = 'idx_immunizations_patient'
          AND n.nspname = current_schema()
    ) THEN
        CREATE INDEX idx_immunizations_patient ON immunizations(patient_id);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = 'idx_immunizations_encounter'
          AND n.nspname = current_schema()
    ) THEN
        CREATE INDEX idx_immunizations_encounter ON immunizations(encounter_id);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = 'idx_immunizations_date'
          AND n.nspname = current_schema()
    ) THEN
        CREATE INDEX idx_immunizations_date ON immunizations(date);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = 'idx_immunizations_code'
          AND n.nspname = current_schema()
    ) THEN
        CREATE INDEX idx_immunizations_code ON immunizations(code);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = 'idx_immunizations_daterange'
          AND n.nspname = current_schema()
    ) THEN
        CREATE INDEX idx_immunizations_daterange
          ON immunizations USING gist (tsrange(date, date + interval '1 second'));
    END IF;
END;
$$;

-- Trigger
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'update_immunizations_updated_at'
          AND tgrelid = 'immunizations'::regclass
    ) THEN
        CREATE TRIGGER update_immunizations_updated_at
            BEFORE UPDATE ON immunizations
            FOR EACH ROW
            EXECUTE FUNCTION update_timestamp();
    END IF;
END;
$$;

-- Views
CREATE OR REPLACE VIEW patient_immunization_history AS
SELECT 
    patient_id,
    code,
    description,
    COUNT(*) as doses_received,
    MIN(date) as first_dose_date,
    MAX(date) as last_dose_date
FROM immunizations
GROUP BY patient_id, code, description
ORDER BY patient_id, code;

DROP MATERIALIZED VIEW IF EXISTS monthly_immunization_stats;
CREATE MATERIALIZED VIEW monthly_immunization_stats AS
SELECT 
    date_trunc('month', date) AS month,
    code,
    description,
    COUNT(*) as total_immunizations,
    COUNT(DISTINCT patient_id) as unique_patients,
    SUM(base_cost) as total_cost
FROM immunizations
GROUP BY 
    date_trunc('month', date),
    code,
    description
WITH DATA;

CREATE INDEX IF NOT EXISTS idx_monthly_stats_month 
  ON monthly_immunization_stats(month);
CREATE INDEX IF NOT EXISTS idx_monthly_stats_code 
  ON monthly_immunization_stats(code);

-- Example immunization schedule reference table
CREATE TABLE IF NOT EXISTS immunization_schedule (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    vaccine_code VARCHAR(10) NOT NULL,
    recommended_age_months INTEGER NOT NULL,
    dose_number INTEGER NOT NULL,
    is_required BOOLEAN DEFAULT true,
    UNIQUE (vaccine_code, dose_number)
);
CREATE TABLE IF NOT EXISTS observations (
    date_time TIMESTAMP WITH TIME ZONE NOT NULL,
    patient_id UUID NOT NULL,
    encounter_id UUID NOT NULL,
    category VARCHAR(50) NOT NULL,
    code VARCHAR(20) NOT NULL,
    description TEXT NOT NULL,
    value NUMERIC NOT NULL,
    units VARCHAR(20) NOT NULL,
    type VARCHAR(20) NOT NULL,
    PRIMARY KEY (date_time, patient_id, encounter_id, code),
    CONSTRAINT category_check CHECK (category IN ('vital-signs', 'laboratory')),
    CONSTRAINT type_check CHECK (type = 'numeric')
);

-- Indexes
DO $$
BEGIN
    IF NOT EXISTS (
      SELECT 1 FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE c.relname = 'idx_patient_date'
        AND n.nspname = current_schema()
    ) THEN
      CREATE INDEX idx_patient_date 
        ON observations(patient_id, date_time);
    END IF;

    IF NOT EXISTS (
      SELECT 1 FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE c.relname = 'idx_encounter'
        AND n.nspname = current_schema()
    ) THEN
      CREATE INDEX idx_encounter ON observations(encounter_id);
    END IF;
END;
$$;

COMMENT ON TABLE observations IS 'Clinical observations including vital signs and lab results';
COMMENT ON COLUMN observations.date_time IS 'Timestamp for the observation';
COMMENT ON COLUMN observations.patient_id IS 'Unique ID of the patient';
COMMENT ON COLUMN observations.encounter_id IS 'Unique ID of the encounter';
COMMENT ON COLUMN observations.category IS 'Observation category (vital-signs or laboratory)';
COMMENT ON COLUMN observations.code IS 'Clinical code of the observation';
COMMENT ON COLUMN observations.description IS 'Textual description of the observation';
COMMENT ON COLUMN observations.value IS 'Numerical result of the observation';
COMMENT ON COLUMN observations.units IS 'Units of the numerical value';
COMMENT ON COLUMN observations.type IS 'Data type of the observation (numeric)';
CREATE TABLE IF NOT EXISTS patients (
    id UUID PRIMARY KEY,
    birthdate DATE NOT NULL,
    deathdate DATE,
    ssn VARCHAR(11),
    drivers_license VARCHAR(9),
    passport VARCHAR(10),
    prefix VARCHAR(4),
    first_name VARCHAR(100) NOT NULL,
    middle_name VARCHAR(100),
    last_name VARCHAR(100) NOT NULL,
    suffix VARCHAR(10),
    maiden_name VARCHAR(100),
    marital_status CHAR(1),
    race VARCHAR(20) NOT NULL,
    ethnicity VARCHAR(20) NOT NULL,
    gender CHAR(1) NOT NULL,
    birthplace TEXT NOT NULL,
    address TEXT NOT NULL,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(50) NOT NULL,
    county VARCHAR(100),
    fips VARCHAR(5),
    zip VARCHAR(5),
    latitude NUMERIC(17,14) NOT NULL,
    longitude NUMERIC(17,14) NOT NULL,
    healthcare_expenses NUMERIC(10,2) NOT NULL,
    healthcare_coverage NUMERIC(10,2) NOT NULL,
    income NUMERIC(10,2) NOT NULL,
    CONSTRAINT gender_check CHECK (gender IN ('M','F')),
    CONSTRAINT marital_status_check CHECK (marital_status IN ('M','S','D') OR marital_status IS NULL),
    CONSTRAINT race_check CHECK (race IN ('white','black','native') OR race IS NULL),
    CONSTRAINT ethnicity_check CHECK (ethnicity IN ('hispanic','nonhispanic') OR ethnicity IS NULL),
    CONSTRAINT ssn_format CHECK (ssn ~ '^[0-9]{3}-[0-9]{2}-[0-9]{4}$' OR ssn IS NULL),
    CONSTRAINT drivers_format CHECK (drivers_license ~ '^S[0-9]{8}$' OR drivers_license IS NULL),
    CONSTRAINT passport_format CHECK (passport ~ '^X[0-9]{8}X$' OR passport IS NULL)
);

-- Indexes
DO $$
BEGIN
    IF NOT EXISTS (
      SELECT 1 FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE c.relname = 'idx_patient_birthdate'
        AND n.nspname = current_schema()
    ) THEN
      CREATE INDEX idx_patient_birthdate ON patients(birthdate);
    END IF;

    IF NOT EXISTS (
      SELECT 1 FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE c.relname = 'idx_patient_deathdate'
        AND n.nspname = current_schema()
    ) THEN
      CREATE INDEX idx_patient_deathdate ON patients(deathdate);
    END IF;

    IF NOT EXISTS (
      SELECT 1 FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE c.relname = 'idx_patient_ssn'
        AND n.nspname = current_schema()
    ) THEN
      CREATE INDEX idx_patient_ssn ON patients(ssn);
    END IF;

    IF NOT EXISTS (
      SELECT 1 FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE c.relname = 'idx_patient_location'
        AND n.nspname = current_schema()
    ) THEN
      CREATE INDEX idx_patient_location ON patients(state, county, city);
    END IF;
END;
$$;

COMMENT ON TABLE patients IS 'Patient demographic and identifying information';
COMMENT ON COLUMN patients.id IS 'Unique identifier for the patient';
COMMENT ON COLUMN patients.birthdate IS 'Date of birth';
COMMENT ON COLUMN patients.deathdate IS 'Date of death, if any';
COMMENT ON COLUMN patients.ssn IS 'Social Security Number';
COMMENT ON COLUMN patients.drivers_license IS 'State drivers license number';
COMMENT ON COLUMN patients.passport IS 'Passport number';
COMMENT ON COLUMN patients.prefix IS 'Name prefix (Mr., Mrs., Ms., etc.)';
COMMENT ON COLUMN patients.first_name IS 'Patient first name';
COMMENT ON COLUMN patients.middle_name IS 'Patient middle name';
COMMENT ON COLUMN patients.last_name IS 'Patient last name';
COMMENT ON COLUMN patients.suffix IS 'Name suffix (e.g. Jr, Sr, III)';
COMMENT ON COLUMN patients.maiden_name IS 'Maiden name';
COMMENT ON COLUMN patients.marital_status IS 'Marital status (M=Married, S=Single, D=Divorced)';
COMMENT ON COLUMN patients.race IS 'Patient race';
COMMENT ON COLUMN patients.ethnicity IS 'Patient ethnicity';
COMMENT ON COLUMN patients.gender IS 'Patient gender (M=Male, F=Female)';
COMMENT ON COLUMN patients.birthplace IS 'Place of birth';
COMMENT ON COLUMN patients.address IS 'Street address';
COMMENT ON COLUMN patients.city IS 'City';
COMMENT ON COLUMN patients.state IS 'State';
COMMENT ON COLUMN patients.county IS 'County';
COMMENT ON COLUMN patients.fips IS 'FIPS code';
COMMENT ON COLUMN patients.zip IS 'ZIP code';
COMMENT ON COLUMN patients.latitude IS 'Latitude';
COMMENT ON COLUMN patients.longitude IS 'Longitude';
COMMENT ON COLUMN patients.healthcare_expenses IS 'Total healthcare expenses to date';
COMMENT ON COLUMN patients.healthcare_coverage IS 'Total healthcare coverage provided to date';
COMMENT ON COLUMN patients.income IS 'Annual income';
