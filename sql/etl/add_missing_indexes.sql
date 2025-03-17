-- Add missing indexes to improve ETL performance

-- Add index on omop.concept.concept_code (used in JOIN)
CREATE INDEX IF NOT EXISTS idx_concept_concept_code ON omop.concept (concept_code);

-- Add index on omop.concept.vocabulary_id (used in WHERE)
CREATE INDEX IF NOT EXISTS idx_concept_vocabulary_id ON omop.concept (vocabulary_id);

-- Add index on omop.concept.domain_id (used in WHERE)
CREATE INDEX IF NOT EXISTS idx_concept_domain_id ON omop.concept (domain_id);

-- Add index on omop.concept.invalid_reason (used in WHERE)
CREATE INDEX IF NOT EXISTS idx_concept_invalid_reason ON omop.concept (invalid_reason);

-- Add index on population.observations_typed.code (used in JOIN)
CREATE INDEX IF NOT EXISTS idx_observations_typed_code ON population.observations_typed (code);

-- Add index on population.observations_typed.category (used in WHERE)
CREATE INDEX IF NOT EXISTS idx_observations_typed_category ON population.observations_typed (category);

-- Add combined index for the LOINC query
CREATE INDEX IF NOT EXISTS idx_observations_typed_code_category ON population.observations_typed (code, category);

-- Log the results
SELECT 'Missing indexes created successfully' AS message;

-- Analyze tables to update statistics
ANALYZE omop.concept;
ANALYZE population.observations_typed;

SELECT 'Tables analyzed successfully' AS message;
