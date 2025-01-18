# OMOP ETL Project

Welcome to our **OMOP ETL** repository! This project demonstrates **end‐to‐end extraction, transformation, and loading** of healthcare data from a **source/staging schema** into the **OMOP Common Data Model (CDM)** (version 5.4). Below you will find an overview of the repository’s purpose, how we structure the staging and mapping steps, and example SQL scripts for loading each OMOP domain.

![cdm54](https://github.com/user-attachments/assets/60533163-a718-44f0-9779-d15a9f1b0ff6)
---

## Table of Contents

1. [Overview](#overview)  
2. [Project Structure](#project-structure)  
3. [Staging Schema and Mapping](#staging-schema-and-mapping)  
4. [ETL Steps for OMOP Domains](#etl-steps-for-omop-domains)  
   - 4.1 [Person](#person)  
   - 4.2 [Observation Period](#observation-period)  
   - 4.3 [Visit Occurrence](#visit-occurrence)  
   - 4.4 [Condition Occurrence](#condition-occurrence)  
   - 4.5 [Drug Exposure](#drug-exposure)  
   - 4.6 [Device Exposure](#device-exposure)  
   - 4.7 [Measurement vs. Observation](#measurement-vs-observation)  
   - 4.8 [Immunizations](#immunizations)  
   - 4.9 [Care Plans, Notes, Other Text Data](#care-plans-notes-other-text-data)  
   - 4.10 [Death](#death)  
   - 4.11 [Cost / Payer Plan Period / Fact Relationship / Claims](#cost-payer_plan_period-fact_relationship-claims)  
5. [References and Further Reading](#references)

---

## Overview

The **OMOP Common Data Model** standardizes the representation of healthcare data for use in analytics, research, and interoperability. In this repository, we show how to:

1. **Create a staging schema** to store raw extracts and mapping tables.  
2. **Map** local source codes (e.g., ICD, CPT, NDC, RxNorm, etc.) to standard OMOP concept IDs.  
3. **Generate** integer IDs for persons, visits, conditions, etc. (since OMOP requires integer surrogate keys).  
4. **Load** each OMOP domain (e.g., `person`, `visit_occurrence`, `condition_occurrence`) via targeted SQL insert statements.  

We assume you have:

- A source or staging area with raw tables like `patients_raw`, `encounters_raw`, `conditions_raw`, `medications_raw`, etc.  
- Some means of code mapping (a `local_to_omop_concept_map` table, or a CASE statement, or an external terminologies server).  
- A **final** OMOP schema named something like `omop_cdm`, or dynamically referenced as `@cdmDatabaseSchema`.

---
## Staging Schema and Mapping

A **staging schema** separates your raw source data from the final OMOP schema. Inside it, you typically define:

- **Raw Extract Tables** (e.g., `patients_raw`, `encounters_raw`, etc.) to store data directly from your source system.  
- **Mapping Tables** for:
  - **IDs**: bridging your local `patient_id`, `encounter_id`, etc. to OMOP integer keys (`person_id`, `visit_occurrence_id`).  
  - **Concept Codes**: linking local codes (ICD, SNOMED, RxNorm, CPT) to standard OMOP concept IDs.  
- **Lookup Tables** for gender, race, ethnicity, if your data has text fields needing concept IDs.  

Example: `staging.person_map(source_patient_id, person_id)` uses a sequence to assign each unique `source_patient_id` a new integer `person_id`.

---

## ETL Steps for OMOP Domains

Below is a summary of the key ETL steps for each OMOP table or domain. See the **`sql/` folder** for sample scripts.

### 4.1 Person

- **Source**: `patients_raw`  
- **Insert** into `person`: generate `person_id` from `staging.person_map`.  
- **Map** fields:
  - `year_of_birth`, `month_of_birth`, `day_of_birth` from birthdate.  
  - `gender_concept_id`, `race_concept_id`, `ethnicity_concept_id` from lookup tables.  
  - `person_source_value` to store original ID.  

### 4.2 Observation Period

- **Source**: computed earliest and latest record dates from encounters, conditions, meds, etc.  
- **Insert** into `observation_period`: each person gets at least one row.  
  - `observation_period_start_date`, `observation_period_end_date`.  
  - `period_type_concept_id` (e.g., 44814724 = “EHR record”).

### 4.3 Visit Occurrence

- **Source**: `encounters_raw`  
- **Insert** into `visit_occurrence`: generate `visit_occurrence_id`.  
  - **Map** `encounter_class` to `visit_concept_id` (9201=Inpatient, 9202=Outpatient, etc.).  
  - **Set** `visit_type_concept_id` (e.g., 44818518 = “Visit derived from EHR”).  
  - **Link** to `person_id`, optional `provider_id`, `care_site_id`.

### 4.4 Condition Occurrence

- **Source**: `conditions_raw`  
- **Insert** into `condition_occurrence`:  
  - `condition_concept_id` from your local code → standard code map.  
  - `condition_start_date/datetime`, `condition_end_date/datetime`.  
  - `condition_type_concept_id` (e.g., 32020 = “EHR problem list entry”).  
  - `visit_occurrence_id` if relevant.

### 4.5 Drug Exposure

- **Source**: `medications_raw`  
- **Insert** into `drug_exposure`:  
  - `drug_concept_id` from NDC or RxNorm mapping.  
  - `drug_exposure_start_date`, `_end_date`.  
  - `drug_type_concept_id` (e.g., 38000177 = “Prescription written”).  
  - Optional fields like `refills`, `quantity`, `days_supply`, `sig`.

### 4.6 Device Exposure

- **Source**: `devices_raw`  
- **Insert** into `device_exposure`:  
  - `device_concept_id` from local code → standard concept.  
  - `device_exposure_start_date/datetime`, `_end_date/datetime`.  
  - `device_type_concept_id` (44818707 = “Device Recorded from EHR”).  
  - `unique_device_id` if you have a UDI.

### 4.7 Measurement vs. Observation

- **Measurement**: numeric labs, vital signs, etc.  
  - **Source**: filter `observations_raw` where `category IN ('laboratory','vital-signs') AND value IS NOT NULL`.  
  - Insert into `measurement` with `measurement_concept_id`, `value_as_number`, `unit_concept_id`, etc.

- **Observation**: non‐numeric or textual data, social history, patient status, etc.  
  - **Source**: the rest of `observations_raw`.  
  - Insert into `observation` with `observation_concept_id`, `value_as_string`.

### 4.8 Immunizations

- Can be **`drug_exposure`** (if mapped to RxNorm) **or** **`procedure_occurrence`** (if codes are CVX/CPT/SNOMED procedure).  
- **Decide** which domain best fits your source codes.  
- Insert with the same pattern as standard drug or procedure ETL.

### 4.9 Care Plans, Notes, Other Text Data

- **Care Plans**:
  - Often stored in `observation` if textual, or `episode` if representing a distinct “episode of care.”  
- **Notes**:
  - Insert into `note` table, with `note_id`, `person_id`, `note_text`, `note_type_concept_id` (44814645 = “Clinical note”).

### 4.10 Death

- **Source**: check if `patients_raw.deathdate` is not null.  
- Insert one row per decedent into `death`, with:
  - `person_id`  
  - `death_date`  
  - `death_type_concept_id` (38003565 = “EHR reported death”)  
  - Optional cause of death if coded.

### 4.11 Cost / Payer Plan Period / Fact Relationship / Claims

- **Payer Plan Period**:
  - Insert coverage intervals for each patient, e.g., from `patient_expenses_raw`.  
- **Cost**:
  - For each claim line or transaction, identify the domain event (`drug_exposure_id`, `procedure_occurrence_id`, etc.) and create a row in `cost`, setting `cost_domain_id`, `cost_event_id`, plus `total_charge`, `total_cost`, `total_paid`, etc.  
  - Optionally link to `payer_plan_period_id`.
- **Fact Relationship** (optional):
  - Link facts across domains by specifying `domain_concept_id_1`, `fact_id_1`, `domain_concept_id_2`, `fact_id_2`, and a suitable `relationship_concept_id`.

---

## References and Further Reading

- **[OMOP CDM GitHub](https://github.com/ohdsi/commonDataModel)** – Official OMOP Common Data Model repository by OHDSI.  
- **[OHDSI Book of OHDSI](https://ohdsi.github.io/TheBookOfOhdsi/)** – Comprehensive guide on the CDM, analytics, and use cases.  
- **[Vocabulary Documentation](https://www.ohdsi.org/web/wiki/doku.php?id=documentation:vocabulary:start)** – Detailed info on concept mapping, domain tables, and standard concepts.  
- **[Staging Approach](https://ohdsi.github.io/TheBookOfOhdsi/ETLDesign.html)** – Best practices for setting up a staging area for your ETL.

---

## Questions or Contributions?

Feel free to **open an Issue** or **submit a Pull Request** if you have improvements or questions about mapping your own data to OMOP. We appreciate feedback, bug reports, and collaboration!

**Happy ETL-ing!**

