-- Sample Query for Hospital Database
-- Run with:
-- docker exec -i hospital_db psql -U admin -d hospital_db < sql/sample_queries.sql
-- 1. Count all patients
SELECT COUNT(*) as total_patients FROM patients;
