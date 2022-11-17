-- =================================================================
-- Create a Spectrum link to the domain layer.
-- As each new domain is deployed, this will
-- be automatically refreshed.
-- =================================================================
CREATE EXTERNAL SCHEMA domain from data catalog
database 'domain'
iam_role 'arn:aws:iam::<account>:role/redshift-spectrum-role'
create external database if not exists;

-- =================================================================
--
-- MATERIALIZED VIEWS
--
-- =================================================================

-- =================================================================
-- MV_AGED_INCIDENT
-- DPR-130 https://dsdmoj.atlassian.net/browse/DPR-130
-- 
-- AS A safety analyst
-- I WANT to know the age of prisoners at the time of an incident,
-- SO THAT the data remains accurate
--
-- =================================================================

CREATE MATERIALIZED VIEW MV_AGED_INCIDENT
-- AUTO REFRESH YES -- apparently it is not available on external tables
AS
select i.*, datediff(year, date(birth_date), date(incident_date)) as age_at_incident
from domain.incident i join domain.demographics d on i.booking_id = d.id


-- =================================================================
-- MV_AGED_INCIDENT_BINNING
-- DPR-130 https://dsdmoj.atlassian.net/browse/DPR-130
-- 
-- AS A safety analyst
-- I WANT to know the age of prisoners at the time of an incident,
-- SO THAT the data remains accurate
--
-- =================================================================

CREATE MATERIALIZED VIEW MV_AGED_INCIDENT_BINNED
AS
select mv.*,
case
    when age_at_incident between 0 and 19 then 'Under 20'
    when age_at_incident between 20 and 29 then '20-29'
    when age_at_incident between 30 and 39 then '30-39'
    when age_at_incident between 40 and 49 then '40-49'
    when age_at_incident between 50 and 59 then '50-59'
    when age_at_incident between 60 and 69 then '60-69'
    when age_at_incident between 70 and 79 then '70-79'
    when age_at_incident between 80 and 89 then '80-89'
    when age_at_incident between 90 and 200 then 'Over 90'
end as age_category
from MV_AGED_INCIDENT mv