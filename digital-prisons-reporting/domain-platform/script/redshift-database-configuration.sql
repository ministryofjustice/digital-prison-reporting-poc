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
    when age_at_incident between 0 and 20 then 'Under 21'
    when age_at_incident between 21 and 25 then '21-25'
    when age_at_incident between 26 and 30 then '26-30'
    
    when age_at_incident between 31 and 35 then '31-35'
    when age_at_incident between 36 and 40 then '36-40'
    
    when age_at_incident between 41 and 45 then '41-45'
    when age_at_incident between 46 and 50 then '46-50'
    
    when age_at_incident between 51 and 55 then '51-55'
    when age_at_incident between 56 and 60 then '56-60'
    
    when age_at_incident between 61 and 65 then '61-65'
    when age_at_incident between 66 and 70 then '66-70'
    
    when age_at_incident between 71 and 200 then 'Over 70'
end as age_category
from MV_AGED_INCIDENT mv