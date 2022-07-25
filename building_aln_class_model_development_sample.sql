with dw_to_aln_building_matching as (
    select
         a.id_mon
        ,b."ApartmentId" id_aln
        ,b."LastDateUpdated"
    from "EXT_DATA"."ALN_JOIN"."BUILDING_MATCHED" a
    join ext_data.aln_data."Apartment" b on a.id_aln = b."ApartmentId"
    qualify row_number() over (partition by a.id_mon order by b."LastDateUpdated" desc) = 1
)
, building_profile as (
    select   b.id_aln
            ,b.id_mon buildingid
            ,a."PricingTier" Property_PricingTier
            ,case when a."PricingTier" = 'A' then 1 
              when a."PricingTier" = 'B' then 2
              when a."PricingTier" = 'C' then 3
              when a."PricingTier" = 'D' then 4 end Property_PricingTier_num
            ,c.building_class inferred_building_class
            ,case when c.building_class = 'A' then 1 
              when c.building_class = 'B' then 2
              when c.building_class = 'C' then 3
              when c.building_class = 'D' then 4 end  inferred_building_class_num
            ,d.latitude
            ,d.longitude
            ,d.zip
            ,d.state
            ,2022 - d.year_built  building_age
            ,d.units
            ,case when d.website is not null then 1 else 0 end webpage_available
            ,d.enrollment_status
            ,d.territory
            ,d.building_activation_date
            ,d.landlord_source_id
            ,d.id
    from EXT_DATA.ALN_DATA."ApartmentPropertyExtension" a
    join dw_to_aln_building_matching b on b.id_aln = a."ApartmentId"
    join "EXT_DATA"."ALN_FEATURE"."ALN_BUILDING_CLASS_ESTIMATE" c on c.building_aln_id=a."ApartmentId"
    join tg_dw_db.public.building d on d.source_id = b.id_mon
)
, landlord as (
    select
         a.source_id 
        ,a.state
        ,case when a.phone_number is null then 0 else 1 end phone_available
    from tg_dw_db.public.landlord a
    where a.source_id in (select landlord_source_id from building_profile)
)
, deal_data as (
    select 
         a.buildingid
        ,count(distinct b.source_id) cnt_deals
        ,count(distinct d."tg_deal_id") cnt_default_deals
        ,count(distinct case when b.deal_status = 'deal-closed' then b.source_id end) cnt_closed_deals
        ,count(b.us_citizen_deal) us_citizen_deals_cnt
        ,min(b.deal_creation_date) min_deal_created
        ,avg(b.monthly_rent) avg_monthly_rent
        ,avg(b.gross_monthly_rent) avg_gross_monthly_rent
        ,avg(case when b.months_covered > 0 then b.months_covered end) avg_months_covered
        ,avg(b.landlord_security_deposit) avg_landlord_security_deposit
        ,avg(b.cotenants_for_allocation) avg_cotenants_for_allocation
        ,avg(b.claims_paid/b.gross_monthly_rent) avg_claims_paid_ratio
        ,avg(c.applicant_annual_income_stated) avg_applicant_annual_income_stated
        ,avg(c.applicant_annual_income_actual) avg_applicant_annual_income_actual
        ,avg(c.applicant_age) avg_applicant_age
        ,avg(c.credit_score) avg_credit_score
        ,count(case when lower(c.applicant_employment) like 'student' then 1 end)/count(distinct c.id) student_ratio
        ,count(case when lower(c.applicant_employment) like '%employed%' then 1 end)/count(distinct c.id) employed_ratio
        ,count(case when lower(c.applicant_employment) like '%self%' then 1 end)/count(distinct c.id) self_employed_ratio
    from building_profile a
    join tg_dw_db.public.deal b on b.building_id = a.id
    join tg_dw_db.public.application c on c.deal_source_id = b.source_id
    left join "TG_APP_DB"."TG_DATA_PROD"."CLAIM" d on d."tg_deal_id" = b.source_id
    where 1=1
    group by a.buildingid
)
/* From this point on, every WITH statement is for gathering features. Development sample is already defined in
   previous WITH statements. First set for features is college distance data. 
   Source for this table comes from snowflake marketplace. Definitions for the columns are not available in snowflake.
   Column definitions were gathered from external source and are loaded into the google drive:
   https://drive.google.com/file/d/1zyEfFPXRjirW8HW7BPb_Kx6JTcf9uxG6/view?usp=sharing 
   In this sql snippet, every building is joined to every college to calculate each possible distance.
   Distance is calculated using HAVERSINE inbuilt function.
   Note: College data is assumed to be static and might be updated once a year (or less frequently)
*/
, colleges_distances as (
    select 
        a.buildingid building_id 
        ,a.latitude
        ,a.longitude
        ,b.instnm
        ,b.latitude
        ,b.longitude
        ,b.highdeg -- 0 - nondegree, 1-certificate, 2-associates degree, 3-bachelors degree, 4-graduate
        -- CCSIZSET comes from carnegie classification: https://carnegieclassifications.iu.edu/classification_descriptions/size_setting.php
        ,case when b.CCSIZSET in (1,2,6,7,8,9,10,11) then 'small'
            when b.CCSIZSET in (3,12,13,14) then 'medium'
            when b.CCSIZSET in (4,5,15,16,17) then 'large' end college_size 
        ,case when b.CCSIZSET in (6,9,12,15) then 'nonresidential'
            when b.CCSIZSET in (7,10,13,16) then 'residential'
            when b.CCSIZSET in (8,11,14,17) then 'highly residential' end college_setting
        ,ifnull(b.COSTT4_A,b.COSTT4_P) avg_cost_1year
        ,b.C150_4 completion_rate-- Completion rate for first-time, full-time students at four-year institutions
        ,round(haversine(a.latitude, a.longitude, b.latitude, b.longitude),2) distance_km
    from building_profile  a
    left join ext_college_scorecard.college_scorecard.merged2018_19_pp b 
    where 1=1
        and b.latitude is not null 
        and b.longitude is not null
        and b.CURROPER = 1 -- operational
)
/* Contains college features per building.
   Features include both counts of coleges in specific set of radii as well as 
   drill down of what kind of colleges these are, what are the costs, complition rates and sizes.
   distance ranges were chosen by discussing with the team and subjective opinion. 
*/
, building_college as (
select 
    a.building_id
    ,min(distance_km) closest_college_km
    ,sum(case when distance_km <= 1 then 1 else 0 end) college_cnt_1km
    ,sum(case when distance_km <= 2 then 1 else 0 end) college_cnt_2km
    ,sum(case when distance_km <= 5 then 1 else 0 end) college_cnt_5km
    ,sum(case when distance_km <= 10 then 1 else 0 end) college_cnt_10km
    ,sum(case when distance_km <= 25 then 1 else 0 end) college_cnt_25km
    ,sum(case when distance_km <= 50 then 1 else 0 end) college_cnt_50km
    ,sum(case when distance_km <= 75 then 1 else 0 end) college_cnt_75km
    
    ,sum(case when distance_km <= 1 and highdeg = 4 then 1 else 0 end) graduate_deg_uni_cnt_1km
    ,sum(case when distance_km <= 2 and highdeg = 4 then 1 else 0 end) graduate_deg_uni_cnt_2km
    ,sum(case when distance_km <= 5 and highdeg = 4 then 1 else 0 end) graduate_deg_uni_cnt_5km
    ,sum(case when distance_km <= 10 and highdeg = 4 then 1 else 0 end) graduate_deg_uni_cnt_10km
    ,sum(case when distance_km <= 25 and highdeg = 4 then 1 else 0 end) graduate_deg_uni_cnt_25km
    ,sum(case when distance_km <= 50 and highdeg = 4 then 1 else 0 end) graduate_deg_uni_cnt_50km
    
    ,sum(case when distance_km <= 1 and highdeg = 3 then 1 else 0 end) bachelors_deg_uni_cnt_1km
    ,sum(case when distance_km <= 2 and highdeg = 3 then 1 else 0 end) bachelors_deg_uni_cnt_2km
    ,sum(case when distance_km <= 5 and highdeg = 3 then 1 else 0 end) bachelors_deg_uni_cnt_5km
    ,sum(case when distance_km <= 10 and highdeg = 3 then 1 else 0 end) bachelors_deg_uni_cnt_10km
    ,sum(case when distance_km <= 25 and highdeg = 3 then 1 else 0 end) bachelors_deg_uni_cnt_25km
    ,sum(case when distance_km <= 50 and highdeg = 3 then 1 else 0 end) bachelors_deg_uni_cnt_50km
    
    ,sum(case when distance_km <= 1 and highdeg <= 2 then 1 else 0 end) other_deg_uni_cnt_1km
    ,sum(case when distance_km <= 2 and highdeg <= 2 then 1 else 0 end) other_deg_uni_cnt_2km
    ,sum(case when distance_km <= 5 and highdeg <= 2 then 1 else 0 end) other_deg_uni_cnt_5km
    ,sum(case when distance_km <= 10 and highdeg <= 2 then 1 else 0 end) other_deg_uni_cnt_10km
    ,sum(case when distance_km <= 25 and highdeg <= 2 then 1 else 0 end) other_deg_uni_cnt_25km
    ,sum(case when distance_km <= 50 and highdeg <= 2 then 1 else 0 end) other_deg_uni_cnt_50km
    
    ,sum(case when distance_km <= 1 and college_size = 'small' then 1 else 0 end) small_college_cnt_1km
    ,sum(case when distance_km <= 2 and college_size = 'small' then 1 else 0 end) small_college_cnt_2km
    ,sum(case when distance_km <= 5 and college_size = 'small' then 1 else 0 end) small_college_cnt_5km
    ,sum(case when distance_km <= 10 and college_size = 'small' then 1 else 0 end) small_college_cnt_10km
    
    ,sum(case when distance_km <= 1 and college_size = 'medium' then 1 else 0 end) medium_college_cnt_1km
    ,sum(case when distance_km <= 2 and college_size = 'medium' then 1 else 0 end) medium_college_cnt_2km
    ,sum(case when distance_km <= 5 and college_size = 'medium' then 1 else 0 end) medium_college_cnt_5km
    ,sum(case when distance_km <= 10 and college_size = 'medium' then 1 else 0 end) medium_college_cnt_10km
    
    ,sum(case when distance_km <= 1 and college_size = 'large' then 1 else 0 end) large_college_cnt_1km
    ,sum(case when distance_km <= 2 and college_size = 'large' then 1 else 0 end) large_college_cnt_2km
    ,sum(case when distance_km <= 5 and college_size = 'large' then 1 else 0 end) large_college_cnt_5km
    ,sum(case when distance_km <= 10 and college_size = 'large' then 1 else 0 end) large_college_cnt_10km
    
    ,sum(case when distance_km <= 1 and college_setting = 'residential' then 1 else 0 end) residental_college_cnt_1km
    ,sum(case when distance_km <= 2 and college_setting = 'residential' then 1 else 0 end) residental_college_cnt_2km
    ,sum(case when distance_km <= 5 and college_setting = 'residential' then 1 else 0 end) residental_college_cnt_5km
    ,sum(case when distance_km <= 10 and college_setting = 'residential' then 1 else 0 end) residental_college_cnt_10km
    
    ,round(avg(case when distance_km <= 1 then avg_cost_1year end), 2) avg_college_cost_1km
    ,round(avg(case when distance_km <= 2 then avg_cost_1year end), 2) avg_college_cost_2km
    ,round(avg(case when distance_km <= 5 then avg_cost_1year end), 2) avg_college_cost_5km
    ,round(avg(case when distance_km <= 10 then avg_cost_1year end), 2) avg_college_cost_10km
    ,round(avg(case when distance_km <= 50 then avg_cost_1year end), 2) avg_college_cost_50km
    
    ,round(avg(case when distance_km <= 1 then completion_rate end), 2) avg_college_completion_rate_1km
    ,round(avg(case when distance_km <= 2 then completion_rate end), 2) avg_college_completion_rate_2km
    ,round(avg(case when distance_km <= 5 then completion_rate end), 2) avg_college_completion_rate_5km
    ,round(avg(case when distance_km <= 10 then completion_rate end), 2) avg_college_completion_rate_10km
    ,round(avg(case when distance_km <= 50 then completion_rate end), 2) avg_college_completion_rate_50km
from colleges_distances a
group by a.building_id
)
/* Unemployment data gathered by risk team. 
   Data is filtered to include rows up to and including May 2020. Reason for this is
   we want to have as little intersection with the taget period as possibe, to minimize
   risk of using future variables (meaning using future to predict future, when in reality we should use past to predict future).
   Remember, application approval statuses are collected from June 2020.
   More information about what msa codes are can be found here: https://www.investopedia.com/terms/m/msa.asp
*/
, unemployment as (
    select 
         f.msa_code
        ,round(avg(unemployment_rate), 2) avg_unemployment_rate_1y
    from ML.RISK_MODEL_TRAINING.unemployment f 
    where 1=1 
    and (f.year = 2020 and f.month <= 5) or (f.year = 2019 and f.month > 5)
    group by f.msa_code
)
/* Hospita data is also assumed as static (like college data). 
   Data was found online: https://hifld-geoplatform.opendata.arcgis.com/datasets/geoplatform::hospitals-1/about
   it was loaded as .csv from dbt.
*/
, hospital_distances as (
    select
         a.buildingid building_id 
        ,a.latitude
        ,a.longitude
        ,b.latitude
        ,b.longitude
        ,case when b.type = 'GENERAL ACUTE CARE' then 1 else 0 end general_acute_care_hospital
        ,case when b.owner like '%GOVERNMENT%' then 1 else 0 end government_hospital
        ,b.beds -- gives idea about size of hospital
        ,round(haversine(a.latitude, a.longitude, b.latitude, b.longitude),2) distance_km
    from building_profile a
    left join reports.dbt_bchikvinidze.hospitals b 
    where 1=1
        and b.status = 'OPEN'      
)
/* Feature creation logic is similar to college data feature creation logic.
*/
, building_hospital as (
    select
         a.building_id
        ,min(distance_km) closest_hospital_km
        ,sum(case when distance_km <= 2 then 1 else 0 end) hospital_cnt_2km
        ,sum(case when distance_km <= 5 then 1 else 0 end) hospital_cnt_5km
        ,sum(case when distance_km <= 10 then 1 else 0 end) hospital_cnt_10km
        ,sum(case when distance_km <= 50 then 1 else 0 end) hospital_cnt_50km
        
        ,sum(case when distance_km <= 2 and general_acute_care_hospital = 1 then 1 else 0 end) general_acute_care_hospital_cnt_2km
        ,sum(case when distance_km <= 5 and general_acute_care_hospital = 1 then 1 else 0 end) general_acute_care_hospital_cnt_5km
        ,sum(case when distance_km <= 10 and general_acute_care_hospital = 1 then 1 else 0 end) general_acute_care_hospital_cnt_10km
        
        ,sum(case when distance_km <= 2 and government_hospital = 1 then 1 else 0 end) government_hospital_cnt_2km
        ,sum(case when distance_km <= 5 and government_hospital = 1 then 1 else 0 end) government_hospital_cnt_5km
        ,sum(case when distance_km <= 10 and government_hospital = 1 then 1 else 0 end) government_hospital_cnt_10km
        
        ,round(avg(case when distance_km <= 2 and a.beds > 0 then a.beds end)) hospital_avg_beds_2km
        ,round(avg(case when distance_km <= 5 and a.beds > 0 then a.beds end)) hospital_avg_beds_5km
        ,round(avg(case when distance_km <= 10 and a.beds > 0 then a.beds end)) hospital_avg_beds_10km
    from hospital_distances a
    group by a.building_id
)
/* Source of this WITH is csv that I got from michael (originally from tyler), then used dbt for importing.
   Note that this file containes dublicates, meaning one zip could correspond to more than one cbsa.
   Aggregate function was used to avoid dupes (3.6k dupe out of total of 32.3k). 
   For now there is no way to decide exactly which cbsa code should be used in case of dupes so I'm sticking with
   max function for simplicity. 
*/
, cbsa_zip_mapping as (
    select 
         zip
        ,max(cbsa) cbsa_code
    from reports.dbt_bchikvinidze.cbsa_zip_mapping -- 
    group by zip -- I have some dupes, reason unknown. That's why I'm taking maximum of cbsa codes
)
/* Per capita income is gathered by risk team. source: – BEA, CAINC1 Personal Income Summary: https://www.bea.gov/data/income-saving/personal-income-county-metro-and-other-areas
   reason for grouping is again to avoid dupes (this one had just one dupe)
*/
, per_capita_income as (
    select 
         a.cbsa_code
        ,max(a.per_capita_income) per_capita_income
    from ml.risk_model_training.per_capita_income a
    where a.year = 2019
    group by a.cbsa_code
    having count(1)=1 -- I moving this table to 'with' statement because there is one dupe
)
/* Law enforcement data is handled as static data, like college and hospital data.
   original source is here: https://hifld-geoplatform.opendata.arcgis.com/datasets/local-law-enforcement-locations/explore?location=36.557550%2C-76.088187%2C3.86&showTable=true
*/
, law_enforcement_distances as (
    select 
         a.buildingid building_id 
        ,round(haversine(a.latitude, a.longitude, b.latitude, b.longitude),2) distance_km
    from building_profile  a
    left join reports.dbt_bchikvinidze.law_enforcement_locations b
    where 1=1
    and b.status = 'OPEN'
)
/* Structure for law enforcement features is same as for college and hospital features.*/
, building_law_enforcement as (
    select
         a.building_id
        ,min(distance_km) closest_law_enforcement_km
        ,sum(case when distance_km <= 2 then 1 else 0 end) law_enforcement_cnt_2km
        ,sum(case when distance_km <= 5 then 1 else 0 end) law_enforcement_cnt_5km
        ,sum(case when distance_km <= 10 then 1 else 0 end) law_enforcement_cnt_10km
    from law_enforcement_distances a
    group by a.building_id
)
/* Public and private school data. original source is from:
   https://hifld-geoplatform.opendata.arcgis.com/datasets/private-schools-1/about
   https://hifld-geoplatform.opendata.arcgis.com/datasets/geoplatform::public-schools/about
   These tables did not have identical columns, so inference was needed to identify private elementary schools.
   "level_" = 1 in private schools table was handled as elementary school by comparing level distribution with public school level_ distribution.
*/
, schools_union as (
    select
         a.objectid
        ,a.latitude
        ,a.longitude
        ,a.enrollment
        ,case when a.ft_teacher <> 0 then a.enrollment/a.ft_teacher end student_per_teacher
        ,'PUBLIC' type
        ,case when level_ in ('ELEMENTARY', 'PREKINDERGARTEN') then 'ELEMENTARY' end level
    from reports.dbt_bchikvinidze.public_schools a
    
    UNION ALL
    
    select 
         b.fid as objectid
        ,b.latitude
        ,b.longitude
        ,b.enrollment
        ,case when b.ft_teacher <> 0 then b.enrollment/b.ft_teacher end student_per_teacher
        ,'PRIVATE' type
        ,case when level_ = 1 then 'ELEMENTARY' end level
    from reports.dbt_bchikvinidze.private_schools b
)
/* Intermediary table to calculate distance between all pairs of buildings and schools*/
, schools_distances as (
    select
         a.buildingid building_id 
        ,b.type
        ,b.level
        ,b.objectid
        ,round(haversine(a.latitude, a.longitude, b.latitude, b.longitude), 2) distance_km 
    from building_profile  a
    join schools_union b
)
/* Features here are calculated by the same style as other POI features (college,hospital,law enforcement)
   This time shorter distances are considered (not more than 10KM) because, as opposed to far-away colleges,
   kids usually go to nearby school.
*/
, building_schools as ( -- takes 4.5 minutes
    select
         a.building_id
        ,min(case when a.type = 'PRIVATE' then distance_km end) closest_private_school_km
        ,min(case when a.type = 'PUBLIC' then distance_km end) closest_public_school_km
    
        ,min(case when a.level = 'ELEMENTARY' then distance_km end) closest_private_elem_school_km
        ,min(case when a.level = 'ELEMENTARY' then distance_km end) closest_public_elem_school_km
    
        ,sum(case when distance_km <= 2 and a.type = 'PRIVATE' then 1 else 0 end) private_school_cnt_2km
        ,sum(case when distance_km <= 5 and a.type = 'PRIVATE' then 1 else 0 end) private_school_cnt_5km
        ,sum(case when distance_km <= 10 and a.type = 'PRIVATE' then 1 else 0 end) private_school_cnt_10km
    
        ,sum(case when distance_km <= 2 and a.type = 'PUBLIC' then 1 else 0 end) public_school_cnt_2km
        ,sum(case when distance_km <= 5 and a.type = 'PUBLIC' then 1 else 0 end) public_school_cnt_5km
        ,sum(case when distance_km <= 10 and a.type = 'PUBLIC' then 1 else 0 end) public_school_cnt_10km
    from schools_distances a
    group by a.building_id
)
/* collected by risk team. source from census:  ACS B25031 and ACS B25063
   Aggregate function is used because sometimes more than one area corresponds to one msa code.
*/
, gross_rent as (
    select
     a.msa_code
    ,a.year
    ,avg(a.median_gross_rent_all_bedrooms) median_gross_rent_all_bedrooms
    ,avg(a.median_gross_rent_no_bedrooms) median_gross_rent_no_bedrooms
    ,avg(a.median_gross_rent_one_bedroom) median_gross_rent_one_bedroom
    from ML.RISK_MODEL_TRAINING.gross_rent a
    group by a.msa_code, a.year
)
/* intermediary table for more feature engineering with POI data.
   Result of this WITH is counts by zip code and POI.
*/
, points_of_interest_per_zip as (
    select try_to_number(zip) zip, count(1) cnt, 'law_enforcement' poi from reports.dbt_bchikvinidze.law_enforcement_locations group by zip
    union all
    select try_to_number(zip) zip, count(1) cnt, 'private_schools' poi from reports.dbt_bchikvinidze.private_schools  group by zip
    union all
    select try_to_number(zip) zip, count(1) cnt, 'public_schools' poi from reports.dbt_bchikvinidze.public_schools  group by zip
    union all
    select try_to_number(zip) zip, count(1) cnt, 'hospitals' poi from  reports.dbt_bchikvinidze.hospitals  group by zip
    union all
    select try_to_number(zip) zip, count(1) cnt, 'colleges' poi from ext_college_scorecard.college_scorecard.merged2018_19_pp   group by zip
)
/* count of each POI category in zip */
, poi_per_zip as (
    select 
        a.zip
        ,sum(case when poi = 'law_enforcement' then cnt end) law_enforcement_cnt
        ,sum(case when poi = 'private_schools' then cnt end) private_school_cnt
        ,sum(case when poi = 'public_schools' then cnt end) public_school_cnt
        ,sum(case when poi = 'hospitals' then cnt end) hospital_cnt
        ,sum(case when poi = 'colleges' then cnt end) college_cnt
    from points_of_interest_per_zip a
    group by a.zip
)
, fin as (
    select 
     a.id_aln
    ,a.buildingid
    ,a.Property_PricingTier
    ,a.Property_PricingTier_num
    ,a.inferred_building_class
    ,a.inferred_building_class_num
    ,a.state
    ,a.building_age
    ,a.units
    ,a.webpage_available
    ,a.enrollment_status
    ,a.territory
    ,a.building_activation_date
    ,d.closest_college_km
    ,d.college_cnt_2km
    ,d.college_cnt_10km
    ,d.college_cnt_25km
    ,d.college_cnt_50km
    ,d.graduate_deg_uni_cnt_2km
    ,d.graduate_deg_uni_cnt_10km
    ,d.graduate_deg_uni_cnt_25km
    ,d.graduate_deg_uni_cnt_50km
    ,d.bachelors_deg_uni_cnt_2km
    ,d.bachelors_deg_uni_cnt_10km
    ,d.bachelors_deg_uni_cnt_25km
    ,d.bachelors_deg_uni_cnt_50km
    ,d.small_college_cnt_10km
    ,d.medium_college_cnt_10km
    ,d.large_college_cnt_10km
    ,d.avg_college_cost_2km
    ,d.avg_college_cost_5km
    ,d.avg_college_cost_10km
    ,d.avg_college_cost_50km
    ,d.avg_college_completion_rate_5km
    ,d.avg_college_completion_rate_10km
    ,d.avg_college_completion_rate_50km
    ,f.avg_unemployment_rate_1y
    ,g.median_gross_rent_one_bedroom
    ,k.per_capita_income
    ,try_to_number(k2.median_household_income) median_household_income
    ,try_to_number(k2.median_family_income) median_family_income
    ,h.closest_hospital_km
    ,h.hospital_cnt_2km
    ,h.hospital_cnt_5km
    ,h.hospital_cnt_10km
    ,h.hospital_cnt_50km
    ,h.general_acute_care_hospital_cnt_2km
    ,h.general_acute_care_hospital_cnt_5km
    ,h.general_acute_care_hospital_cnt_10km
    ,h.government_hospital_cnt_2km
    ,h.government_hospital_cnt_5km
    ,h.government_hospital_cnt_10km
    ,h.hospital_avg_beds_2km
    ,h.hospital_avg_beds_5km
    ,h.hospital_avg_beds_10km
    ,m.closest_law_enforcement_km
    ,m.law_enforcement_cnt_2km
    ,m.law_enforcement_cnt_5km
    ,m.law_enforcement_cnt_10km
    ,n.closest_private_school_km
    ,n.closest_public_school_km
    ,n.closest_private_elem_school_km
    ,n.closest_public_elem_school_km
    ,n.private_school_cnt_2km
    ,n.private_school_cnt_5km
    ,n.private_school_cnt_10km
    ,n.public_school_cnt_2km
    ,n.public_school_cnt_5km
    ,n.public_school_cnt_10km
    ,o.median_age
    ,p.population_density
    ,round(p.population/q.public_school_cnt, 1) population_per_public_school
    ,round(p.population/q.private_school_cnt, 1) population_per_private_school
    ,round(p.population/q.hospital_cnt, 1) population_per_hospital
    ,round(p.population/q.college_cnt, 1) population_per_college
    ,round(p.population/q.law_enforcement_cnt, 1) population_per_law_enforcement
    ,z.cnt_deals
    ,z.cnt_default_deals
    ,z.cnt_closed_deals
    ,z.us_citizen_deals_cnt
    ,z.min_deal_created
    ,round(months_between(z.min_deal_created, sysdate()), 2) months_active
    ,round(z.cnt_closed_deals/z.cnt_deals, 2) closed_deal_ratio
    ,round(z.cnt_deals/(a.units*months_active), 2) cnt_deals_norm
    ,round(z.cnt_default_deals/(a.units*months_active), 2) cnt_defaulted_deals_norm
    ,round(z.cnt_closed_deals/(a.units*months_active), 2) cnt_closed_deals_norm
    ,round(z.us_citizen_deals_cnt/(a.units*months_active), 2) us_citizen_deals_cnt_norm
    ,round(z.avg_gross_monthly_rent, 2) avg_gross_monthly_rent
    ,round(z.avg_months_covered, 2) avg_months_covered
    ,round(z.avg_landlord_security_deposit, 2) avg_landlord_security_deposit
    ,round(z.avg_cotenants_for_allocation, 2) avg_cotenants_for_allocation
    ,round(z.avg_claims_paid_ratio, 2) avg_claims_paid_ratio
    ,round(z.avg_applicant_annual_income_stated, 2) avg_applicant_annual_income_stated
    ,round(z.avg_applicant_annual_income_actual, 2) avg_applicant_annual_income_actual
    ,round(z.avg_applicant_age, 2) avg_applicant_age
    ,round(z.avg_credit_score, 2) avg_credit_score
    ,round(avg_gross_monthly_rent/NULLIFZERO(avg_applicant_annual_income_actual)) avg_rent_over_income_actual
    ,round(avg_gross_monthly_rent/NULLIFZERO(avg_applicant_annual_income_stated)) avg_rent_over_income_stated
    ,round(z.student_ratio, 2) student_ratio
    ,round(z.employed_ratio, 2) employed_ratio
    ,round(z.self_employed_ratio, 2) self_employed_ratio
    ,case when ws.walk_score=-1 then null else ws.walk_score end walk_score
    ,case when ws.transit_score=-1 then null else ws.transit_score end transit_score
    ,case when ws.bike_score=-1 then null else ws.bike_score end bike_score
    from building_profile a
    left join building_college d on a.buildingid = d.building_id
    left join TG_APP_DB.TG_MANUAL.ZIP_METRO_MAPPING e on e.zip_code = try_to_number(a.zip)
    left join unemployment f on f.msa_code = e.msa_number
    left join gross_rent g on g.msa_code = e.msa_number and g.year = 2019
    left join building_hospital h on h.building_id = a.buildingid
    left join cbsa_zip_mapping j on j.zip = try_to_number(a.zip)
    left join per_capita_income k on k.cbsa_code = j.cbsa_code
    left join "REPORTS".DBT_BCHIKVINIDZE.MEDIAN_INCOME_BY_ZIP k2 on try_to_number(k2.zip) = try_to_number(a.zip)
    left join building_law_enforcement m on m.building_id = a.buildingid
    left join building_schools n on n.building_id = a.buildingid
    left join reports.dbt_bchikvinidze.median_age_zip o on o.zip_code = try_to_number(a.zip)
    left join reports.dbt_bchikvinidze.population_density_zip p on try_to_number(p.zip) = try_to_number(a.zip)
    left join poi_per_zip q on q.zip = try_to_number(a.zip)
    left join landlord w on w.source_id = a.landlord_source_id
    left join deal_data z on z.buildingid = a.buildingid
    left join ext_walk_score.public.walk_score  ws on ws.source_id=a.buildingid
    where 1=1
)
, market_features as (
    select 
         a.state
        ,NULLIFZERO(median(a.closest_college_km))  state_median_closest_college_km
        ,NULLIFZERO(median(a.college_cnt_50km))  state_median_college_cnt_50km
        ,NULLIFZERO(median(a.avg_college_cost_50km))  state_median_avg_college_cost_50km
        ,NULLIFZERO(median(a.avg_college_completion_rate_50km))  state_median_avg_college_completion_rate_50km
        ,NULLIFZERO(median(a.avg_unemployment_rate_1y))  state_median_avg_unemployment_rate_1y
        ,NULLIFZERO(median(a.median_gross_rent_one_bedroom))  state_median_median_gross_rent_one_bedroom
        ,NULLIFZERO(median(a.per_capita_income))  state_median_per_capita_income
        ,NULLIFZERO(median(a.median_household_income))  state_median_median_household_income
        ,NULLIFZERO(median(a.closest_hospital_km))  state_median_closest_hospital_km
        ,NULLIFZERO(median(a.hospital_cnt_50km))  state_median_hospital_cnt_50km
        ,NULLIFZERO(median(a.hospital_avg_beds_10km))  state_median_hospital_avg_beds_10km
        ,NULLIFZERO(median(a.law_enforcement_cnt_10km))  state_median_law_enforcement_cnt_10km
        ,NULLIFZERO(median(a.closest_private_school_km))  state_median_closest_private_school_km
        ,NULLIFZERO(median(a.closest_public_school_km))  state_median_closest_public_school_km
        ,NULLIFZERO(median(a.closest_private_elem_school_km))  state_median_closest_private_elem_school_km
        ,NULLIFZERO(median(a.closest_public_elem_school_km))  state_median_closest_public_elem_school_km
        ,NULLIFZERO(median(a.private_school_cnt_10km))  state_median_private_school_cnt_10km
        ,NULLIFZERO(median(a.public_school_cnt_10km))  state_median_public_school_cnt_10km
        ,NULLIFZERO(median(a.median_age))  state_median_median_age
        ,NULLIFZERO(median(a.population_density))  state_median_population_density
        ,NULLIFZERO(median(a.closed_deal_ratio))  state_median_closed_deal_ratio
        ,NULLIFZERO(median(a.cnt_closed_deals_norm))  state_median_cnt_closed_deals_norm
        ,NULLIFZERO(median(a.avg_gross_monthly_rent))  state_median_avg_gross_monthly_rent
        ,NULLIFZERO(median(a.avg_cotenants_for_allocation))  state_median_avg_cotenants_for_allocation
        ,NULLIFZERO(median(a.avg_applicant_annual_income_stated))  state_median_avg_applicant_annual_income_stated
        ,NULLIFZERO(median(a.avg_applicant_annual_income_actual))  state_median_avg_applicant_annual_income_actual
        ,NULLIFZERO(median(a.avg_credit_score))  state_median_avg_credit_score
        ,NULLIFZERO(median(a.avg_rent_over_income_actual))  state_median_avg_rent_over_income_actual
        ,NULLIFZERO(median(a.avg_rent_over_income_stated))  state_median_avg_rent_over_income_stated
        ,NULLIFZERO(median(a.student_ratio))  state_median_student_ratio
        ,NULLIFZERO(median(a.employed_ratio))  state_median_employed_ratio
        ,NULLIFZERO(median(a.self_employed_ratio))  state_median_self_employed_ratio
        ,NULLIFZERO(median(a.walk_score))  state_median_walk_score
        ,NULLIFZERO(median(a.transit_score))  state_median_transit_score
        ,NULLIFZERO(median(a.bike_score))  state_median_bike_score
    from fin a
    group by a.state
)
select  
     a.*
    ,round(a.closest_college_km/b.state_median_closest_college_km, 2) ratio_state_median_closest_college_km
    ,round(a.college_cnt_50km/b.state_median_college_cnt_50km, 2) ratio_state_median_college_cnt_50km
    ,round(a.avg_college_cost_50km/b.state_median_avg_college_cost_50km, 2) ratio_state_median_avg_college_cost_50km
    ,round(a.avg_college_completion_rate_50km/b.state_median_avg_college_completion_rate_50km, 2) ratio_state_median_avg_college_completion_rate_50km
    ,round(a.avg_unemployment_rate_1y/b.state_median_avg_unemployment_rate_1y, 2) ratio_state_median_avg_unemployment_rate_1y
    ,round(a.median_gross_rent_one_bedroom/b.state_median_median_gross_rent_one_bedroom, 2) ratio_state_median_median_gross_rent_one_bedroom
    ,round(a.per_capita_income/b.state_median_per_capita_income, 2) ratio_state_median_per_capita_income
    ,round(a.median_household_income/b.state_median_median_household_income, 2) ratio_state_median_median_household_income
    ,round(a.closest_hospital_km/b.state_median_closest_hospital_km, 2) ratio_state_median_closest_hospital_km
    ,round(a.hospital_cnt_50km/b.state_median_hospital_cnt_50km, 2) ratio_state_median_hospital_cnt_50km
    ,round(a.hospital_avg_beds_10km/b.state_median_hospital_avg_beds_10km, 2) ratio_state_median_hospital_avg_beds_10km
    ,round(a.law_enforcement_cnt_10km/b.state_median_law_enforcement_cnt_10km, 2) ratio_state_median_law_enforcement_cnt_10km
    ,round(a.closest_private_school_km/b.state_median_closest_private_school_km, 2) ratio_state_median_closest_private_school_km
    ,round(a.closest_public_school_km/b.state_median_closest_public_school_km, 2) ratio_state_median_closest_public_school_km
    ,round(a.closest_private_elem_school_km/b.state_median_closest_private_elem_school_km, 2) ratio_state_median_closest_private_elem_school_km
    ,round(a.closest_public_elem_school_km/b.state_median_closest_public_elem_school_km, 2) ratio_state_median_closest_public_elem_school_km
    ,round(a.private_school_cnt_10km/b.state_median_private_school_cnt_10km, 2) ratio_state_median_private_school_cnt_10km
    ,round(a.public_school_cnt_10km/b.state_median_public_school_cnt_10km, 2) ratio_state_median_public_school_cnt_10km
    ,round(a.median_age/b.state_median_median_age, 2) ratio_state_median_median_age
    ,round(a.population_density/b.state_median_population_density, 2) ratio_state_median_population_density
    --,round(a.closed_deal_ratio/b.state_median_closed_deal_ratio, 2) ratio_state_median_closed_deal_ratio
    --,round(a.cnt_closed_deals_norm/b.state_median_cnt_closed_deals_norm, 2) ratio_state_median_cnt_closed_deals_norm
    ,round(a.avg_gross_monthly_rent/b.state_median_avg_gross_monthly_rent, 2) ratio_state_median_avg_gross_monthly_rent
    ,round(a.avg_cotenants_for_allocation/b.state_median_avg_cotenants_for_allocation, 2) ratio_state_median_avg_cotenants_for_allocation
    ,round(a.avg_applicant_annual_income_stated/b.state_median_avg_applicant_annual_income_stated, 2) ratio_state_median_avg_applicant_annual_income_stated
    ,round(a.avg_applicant_annual_income_actual/b.state_median_avg_applicant_annual_income_actual, 2) ratio_state_median_avg_applicant_annual_income_actual
    ,round(a.avg_credit_score/b.state_median_avg_credit_score, 2) ratio_state_median_avg_credit_score
    --,round(a.avg_rent_over_income_actual/b.state_median_avg_rent_over_income_actual, 2) ratio_state_median_avg_rent_over_income_actual
    --,round(a.avg_rent_over_income_stated/b.state_median_avg_rent_over_income_stated, 2) ratio_state_median_avg_rent_over_income_stated
    --,round(a.student_ratio/b.state_median_student_ratio, 2) ratio_state_median_student_ratio
    --,round(a.employed_ratio/b.state_median_employed_ratio, 2) ratio_state_median_employed_ratio
    --,round(a.self_employed_ratio/b.state_median_self_employed_ratio, 2) ratio_state_median_self_employed_ratio
    ,round(a.walk_score/b.state_median_walk_score, 2) ratio_state_median_walk_score
    ,round(a.transit_score/b.state_median_transit_score, 2) ratio_state_median_transit_score
    ,round(a.bike_score/b.state_median_bike_score, 2) ratio_state_median_bike_score
from fin a
left join market_features b on a.state = b.state
