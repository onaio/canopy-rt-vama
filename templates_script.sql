---Creates the templates schema to host the templates from Inform
create schema if not exists templates;
alter schema templates owner to rt_vama;

---Creates the CSV schema to host the location templates and any CSV file that is uploaded to the database a
create schema if not exists csv;
alter schema csv owner to rt_vama;


---Creates the schema that hosts the final views for visualization
create schema if not exists reporting;
alter schema reporting owner to rt_vama;

---Creates a staging schema that hosts the views that have labels from the registry table and views that can be joined to create a final view for visualization
create schema if not exists staging;
alter schema staging owner to rt_vama;

---Supplemental Immunization Activity labels table
--- When pulling data from Inform, the connector separates the labels from the actual values by creating separate tables. Labels are normally defined on the choices worksheet of the XLSForm
--- This view extracts labels from the registry table for the SIA form
create or replace view staging.sia_labels as 
(
-- Extract the json column we want, limit by 1 row since the data has a row for each filled record.
with dd as
(
select 
json -> 'xform:choices' as data 
from templates.registry
where uri = 'Supplemental_Immunization_Activity?t=json&v=202302071328'
limit 1
), 
-- Strip down the json to the columns we want, question and label details (this includes the language(s) in the form).
unnest_1 as (
select 
js.key as question, 
js.value as label_details
from dd, jsonb_each(dd.data) as js
), 
-- Strip down the json further this time getting the code (name column in the xlsform)
unnest_2 as (
select unnest_1.question, 
js1.key as code, js1.value as language
from unnest_1, jsonb_each(unnest_1.label_details) as js1
)
-- Since the data has a slash(/) sperator for the choices question, we are reversing the order then get the first object before the first slash then reverse it back.
select 
    reverse(split_part(reverse(unnest_2.question), '/', 1)) question,
    unnest_2.code,
    -- Since languages are dynamic,we add this manually for each of the languages used ("und" is when no language has been specified on the form)
    unnest_2.language ->> 'und' AS label
from unnest_2
);
alter view staging.sia_labels owner to rt_vama;


---Supplemental Immunization Activity
----Template link: https://inform.unicef.org/uniceftemplates/635/759
--- This script unnests the SIA template with actual values
--- The location information has been loaded from external CSV files which have been uploaded to the csv schema
create or replace view staging.sia_actuals as 
(
select 
  siav.parent_id as submission_id,
  sia.date_vaccination_activity,
  a1.label as admin1,
  a2.label as admin2,
  a3.label as admin3,
  a4.label as admin4,
  bg.latitude,
  bg.longitude,
  a5.label as admin5,
  sia.vaccine_label as vaccine_administered,
  siav.age_group_label,
  unnest(array['Vaccinated','Vaccinated','Deferred Vaccinated','Deferred Vaccinated','Refused Vaccinated','Refused Vaccinated','Deferred','Deferred','Refused','Refused']) as coverage_category,
  unnest(array['Males','Females','Males','Females','Males','Females','Males','Females','Males','Females']) as indicator_category,
  unnest(array[vaccinated_males,vaccinated_females,vaccinated_males_previously_deferred,vaccinated_females_previously_deferred,vaccinated_males_previously_refused,vaccinated_females_previously_refused,deferred_males,deferred_females,refused_males,refused_females]) as indicator_value
from templates.supplemental_immunization_activity_vaccine siav 
left join templates.supplemental_immunization_activity sia on siav.parent_id=sia.id  --- Adds the fields assosciated with the repeat group data
left join csv.admin1 a1 on sia.admin1=a1.name::text ---Adds admin 1 labels using the admin name column
left join csv.admin2 a2 on sia.admin2=a2.name::text ---Adds admin 2 labels using the admin name column
left join csv.admin3 a3 on sia.admin3=a3.name::text ---Adds admin 3 labels using the admin name column
left join csv.admin4 a4 on sia.admin4=a4.name::text ---Adds admin 4 labels using the admin name column
left join csv.admin5 a5 on sia.admin5=a5.name::text ---Adds admin 5 labels using the admin name column
left join csv.barangay_gps bg on sia.admin4=bg.barangay_code::text
);
alter view staging.sia_actuals owner to rt_vama;


---This script creates a view that aggregates the actual values to admin 4 level so that the actuals can be matched to the daily target values
create or replace view staging.aggregated_sia_actuals_target as
(
with 
targets as 
--Retrieves the campaign target from the targets form, then computes the daily target based on the number of days the campaign is supposed to take.
--The link to the targets form: https://inform.unicef.org/uniceftemplates/635/977 
(
select 
 siat.id,
 siat.admin0,
 siat.admin1,
 siat.admin2,
 siat.admin3,
 siat.admin4,
 siat.admin5,
 siat.campaign_start_date,
 siat.campaign_end_date,
 (siat.campaign_end_date - siat.campaign_start_date) as campaign_days,
 siat.vaccine_label,
 siattc.no_children as campaign_target,
 siattc.no_children / (siat.campaign_end_date - siat.campaign_start_date) as daily_target,
 siattc.age_group_label 
from templates.supplemental_immunization_activity_target siat  
left join templates.supplemental_immunization_activity_target_target_children siattc on siattc.parent_id=siat.id 
),
actuals as 
---Retrieves the actual values from the vaccination coverage group
(
select 
  sia.date_vaccination_activity,
  sia.admin1,
  sia.admin2,
  sia.admin3,
  sia.admin4,
  sia.admin5,
  sia.vaccine_label as vaccine_administered,
  SUM(siav.vaccinated_males + siav.vaccinated_females) as total_vaccinated,
  siav.age_group_label 
from templates.supplemental_immunization_activity_vaccine siav 
left join templates.supplemental_immunization_activity sia on siav.parent_id=sia.id
group by 1,2,3,4,5,6,7,9
),
vaccine_doses as
----Aggregates the vials used, vials discarded and vaccine dose up to admin 4
(
select 
 sia.date_vaccination_activity,
 sia.admin1,
 sia.admin2,
 sia.admin3,
 sia.admin4,
 sia.admin5,
 sia.vaccine_label,
 sia.vial_dosage,
 SUM(sia.vials_used) as vials_used,
 SUM(sia.vial_dosage::int*sia.vials_used) as vaccine_dose,
 SUM(sia.vials_discarded) as vials_discarded 
from templates.supplemental_immunization_activity sia
group by 1,2,3,4,5,6,7,8
)
select 
 date,
 a1.label as admin1,
 a2.label as admin2,
 a3.label as admin3,
 a4.label as admin4,
 a5.label as admin5,
 a.vaccine_administered,
 a.total_vaccinated
 ,
 t.daily_target,
 vd.vials_used,
 vd.vials_discarded,
 vd.vaccine_dose
from csv.hard_coded_dates hcd 
left join actuals a on a.date_vaccination_activity=hcd.date
left join targets t on a.vaccine_administered=t.vaccine_label and t.age_group_label=a.age_group_label and a.date_vaccination_activity between t.campaign_start_date and t.campaign_end_date 
left join csv.admin1 a1 on a.admin1=a1.name::text ---Adds admin 1 labels using the admin name column
left join csv.admin2 a2 on a.admin2=a2.name::text ---Adds admin 2 labels using the admin name column
left join csv.admin3 a3 on a.admin3=a3.name::text ---Adds admin 3 labels using the admin name column
left join csv.admin4 a4 on a.admin4=a4.name::text ---Adds admin 4 labels using the admin name column
left join csv.admin5 a5 on a.admin5=a5.name::text ----Adds admin 5 labels using the admin name column
left join vaccine_doses vd on vd.date_vaccination_activity=hcd.date and vd.vaccine_label=a.vaccine_administered and a.admin5=vd.admin5 --matches the value at reporting date, vaccine and admin5 level
where hcd.date<=now()::date and a.date_vaccination_activity is not null ----Filters dates that are not within the actuals form
);
alter view staging.aggregated_sia_actuals_target owner to rt_vama;

---This view creates the social mobilization indicators
--- The link to the template: https://inform.unicef.org/uniceftemplates/635/765
--- The social mobilization indicators data entails communication activities during or after a campaign.
create or replace view staging.social_mobilization_indicators as 
(
with rumors_misinformation as 
(
----This section unnests the select multiple option under Rumors and Misinformation
select  
 id,
 unnest(array['Vaccine related','Vaccination team','Vaccination campaign']) as indicators_category,
 unnest(array[vaccine_related,vaccination_team,vaccination_campaign]) as indicators_value
from templates.spv_social_mobilization_indicators ssmi 
),
mobilization_indicators as
(
--This section creates a tidy section of the social mobilization activities such as number of households visited etc.
select 
 id,
 unnest(array['Social mobilisers/community volunteers engaged','Households visited','Group meetings/learning sessions with caregivers conducted','Religious institutions visited','Advocacy meetings with community leaders','Posters and banners produced and displayed']) as indicator_category,
 unnest(array[social_mobilisers,hhs_visited,learning_sessions,religious_institutions,advocacy_meetings,posters_banners]) as indicator_value
from templates.spv_social_mobilization_indicators ssmi 
),
----This section creates a tidy section of the refusals addressed from the spv_social_mobilization_indicators_refusals repeat group data table
refusals_addressed as
(
select 
parent_id as id,
unnest(array['Refusals Addressed']) as indicator_category,
unnest(array[refusals_addressed]) as indicator_value
from templates.spv_social_mobilization_indicators_refusals ssmir 
),
----This section unions all the subsections above
combined_indicators as 
(
select * from rumors_misinformation
union all 
select * from mobilization_indicators
union all 
select * from refusals_addressed
)
select
  c.id, 
  ssmi.assessment_date,
  a1.label as admin1,
  a2.label as admin2,
  a3.label as admin3,
  a4.label as admin4,
  case 
  	when ssmi.conducted_by='others' then conducted_by_other else conducted_by 
  end as conducted_by,  
  c.indicators_category,
  c.indicators_value
from combined_indicators c  
left join templates.spv_social_mobilization_indicators ssmi on c.id=ssmi.id
left join csv.admin1 a1 on ssmi.admin1=a1.name::text ---Adds admin 1 labels using the admin name column
left join csv.admin2 a2 on ssmi.admin2=a2.name::text ---Adds admin 2 labels using the admin name column
left join csv.admin3 a3 on ssmi.admin3=a3.name::text ---Adds admin 3 labels using the admin name column
left join csv.admin4 a4 on ssmi.admin4=a4.name::text ---Adds admin 4 labels using the admin name column
);

alter view staging.social_mobilization_indicators owner to rt_vama;

 -----Synchronized Vaccination Monitoring Tool
----Inform link: https://inform.unicef.org/uniceftemplates/635/847
---This view creates a tidy table of the Synchronized Vaccination Monitoring Tool form
create or replace view staging.monitoring_tool as 
(
select 
  svmt.id,
  svmt.date_vaccination_activity,
  a1.label as admin1,
  a2.label as admin2,
  a3.label as admin3,
  a4.label as admin4,
  bg.latitude,
  bg.longitude,
  a5.label as admin5,
  svmt.vaccine_label,
  unnest(array[part1_indicator1,part1_indicator2,part1_indicator3,part1_indicator4,part1_indicator5,part1_indicator6,part1_indicator7,part1_indicator8,part1_indicator9,part1_indicator10,part1_indicator11,part1_indicator12,part1_indicator13,part1_indicator14,part1_indicator15,part1_indicator16,part1_indicator17,part1_indicator18,part1_indicator19,part1_indicator20,part1_indicator21,part1_indicator22,part1_indicator23,part1_indicator24,part2_indicator1,part2_indicator2,part2_indicator3,part2_indicator4,part2_indicator5,part2_indicator6,part2_indicator7,part2_indicator8,part2_indicator9,part2_indicator10,part2_indicator11,part2_indicator12,part2_indicator13,part2_indicator14,part2_indicator15,part2_indicator16,part2_indicator17,part2_indicator18,part2_indicator19,part2_indicator20,part2_indicator21,part2_indicator22,part2_indicator23,part2_indicator24,part2_indicator25,part2_indicator26,part2_indicator27,part2_indicator28,part2_indicator29,part2_indicator30]) as indicators_value,
  --]) as indicators_value,
  unnest(array[part1_indicator1_remarks,part1_indicator2_remarks,part1_indicator3_remarks,part1_indicator4_remarks,part1_indicator5_remarks,part1_indicator6_remarks,part1_indicator7_remarks,part1_indicator8_remarks,part1_indicator9_remarks,part1_indicator10_remarks,part1_indicator11_remarks,part1_indicator12_remarks,part1_indicator13_remarks,part1_indicator14_remarks,part1_indicator15_remarks,part1_indicator16_remarks,part1_indicator17_remarks,part1_indicator18_remarks,part1_indicator19_remarks,part1_indicator20_remarks,part1_indicator21_remarks,part1_indicator22_remarks,part1_indicator23_remarks,part1_indicator24_remarks,part2_indicator1_remarks,part2_indicator2_remarks,part2_indicator3_remarks,part2_indicator4_remarks,part2_indicator5_remarks,part2_indicator6_remarks,part2_indicator7_remarks,part2_indicator8_remarks,part2_indicator9_remarks,part2_indicator10_remarks,part2_indicator11_remarks,part2_indicator12_remarks,part2_indicator13_remarks,part2_indicator14_remarks,part2_indicator15_remarks,part2_indicator16_remarks,part2_indicator17_remarks,part2_indicator18_remarks,part2_indicator19_remarks,part2_indicator20_remarks,part2_indicator21_remarks,part2_indicator22_remarks,part2_indicator23_remarks,part2_indicator24_remarks,part2_indicator25_remarks,part2_indicator26_remarks,part2_indicator27_remarks,part2_indicator28_remarks,part2_indicator29_remarks,part2_indicator30_remarks]) as indicators_remarks,
  --]) as indicators_remarks,
  unnest(array['Presence of data board','Presence of health center microplan','Presence of spot map','Indication of population/specific target','Inclusion of activities for social preparation','Inclusion of dialogues with local officials/CSG','Public announcements are made','Evidence that social mobilization were done','Presence of activities to enable access in hard to reach areas are expected','Training of vaccination teams on comms and social mobilization','Presence of daily itinerary schedule','Presence of specific vaccination strategy','Supervisory plan','Presence of separate sheet for vaccines and other logistic calculations','Enough campaign forms','Enough mother/child book or vaccination cards','Presence of transportation support','Response/referral for AEFI','Presence of contingency plan to include emergencies in case of absence of vaccination team member','Schedule for mop ups','Plan for RCA intra-campaign','Evidence of regular feedback meeting','Health care waste plan','Follow up visits','Presence of health facility management plan','Presence of continuous electricity supply','Presence of generator/solar power that can be used in case of intermittent power supply','Presence of refrigiration that can be used for vaccine','Vaccines placed in separate box','Proper label is used for vaccine','Vaccines are stored with appropriate temperature','Presence of adequate temperature monitoring devices','Conduct of regular temperature monitoring','Proper temperature monitoring','Note of temperature breach','Availability of ice pack freezing capacity','Recording of vaccines that are issued daily','Proper filling up of forms','Presence of enough vaccine carriers','Presence of enough ice packs','Providing immunzation at a fixed post','Presence of vaccine carrier that is separately label','Use of resealable plastic','Use of resealable plastic for used vials','Return of reusable vials','Accounting of all collected vials','Presence of vaccine accountability monitor','Placing of collected vials in a secured container','Empty vials, sealed properly','Returning of un-opened/un used vial','Account of used and unused vials','Missing vials identified','Replaced vials identified','Damaged vials']) as indicators_label
  --]) as indicators_label 
from templates.synchronized_vaccination_monitoring_tool svmt
left join csv.admin1 a1 on svmt.admin1=a1.name::text ---Adds admin 1 labels using the admin name column
left join csv.admin2 a2 on svmt.admin2=a2.name::text ---Adds admin 2 labels using the admin name column
left join csv.admin3 a3 on svmt.admin3=a3.name::text ---Adds admin 3 labels using the admin name column
left join csv.admin4 a4 on svmt.admin4=a4.name::text ---Adds admin 4 labels using the admin name column
left join csv.admin5 a5 on svmt.admin5=a5.name::text ---Adds admin 5 labels using the admin name column
left join csv.barangay_gps bg on svmt.admin4=bg.barangay_code::text
);

alter view staging.monitoring_tool owner to rt_vama;