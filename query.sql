use cs3380;

select top 10 
    counties.county_code, 
    counties.state_code, 
    count(distinct schools.school_code) as schools_count, 
    count(distinct libraries.library_id) as libraries_count 
from counties 
join states on counties.state_code = states.state_code 
join libraries on states.state_code = libraries.state_code and counties.county_code = libraries.county_code 
join schools on states.state_code = schools.state_code 
group by counties.county_code, counties.state_code 
order by libraries_count desc, schools_count desc;