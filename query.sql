use cs3380;

-- Top 10 counties ordered by libraries count then by schools_count
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

-- Counties with the average proportion of librarians as employees < 15%
select 
    counties.county_code, 
    counties.state_code, 
    format(avg(((librarians / (librarians + employees)) * 100)), 'N2') as average_percentage_of_librarians_in_library_staff_for_county
from counties 
join states on counties.state_code = states.state_code 
join libraries on states.state_code = libraries.state_code and counties.county_code = libraries.county_code 
join staff_members_counts on staff_members_counts.staff_members_count_id = libraries.staff_members_count_id 
where (librarians + employees) > 0 
group by counties.county_code, counties.state_code 
having avg(((librarians / (librarians + employees)) * 100)) < 15.00 
order by avg(((librarians / (librarians + employees)) * 100))

-- Top 20 counties with highest county population over libraries count 
select
    counties.state_code
    county_population, 
    count(distinct libraries.library_id) as library_count, 
    (county_population / count(distinct libraries.library_id)) as county_population_over_library_count
from counties 
join states on counties.state_code = states.state_code 
join libraries on states.state_code = libraries.state_code and counties.county_code = libraries.county_code 
join schools on states.state_code = schools.state_code 
group by counties.county_code, counties.state_code, county_population
order by county_population_over_library_count desc; 

-- Library with total operating revenue closest to n
select 
    library_name, 
    (local_government_operating_revenue + state_government_operating_revenue + federal_government_operating_revenue + other_operating_revenue) as total_operating_revenue
from libraries
join operating_revenues on libraries.operating_revenue_id = operating_revenues.operating_revenue_id
order by total_operating_revenue 

