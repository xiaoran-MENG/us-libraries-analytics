use cs3380;

drop table if exists schools;
drop table if exists libraries;
drop table if exists counties;
drop table if exists states;
drop table if exists databases_counts;
drop table if exists operating_revenues;
drop table if exists capital_revenues;
drop table if exists staff_members_counts;
drop table if exists collection_expenditures;
drop table if exists employee_expenditures;

create table states (
    state_code integer primary key,
    state_alpha_code varchar(100) not null
);

create table schools (
    school_code integer primary key,
    school_name text not null,
    state_code integer references states(state_code)
);

create table counties (
    state_code integer not null,
    county_code integer not null,
    county_population integer not null,
    county_name text not null,
    primary key (state_code, county_code),
    foreign key (state_code) references states(state_code)
);

create table staff_members_counts (
    staff_members_count_id integer primary key,
    librarians numeric not null,
    employees numeric not null
);

create table operating_revenues (
    operating_revenue_id integer primary key,
    local_government_operating_revenue numeric not null,
    state_government_operating_revenue numeric not null,
    federal_government_operating_revenue numeric not null,
    other_operating_revenue numeric not null
);

create table employee_expenditures (
    employee_expenditure_id integer primary key,
    salaries numeric not null,
    benefits numeric not null
);

create table collection_expenditures (
    collection_expenditure_id integer primary key,
    print_collection_expenditures numeric not null,
    digital_collection_expenditures numeric not null,
    other_collection_expenditures numeric not null,
);

create table capital_revenues (
    capital_revenue_id integer primary key,
    local_government_capital_revenue numeric not null,
    state_government_capital_revenue numeric not null,
    federal_government_capital_revenue numeric not null,
    other_capital_revenue numeric not null
);

create table databases_counts (
    databases_count_id integer primary key,
    local_cooperative_agreements integer not null,
    state_licensed_databases integer not null,
);

create table libraries (
    library_id varchar(100) primary key,
    library_name text not null,
    street_address text not null,
    city text not null,
    zipcode integer not null,
    longitude numeric not null,
    latitude numeric not null,
    state_code integer not null,
    county_code integer not null,

    staff_members_count_id integer references staff_members_counts(staff_members_count_id),
    operating_revenue_id integer references operating_revenues(operating_revenue_id),
    employee_expenditure_id integer references employee_expenditures(employee_expenditure_id),
    collection_expenditure_id integer references collection_expenditures(collection_expenditure_id),
    capital_revenue_id integer references capital_revenues(capital_revenue_id),
    databases_count_id integer references databases_counts(databases_count_id),
    foreign key (state_code, county_code) references counties(state_code, county_code)
);