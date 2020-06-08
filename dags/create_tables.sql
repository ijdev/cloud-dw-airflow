CREATE TABLE public.i94staging (
	ccid bigint NOT NULL PRIMARY KEY,
    i94yr bigint,
    i94mon bigint,
    i94cit bigint,
    i94res int, 
    i94port varchar(10),
    arrdate bigint,
    i94mode bigint,
    i94addr varchar(10),
    depdate bigint,
    i94bir int,
    i94visa int,
    count bigint,
    dtadfile bigint,
    visapost varchar(10),
    occup varchar(10),
    entdepa varchar(10),
    entdepd varchar(10),
    entdepu varchar(10),
    matflag varchar(10),
    biryear bigint,
    dtaddto varchar(256),
    gender varchar(10),
    insnum int,
    airline varchar(10),
    admnum varchar(256),
    fltno varchar(50),
    visatype varchar(50)
);


CREATE TABLE public.demo_staging (
	city varchar(256) NOT NULL PRIMARY KEY,
    state varchar(256) NOT NULL,
	median_age REAL,
    male_pop REAL,
    female_pop REAL,
    total_pop bigint,
    no_vet REAL,
    foreign_born REAL,
    avg_household_size REAL,
    state_code varchar(10),
    race varchar(256) NOT NULL,
    count bigint
);

CREATE TABLE public.state (
	state_code varchar(10) NOT NULL PRIMARY KEY,
    state varchar(256)
);

CREATE TABLE public.city (
	city varchar(256) NOT NULL PRIMARY KEY,
    state varchar(256),
	median_age REAL,
    male_pop REAL,
    female_pop REAL,
    total_pop bigint,
    no_vet REAL,
    foreign_born REAL,
    avg_household_size REAL,
    state_code varchar(10) REFERENCES state(state_code),
    race varchar(256) NOT NULL,
    count bigint
);
CREATE TABLE public.country_lookup (
	code int NOT NULL PRIMARY KEY,
	country varchar
);

CREATE TABLE public.port_lookup (
	port_code varchar(10) NOT NULL PRIMARY KEY,
    port_name varchar(256)
);

CREATE TABLE public.visa_lookup(
	visa_code varchar(10) NOT NULL PRIMARY KEY,
    visa_type varchar(256)
);

CREATE TABLE public.country (
	code int NOT NULL PRIMARY KEY,
	country varchar
);


CREATE TABLE public.port (
	port_code varchar(10) NOT NULL PRIMARY KEY,
    port_name varchar(256)
);
CREATE TABLE public.visa(
	visa_code varchar(10) NOT NULL PRIMARY KEY,
    visa_type varchar(256)
);
CREATE TABLE public.i94fact (
	ccid bigint NOT NULL PRIMARY KEY,
    country_code int REFERENCES country(code), 
    state_code varchar(10) REFERENCES state(state_code),
    port_code varchar(10) REFERENCES port(port_code),
    age_bir int,
    visa_code varchar(10) REFERENCES visa(visa_code),
    biryear int,
    gender varchar(10)
);
