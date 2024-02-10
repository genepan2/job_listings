-- DIMENSION TABLES
CREATE TABLE IF NOT EXISTS dimJobs (
    job_id SERIAL PRIMARY KEY,
    title VARCHAR(255),
    title_cleaned VARCHAR(255),
    description TEXT,
    url VARCHAR(255),
    source_identifier VARCHAR(255),
    fingerprint VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS dimCompanies (
    company_id SERIAL PRIMARY KEY,
    name VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS dimLocations (
    location_id SERIAL PRIMARY KEY,
    city VARCHAR(255),
    country VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS dimLanguages (
    language_id SERIAL PRIMARY KEY,
    name VARCHAR(255)
);
CREATE TYPE SourceTypeEnum AS ENUM ('origin', 'meta', 'hybrid');
CREATE TABLE IF NOT EXISTS dimSources (
    source_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    url VARCHAR(255),
    type SourceTypeEnum,
    is_api BOOLEAN DEFAULT FALSE
);
CREATE TABLE IF NOT EXISTS dimJobLevels (
    job_level_id SERIAL PRIMARY KEY,
    name VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS dimSearchKeywords (
    search_keyword_id SERIAL PRIMARY KEY,
    name VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS dimSearchLocations (
    search_location_id SERIAL PRIMARY KEY,
    name VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS dimDates (
    date_id SERIAL PRIMARY KEY,
    date_unique VARCHAR(12),
    year INT NOT NULL,
    month INT NOT NULL,
    week INT NOT NULL,
    day INT NOT NULL,
    hour INT NOT NULL,
    minute INT NOT NULL,
    week_day INT NOT NULL,
    is_holiday BOOLEAN DEFAULT FALSE
);
CREATE TABLE IF NOT EXISTS dimEmployments (
    employment_id SERIAL PRIMARY KEY,
    name VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS dimIndustries (
    industry_id SERIAL PRIMARY KEY,
    name VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS dimSkillCategories (
    skill_category_id SERIAL PRIMARY KEY,
    name VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS dimTechnologyCategories (
    technology_category_id SERIAL PRIMARY KEY,
    name VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS dimSkills (
    skill_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    skill_category_key INT REFERENCES dimSkillCategories(skill_category_id)
);
CREATE TABLE IF NOT EXISTS dimTechnologies (
    technology_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    technology_category_key INT REFERENCES dimTechnologyCategories(technology_category_id)
);
-- FACT TABLE
CREATE TABLE IF NOT EXISTS fctJobListings (
    job_listing_id SERIAL PRIMARY KEY,
    job_key INT REFERENCES dimJobs(job_id),
    company_key INT REFERENCES dimCompanies(company_id),
    source_key INT REFERENCES dimSources(source_id),
    search_date_key INT REFERENCES dimDates(date_id),
    -- References to dimDates
    -- search_word_key INT REFERENCES dimSearchKeywords(),
    -- search_location_key INT REFERENCES dimSearchLocations(),
    publish_date_key INT REFERENCES dimDates(date_id),
    -- References to dimDates
    close_date_key INT REFERENCES dimDates(date_id),
    -- References to dimDates
    language_key INT REFERENCES dimLanguages(language_id),
    job_level_key INT REFERENCES dimJobLevels(job_level_id),
    employment_key INT REFERENCES dimEmployments(employment_id),
    industry_key INT REFERENCES dimIndustries(industry_id),
    job_apps_count INT,
    list_dur_days INT,
    scrape_dur_ms INT
);
-- BRIDGE TABLES
CREATE TABLE IF NOT EXISTS JobLocationsBridge (
    job_location_id SERIAL PRIMARY KEY,
    job_listing_key INT REFERENCES fctJobListings(job_listing_id),
    location_key INT REFERENCES dimLocations(location_id)
);
-- CREATE TABLE IF NOT EXISTS JobLocationsBridge (
--     location_key INT,
--     job_listing_key INT,
--     PRIMARY KEY (location_key, job_listing_key),
--     FOREIGN KEY (location_key) REFERENCES dimLocations(location_id)
--     FOREIGN KEY (job_listing_key) REFERENCES fctJobListings(job_listing_id),
-- );
CREATE TABLE IF NOT EXISTS JobSkillsBridge (
    job_skill_id SERIAL PRIMARY KEY,
    job_listing_key INT REFERENCES fctJobListings(job_listing_id),
    skill_key INT REFERENCES dimSkills(skill_id)
);
CREATE TABLE IF NOT EXISTS JobTechnologiesBridge (
    job_technology_id SERIAL PRIMARY KEY,
    job_listing_key INT REFERENCES fctJobListings(job_listing_id),
    technology_key INT REFERENCES dimTechnologies(technology_id)
);
CREATE TABLE IF NOT EXISTS JobSearchKeywordBridge (
    job_search_keyword_id SERIAL PRIMARY KEY,
    job_listing_key INT REFERENCES fctJobListings(job_listing_id),
    search_keyword_key INT REFERENCES dimSearchKeywords(search_keyword_id)
);
CREATE TABLE IF NOT EXISTS JobSearchLocationBridge (
    job_search_keyword_id SERIAL PRIMARY KEY,
    job_listing_key INT REFERENCES fctJobListings(job_listing_id),
    search_location_key INT REFERENCES dimSearchLocations(search_location_id)
);
---------------------
-- INSERT Initial Data
INSERT INTO dimLocations (city, country)
VALUES ('Other', 'Global'),
    ('Remote', 'Global'),
    ('Hybrid', 'Global'),
    ('Berlin', 'Germany'),
    ('Munich', 'Germany'),
    ('Hamburg', 'Germany'),
    ('Cologne', 'Germany'),
    ('Frankfurt', 'Germany');
INSERT INTO dimLanguages (name)
VALUES ('Other'),
    ('English'),
    ('German');
INSERT INTO dimSources (name, url, type, is_api)
VALUES (
        'LinkedIn',
        'https://www.linkedin.com/',
        'origin',
        false
    ),
    (
        'WhatJobs',
        'https://de.whatjobs.com/',
        'meta',
        false
    ),
    (
        'TheMuse',
        'https://www.themuse.com/',
        'meta',
        true
    );
INSERT INTO dimJobLevels (name)
VALUES ('Other'),
    ('Student'),
    ('Internship'),
    ('Entry'),
    ('Middle'),
    ('Senior'),
    ('Lead'),
    ('Head');
INSERT INTO dimSearchKeywords (name)
VALUES ('Other'),
    ('Big Data Engineer'),
    ('Business Intelligence Engineer'),
    ('Data Analyst'),
    ('Data and Analytics'),
    ('Data Engineer'),
    ('Data Science'),
    ('Data Scientist'),
    ('Data'),
    ('Machine Learning Engineer');
INSERT INTO dimEmployments (name)
VALUES ('Other'),
    ('Full-time'),
    ('Part-time'),
    ('Contract');
INSERT INTO dimIndustries (name)
VALUES ('Other'),
    ('IT Services and IT Consulting'),
    ('Technology, Information and Internet'),
    ('Information Technology & Services'),
    ('Computer and Network Security'),
    ('Computer Games'),
    ('Computer Hardware'),
    ('Computer Networking'),
    ('Computer Software'),
    ('Consumer Electronics');