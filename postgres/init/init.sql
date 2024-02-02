---------------------
-- DIMENSION TABLES

CREATE TABLE IF NOT EXISTS dimJobs (
    JobId SERIAL PRIMARY KEY,
    Title VARCHAR(255),
    TitleCleaned VARCHAR(255),
    Description TEXT,
    Url VARCHAR(255),
    SourceListingIdentifier VARCHAR(255),
    Fingerprint VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dimLocations (
    LocationId SERIAL PRIMARY KEY,
    City VARCHAR(255),
    Country VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dimLanguages (
    LanguageId SERIAL PRIMARY KEY,
    Name VARCHAR(255)
);

CREATE TYPE SourceTypeEnum AS ENUM ('origin', 'meta', 'hybrid');
CREATE TABLE IF NOT EXISTS dimSources (
    SourceId SERIAL PRIMARY KEY,
    Name VARCHAR(255),
    Url VARCHAR(255),
    Type SourceTypeEnum,
    isApi BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS dimJobLevels (
    JobLevelId SERIAL PRIMARY KEY,
    Name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dimSearchKeywords (
    SearchKeywordId SERIAL PRIMARY KEY,
    Name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dimDates (
    DateId SERIAL PRIMARY KEY,
    Year INT NOT NULL,
    Month INT NOT NULL,
    Week INT NOT NULL,
    Day INT NOT NULL,
    Hour INT NOT NULL,
    Minute INT NOT NULL,
    WeekDay INT NOT NULL,
    isHoliday BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS dimEmployments (
    EmploymentId SERIAL PRIMARY KEY,
    Name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dimIndustries (
    IndustryId SERIAL PRIMARY KEY,
    Name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dimSkills (
    SkillId SERIAL PRIMARY KEY,
    Name VARCHAR(255),
    SkillCategoryKey INT REFERENCES dimSkillCategory(SkillCategoryId)
);

CREATE TABLE IF NOT EXISTS dimTechnologies (
    TechnologyId SERIAL PRIMARY KEY,
    Name VARCHAR(255),
    TechnologyCategoryKey INT REFERENCES dimTechnologyCategory(TechnologyCategoryId)
);

CREATE TABLE IF NOT EXISTS dimSkillCategory (
    SkillCategoryId SERIAL PRIMARY KEY,
    Name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dimTechnologyCategory (
    TechnologyCategoryId SERIAL PRIMARY KEY,
    Name VARCHAR(255)
);

---------------------
-- BRIDGE TABLES

CREATE TABLE IF NOT EXISTS JobLocationsBridge (
    JobLocationId SERIAL PRIMARY KEY,
    JobListingKey INT REFERENCES fctJobListings(JobListingId),
    LocationKey INT REFERENCES dimLocations(LocationId)
);

CREATE TABLE IF NOT EXISTS JobSkillsBridge (
    JobSkillId SERIAL PRIMARY KEY,
    JobListingKey INT REFERENCES fctJobListings(JobListingId),
    SkillKey INT REFERENCES dimSkills(SkillId)
);

CREATE TABLE IF NOT EXISTS JobTechnologiesBridge (
    JobTechnologyId SERIAL PRIMARY KEY,
    JobListingKey INT REFERENCES fctJobListings(JobListingId),
    TechnologyKey INT REFERENCES dimTechnology(TechnologyId)
);

CREATE TABLE IF NOT EXISTS JobSearchKeywordBridge (
    JobSearchKeywordId SERIAL PRIMARY KEY,
    JobListingKey INT REFERENCES fctJobListings(JobListingId),
    SearchKeywordKey INT REFERENCES dimSearchKeywords(SearchKeywordId)
);

---------------------
-- FACT TABLE

CREATE TABLE IF NOT EXISTS fctJobListings (
    JobListingId SERIAL PRIMARY KEY,
    JobKey INT REFERENCES dimJobs(JobId),
    SourceKey INT REFERENCES dimSources(SourceId),
    SearchDateKey INT REFERENCES dimDate(DateId), -- References to dimDate
    SearchWordKey VARCHAR(255),
    SearchLocationKey VARCHAR(255),
    PublishDateKey INT REFERENCES dimDate(DateId), -- References to dimDate
    CloseDateKey INT REFERENCES dimDate(DateId), -- References to dimDate
    LanguageKey INT REFERENCES dimLanguages(LanguageId),
    JobLevelKey INT REFERENCES dimJobLevels(JobLevelId),
    EmploymentKey INT REFERENCES dimEmployments(EmploymentId),
    IndustryKey INT REFERENCES dimIndustries(IndustryId),
    NumberOfApplications INT,
    ListingDurationDays INT,
    ScrapeDurationMilliseconds INT
);


---------------------
-- INSERT Initial Data

INSERT INTO dimLocations (City, Country) VALUES
('Other', 'Global'),
('Remote', 'Global'),
('Hybrid', 'Global'),
('Berlin', 'Germany'),
('Munich', 'Germany'),
('Hamburg', 'Germany'),
('Cologne', 'Germany'),
('Frankfurt', 'Germany');

INSERT INTO dimLanguages (Name) VALUES
('Other'),
('English'),
('German');

INSERT INTO dimSources (Name, Url, Type, isApi) VALUES
('LinkedIn', 'https://www.linkedin.com/', 'origin', false),
('WhatJobs', 'https://de.whatjobs.com/', 'meta', false),
('TheMuse', 'https://www.themuse.com/', 'meta', true);

INSERT INTO dimJobLevels (Name) VALUES
('Other'),
('Student'),
('Internship'),
('Entry'),
('Middle'),
('Senior'),
('Lead'),
('Head');

INSERT INTO dimSearchKeywords (Name) VALUES
('Other'),
('Big Data Engineer'),
('Business Intelligence Engineer'),
('Data Analyst'),
('Data and Analytics'),
('Data Engineer'),
('Data Science'),
('Data Scientist'),
('Data'),
('Machine Learning Engineer');

INSERT INTO dimEmployments (Name) VALUES
('Other'),
('Full-time'),
('Part-time'),
('Contract');

INSERT INTO dimIndustries (Name) VALUES
('Other'),
('IT Services and IT Consulting'),
('Technology, Information and Internet'),
('Information Technology & Services'),
('Computer and Network Security'),
('Computer Games'),
('Computer Hardware'),
('Computer Networking'),
('Computer Software'),
('Consumer Electronics');