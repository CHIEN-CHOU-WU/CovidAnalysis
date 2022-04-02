USE Covid_data;

-- Create table
CREATE TABLE coviddeaths (
	iso_code varchar(255),
	continent varchar(255),
	location varchar(255),
	date varchar(255),
	population bigint,
	total_cases bigint,
	new_cases bigint,
	new_cases_smoothed float,
	total_deaths bigint,
	new_deaths bigint,
	new_deaths_smoothed double,
	total_cases_per_million double,
	new_cases_per_million double,
	new_cases_smoothed_per_million float,
	total_deaths_per_million float,
	new_deaths_per_million float,
	new_deaths_smoothed_per_million float,
	reproduction_rate float,
	icu_patients int,
	icu_patients_per_million float,
	hosp_patients int,
	hosp_patients_per_million float,
	weekly_icu_admissions int,
	weekly_icu_admissions_per_million float,
	weekly_hosp_admissions int,
	weekly_hosp_admissions_per_million float
);
CREATE TABLE covidvaccinations (
	iso_code varchar(255),
	continent varchar(255),
	location varchar(255),
	date varchar(255),
	total_tests bigint,
	new_tests bigint,
	total_tests_per_thousand double,
	new_tests_per_thousand double,
	new_tests_smoothed double,
	new_tests_smoothed_per_thousand double,
	positive_rate float,
	tests_per_case float,
	tests_units varchar(255),
	total_vaccinations bigint,
	people_vaccinated bigint,
	people_fully_vaccinated bigint,
	total_boosters bigint,
	new_vaccinations bigint,
	new_vaccinations_smoothed double,
	total_vaccinations_per_hundred double,
	people_vaccinated_per_hundred double,
	people_fully_vaccinated_per_hundred double,
	total_boosters_per_hundred double,
	new_vaccinations_smoothed_per_million double,
	new_people_vaccinated_smoothed double,
	new_people_vaccinated_smoothed_per_hundred double,
	stringency_index float,
	population_density float,
	median_age double,
	aged_65_older double,
	aged_70_older double,
	gdp_per_capita double,
	extreme_poverty double,
	cardiovasc_death_rate double,
	diabetes_prevalence double,
	female_smokers double,
	male_smokers double,
	handwashing_facilities double,
	hospital_beds_per_thousand double,
	life_expectancy double,
	human_development_index double,
	excess_mortality_cumulative_absolute double,
	excess_mortality_cumulative double,
	excess_mortality double,
	excess_mortality_cumulative_per_million double
);

-- Load data from csv
LOAD DATA LOCAL INFILE '/Users/chienchouwu/Desktop/CovidDeaths.csv' 
INTO TABLE coviddeaths 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/Users/chienchouwu/Desktop/covidvaccinations.csv' 
INTO TABLE covidvaccinations 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Convert date to right format
SET SQL_SAFE_UPDATES = 0;
UPDATE coviddeaths
SET date = str_to_date(date, '%m/%d/%y');
UPDATE covidvaccinations
SET date = str_to_date(date, '%m/%d/%y');

-- Select data
SELECT *
FROM coviddeaths
WHERE continent != ""
ORDER BY 3,4;

-- Total Cases vs Total Deaths
-- Shows likelihood of dying if you contract covid in Taiwan
SELECT location, date, total_cases,total_deaths, (total_deaths/total_cases)*100 AS DeathPercentage
FROM coviddeaths
WHERE location LIKE '%Taiwan%'
AND continent != ''
ORDER BY 1,2;

-- Total Cases vs Population
-- Shows what percentage of population infected with Covid
SELECT location, date, population, total_cases,  (total_cases/population)*100 AS PercentPopulationInfected
FROM coviddeaths
ORDER BY 1,2;

-- Countries with Highest Infection Rate compared to Population
SELECT location, population, MAX(total_cases) AS HighestInfectionCount,  Max((total_cases/population))*100 AS PercentPopulationInfected
FROM coviddeaths
GROUP BY Location, Population
ORDER BY PercentPopulationInfected DESC;

-- Countries with Highest Death Count per Population
SELECT location, MAX(cast(total_deaths as UNSIGNED)) AS TotalDeathCount
FROM coviddeaths
WHERE continent != ''
GROUP BY location
ORDER BY TotalDeathCount DESC;

-- BREAKING THINGS DOWN BY CONTINENT
-- Showing contintents with the highest death count per population
SELECT continent, MAX(cast(Total_deaths as UNSIGNED)) AS TotalDeathCount
FROM coviddeaths
WHERE continent != ''
GROUP BY continent
ORDER BY TotalDeathCount DESC;

-- GLOBAL NUMBERS
SELECT date, SUM(new_cases) AS total_cases, SUM(cast(new_deaths AS UNSIGNED)) AS total_deaths, SUM(cast(new_deaths AS UNSIGNED))/SUM(New_Cases)*100 AS DeathPercentage
FROM coviddeaths
WHERE continent != ''
-- GROUP BY date
ORDER BY 1,2;

-- Total Population vs Vaccinations
-- Shows Percentage of Population that has recieved at least one Covid Vaccine
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS RollingPeopleVaccinated
FROM coviddeaths dea
JOIN covidvaccinations vac
	ON dea.location = vac.location
    AND dea.date = vac.date
WHERE dea.continent != ''
ORDER BY 2, 3;

-- Using CTE to perform Calculation on Partition By in previous query
WITH PopvsVac (continent, location, date, population, new_vaccinations, RollingPeopleVaccinated)
AS
(
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS RollingPeopleVaccinated
FROM coviddeaths dea
JOIN covidvaccinations vac
	ON dea.location = vac.location
    AND dea.date = vac.date
WHERE dea.continent != ''
)
SELECT *, (RollingPeopleVaccinated/population)*100
FROM PopvsVac;

-- Using Temp Table to perform Calculation on Partition By in previous query
DROP TABLE if exists PercentPopulationVaccinated;
CREATE TABLE PercentPopulationVaccinated
(
continent varchar(255),
location varchar(255),
date datetime,
population numeric,
new_vaccinations numeric,
RollingPeopleVaccinated numeric
);

INSERT INTO PercentPopulationVaccinated
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS RollingPeopleVaccinated
FROM coviddeaths dea
JOIN covidvaccinations vac
	ON dea.location = vac.location
    AND dea.date = vac.date
WHERE dea.continent != '';

Select *, (RollingPeopleVaccinated/Population)*100
From PercentPopulationVaccinated;


-- Creating View to store data for later visualizations
CREATE VIEW PercentPopulationVaccinatedVIEW AS
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS RollingPeopleVaccinated
FROM coviddeaths dea
JOIN covidvaccinations vac
	ON dea.location = vac.location
    AND dea.date = vac.date
WHERE dea.continent != '';




