-----------------------------DATA CLEANING ON COMPANY LAYOFFS----------------------------
----------------------------------------STEPS-----------------------------------------
-- 1. REMOVE DUPLICATES
-- 2. STANDARIZED THE DATA (Extra Spaces,)
-- 3. HANDLING NULL VALUES and BLANK VALUES/MISSING VALUES
-- 4. REMOVE UN-NECESSARY COLUMNS

-- CREATING A TABLE SAME AS 'LAYOFFS TABLE' TO CLEAN THE RAW DATA

SELECT * FROM layoffs;

CREATE TABLE layoffs_staging AS 
SELECT * FROM layoffs;

-- 1. REMOVING DUPLICATES
-- IDENTIFYING DUPLICATES

WITH duplicates_cte AS(
SELECT * ,
ROW_NUMBER() OVER ( PARTITION BY  company, location, industry, total_laid_off, 	percentage_laid_off,date, stage,country,funds_raised_millions
) AS row_no
FROM layoffs_staging
)
SELECT * 
FROM duplicates_cte 
WHERE row_no > 1;

-- Checking, we get the right duplicates

SELECT * FROM layoffs_staging 
WHERE company = 'Cazoo';

-- CREATING ANOTHER TABLE TO REMOVE DUPLICATES (COZ CTE DOESN'T ALLOW UPDATING)

CREATE TABLE layoffs_staging2 AS
SELECT * ,
ROW_NUMBER() OVER ( PARTITION BY  company, location, industry, total_laid_off, 	percentage_laid_off,date, stage,country,funds_raised_millions
) AS row_no
FROM layoffs_staging;

SELECT * FROM layoffs_staging2;

-- REMOVING DUPLICATES

DELETE FROM layoffs_staging2
WHERE row_no > 1;

-- DUPLICATES REMOVED

-- 2. STANDARIZED THE DATA
-- TRIMMING THE DATA

SELECT * FROM layoffs_staging2;

SELECT company, TRIM(company) AS Trim_company
FROM layoffs_Staging;

UPDATE layoffs_staging2
SET company =  TRIM(company);

UPDATE layoffs_staging2
SET location =  TRIM(location);

UPDATE layoffs_staging2
SET industry =  TRIM(industry);

UPDATE layoffs_staging2
SET total_laid_off =  TRIM(total_laid_off);

UPDATE layoffs_staging2
SET percentage_laid_off =  TRIM(percentage_laid_off);

UPDATE layoffs_staging2
SET date =  TRIM(date);

UPDATE layoffs_staging2
SET stage =  TRIM(stage);

UPDATE layoffs_staging2
SET country =  TRIM(country);

UPDATE layoffs_staging2
SET funds_raised_millions =  TRIM(funds_raised_millions);

SELECT DISTINCT(industry) 
FROM layoffs_staging2
ORDER BY 1;

SELECT industry
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_Staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT(country) 
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_Staging2
SET country = 'United States'
WHERE country LIKE 'United States%';

------------------------------------------------------------------------------------
-- CONVERTING TEXT TO DATE DATA TYPE HAVING NULL

SELECT date FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
ALTER COLUMN date TYPE DATE USING CASE WHEN date = 'NULL' THEN NULL ELSE TO_DATE(DATE, 'mm/dd/yyyy') END; 

SELECT date FROM layoffs_staging2;

-- 3. HANDLING NULL VALUES and BLANK VALUES/MISSING VALUES
-- IDENTIFYING NULL VALUES and BLANK VALUES/MISSING VALUES

SELECT * FROM layoffs_staging2
WHERE industry IS NULL OR industry = 'NULL';

-- HARD CODED QUERY TO HANDLE NULL VALUES / MISSING VALUES

SELECT * FROM layoffs_staging2
WHERE company = 'Airbnb';
UPDATE layoffs_staging2
SET industry = 'Travel'
WHERE company = 'Airbnb';

SELECT * FROM layoffs_staging2
WHERE company = 'Carvana';
UPDATE layoffs_staging2
SET industry = 'Transportation'
WHERE company = 'Carvana';

SELECT * FROM layoffs_staging2
WHERE industry IS NULL OR industry = 'NULL';

-- DYNAMIC QUERY TO HANDLE NULL VALUES / MISSING VALUES

SELECT t1.industry, t2.industry
FROM layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
ON t1.company = t2.company 
WHERE t1.company IS NULL AND t2.company IS NOT NULL;

UPDATE layoffs_staging2 AS t1
SET industry = (
  SELECT t2.industry
  FROM layoffs_staging2 AS t2
  WHERE t1.company = t2.company
  AND t2.industry IS NOT NULL
)
WHERE industry IS NULL;

-- 4. REMOVE UN-NECESSARY COLUMNS

SELECT * FROM layoffs_staging2

SELECT company,total_laid_off,percentage_laid_off FROM layoffs_staging2
WHERE total_laid_off = 'NULL' AND percentage_laid_off = 'NULL';

DELETE FROM layoffs_staging2
WHERE total_laid_off = 'NULL' AND percentage_laid_off = 'NULL';

ALTER TABLE layoffs_staging2
DROP COLUMN row_no;

SELECT * FROM layoffs_staging2;

----------------------------THAT's ALL FOR DATA CLEANING----------------------------





















