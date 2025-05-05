CREATE DATABASE world_layoffs;

USE world_layoffs;

SELECT *
FROM layoffs_Staging2;

CREATE TABLE layoffs_staging LIKE layoffs;

INSERT INTO layoffs_staging
SELECT * 
FROM layoffs;

SELECT *
FROM layoffs_staging;

WITH duplicate_cte AS
(SELECT *, ROW_NUMBER() 
OVER(PARTITION BY 
company, location, industry, total_laid_off, 
percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM 
layoffs_staging)
SELECT *
FROM duplicate_cte 
WHERE row_num > 1 ;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging2
SELECT *, ROW_NUMBER() 
OVER(PARTITION BY 
company, location, industry, total_laid_off, 
percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM 
layoffs_staging;

SET SQL_SAFE_UPDATES=0;

DELETE 
FROM layoffs_staging2
WHERE 
total_laid_off IS NULL AND percentage_laid_off IS NULL;
    
    SELECT * 
    FROM layoffs_staging2
    WHERE industry LIKE "Crypto%";

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY country;

UPDATE layoffs_staging2
SET country = "United States" WHERE country LIKE "United States%";

SELECT * 
FROM layoffs_staging2
WHERE industry LIKE 'crypto%';


SELECT `date`, str_to_date(`date`, '%m/%d/%y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` date;

UPDATE layoffs_staging
SET company= TRIM(company);

SELECT *
FROM Layoffs_staging2 AS A1
JOIN layoffs_staging2 AS A2
WHERE A1.company = A2.company
ORDER BY A1.company;

UPDATE layoffs_staging2
SET industry= 'Travel'
WHERE company= 'Airbnb';

UPDATE layoffs_staging2
SET industry= 'Transportation'
WHERE company= 'Carvana';

UPDATE layoffs_staging2
SET industry= 'Consumer'
WHERE company= 'Juul';

UPDATE layoffs_staging2
SET industry = "Crypto"
WHERE industry LIKE 'crypto%' ;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- END OF PROJECT

SELECT * 
FROM layoffs_staging2;
