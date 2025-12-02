# SQL commands
===================COPY TABLE FROM HW2===================
USE ROLE TRAINING_ROLE;
USE DATABASE FIVETRAN_DATABASE;

CREATE SCHEMA IF NOT EXISTS HW3_CHEETAH;

GRANT USAGE ON SCHEMA FIVETRAN_DATABASE.HW3_CHEETAH TO ROLE TRAINING_ROLE;
GRANT CREATE TABLE ON SCHEMA FIVETRAN_DATABASE.HW3_CHEETAH TO ROLE TRAINING_ROLE;

CREATE OR REPLACE TABLE FIVETRAN_DATABASE.HW3_CHEETAH.STAGING_ARTICLE_DETAILS AS
SELECT *
FROM FIVETRAN_DATABASE.HW2_CHEETAH.ARTICLE_DETAILS;

CREATE OR REPLACE TABLE FIVETRAN_DATABASE.HW3_CHEETAH.STAGING_PAGE_VIEWS AS
SELECT *
FROM FIVETRAN_DATABASE.HW2_CHEETAH.PAGEVIEWS;

SHOW TABLES IN SCHEMA FIVETRAN_DATABASE.HW3_CHEETAH;

===================STAGING PAGE VIEWS===================
select *
from FIVETRAN_DATABASE.HW3_CHEETAH.STAGING_PAGE_VIEWS

===================STAGING ARTICLE DETAILS===================
select *
from FIVETRAN_DATABASE.HW3_CHEETAH.STAGING_ARTICLE_DETAILS

===================FACT PAGE VIEWS===================
with dimension as (select article_id, title, article, page_length, 
        cast(last_revision_date as date) as  last_revision_date
from {{ ref('stg_article_details') }})

, fact as (select article_id, b.article, cast(date as date) as view_date, views
from {{ ref('stg_page_views') }} b
left join dimension a on a.article = b.article)

SELECT
  article_id,
  article,
  view_date,
  views,

  -- 7-day rolling average of views per article (min_periods=1)
  ROUND(
    AVG(views) OVER (
      PARTITION BY article_id
      ORDER BY view_date
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )
  , 2) AS views_7d_avg,

  -- Cumulative sum of views per article
  SUM(views) OVER (
    PARTITION BY article_id
    ORDER BY view_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS cum_views,

  -- Share of total views on that date (across all articles), in %
  ROUND(
    100 * views / NULLIF(
      SUM(views) OVER (PARTITION BY view_date),
      0
    )
  , 2) AS percent_views
FROM fact
===================FACT ARTICLE DETAILS===================
select 
    article_id, 
    title, 
    article, 
    page_length, 
    cast(last_revision_date as date) as last_revision_date
from {{ ref('stg_article_details') }}

===================ANALYSIS===================
select title, sum(views) as total_views
from FIVETRAN_DATABASE.HW3_CHEETAH.FCT_PAGE_VIEWS
left join FIVETRAN_DATABASE.HW3_CHEETAH.STAGING_ARTICLE_DETAILS using(article_id)
group by title
order by sum(views) desc

# dbt commands
conda activate dbt311
dbt debug
dbt run
dbt test
dbt docs generate
dbt docs serve
