-- Calculate total session count per continent
WITH sessions_by_continent AS (
 SELECT
   sp.continent,
   COUNT(*) AS session_count
 FROM `data-analytics-mate.DA.session_params` sp
 WHERE sp.continent IS NOT NULL
 GROUP BY sp.continent
),

-- Calculate revenue per session, categorized by device
orders_revenue AS (
 SELECT
   sp.continent,
   LOWER(sp.device) AS device, -- Standardize device name
   o.ga_session_id,
   SUM(p.price) AS revenue
 FROM `data-analytics-mate.DA.order` o
 JOIN `data-analytics-mate.DA.product` p
   ON p.item_id = o.item_id
 JOIN `data-analytics-mate.DA.session` s
   ON s.ga_session_id = o.ga_session_id
 JOIN `data-analytics-mate.DA.session_params` sp
   ON sp.ga_session_id = s.ga_session_id
 WHERE sp.continent IS NOT NULL
 GROUP BY sp.continent, device, o.ga_session_id
),

-- Aggregate total and device-specific revenue per continent
revenue_by_continent AS (
 SELECT
   continent,
   SUM(revenue) AS revenue,
   SUM(CASE WHEN device = 'mobile'  THEN revenue ELSE 0 END) AS revenue_mobile, -- Revenue from Mobile
   SUM(CASE WHEN device = 'desktop' THEN revenue ELSE 0 END) AS revenue_desktop -- Revenue from Desktop
 FROM orders_revenue
 GROUP BY continent
),

-- Calculate unique account counts and verified account counts per continent
accounts_by_continent AS (
 SELECT
   sp.continent,
   COUNT(DISTINCT a_s.account_id) AS account_count,
   COUNT(DISTINCT IF(a.is_verified = 1, a_s.account_id, NULL)) AS verified_account -- Count of verified accounts
 FROM `data-analytics-mate.DA.account_session` a_s
 JOIN `data-analytics-mate.DA.session_params` sp
   ON sp.ga_session_id = a_s.ga_session_id
 JOIN `data-analytics-mate.DA.account` a
   ON a.id = a_s.account_id
 WHERE sp.continent IS NOT NULL
 GROUP BY sp.continent
),

-- Join all metric sets (Sessions, Revenue, Accounts) by continent
joined AS (
 SELECT
   sbc.continent,
   COALESCE(rbc.revenue, 0)         AS revenue,
   COALESCE(rbc.revenue_mobile, 0)  AS revenue_mobile,
   COALESCE(rbc.revenue_desktop, 0) AS revenue_desktop,
   COALESCE(abc.account_count, 0)   AS account_count,
   COALESCE(abc.verified_account,0) AS verified_account,
   sbc.session_count
 FROM sessions_by_continent sbc
 LEFT JOIN revenue_by_continent  rbc ON rbc.continent = sbc.continent
 LEFT JOIN accounts_by_continent abc ON abc.continent = sbc.continent
),

-- Calculate total revenue across ALL continents using a Window Function
with_total AS (
 SELECT
   *,
   SUM(revenue) OVER () AS total_revenue_all -- Total for calculating percentage share
 FROM joined
)

-- Final SELECT: Format output columns and calculate the final percentage share
SELECT
 continent AS Continent,
 revenue AS Revenue,
 revenue_mobile AS `Revenue from Mobile`,
 revenue_desktop AS `Revenue from Desktop`,
 SAFE_DIVIDE(revenue, total_revenue_all) * 100 AS `% Revenue from Total`, -- Revenue share calculation
 account_count AS `Account Count`,
 verified_account AS `Verified Account`,
 session_count AS `Session Count`
FROM with_total
ORDER BY Continent;
