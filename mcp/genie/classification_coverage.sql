-- Classification Coverage
-- Shows which catalogs/schemas have adequate data classification
SELECT
    ri.domain as catalog_name,
    ri.resource_type,
    COUNT(DISTINCT ri.resource_id) as total_resources,
    COUNT(DISTINCT CASE WHEN ri.tags['data_classification'] IS NOT NULL THEN ri.resource_id END) as classified,
    COUNT(DISTINCT CASE WHEN ri.tags['data_steward'] IS NOT NULL THEN ri.resource_id END) as with_steward,
    COUNT(DISTINCT CASE WHEN ri.tags['data_classification'] IN ('pii', 'confidential', 'restricted') THEN ri.resource_id END) as sensitive,
    ROUND(
        COUNT(DISTINCT CASE WHEN ri.tags['data_classification'] IS NOT NULL THEN ri.resource_id END) * 100.0
        / NULLIF(COUNT(DISTINCT ri.resource_id), 0), 1
    ) as classification_pct
FROM ${catalog}.${schema}.resource_inventory ri
WHERE ri.scan_id = (SELECT MAX(scan_id) FROM ${catalog}.${schema}.resource_inventory)
GROUP BY ri.domain, ri.resource_type
ORDER BY classification_pct ASC
