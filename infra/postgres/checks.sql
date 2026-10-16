\echo '== 1. rows in vendor_items (0 means nothing was loaded)'
SELECT count(*) AS total_rows FROM public.vendor_items;

\echo '== 2. rows per store (top 5)'
SELECT store_id, count(*) AS items FROM public.vendor_items GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 5;

\echo '== 3. groceries vs menu items'
SELECT CASE WHEN item_ref LIKE 'SKU-%' THEN 'grocery' ELSE 'menu' END AS item_type, count(*) FROM public.vendor_items GROUP BY 1;

\echo '== 4. raw dirty values are kept on purpose (text in price, odd flags)'
SELECT item_price, currency, is_available, updated_at FROM public.vendor_items
WHERE item_price !~ '^-?[0-9]+(\.[0-9]+)?$' OR is_available NOT IN ('true','false') LIMIT 5;

\echo '== 5. typos left for the QA gate (null names, non-numeric or <= 0 prices)'
SELECT count(*) FILTER (WHERE item_name IS NULL) AS null_names,
       count(*) FILTER (WHERE item_price ~ '^-?[0-9]+(\.[0-9]+)?$' AND item_price::numeric <= 0) AS non_positive_prices
FROM public.vendor_items;

\echo '== 6. CDC plumbing: publication, and replication slot (exists after make connector)'
SELECT pubname FROM pg_publication;
SELECT slot_name, plugin, active FROM pg_replication_slots;
