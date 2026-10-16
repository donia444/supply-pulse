\echo ''
\echo '=== 1) WAL level: must be "logical" so Debezium can read changes ==='
SHOW wal_level;

\echo ''
\echo '=== 2) Replica identity of vendor_items: must be FULL (old row on update/delete) ==='
SELECT relname AS table_name,
       CASE relreplident WHEN 'f' THEN 'FULL' WHEN 'd' THEN 'default' WHEN 'n' THEN 'nothing' ELSE 'index' END AS replica_identity
FROM pg_class WHERE relname = 'vendor_items';

\echo ''
\echo '=== 3) Publication: the tables Postgres exposes to Debezium ==='
SELECT pubname, schemaname, tablename FROM pg_publication_tables ORDER BY schemaname, tablename;

\echo ''
\echo '=== 4) CDC user: must have the replication privilege ==='
SELECT rolname, rolreplication AS can_replicate FROM pg_roles WHERE rolname = 'cdc_user';

\echo ''
\echo '=== 5) Rows per table (all source data lives here) ==='
SELECT 'partners_system.vendors' AS table_name, count(*) AS rows FROM partners_system.vendors
UNION ALL SELECT 'catalog_pim.products', count(*) FROM catalog_pim.products
UNION ALL SELECT 'restaurant_portal.menu_items', count(*) FROM restaurant_portal.menu_items
UNION ALL SELECT 'customer_app.customers', count(*) FROM customer_app.customers
UNION ALL SELECT 'customer_app.orders', count(*) FROM customer_app.orders
UNION ALL SELECT 'customer_app.order_items', count(*) FROM customer_app.order_items
UNION ALL SELECT 'wms.inventory_movements', count(*) FROM wms.inventory_movements
UNION ALL SELECT 'public.vendor_items', count(*) FROM public.vendor_items
ORDER BY 1;

\echo ''
\echo '=== 6) Sample rows (raw values are kept on purpose) ==='
SELECT store_id, item_ref, item_name, item_price, currency, qty_on_hand, is_available, updated_at
FROM public.vendor_items ORDER BY store_id, item_ref LIMIT 5;
SELECT order_id, customer_id, vendor_id, order_ts, status, total_amount
FROM customer_app.orders ORDER BY row_id LIMIT 5;

