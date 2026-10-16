-- Validation of the serving schema. Run:
--   Get-Content infra\postgres\serving_checks.sql | docker compose exec -T postgres psql -U qcommerce_user -d qcommerce
-- For section 6 first copy the answer key into the container:
--   docker compose cp data\_answer_key\expected_scd2_vendor_items.csv postgres:/tmp/expected.csv

\echo '== 1. volumes (expect ~12,587 items, 86 deleted, ~12,501 live)'
SELECT count(*) AS items, count(*) FILTER (WHERE is_deleted) AS deleted,
       count(*) FILTER (WHERE NOT is_deleted) AS live FROM serving.vendor_item_latest;

\echo '== 2. invalid values in the latest state (every number must be 0)'
SELECT count(*) FILTER (WHERE price_egp <= 0)                                   AS bad_price,
       count(*) FILTER (WHERE currency <> 'EGP')                                AS bad_currency,
       count(*) FILTER (WHERE item_name IS NULL OR btrim(item_name) = '')       AS null_name,
       count(*) FILTER (WHERE item_type = 'grocery' AND (qty_on_hand IS NULL OR qty_on_hand < 0)) AS bad_stock,
       count(*) FILTER (WHERE item_type = 'grocery' AND (qty_on_hand > 0) <> is_available)        AS availability_conflict,
       count(*) FILTER (WHERE item_ref !~ '^(SKU-[0-9]{5}|M[0-9]{7})$')         AS bad_item_ref
FROM serving.vendor_item_latest;

\echo '== 3. products in the latest state that are not in the product master (must be 0)'
SELECT count(*) AS unknown_products FROM serving.vendor_item_latest l
WHERE l.item_type = 'grocery' AND NOT EXISTS (SELECT 1 FROM catalog_pim.products p
      WHERE 'SKU-' || lpad(regexp_replace(p.product_id, '\D', '', 'g'), 5, '0') = l.item_ref);

\echo '== 4. ordering: latest must equal the newest non-late history version for price/availability (must be 0)'
SELECT count(*) AS latest_older_than_history FROM serving.vendor_item_latest l
WHERE EXISTS (SELECT 1 FROM serving.vendor_item_price_history h
              WHERE h.vendor_id = l.vendor_id AND h.item_ref = l.item_ref AND NOT h.late_arrival
                AND (h.event_time_utc, h.src_lsn) > (l.event_time_utc, l.src_lsn));

\echo '== 5. history: rows, items with a real price history (>= 2 versions)'
SELECT count(*) AS history_rows, count(DISTINCT (vendor_id, item_ref)) AS items,
       (SELECT count(*) FROM (SELECT 1 FROM serving.vendor_item_price_history
                              GROUP BY vendor_id, item_ref HAVING count(*) >= 2) x) AS items_with_2plus_versions
FROM serving.vendor_item_price_history;

\echo '== 6. latest vs answer key (needs /tmp/expected.csv, see top): mismatches should be 0 or tiny'
CREATE TEMP TABLE expected (vendor_id text, item_ref text, item_type text, version int, price_egp numeric,
    is_available boolean, valid_from_utc text, valid_to_utc text, is_current boolean);
\copy expected FROM '/tmp/expected.csv' WITH (FORMAT csv, HEADER true)
SELECT count(*) FILTER (WHERE l.item_ref IS NULL)                      AS missing_in_serving,
       count(*) FILTER (WHERE l.price_egp <> e.price_egp)              AS price_mismatch,
       count(*) FILTER (WHERE l.is_available <> e.is_available)        AS availability_mismatch
FROM expected e LEFT JOIN serving.vendor_item_latest l
  ON l.vendor_id = e.vendor_id AND l.item_ref = e.item_ref AND NOT l.is_deleted
WHERE e.is_current;
SELECT (SELECT count(*) FROM expected) AS expected_versions,
       (SELECT count(*) FROM serving.vendor_item_price_history) AS our_history_rows;
