-- =============================================================================
-- CONTOH QUERY ANALISIS PADA DATA WAREHOUSE (tabel daily_fraud_summary)
-- Jalankan setelah DAG bank_fraud_batch_pipeline sukses, misalnya lewat DBeaver
-- (localhost:5433, database bank_warehouse, user de_user).
-- =============================================================================

-- 1. Ringkasan keseluruhan: total transaksi, kasus fraud, dan kerugian
SELECT
    SUM(total_transactions)                                        AS total_transaksi,
    SUM(fraud_cases_count)                                         AS total_fraud,
    ROUND(100.0 * SUM(fraud_cases_count) / SUM(total_transactions), 2) AS fraud_rate_pct,
    SUM(total_amount)                                              AS total_nominal,
    SUM(fraud_loss_amount)                                         AS total_kerugian
FROM daily_fraud_summary;

-- 2. Tingkat fraud dan kerugian per kategori merchant
SELECT
    merchant_category,
    SUM(total_transactions)                                        AS transaksi,
    SUM(fraud_cases_count)                                         AS kasus_fraud,
    ROUND(100.0 * SUM(fraud_cases_count) / SUM(total_transactions), 2) AS fraud_rate_pct,
    SUM(fraud_loss_amount)                                         AS kerugian
FROM daily_fraud_summary
GROUP BY merchant_category
ORDER BY kerugian DESC;

-- 3. Jenis fraud dengan kerugian terbesar
SELECT fraud_type, SUM(fraud_cases_count) AS kasus, SUM(fraud_loss_amount) AS kerugian
FROM daily_fraud_summary
WHERE fraud_type <> 'Legitimate'
GROUP BY fraud_type
ORDER BY kerugian DESC;

-- 4. Tren bulanan kerugian akibat fraud
SELECT DATE_TRUNC('month', transaction_date)::date AS bulan,
       SUM(fraud_cases_count) AS kasus,
       SUM(fraud_loss_amount) AS kerugian
FROM daily_fraud_summary
GROUP BY 1
ORDER BY 1;
