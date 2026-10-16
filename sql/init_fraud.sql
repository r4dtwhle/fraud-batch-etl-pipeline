-- =============================================================================
-- SKEMA DATA WAREHOUSE TARGET: ANALITIK KASUS FRAUD PERBANKAN
-- =============================================================================

-- Membuat tabel agregasi target jika tabel tersebut belum pernah dibuat
CREATE TABLE IF NOT EXISTS daily_fraud_summary (
    -- Kolom tanggal transaksi perbankan (tipe DATE murni: YYYY-MM-DD)
    transaction_date DATE NOT NULL,
    
    -- Kategori merchant penerima pembayaran (misal: Grocery, Utilities, Crypto Exchange)
    merchant_category VARCHAR(100) NOT NULL,
    
    -- Klasifikasi jenis kejahatan fraud atau transaksi sah (misal: Legitimate, Synthetic Identity)
    fraud_type VARCHAR(100) NOT NULL,
    
    -- Total seluruh volume transaksi yang terjadi pada grup ini
    total_transactions INT NOT NULL,
    
    -- Total kasus yang terkonfirmasi sebagai tindakan penipuan (is_fraud = 1)
    fraud_cases_count INT NOT NULL,
    
    -- Akumulasi seluruh perputaran uang (maksimal 15 digit, dengan 2 angka di belakang koma)
    total_amount NUMERIC(15, 2) NOT NULL,
    
    -- Akumulasi kerugian finansial bank khusus akibat tindakan penipuan
    fraud_loss_amount NUMERIC(15, 2) NOT NULL,
    
    -- [SINTAKS KUNCI: COMPOSITE PRIMARY KEY]
    -- Kombinasi ketiga kolom ini dijamin unik secara mutlak oleh database engine.
    -- Syarat mutlak ini adalah landasan agar sintaks UPSERT (ON CONFLICT) di Modul 4
    -- dapat berjalan tanpa menimbulkan data duplikat saat eksekusi batch diulang!
    PRIMARY KEY (transaction_date, merchant_category, fraud_type)
);