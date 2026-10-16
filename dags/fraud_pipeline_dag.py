# ==============================================================================
# PIPELINE DAG AIRFLOW: AUTOMATED BATCH FRAUD ETL ORCHESTRATION
# ==============================================================================

# Mengimpor modul waktu Python untuk penentuan jadwal awal dan jeda percobaan
from datetime import datetime, timedelta

# Mengimpor modul utama DAG dari Apache Airflow
# pyrefly: ignore [missing-import]
from airflow import DAG

# Mengimpor operator untuk mengeksekusi skrip terminal shell Linux
# pyrefly: ignore [missing-import]
from airflow.operators.bash import BashOperator

# Mengimpor operator khusus untuk mengeksekusi query database PostgreSQL
# pyrefly: ignore [missing-import]
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Konfigurasi parameter umum yang berlaku untuk seluruh tugas di dalam DAG
default_args = {
    'owner': 'risk_engineering_team',     # Identitas tim pemilik pipeline
    'depends_on_past': False,             # Eksekusi hari ini tidak bergantung pada hari kemarin
    'start_date': datetime(2026, 9, 1),   # Batas awal mulainya jadwal alur kerja
    'retries': 1,                         # Otomatis mencoba ulang 1 kali jika tugas gagal
    'retry_delay': timedelta(minutes=1),  # Jeda waktu 1 menit sebelum eksekusi percobaan ulang
}

# Blok pembungkus alur kerja (DAG Context Manager)
with DAG(
    dag_id='bank_fraud_batch_pipeline',   # Nama unik DAG yang tampil pada antarmuka Web UI Airflow
    default_args=default_args,            # Menerapkan parameter umum di atas
    description='Pipeline Batch ETL Deteksi dan Rekapitulasi Kasus Fraud',
    schedule_interval='@daily',           # Menjalankan alur secara otomatis setiap pergantian tengah malam
    # [SINTAKS KUNCI: MENCEGAH OVERLOAD LAPTOP]
    # Nilai False melarang Airflow mengeksekusi puluhan jadwal masa lalu (backfill)
    # yang terlewat sejak start_date, mencegah laptop macet kehabisan CPU!
    catchup=False,
    tags=['fraud', 'pyspark', 'postgres'] # Label kategori pencarian di Web UI
) as dag:

    # --------------------------------------------------------------------------
    # TASK 1: MENJALANKAN LOGIKA TRANSFORMASI PYSPARK
    # --------------------------------------------------------------------------
    # BashOperator memanggil interpreter Python di dalam container scheduler
    # untuk mengeksekusi skrip pemrosesan PySpark yang telah diuji di Bagian 3
    task_pyspark_transform = BashOperator(
        task_id='run_pyspark_fraud_transform',
        bash_command='python /opt/airflow/scripts/spark_fraud_etl.py'
    )

    # --------------------------------------------------------------------------
    # TASK 2: MEMINDAHKAN DATA DENGAN STRATEGI UPSERT IDEMPOTEN
    # --------------------------------------------------------------------------
    # PostgresOperator membuka koneksi database dan menjalankan skrip SQL
    task_upsert_dwh = PostgresOperator(
        task_id='upsert_to_fraud_warehouse',
        
        # Mengambil kredensial koneksi yang didaftarkan di Menu Admin Airflow
        postgres_conn_id='postgres_fraud_conn',
        
        # [SINTAKS KUNCI: QUERY UPSERT IDEMPOTEN]
        # 1. INSERT INTO ... SELECT: Memindahkan data dari tabel staging ke tabel target.
        # 2. ON CONFLICT (...): Mendeteksi jika kombinasi PK sudah ada di tabel tujuan.
        # 3. DO UPDATE SET: Memperbarui kolom angka dengan data baru (EXCLUDED) tanpa baris ganda!
        sql="""
            INSERT INTO daily_fraud_summary (
                transaction_date, 
                merchant_category, 
                fraud_type, 
                total_transactions, 
                fraud_cases_count, 
                total_amount, 
                fraud_loss_amount
            )
            SELECT 
                transaction_date, 
                merchant_category, 
                fraud_type, 
                total_transactions, 
                fraud_cases_count, 
                total_amount, 
                fraud_loss_amount
            FROM staging_fraud_summary
            ON CONFLICT (transaction_date, merchant_category, fraud_type)
            DO UPDATE SET 
                total_transactions = EXCLUDED.total_transactions,
                fraud_cases_count = EXCLUDED.fraud_cases_count,
                total_amount = EXCLUDED.total_amount,
                fraud_loss_amount = EXCLUDED.fraud_loss_amount;
        """
    )

    # --------------------------------------------------------------------------
    # TASK DEPENDENCY CHAIN (URUTAN EKSEKUSI KERJA)
    # --------------------------------------------------------------------------
    # Operator bitshift '>>' menegaskan urutan berarah:
    # Task PySpark harus selesai 100% dengan status sukses terlebih dahulu,
    # barulah Task Upsert SQL dijalankan ke database target warehouse.
    task_pyspark_transform >> task_upsert_dwh
