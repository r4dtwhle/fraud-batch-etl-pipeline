# Data

| File | Keterangan |
|---|---|
| `sample_bank_fraud.csv` | Sampel 5.000 baris pertama, untuk unit test dan uji coba cepat |
| `bank_fraud.csv` | Dataset lengkap: 1.000.000 transaksi, 26 kolom, periode 2020-2024 (**tidak disertakan**, 153 MB) |

Dataset berisi transaksi perbankan **sintetis** yang disediakan pada praktikum Analisis Big Data. Dataset lengkap tidak disertakan karena ukurannya melebihi batas file GitHub (100 MB).

Untuk menjalankan pipeline penuh, letakkan `bank_fraud.csv` di folder ini. Untuk mencoba dengan data sampel, set environment variable:

```bash
FRAUD_INPUT_PATH=/opt/airflow/data/sample_bank_fraud.csv
```

## Kolom yang dipakai pipeline

| Kolom | Tipe | Keterangan |
|---|---|---|
| `transaction_id` | string | ID unik transaksi |
| `transaction_date` | date | Tanggal transaksi (`YYYY-MM-DD`) |
| `merchant_category` | string | Kategori merchant (15 kategori) |
| `transaction_amount` | double | Nominal transaksi |
| `is_fraud` | int | 1 = fraud, 0 = sah |
| `fraud_type` | string | Jenis fraud (6 jenis) atau `None` untuk transaksi sah |

Kolom lain (lokasi, perangkat, profil nasabah, riwayat transaksi) tersedia untuk analisis lanjutan.
