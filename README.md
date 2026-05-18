# Real-Time E-Commerce Orders Data Pipeline


##  Anggota Kelompok
| Nama                   | NRP        | Kontribusi |
| ---------------------- | ---------- | ---------- |
| Lyonel Oliver Dwiputra | 5025241145 | 50%        |
| Hosea Felix Sanjaya    | 5025241177 | 50%        |

> **Proyek Tugas Modul 3 Big Data · End-to-End Modern Data Stack Implementation**

Proyek ini membangun *pipeline* arsitektur data untuk mengekstrak aliran data transaksi e-commerce secara *real-time* dari **REST API**, memproses dan mengagregasinya dengan **Apache Spark**, mengorkestrasinya via **Apache Airflow**, menyimpannya ke Data Warehouse **ClickHouse**, dan menyajikan *Business Insights* melalui *Dashboard* interaktif di **Metabase**. Semuanya dikemas dan dijalankan di dalam ekosistem **Docker**.

Arsitektur mengadopsi pendekatan **micro-batching** dengan interval penarikan data secara otomatis setiap 10 menit.

---

## Arsitektur Sistem

~~~text
E-Commerce API (96.9.212.102:8000/orders)
     ↓  (Fetch Random Orders / 10 Menit)
[Ingestion — Python Requests]
     ↓  Save as .parquet
[Data Lake — Local Volume]
     ↓  Read & Aggregate (Flattening)
[Processing — Apache Spark]
     ↓  Truncate & Insert
[Data Warehouse — ClickHouse]
     ↓  Direct Connection
[BI Dashboard — Metabase]

↻ Seluruh siklus diatur secara otomatis oleh Apache Airflow
~~~

** Metrik Analisis Utama (Business Insights):**
- **Tren Trafik Jam Sibuk (Peak Hours):** Mengidentifikasi jam terjadinya lonjakan pesanan tertinggi.
- **Distribusi Pesanan per Shift (Time-Slicing):** Persentase volume pesanan berdasarkan shift waktu (Morning, Afternoon, Evening, Late Night).
- **Total Volume Pesanan:** Kalkulasi kumulatif dari transaksi yang berhasil diproses.

---

## Tech Stack

| Komponen | Teknologi | Peran |
|----------|-----------|-------|
| **Orchestration** | Apache Airflow 2.9 | Menjadwalkan dan mengotomatisasi *task dependencies* |
| **Data Processing**| Apache Spark / PySpark 3.5 | Melakukan agregasi data JSON mentah secara in-memory |
| **Data Warehouse** | ClickHouse | Column-oriented OLAP database untuk analitik super cepat |
| **BI & Dashboard** | Metabase | Layer visualisasi data (*Front-end*) |
| **Infrastructure** | Docker & Docker Compose | Kontainerisasi seluruh *environment* |
| **Language** | Python 3.11 | Eksekusi *script* API dan logika Spark |

---

## Struktur Proyek

*(Catatan: Proyek ini menggunakan kerangka repositori `wikipedia-realtime-pipeline`, namun seluruh logika internal telah direkayasa ulang untuk memproses aliran data Orders e-commerce).*

~~~text
wikipedia-realtime-pipeline/
├── dags/
│   ├── scripts/
│   │   ├── fetch_wikipedia_stream.py       # Ekstraksi data API /orders → Data Lake (.parquet)
│   │   └── process_wikipedia_spark.py      # PySpark: Agregasi jumlah order per jam → ClickHouse
│   └── wikipedia_pipeline.py               # Definisi DAG Airflow (orders_realtime_stream)
├── data_lake/                              # Folder penampungan sementara file mentah
├── docker-compose.yml                      # Konfigurasi container, networks, dan ports
├── Dockerfile                              # Custom Airflow image (inklusi Java JRE untuk Spark)
└── requirements.txt                        # Dependensi Python library tambahan
~~~

---

## Panduan Eksekusi

### Prasyarat:
- **Docker Desktop** terinstal dan dalam status *Running*.
- Menggunakan **Git Bash** di Windows.

### Step 1 — Inisialisasi Database Airflow
Buka terminal di dalam folder utama proyek, lalu jalankan perintah ini untuk menginisialisasi database metadata milik Airflow (PostgreSQL):
~~~bash
docker-compose up airflow-init
~~~
*(Tunggu hingga muncul pesan `Database migrating done!` atau `exited with code 0`. Tekan `Ctrl + C` jika stuck di terminal).*

### Step 2 — Jalankan Seluruh Infrastruktur
Nyalakan seluruh *services* (Airflow, Metabase, ClickHouse, Postgres) berjalan di latar belakang:
~~~bash
docker-compose up -d
~~~
*(Tunggu sekitar 1–2 menit agar Airflow Webserver selesai melakukan proses booting).*

### Step 3 — Aktifkan Orkestrasi di Airflow UI
1. Buka browser dan akses **http://localhost:8081** (karena port `8080` saya sudah terpakai).
2. Login menggunakan kredensial default (Username: `admin`, Password: `admin`).
3. Cari DAG bernama **`orders_realtime_stream`**.
4. Geser sakelar di sebelah kiri nama DAG.
5. Klik ikon ▶️ **Trigger DAG** untuk memaksa proses penarikan data berjalan sekarang. 
6. Cek tab **Grid** untuk melihat indikator sukses (balok hijau) pada *Task 1* dan *Task 2*.


<img width="1877" height="981" alt="image" src="https://github.com/user-attachments/assets/ddc50273-35a4-4b03-858d-4e494082cc5a" />


---

### Step 4 — Validasi Data di ClickHouse
Masuk ke dalam terminal interaktif ClickHouse untuk memvalidasi bahwa data pesanan berhasil diagregasi oleh Spark:
~~~bash
docker exec -it wikipedia-realtime-pipeline-clickhouse-server-1 clickhouse-client --user admin --password rahasia
~~~

Jalankan *query* SQL analitik berikut untuk menguji *Business Insights*:

**1. Menampilkan Data Agregasi Pertama:**
~~~sql
SELECT * FROM analytics.orders_by_hour;
~~~

<img width="945" height="475" alt="image" src="https://github.com/user-attachments/assets/012df2ab-7180-40f4-a60f-486ba6c47b03" />


**2. Mencari Jam Paling Sibuk (Peak Hour):**
~~~sql
SELECT order_hour_of_day AS Jam, total_orders AS Total_Pesanan
FROM analytics.orders_by_hour
ORDER BY total_orders DESC
LIMIT 3;
~~~


<img width="447" height="217" alt="image" src="https://github.com/user-attachments/assets/baf91af8-edcf-48e7-b313-3227b23e07e0" />


**3. Distribusi Pesanan Berdasarkan Shift Waktu:**
~~~sql
SELECT 
    CASE 
        WHEN order_hour_of_day BETWEEN 6 AND 11 THEN 'Morning (06-11)'
        WHEN order_hour_of_day BETWEEN 12 AND 17 THEN 'Afternoon (12-17)'
        WHEN order_hour_of_day BETWEEN 18 AND 23 THEN 'Evening (18-23)'
        ELSE 'Late Night (00-05)'
    END AS Time_Shift,
    SUM(total_orders) AS Total_Orders
FROM analytics.orders_by_hour
GROUP BY Time_Shift
ORDER BY Total_Orders DESC;
~~~


<img width="496" height="236" alt="image" src="https://github.com/user-attachments/assets/aecc46c2-12db-464d-8f25-8355bd610516" />


*(Ketik `exit` untuk keluar dari terminal ClickHouse dan kembali ke Git Bash).*

---

### Step 5 — Akses Dashboard Visualisasi (Metabase)
1. Buka browser dan akses **http://localhost:3000**.
2. Lakukan *setup* akun dan hubungkan dengan *database* ClickHouse menggunakan kredensial internal jaringan Docker berikut:
   * **Database type:** `ClickHouse`
   * **Host:** `clickhouse-server`
   * **Port:** `8123`
   * **Database name:** `analytics`
   * **Username:** `admin` 
   * **Password:** `rahasia`



Jalankan *query* SQL analitik agar data lebih jelas:

**1. Distribusi Pesanan Berdasarkan Shift Waktu:**
~~~sql
SELECT 
    CASE 
        WHEN order_hour_of_day BETWEEN 6 AND 11 THEN 'Morning (06-11)'
        WHEN order_hour_of_day BETWEEN 12 AND 17 THEN 'Afternoon (12-17)'
        WHEN order_hour_of_day BETWEEN 18 AND 23 THEN 'Evening (18-23)'
        ELSE 'Late Night (00-05)'
    END AS Time_Shift,
    SUM(total_orders) AS Total_Orders
FROM analytics.orders_by_hour
GROUP BY Time_Shift
ORDER BY Total_Orders DESC;
~~~

<img width="1520" height="927" alt="image" src="https://github.com/user-attachments/assets/179fcde1-cdf3-4cd5-8578-c38ec149b311" />

**Penjelasan:** *Donut chart* ini membagi 100 pesanan ke dalam 4 shift waktu pakai fungsi `CASE WHEN` di SQL. Shift *Afternoon* (12:00-17:00) paling ramai dengan 48% transaksi. Sisanya menyusul: *Morning* (27%), *Evening* (23%), dan *Night* (00:00-05:00) jadi waktu yang paling sepi dengan cuma 2%.

<br>

**2. Menampilkan Total Order:**
~~~sql
SELECT SUM(total_orders) AS "Total Orders"
FROM analytics.orders_by_hour;
~~~

<img width="1518" height="981" alt="image" src="https://github.com/user-attachments/assets/2f8a7b7b-c078-4b86-9f02-832d38c22055" />

**Penjelasan:** Metrik tunggal ini menampilkan total semua pesanan yang sukses ditarik dari API, diproses Spark, sampai masuk ke ClickHouse. Angka ini jadi bukti kalau *pipeline* data berjalan lancar dari awal sampai akhir.

<br>

**3. Mencari Jam Paling Sibuk (Peak Hour):**
~~~sql
SELECT order_hour_of_day 
FROM analytics.orders_by_hour 
ORDER BY total_orders DESC 
LIMIT 1;
~~~

<img width="1520" height="981" alt="image" src="https://github.com/user-attachments/assets/4734e43d-2dea-4f39-982c-1bd7346defbc" />

**Penjelasan:** Angka ini menunjukkan jam dengan transaksi paling tinggi (16 berarti pukul 16:00). Data ini penting banget buat ngatur kapasitas server dan nentuin waktu terbaik kalau mau bikin promo atau *flash sale*.

---
<br>

### Tampilan Akhir Dashboard

<img width="1860" height="743" alt="image" src="https://github.com/user-attachments/assets/e72ab862-72e1-4c33-a1ac-a619d08ac3f8" />
<img width="1859" height="458" alt="image" src="https://github.com/user-attachments/assets/2230ec4b-b3ea-449c-b946-54ae6762d71b" />

Bagian ini merangkum pola waktu belanja konsumen:

* **Distribusi Pesanan Berdasarkan Shift Waktu (Donut Chart):** Membagi pesanan ke dalam 4 shift. Shift *Afternoon* (12:00-17:00) paling mendominasi (53%), disusul *Morning* (32%), dan *Evening* (14%). Waktu *Late Night* (00:00-05:00) paling sepi, cuma nyumbang 1% transaksi.
* **Total Orders (Scorecard):** Total pesanan yang berhasil diproses *pipeline* ada 100 pesanan.
* **Jam Paling Sibuk (Scorecard):** Transaksi paling ramai terjadi di jam 16:00.
* **Orders By Hour (Bar Chart):** Rincian transaksi per jam (00:00 - 23:00). Transaksi mulai naik jam 06:00, puncaknya di jam 15:00 dan 16:00 (11 pesanan), lalu perlahan turun sampai malam. Di jam 01:00-05:00 dini hari tidak ada transaksi sama sekali.

<img width="1855" height="904" alt="image" src="https://github.com/user-attachments/assets/dba725c5-7031-4de3-af64-ff73a03f39dc" />

Fokus di bagian ini adalah performa produk dan jangkauan pelanggan:

* **Total Macam Produk:** Ada 828 jenis produk berbeda yang berhasil terjual.
* **Total Seluruh Volume Transaksi Pesanan:** Total barang yang dipesan mencapai 1.040 unit.
* **Kumulatif Pembeli Unik (Unique Users):** Ada 1.040 pelanggan berbeda yang belanja.
* **Produk Paling Dominan Diorder (Area Chart):** Menampilkan daftar *bestseller*. "Banana" paling laku dengan 18 pesanan, diikuti "Bag of Organic Bananas" dengan 8 pesanan. Sisa top 8 lainnya (seperti apel organik, bayam, sampai stroberi) rata-rata laku 5-6 pesanan. Kelihatan banget pelanggan lebih suka produk segar dan organik.

<img width="1858" height="696" alt="image" src="https://github.com/user-attachments/assets/ffc2d525-53a6-4474-ac27-dbe4a1752ecd" />

Bagian ini melihat kebiasaan konsumen berdasarkan kategori produk:

* **Distribusi Total Order Berdasarkan Departemen (Donut Chart):** Dari 1.040 pesanan, paling banyak ada di departemen `produce` (produk segar) sebesar 28,08%, lalu `dairy eggs` (susu & telur) 17,98%, dan `snacks` 9,81%. Ini nunjukin kalau orang-orang paling sering beli kebutuhan pokok harian.
* **Pemetaan Intensitas Pembelian Ulang Per Departemen (Bar Chart):** Mengukur seberapa sering produk dibeli ulang (*reorder*). Kategori `produce` paling tinggi (hampir 190 kali), diikuti `dairy eggs` (sekitar 140 kali). Jadi, produk segar dan olahan susu itu nggak cuma laris, tapi juga paling sering di-*repeat order* sama pelanggan.