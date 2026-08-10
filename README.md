# ecommerce-nifi-pipeline

Apache NiFi ETL pipeline: data e-commerce dari **PostgreSQL (OLTP) → near-real-time streaming sync → ClickHouse (OLAP star schema) → analitik penjualan**.

## Background Project

Data e-commerce biasanya tersimpan di database operasional (OLTP) yang dirancang untuk transaksi cepat, bukan untuk analitik. Masalah yang muncul saat data ini mau dianalisis:

1. **Data tersebar di banyak tabel relasional** (orders, customers, order_items, products, sellers, payments, reviews) — untuk satu insight penjualan, harus join banyak tabel.
2. **Query analitik berat di OLTP bikin lambat & berisiko** — agregasi seperti revenue per bulan atau top kategori butuh scan besar, dijalankan terus di database transaksional bisa mengganggu kinerja aplikasi operasional.
3. **Laporan butuh data yang selalu up-to-date** — tim analitik butuh data terbaru, tapi menyalin manual tiap hari tidak praktis dan rawan salah.
4. **Format data belum siap analitik** — tipe data belum konsisten (harga, tanggal), belum ada struktur star schema untuk query agregat yang efisien.

## Arsitektur

Pipeline ini membangun jembatan **OLTP → OLAP** dengan Apache NiFi:

```
┌─────────────────┐   polling incremental   ┌──────────────────┐
│   PostgreSQL     │ ◀───────────────────── │                  │
│   (OLTP, source) │                        │   Apache NiFi    │
│                  │  QueryDatabaseTable    │                  │
└──────────────────┘  Record (tiap ~10 dtk) └──────────────────┘
         ▲                                        │ transform (cast,
         │ data di-seed via DBeaver                │ bersihkan, join)
         │ (schema dibuat manual)                 │
   ┌─────┴──────┐                                 ▼
   │  Olist data │                       ┌──────────────────┐
   │ (Kaggle)    │                       │    ClickHouse    │
   └────────────┘                        │ (OLAP, star schema)
                                         │ fact_sales + dims │
                                         └──────────────────┘
                                                   │ SQL queries
                                                   ▼
                                    Analitik: revenue, top kategori,
                                    top seller, kinerja per state
```

**Kenapa NiFi?** NiFi menangani data flow visual source Postgres ditarik secara **incremental (near real-time)**, di transform, lalu dikirim ke destinasinya (ClickHouse). Pipeline ini otomatis mendeteksi data baru di PostgreSQL dan menyinkronkannya ke ClickHouse tanpa intervensi manual.

**Alur kerja:**

1. **Streaming sync (NiFi)** — NiFi mem polling PostgreSQL tiap 10 detik (`QueryDatabaseTableRecord`, incremental berdasarkan kolom watermark), mengambil data baru/berubah, melakukan transformasi (cast tipe, bersihkan data, bangun surrogate key), lalu menulis ke ClickHouse (`PutClickHouse`).
2. **Analitik** — hasilnya adalah star schema di ClickHouse (`fact_sales` + dimension tables) yang siap di query: revenue per bulan, top kategori, top seller, kinerja per state.

**Hasil (result):** database analitik (ClickHouse) yang selalu sinkron dengan data operasional (PostgreSQL), dengan struktur star schema yang optimal untuk query agregat. Pipeline berjalan terus-menerus — **Data baru yang masuk ke PostgreSQL dan dalam ~10 detik order itu sudah muncul di ClickHouse**.

## Struktur Folder

```
ecommerce-nifi-pipeline/
├── docker-compose.yml        # NiFi, PostgreSQL, ClickHouse (stateful di named volumes Docker)
├── .env.example              # template credentials (copy ke .env, jangan commit .env)
├── .gitignore
├── README.md
├── sql/
│   ├── postgres/             # (referensi) DDL raw layer Olist — dibuat manual via DBeaver
│   └── clickhouse/           # init DDL star schema (jalan sekali di first boot)
│       ├── 01_star_schema.sql
│       └── 02_analytics_queries.sql
├── flow/                     # definisi flow NiFi (export dari UI, importable)
│   └── streaming.json        # flow streaming: PostgreSQL → ClickHouse
├── scripts/
│   └── check_pipeline.sh     # cek counts & drift Postgres vs ClickHouse
└── .ignored/                 # file kerja lokal (spec/plan skill) — gitignored
```

## Quickstart

```bash
cp .env.example .env          # isi password
docker compose up -d
```

- PostgreSQL (DBeaver): `localhost:5432`, buat schema raw dari dataset Olist (lihat referensi di `sql/postgres/`)
- ClickHouse: `localhost:8123`, DDL star schema jalan otomatis di first boot
- NiFi UI: http://localhost:8080/nifi — import `flow/streaming.json`, set DBCP ke Postgres & ClickHouse, start flow

## Data Persistence

Semua state disimpan di **Docker named volumes** (`pg_data`, `ch_data`, `nifi_data`), bukan di filesystem local Mac. Reset dengan `docker compose down -v`.
