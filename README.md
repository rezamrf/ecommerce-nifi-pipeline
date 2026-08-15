# ecommerce-nifi-pipeline

Apache NiFi ETL pipeline: data e-commerce dari **PostgreSQL (OLTP) → near-real-time streaming sync → ClickHouse (OLAP) → analitik penjualan & datamart**.

## Background Project

Pernah ngalamin tim analitik butuh 15 menit cuma buat ngeluarin report penjualan harian? Gara-gara query agregasi harus full-scan puluhan juta baris di database transaksional. Masalahnya, sementara query itu jalan, aplikasi e-commerce-nya jadi lemot. Checkout timeout, koneksi pool habis, customer ngeluh.

Ini bukan bug, tapi design mismatch. Database transaksional dirancang buat operasi cepat: insert order, update stock, cari data by ID. Index-nya b-tree, locking-nya per-row, schema-nya normalized. Cocok buat aplikasi yang butuh response cepat dan konsistensi transaksi.

Tapi tim analitik butuh yang beda: scan jutaan baris, agregasi per kategori/region/waktu, join beberapa tabel sekaligus. Query kayak gini bikin database transaksional kewalahan. Dan kalau dipaksa jalan bersamaan, keduanya saling ganggu.

Dulu solusinya biasa pakai export CSV manual atau ETL batch schedule harian. Tapi itu artinya data analitik selalu telat satu hari. Tim bisnis bikin keputusan pakai data kemarin, bukan kondisi real sekarang.

Project ini jadi solusi: pisahkan beban kerja dengan nge-sync data dari PostgreSQL (OLTP) ke ClickHouse (OLAP) secara near-real-time pakai Apache NiFi. Database transaksional tetep fokus handle traffic aplikasi, sementara query analitik berat dikerjain di ClickHouse yang memang didesain buat itu.

## Problem Solving

Pipeline ini memisahkan beban: **data dipindahkan dari DB prod (PostgreSQL) ke DB analitik (ClickHouse OLAP) menggunakan Apache NiFi sebagai ETL near-real-time.**

![Arsitektur Pipeline](docs/arsitektur.png)

**Kenapa ClickHouse?** Database kolumnar OLAP — agregasi miliaran baris sub-sekon, kompresi tinggi, cocok untuk query analitik berat.

**Kenapa NiFi?** Visual dataflow, fault-tolerant, incremental polling otomatis (`QueryDatabaseTableRecord`), transform ringan (cast, bersihkan) sebelum sinkron ke ClickHouse. Pipeline jalan terus-menerus — data baru di Postgres → ~10 detik masuk ClickHouse.

## Alur Kerja

1. **Streaming sync (NiFi)** — NiFi mem-polling 9 tabel PostgreSQL tiap ~10 detik (`QueryDatabaseTableRecord`, incremental berdasarkan kolom watermark `updated_at`), ambil delta baru/berubah, tulis langsung ke tabel landing di ClickHouse (nama tabel sama: `orders`, `customers`, `order_items`, dst). Avro logical types handle timestamp, gak perlu transform manual.
2. **Datamart di ClickHouse** — query analitik berat (revenue, top kategori, top seller, kinerja per state) dibuat sebagai **VIEW** di ClickHouse yang join tabel landing. View ini selalu mengikuti data terbaru karena pipeline streaming terus update tabel landing.
3. **Result** — DB prod bebas fokus transaksi. Tim analitik query ke ClickHouse (VIEW) tanpa mengganggu operasi. Data selalu segar, tidak perlu extract manual.

## Struktur Folder

```
ecommerce-nifi-pipeline/
├── docker-compose.yml        # NiFi, PostgreSQL, ClickHouse (stateful di named volumes Docker)
├── .env.example              # template credentials (copy ke .env, jangan commit .env)
├── .gitignore
├── Dockerfile                # custom NiFi image dengan JDBC drivers
├── README.md
├── extensions/               # JDBC drivers untuk NiFi (di-copy ke container via Dockerfile)
│   ├── clickhouse-jdbc-0.6.0.jar
│   └── postgresql-42.7.3.jar
├── flow/                     # definisi flow NiFi (export dari UI, importable)
│   └── NiFi_Flow.json        # full export flow
├── sql/
│   ├── postgres/             # (referensi) DDL source Olist — dibuat manual via DBeaver
│   │   └── 01_raw_tables.sql
│   └── clickhouse/           # init DDL landing tables
│       └── 01_raw_tables.sql
└── .ignored/                 # file kerja lokal (spec/plan skill) — gitignored
```

## Data Persistence

Semua state disimpan di **Docker named volumes** (`pg_data`, `ch_data`, `nifi_data`), bukan di filesystem local Mac. Reset dengan `docker compose down -v`.