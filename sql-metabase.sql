# =========================================================
# NUMBER CARD — KPI Utama (Single Value)
# =========================================================
# Q1 — Total Macam Produk
SELECT COUNT(*) FROM analytics.orders_top_products;

# Q2 — Total Seluruh Volume Transaksi Pesanan
SELECT SUM(total_orders) FROM analytics.orders_top_products;

# Q3 — Kumulatif Pembeli Unik (Unique Users)
SELECT SUM(unique_users) FROM analytics.orders_top_products;


# =========================================================
# BAR CHART — Perbandingan Komoditas Terlaris
# =========================================================
# Q4 — Top 10 Produk Paling Dominan Diorder
SELECT product_name, total_orders
FROM analytics.orders_top_products
ORDER BY total_orders DESC
LIMIT 10;


# =========================================================
# PIE / DONUT CHART — Proporsi Pasar Berdasarkan Kategori
# =========================================================
# Q5 — Distribusi Total Order Berdasarkan Departemen
SELECT department, SUM(total_orders)
FROM analytics.orders_top_products
GROUP BY department
ORDER BY SUM(total_orders) DESC;


# =========================================================
# TREEMAP — Visualisasi Kotak Matriks Proporsional
# =========================================================
# Q6 — Pemetaan Intensitas Pembelian Ulang Per Departemen
SELECT department, SUM(reorder_count)
FROM analytics.orders_top_products
GROUP BY department
ORDER BY SUM(reorder_count) DESC;