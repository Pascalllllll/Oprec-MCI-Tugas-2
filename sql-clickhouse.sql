# ===== Validasi Awal =====
SHOW DATABASES;
USE analytics;
SHOW TABLES;
DESCRIBE orders_top_products;

# Mengetahui total variasi produk di batch terbaru
SELECT COUNT(*) FROM orders_top_products;

# ===== Eksplorasi Top Data =====
# 5 Produk paling laku
SELECT product_name, department, total_orders, reorder_count, unique_users
FROM orders_top_products
ORDER BY total_orders DESC
LIMIT 5;

# 10 Produk dengan tingkat reorder tertinggi
SELECT product_name, reorder_count
FROM orders_top_products
ORDER BY reorder_count DESC
LIMIT 10;

# ===== Agregat Dasar Bisnis =====
# Total baris pesanan global
SELECT SUM(total_orders) FROM orders_top_products;

# Total pelanggan unik
SELECT SUM(unique_users) FROM orders_top_products;

# ===== Analisis Komparatif Kolom =====
# Menghitung rasio reorder per total order transaksi
SELECT product_name, reorder_count, total_orders, reorder_count * 100.0 / total_orders AS reorder_ratio
FROM orders_top_products
ORDER BY total_orders DESC
LIMIT 10;