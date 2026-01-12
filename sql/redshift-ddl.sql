-- =========================
-- Dimension: Users
-- =========================
CREATE TABLE dim_user (
    user_id VARCHAR(50) PRIMARY KEY,
    email VARCHAR(255),
    country VARCHAR(50),
    created_at_user DATE
);

-- =========================
-- Dimension: Products
-- =========================
CREATE TABLE dim_product (
    sku VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255),
    category VARCHAR(100),
    price DECIMAL(10,2)
);

-- =========================
-- Fact: Orders (item-level)
-- =========================
CREATE TABLE fact_order (
    order_id VARCHAR(50),
    user_id VARCHAR(50),
    sku VARCHAR(50),
    qty INT,
    item_price DECIMAL(10,2),
    order_amount DECIMAL(10,2),
    currency VARCHAR(10),
    created_at_order TIMESTAMP,
    source VARCHAR(50),
    promo VARCHAR(50),

    PRIMARY KEY (order_id, sku),
    FOREIGN KEY (user_id) REFERENCES dim_user(user_id),
    FOREIGN KEY (sku) REFERENCES dim_product(sku)
);