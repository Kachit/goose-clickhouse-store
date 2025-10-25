CREATE TABLE sales (
                       id UInt32,
                       product_name String,
                       category String,
                       sale_date Date DEFAULT toDate(now()),
                       units_sold UInt32,
                       price_per_unit Float32,
                       region Enum('North America' = 1, 'Europe' = 2, 'Asia' = 3)
)
    ENGINE = MergeTree()
ORDER BY id;