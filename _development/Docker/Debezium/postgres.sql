ALTER SYSTEM SET WAL_LEVEL TO logical;

CREATE TABLE my_table (
    a int,
    b int,
    c float
);

CREATE PUBLICATION postgres_debezium_source_publication FOR TABLE my_table;

SELECT pg_create_logical_replication_slot('postgres_debezium_source_slot', 'pgoutput');
--SELECT pg_drop_replication_slot('postgres_debezium_source_slot');

ALTER TABLE my_table REPLICA IDENTITY FULL;

INSERT INTO my_table
    SELECT round(random() * 100), round(random() * 20), random() * 10
    FROM generate_series(1, 1000);

TRUNCATE TABLE my_table;