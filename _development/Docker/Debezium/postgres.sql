ALTER SYSTEM SET WAL_LEVEL TO logical;

CREATE TABLE IF NOT EXISTS demo_table (
    a int,
    b int,
    c float
);

CREATE PUBLICATION postgres_debezium_source_publication FOR TABLE demo_table;

SELECT pg_create_logical_replication_slot('postgres_debezium_source_slot', 'pgoutput');
--SELECT pg_drop_replication_slot('postgres_debezium_source_slot');

ALTER TABLE demo_table REPLICA IDENTITY FULL;

INSERT INTO demo_table
    SELECT round(random() * 100), round(random() * 20), random() * 10
    FROM generate_series(1, 1000);

DELETE FROM demo_table WHERE b > 15 AND a > 70;
DELETE FROM demo_table WHERE b < 5 AND a < 30;

UPDATE demo_table SET b = 100 WHERE a > 90;

--TRUNCATE TABLE my_table;