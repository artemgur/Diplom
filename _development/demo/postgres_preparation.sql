ALTER SYSTEM SET WAL_LEVEL TO logical;

CREATE TABLE IF NOT EXISTS demo_table (
    a int,
    b int,
    c float
);

CREATE PUBLICATION postgres_debezium_source_publication FOR TABLE demo_table;

SELECT pg_create_logical_replication_slot('postgres_debezium_source_slot', 'pgoutput');

ALTER TABLE demo_table REPLICA IDENTITY FULL;
