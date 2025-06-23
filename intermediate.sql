USE DATABASE LEGACY_DEMO_RDA;
USE SCHEMA KC_TEST;

SELECT
    CURRENT_DATABASE(),
    CURRENT_SCHEMA();

USE WAREHOUSE LEGACY_DEMO;

create
or replace table kc_temp (
    unique_id number autoincrement start 1 increment 1,
    loan_number varchar not null,
    borrower varchar
);