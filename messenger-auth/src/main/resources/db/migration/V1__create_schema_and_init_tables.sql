CREATE SCHEMA IF NOT EXISTS common;

CREATE TABLE common.users (
    id UUID PRIMARY KEY,
    name VARCHAR(255),
    last_name VARCHAR(255),
    email VARCHAR(255) NOT NULL UNIQUE,
    hash_password VARCHAR(255) NOT NULL
);