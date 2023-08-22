CREATE DATABASE postgres_test;

\c postgres_test;

CREATE SCHEMA IF NOT EXISTS public;

CREATE TABLE IF NOT EXISTS public.employees (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100)
);

INSERT INTO public.employees (first_name, last_name, email)
VALUES
('John', 'Doe', 'john.doe@example.com'),
('Jane', 'Doe', 'jane.doe@example.com');

CREATE TABLE IF NOT EXISTS public.employees_test_load (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    loaddate TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO public.employees_test_load (first_name, last_name, email, loaddate)
VALUES
('John', 'Doe', 'john.doe@example.com', TO_TIMESTAMP('2023-08-20', 'YYYY-MM-DD')),
('Jane', 'Doe', 'jane.doe@example.com', TO_TIMESTAMP('2023-08-21', 'YYYY-MM-DD')),
('James', 'Doe', 'james.doe@example.com', TO_TIMESTAMP('2023-08-22', 'YYYY-MM-DD'));
