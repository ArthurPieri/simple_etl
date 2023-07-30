-- Create the database
CREATE DATABASE postgres_test;

-- Connect to the newly created database
\c postgres_test;

-- Create a table
CREATE TABLE employees (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100)
);

-- Insert some data
INSERT INTO employees (first_name, last_name, email)
VALUES
('John', 'Doe', 'john.doe@example.com'),
('Jane', 'Doe', 'jane.doe@example.com');
