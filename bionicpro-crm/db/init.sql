-- init.sql
CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255),
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);

INSERT INTO customers (name, email, created_at)
VALUES
    ('Alice',  'prothetic1@example.com', '2024-01-01'),
    ('Bob',    'prothetic2@example.com', '2024-01-05'),
    ('Charlie','prothetic3@example.com', '2024-01-10')