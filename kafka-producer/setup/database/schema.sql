-- Run this command in PostgreSQL to export schema:
-- pg_dump -h localhost -U your_username -d your_database --schema-only > schema.sql

SELECT current_database();
SELECT current_schema();


CREATE TABLE news_articles (
    id SERIAL PRIMARY KEY,
    title VARCHAR(500) NOT NULL,
    description TEXT,
    author VARCHAR(200),
    url TEXT NOT NULL,
    published_at VARCHAR(50),
    source VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL,
    timestamp BIGINT NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

-- Indexes for better performance
CREATE INDEX idx_category ON news_articles (category);
CREATE INDEX idx_source ON news_articles (source);
CREATE INDEX idx_published_at ON news_articles (published_at);
CREATE INDEX idx_timestamp ON news_articles (timestamp);