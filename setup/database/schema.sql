DROP TABLE IF EXISTS news_articles;

CREATE TABLE news_articles (
    id SERIAL PRIMARY KEY,
    title VARCHAR(500) NOT NULL,
    description TEXT,
    url TEXT NOT NULL,
    author VARCHAR(200),
    published_at VARCHAR(50),
    content TEXT,                            -- ✅ Added this
    source VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL,
    country VARCHAR(50),                     -- ✅ Added this
    language VARCHAR(50),                    -- ✅ Added this
    timestamp BIGINT NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

-- Indexes for better performance
CREATE INDEX idx_category ON news_articles (category);
CREATE INDEX idx_source ON news_articles (source);
CREATE INDEX idx_published_at ON news_articles (published_at);
CREATE INDEX idx_timestamp ON news_articles (timestamp);
