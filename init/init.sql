CREATE TABLE IF NOT EXISTS exchanges (
    exchange_id TEXT NOT NULL,
    exchange_name TEXT NOT NULL,
    trust_score INT NOT NULL,
    trust_score_rank INT NOT NULL
);

CREATE TABLE IF NOT EXISTS exchange_market (
    exchange_id TEXT NOT NULL,
    base TEXT NOT NULL,
    target TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS exchanges_all (
    exchange_id TEXT NOT NULL,
    date TEXT NOT NULL,
    volume_btc FLOAT NOT NULL
);

CREATE TABLE IF NOT EXISTS spreads (
    orderbook_timestamp TEXT NOT NULL,
    book TEXT NOT NULL,
    bid FLOAT NOT NULL,
    ask FLOAT NOT NULL,
    spread FLOAT NOT NULL,
    is_above BOOLEAN NOT NULL
);
