# Pulsar to ClickHouse analytics
Run on a private node able to reach Pulsar TLS and ClickHouse HTTPS.
Uses its own durable exclusive subscription, independent from the dCache bridge.
Acknowledges only after a synchronous target write. Replayed identities are
compared by payload SHA-256; conflicts and malformed messages are quarantined
without overwriting existing events. FINAL views provide logical event uniqueness.
Credentials remain in protected local files; only failure class names are logged.
A deterministic CSV publisher and an attributed UCI Iris fixture are included.
The fixture has 30 samples and four measurements per sample (120 events).
