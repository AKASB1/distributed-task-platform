# Current boundary

JobService writes an in-memory job record, transitions it to QUEUED, and dispatches its ID to a local queue. REST, gRPC, PostgreSQL, Redis, and RabbitMQ are planned.
