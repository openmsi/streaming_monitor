# streaming_monitor

This is a small, handy Python reporting tool that cross-checks producer logs, consumer logs, and Girder contents to diagnose whether files successfully traversed a Kafka/Girder streaming pipeline. It produces a human-readable report and an issue-focused PNG status summary. It is a one-shot command-line report generator, not a live monitor, daemon, dashboard, or continuous watcher.
It matches records by filename only. If two different relative paths contain the same basename, they can collide.
