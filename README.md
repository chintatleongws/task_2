# Social Media Scraping Pipeline

An ELT pipeline for ingesting social-media data through Bright Data and
normalizing platform-specific records for analytics.

```text
RAW DATA -> CLEANED DATA -> ANALYTICS-READY DATA
             |                    |
             +-- de-duplication   +-- business logic
             +-- normalization
```

## Documentation

- [Scraper and transformation guide](docs/scraper-and-transformation.md)

The guide covers `BrightDataAPI`, `Transformation`, configuration, supported
platforms, data flow, output schemas, usage examples, and current limitations.
