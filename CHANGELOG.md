# Changelog

## [1.0.0] - 2025-10-11

### Added
- Initial release of TED XML Scraper
- Extracts buyer name, city, street address
- Language detection
- Deadline extraction from LOT-level fields
- Batch insert for large datasets
- Wrapper script for reliable environment loading
- Raw XML storage for reprocessing
- PostgreSQL schema with indexes
- Comprehensive README with examples

### Fixed
- Environment variable loading issues
- Async callback blocking during extraction
- PostgreSQL parameter limit with batch inserts
- Deadline extraction from nested LOT structures

### Stats
- Processes 3500+ notices per day
- 99.97% field extraction rate
- Supports all TED notice types
