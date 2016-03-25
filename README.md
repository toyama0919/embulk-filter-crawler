# Crawler filter plugin for Embulk

Write short description here and build.gradle file.

## Overview

* **Plugin type**: filter

## Configuration

- **target_key**: base_url column key name (string, require)
- **max_depth_of_crawling**: max depth of crawling (integer, default: unlimited)
- **number_of_crawlers**: parallelism (integer, default: 1)
- **max_pages_to_fetch**: max_pages_to_fetch (integer, default: unlimited)
- **crawl_storage_folder**: crawl_storage_folder (string, require)
- **politeness_delay**: politeness_delay (integer, default: null)
- **user_agent_string**: user_agent_string (string, default: null)
- **output_prefix**: output_prefix (string, default: "")

## Example

```yaml
in:
  type: mysql
  host: dbs04
  user: application
  password: XXXXXXXX
  database: iap
  query: |
    select url from companies limit 100
filters:
  - type: crawler
    target_key: url
    number_of_crawlers: 10
    max_depth_of_crawling: 4
    politeness_delay: 100
    crawl_storage_folder: "/tmp/crawl/%s"
out:
  type: stdout
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
