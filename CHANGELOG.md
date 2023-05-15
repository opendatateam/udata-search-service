# Changelog

## Current (in progress)

- Add `last_update` field to dataset entity. All datasets need to be reindexed to take last_update into account [#40](https://github.com/opendatateam/udata-search-service/pull/40)
- Use `datetime.utcnow` to make sure to handle utc datetimes [#42](https://github.com/opendatateam/udata-search-service/pull/42)

## 2.0.0 (2023-01-09)

- Use redpanda instead of Kafka [#34](https://github.com/opendatateam/udata-search-service/pull/34)
- Remove Kafka integration and use HTTP calls instead [#35](https://github.com/opendatateam/udata-search-service/pull/35)

## 1.0.3 (2022-07-11)

- Externalise Kafka consumer to udata_event_service package. Replace `data` key of messages with `value` [#30](https://github.com/opendatateam/udata-search-service/pull/30)
- Improve loggings [#31](https://github.com/opendatateam/udata-search-service/pull/31) [#32](https://github.com/opendatateam/udata-search-service/pull/32)
- Remove requirements.txt and use pyproject.toml [#33](https://github.com/opendatateam/udata-search-service/pull/33)

## 1.0.2 (2022-06-09)

- Add configurable prefix for index and prefix/suffix in kafka topics [#26](https://github.com/opendatateam/udata-search-service/pull/26)
- Add cross fields in query search for reuses and organization [#27](https://github.com/opendatateam/udata-search-service/pull/27)
- Improve Readme with deployment instructions [#28](https://github.com/opendatateam/udata-search-service/pull/28)
- Remove total hits tracking in search query [#29](https://github.com/opendatateam/udata-search-service/pull/29)

## 1.0.1 (2022-03-30)

- Track total hits in search query

## 1.0.0 (2022-03-30)

- Initial version of udata-search-service
