# Changelog

## Current (in progress)

- Add malus score for archived reuses [#54](https://github.com/opendatateam/udata-search-service/pull/54)
- Add object id in search fields [#55](https://github.com/opendatateam/udata-search-service/pull/55)
- Add resources ids and titles in search fields [#56](https://github.com/opendatateam/udata-search-service/pull/56)
- Upgrade deps [#57](https://github.com/opendatateam/udata-search-service/pull/57)

## 2.2.2 (2025-03-14)

- Add Dataservices views metrics as field factor and sort [#52](https://github.com/opendatateam/udata-search-service/pull/52)

## 2.2.1 (2024-11-29)

- feat: handle multiple tags filter on datasets [#50](https://github.com/opendatateam/udata-search-service/pull/50)

## 2.2.0 (2024-11-07)

- Add badge organization filter on dataset & reuse [#49](https://github.com/opendatateam/udata-search-service/pull/49)
- Add dataservices search [#48](https://github.com/opendatateam/udata-search-service/pull/48)

:warning: To use these new features, you will need to:
    - init ES indices with `udata-search-service init-es`
    - index datasets and dataservices on udata side with `udata search index dataset` and `udata search index dataservice`

## 2.1.0 (2024-10-07)

- Upgrade Flask to 2.2.5 [#44](https://github.com/opendatateam/udata-search-service/pull/44)
- Upgrade to Python 3.11 [#45](https://github.com/opendatateam/udata-search-service/pull/45)

## 2.0.2 (2023-09-01)

- Add dataset's topics in index [#43](https://github.com/opendatateam/udata-search-service/pull/43)

## 2.0.1 (2023-05-16)

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
