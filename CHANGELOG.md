# Changelog

## [0.1.3](https://github.com/cloudquery/plugin-sdk-python/compare/v0.1.2...v0.1.3) (2023-09-01)


### Bug Fixes

* **deps:** Update dependency cloudquery-plugin-pb to v0.0.16 ([#54](https://github.com/cloudquery/plugin-sdk-python/issues/54)) ([add9fa2](https://github.com/cloudquery/plugin-sdk-python/commit/add9fa2cf349f5ccebe4215bfef880748df0a6d0))

## [0.1.2](https://github.com/cloudquery/plugin-sdk-python/compare/v0.1.1...v0.1.2) (2023-09-01)


### Bug Fixes

* **deps:** Update dependency exceptiongroup to v1.1.3 ([#46](https://github.com/cloudquery/plugin-sdk-python/issues/46)) ([baf407d](https://github.com/cloudquery/plugin-sdk-python/commit/baf407d54fa547d4f1747fe8b06c65c8f72ea0f0))
* **deps:** Update dependency grpcio to v1.57.0 ([#47](https://github.com/cloudquery/plugin-sdk-python/issues/47)) ([d1795c7](https://github.com/cloudquery/plugin-sdk-python/commit/d1795c7c577fc459235e14f847d9f64c7ab71721))
* **deps:** Update dependency grpcio-tools to v1.57.0 ([#49](https://github.com/cloudquery/plugin-sdk-python/issues/49)) ([56651c3](https://github.com/cloudquery/plugin-sdk-python/commit/56651c3072bee4bd498cc0ce9bd4e084502b16e1))
* **deps:** Update dependency pandas to v2.1.0 ([#50](https://github.com/cloudquery/plugin-sdk-python/issues/50)) ([7c8e90c](https://github.com/cloudquery/plugin-sdk-python/commit/7c8e90cbb28311a5e05e3078f31d300a7db70c40))
* **deps:** Update dependency pluggy to v1.3.0 ([#51](https://github.com/cloudquery/plugin-sdk-python/issues/51)) ([8e6029c](https://github.com/cloudquery/plugin-sdk-python/commit/8e6029cab3efdd225e4e849756db35f5540373d9))
* **deps:** Update dependency protobuf to v4.24.2 ([#52](https://github.com/cloudquery/plugin-sdk-python/issues/52)) ([7a583bc](https://github.com/cloudquery/plugin-sdk-python/commit/7a583bc48d41d972552d0823dcaaa4f1b656c0c7))

## [0.1.1](https://github.com/cloudquery/plugin-sdk-python/compare/v0.1.0...v0.1.1) (2023-08-14)


### Bug Fixes

* Flatten tables in plugin server getTables call ([#41](https://github.com/cloudquery/plugin-sdk-python/issues/41)) ([37b7bf2](https://github.com/cloudquery/plugin-sdk-python/commit/37b7bf2464d807f33e7e39e96bcb6a1aa68e4e39))

## [0.1.0](https://github.com/cloudquery/plugin-sdk-python/compare/v0.0.11...v0.1.0) (2023-08-14)


### ⚠ BREAKING CHANGES

* Remove docs generation ([#36](https://github.com/cloudquery/plugin-sdk-python/issues/36))

### Features

* Remove docs generation ([#36](https://github.com/cloudquery/plugin-sdk-python/issues/36)) ([a72df48](https://github.com/cloudquery/plugin-sdk-python/commit/a72df48f3b44dc48fca9707b7937748b0dc77b10))


### Bug Fixes

* Add missing metadata fields ([#40](https://github.com/cloudquery/plugin-sdk-python/issues/40)) ([0003c40](https://github.com/cloudquery/plugin-sdk-python/commit/0003c405cac2c57c8d436499e8ba576ab5d04f3b))
* Fix filter_dfs to be recursive, add support for skip_dependent_tables ([#39](https://github.com/cloudquery/plugin-sdk-python/issues/39)) ([149c699](https://github.com/cloudquery/plugin-sdk-python/commit/149c69974c1fd36d7f41ed36e6b545352b29ce5b))

## [0.0.11](https://github.com/cloudquery/plugin-sdk-python/compare/v0.0.10...v0.0.11) (2023-08-08)


### Bug Fixes

* **plugin-tables:** Add missing `skip_dependent_tables` ([#33](https://github.com/cloudquery/plugin-sdk-python/issues/33)) ([a592763](https://github.com/cloudquery/plugin-sdk-python/commit/a5927636505c5d6a36591d4592664a396f07f76c))

## [0.0.10](https://github.com/cloudquery/plugin-sdk-python/compare/v0.0.9...v0.0.10) (2023-08-02)


### Bug Fixes

* Fix generate docs command ([#30](https://github.com/cloudquery/plugin-sdk-python/issues/30)) ([11cfb94](https://github.com/cloudquery/plugin-sdk-python/commit/11cfb94266a9e66880a10e37db8e9efb15ce9721))

## [0.0.9](https://github.com/cloudquery/plugin-sdk-python/compare/v0.0.8...v0.0.9) (2023-08-02)


### Features

* Add override_columns to openapi transformer ([#22](https://github.com/cloudquery/plugin-sdk-python/issues/22)) ([a53bb0e](https://github.com/cloudquery/plugin-sdk-python/commit/a53bb0e0e1eda99705fc96c0c7d0ac79e814a8d7))
* Wire logging with cli flags ([#26](https://github.com/cloudquery/plugin-sdk-python/issues/26)) ([106781b](https://github.com/cloudquery/plugin-sdk-python/commit/106781b0ddfa5d5be77890fc0f9b3fe1cee10848))


### Bug Fixes

* Add better logging for scheduler ([#24](https://github.com/cloudquery/plugin-sdk-python/issues/24)) ([505f94b](https://github.com/cloudquery/plugin-sdk-python/commit/505f94bf67f9835cc3463df854943c6f00c2d281))
* Add more command-line args, use standard logging ([#29](https://github.com/cloudquery/plugin-sdk-python/issues/29)) ([5d52af9](https://github.com/cloudquery/plugin-sdk-python/commit/5d52af99cb98906faee1485ea10edf05c09155e5))
* Emit migrate messages for child relations ([#21](https://github.com/cloudquery/plugin-sdk-python/issues/21)) ([536e163](https://github.com/cloudquery/plugin-sdk-python/commit/536e16303a0ac18f37a468d9e2b97b4438bc596e))
* Fix column resolver resource set ([#23](https://github.com/cloudquery/plugin-sdk-python/issues/23)) ([9936ced](https://github.com/cloudquery/plugin-sdk-python/commit/9936ced487ba387c21385c372f163557bd46ba69))
* Fix exception logging ([#18](https://github.com/cloudquery/plugin-sdk-python/issues/18)) ([2a5996b](https://github.com/cloudquery/plugin-sdk-python/commit/2a5996b553e9cbb4b1e8dc95d22872a866d32c8b))
* Fix extension type definitions ([#20](https://github.com/cloudquery/plugin-sdk-python/issues/20)) ([146c549](https://github.com/cloudquery/plugin-sdk-python/commit/146c5498cb1ecf54a55bb5a60ce2cb0a0228c2ed))
* Fix JSON type handling ([#19](https://github.com/cloudquery/plugin-sdk-python/issues/19)) ([c0cdf55](https://github.com/cloudquery/plugin-sdk-python/commit/c0cdf55a49ebbb1c8ed51022d4a5910f51378e74))
* Fix race in scheduler ([#25](https://github.com/cloudquery/plugin-sdk-python/issues/25)) ([17fee27](https://github.com/cloudquery/plugin-sdk-python/commit/17fee278f05449584c6a781f6560c7f5faf431c6))
* Log error on table resolver exception/error ([#16](https://github.com/cloudquery/plugin-sdk-python/issues/16)) ([a1b07e8](https://github.com/cloudquery/plugin-sdk-python/commit/a1b07e8624a335c7ffc37f58913ec103305fd46a))

## [0.0.8](https://github.com/cloudquery/plugin-sdk-python/compare/v0.0.7...v0.0.8) (2023-08-01)


### Bug Fixes

* **deps:** Update dependency numpy to v1.25.2 ([#11](https://github.com/cloudquery/plugin-sdk-python/issues/11)) ([0d05fc8](https://github.com/cloudquery/plugin-sdk-python/commit/0d05fc8205c9d6f2c35b82647babef25dfd550c5))
* Fix JSON and UUID type checking, add JSON test, consolidate setup.py ([#14](https://github.com/cloudquery/plugin-sdk-python/issues/14)) ([7927d1a](https://github.com/cloudquery/plugin-sdk-python/commit/7927d1aa4ca0f34252e7bfcccacc92d4a0975d46))
* Remove resolver() from schema.Table ([#15](https://github.com/cloudquery/plugin-sdk-python/issues/15)) ([c61a774](https://github.com/cloudquery/plugin-sdk-python/commit/c61a7741a8a8c6d88e10cc91b15b41e5b83bccf0))
* SyncMigrateTableMessage should have the `pa.Schema` argument named as "table" ([#13](https://github.com/cloudquery/plugin-sdk-python/issues/13)) ([a50f0e7](https://github.com/cloudquery/plugin-sdk-python/commit/a50f0e7a82b314a870f8195278ebe2bf9eb5442a))

## [0.0.7](https://github.com/cloudquery/plugin-sdk-python/compare/v0.0.6...v0.0.7) (2023-07-29)


### Features

* Scalar things ([#5](https://github.com/cloudquery/plugin-sdk-python/issues/5)) ([3148c6c](https://github.com/cloudquery/plugin-sdk-python/commit/3148c6ce70c19cda24e357e2b274c0e4130b05eb))
* Update openapi transformers ([15923e0](https://github.com/cloudquery/plugin-sdk-python/commit/15923e0d4924defae5ca2023a4cf08132af775fd))


### Bug Fixes

* Add write support for roundtrip test ([4f3545b](https://github.com/cloudquery/plugin-sdk-python/commit/4f3545b6a1418625f472c750b3ec24525810c3c1))

## [0.0.6](https://github.com/cloudquery/plugin-sdk-python/compare/v0.0.5...v0.0.6) (2023-07-28)


### Features

* Add minimal openapi transformer ([083b9ff](https://github.com/cloudquery/plugin-sdk-python/commit/083b9ff863979f8c6410df8e6980f76a53a71bb9))


### Bug Fixes

* Pytest to run all tests ([#4](https://github.com/cloudquery/plugin-sdk-python/issues/4)) ([1eb5c7f](https://github.com/cloudquery/plugin-sdk-python/commit/1eb5c7fb91e623fa9ab0bb187e4dddab824e8b08))
