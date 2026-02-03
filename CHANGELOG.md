# Changelog

## 1.0.0 (2026-02-03)


### ⚠ BREAKING CHANGES

* FFM storage API removed, use GeneratedTable interface instead

### Features

* add @Query and @Param annotations ([bb3f749](https://github.com/Thejuampi/memris/commit/bb3f749dba8c2f0ebddb2aca658d9249df694340))
* add bound values and parameter indices to queries ([b5193e0](https://github.com/Thejuampi/memris/commit/b5193e07b8ae4919674ee4c5a97d4389794ae3b9))
* add BytecodeTableGenerator for zero-reflection table operations ([6338ea4](https://github.com/Thejuampi/memris/commit/6338ea439a29cee0f8f99c9303e6bc67f4cbc160))
* add comprehensive Spring Data features and performance improvements ([0dec57e](https://github.com/Thejuampi/memris/commit/0dec57ef7747adfed8c45c96290e732525af3a9b))
* Add configurable StringPrefixIndex and StringSuffixIndex for pattern matching queries ([f3f7c68](https://github.com/Thejuampi/memris/commit/f3f7c68116de2d628b20ff7ab2ac6be6ee176a37))
* add DISTINCT flag support to LogicalQuery and CompiledQuery ([ef400c0](https://github.com/Thejuampi/memris/commit/ef400c0b7d46be27c13de93c9622de23cd5fffa5))
* add EntitySaverGenerator for zero-reflection entity save operations ([df25ee6](https://github.com/Thejuampi/memris/commit/df25ee61d4c0b25e751478b7cf3f1aacaccc7c1e))
* add fast EXISTS/COUNT with short-circuit optimization ([6c18f12](https://github.com/Thejuampi/memris/commit/6c18f120089a37eef4c17e45e2dea359a8c6fbb6))
* add IN list handling and proper argument resolution ([f3cad4b](https://github.com/Thejuampi/memris/commit/f3cad4b1476c960828943675c73082eec0872c32))
* add JMH concurrent read/write benchmark with thread groups ([aa86d74](https://github.com/Thejuampi/memris/commit/aa86d7470dea761f3eefbfcb1496bd1fdb3fb96a))
* Add join support to LogicalQuery and relationship metadata ([77ced51](https://github.com/Thejuampi/memris/commit/77ced51bf8d5a792389c23e3a1a8ed62e24727ff))
* add JPQL AST nodes and lexer ([f424801](https://github.com/Thejuampi/memris/commit/f4248018163631a58d1c24f021b5b0339cfb7fb9))
* add JPQL query parser ([1a0a1e7](https://github.com/Thejuampi/memris/commit/1a0a1e7d466e6fcbc14b4f20839bb1fa52089693))
* add MemrisConfiguration builder for type-safe configuration ([bbfcd9a](https://github.com/Thejuampi/memris/commit/bbfcd9a8ba613b7526b52a15bbbbd4f9b753eff3))
* add multi-column ORDER BY and JPQL JOIN compilation ([9e4972c](https://github.com/Thejuampi/memris/commit/9e4972c9a9fd0d7d83b62d93a9074951b40b56b9))
* add presence-tracked columns and join runtime ([561f1b8](https://github.com/Thejuampi/memris/commit/561f1b8e689401874a6d3dbf1f857be3fea5effc))
* add query argument resolution with bound values ([9c8e577](https://github.com/Thejuampi/memris/commit/9c8e57723431f9c463d550a510060dc7a3638fcb))
* Add relationship annotations for join support (simplified - all eager) ([439f970](https://github.com/Thejuampi/memris/commit/439f970dc2cb611e954b3c3234f4ea46d3e1e3bd))
* add relationship support and fix annotation detection ([8fe16dc](https://github.com/Thejuampi/memris/commit/8fe16dceb3c2783bb3e061ea375c96baf1732fb0))
* add repository method dispatch indexing for zero-reflection calls ([4bb11f1](https://github.com/Thejuampi/memris/commit/4bb11f137ceb29d85bcb6e268f3fdb568810e60b))
* Add standard primitive type handlers ([f7b5d18](https://github.com/Thejuampi/memris/commit/f7b5d1805cb152d3ea2272b35d7174d18861414c))
* add top-k limit optimization ([3657dc7](https://github.com/Thejuampi/memris/commit/3657dc7e14778d86b105840ddd19f224ba79866a))
* Complete ECommerceRealWorldTest implementation ([f4d177f](https://github.com/Thejuampi/memris/commit/f4d177f2f248bfb8b6fe8c9e7dbd40d06d7ff447))
* **concurrency:** Implement seqlock for row-level write atomicity ([fdfacf8](https://github.com/Thejuampi/memris/commit/fdfacf8ea2080f9c08fe0f2b059b828cc93b2b64))
* enable BytecodeImplementation with MethodHandle delegation ([a9400f8](https://github.com/Thejuampi/memris/commit/a9400f8c89c3c4b87f1503217ebad9f96aad1cee))
* enable dynamic table growth ([352f172](https://github.com/Thejuampi/memris/commit/352f1723c238a89579a903c8f571b3b3b2c2f330))
* implement deletion with tombstone, free-list reuse, and generation guard ([63d2038](https://github.com/Thejuampi/memris/commit/63d2038760e2e55aff129b11ecb38e692078356a))
* implement LIKE and NOT_LIKE pattern matching ([9fe6535](https://github.com/Thejuampi/memris/commit/9fe65358f7261d6440417b7c6efaa4d40b76c7d3))
* implement lock-free multi-writer concurrency with CAS seqlock ([f59a847](https://github.com/Thejuampi/memris/commit/f59a847b5f5982a44991aa0b11ef000bc3f82588))
* implement MethodHandle-based field access and selective method generation ([276cbeb](https://github.com/Thejuampi/memris/commit/276cbeb4faff8c345aeb6415cd167926636b2337))
* implement Spring Data JPA query method lexer with context-aware parsing ([d698344](https://github.com/Thejuampi/memris/commit/d698344e84fb84a330eca5a06b1a3ab33195c957))
* implement TableManager with TDD ([52f8194](https://github.com/Thejuampi/memris/commit/52f81944a4faf8bf4ee8d0e64cc5ad6c532b5636))
* implement zero-reflection projection materialization and fix JPQL keywords ([a8b4290](https://github.com/Thejuampi/memris/commit/a8b429028d530b2ba43a18d18afa490a067b7ac0))
* integrate @Query parser into QueryPlanner ([6480271](https://github.com/Thejuampi/memris/commit/6480271e03fb367b546ce867480fc74b1f38c33a))
* integrate EntitySaver into runtime - zero-reflection saves ([fd0fcbf](https://github.com/Thejuampi/memris/commit/fd0fcbf16a49cc0af696aeba7c68140a7e263aa7))
* integrate MemrisConfiguration throughout codebase ([11b551b](https://github.com/Thejuampi/memris/commit/11b551b85d0f6cb39a9382df6534f5dbbee6d144))
* integrate TableManager into MemrisRepositoryFactory ([b7c6c06](https://github.com/Thejuampi/memris/commit/b7c6c06c2a246c547252f7a2c4795f9532b37fb7))
* make BytecodeTableGenerator the default table implementation ([974cb09](https://github.com/Thejuampi/memris/commit/974cb099368441b777c5d5170d3ca1f5769f60f7))
* optimize column index to field name resolution ([21ad118](https://github.com/Thejuampi/memris/commit/21ad118f998886745e76d0651852fd6c38fc0b3d))
* publish pages with CAS ([6b90071](https://github.com/Thejuampi/memris/commit/6b9007161699d1afb5fe695d6628461c8e1a9c82))
* rewrite RepositoryBytecodeGenerator with ByteBuddy Advice API ([d4b8a96](https://github.com/Thejuampi/memris/commit/d4b8a960ef8e7aa2b8df901060b099a829c27dac))
* **spring-data:** add modifying jpql updates ([02c66ca](https://github.com/Thejuampi/memris/commit/02c66ca9e52d28831374be82bea3a535adec1c16))
* unified lexer - generic operation classification for all methods ([7e17a1f](https://github.com/Thejuampi/memris/commit/7e17a1f65f9248602554a8c722cbc57b0c0c0304))
* unify Spring Data query planning and operator handling ([3cc503a](https://github.com/Thejuampi/memris/commit/3cc503a3446b10b134b735c53ebc6ca64300d6fc))


### Bug Fixes

* **concurrency:** Fix free-list race condition with lock-free stack ([d4a43aa](https://github.com/Thejuampi/memris/commit/d4a43aa74bbb58a214b31442c20f4e811828a94b))
* **concurrency:** Fix RepositoryRuntime ID counter race condition ([6689cc4](https://github.com/Thejuampi/memris/commit/6689cc480b00c4a8b48b1d7d8dba63c6b6096ce3))
* Correct ID generation to use Long.valueOf for Long types ([f96dbcd](https://github.com/Thejuampi/memris/commit/f96dbcd7254c9986ee5f5be255d6570c5b739642))
* Exclude benchmark tests from mvn test by default ([7b117c2](https://github.com/Thejuampi/memris/commit/7b117c2802a91f347d06f1c419e67efba0719793))
* harden arena isolation tests and concurrency paths ([11e8308](https://github.com/Thejuampi/memris/commit/11e830886a5e8de6afb416aa11d3ae98274e7c2f))
* improve error message for unsupported field types ([e57e669](https://github.com/Thejuampi/memris/commit/e57e669534ee17efe2b08c3cc2faf8f0e5976d85))
* integrate QueryPlanner with entity class for context-aware validation ([8c4259f](https://github.com/Thejuampi/memris/commit/8c4259fe9a75d37181ac4be6d22d2b0eb587abc4))
* resolve ByteBuddy MethodDelegation binding for query methods ([5ad7e42](https://github.com/Thejuampi/memris/commit/5ad7e42e3ba96f66aa3737395d3495f22b80342b))
* resolve SpotBugs issues and remove dead code ([07ee05a](https://github.com/Thejuampi/memris/commit/07ee05a0b0da087a6a9fb56727ce657865bd4202))
* **safety:** Add bounds checking in MethodHandleImplementation ([69f7120](https://github.com/Thejuampi/memris/commit/69f71201e1b2021b3a53d3c17c2d0e2331fc119e))
* stabilize metadata mapping and sync repo config ([a7923f0](https://github.com/Thejuampi/memris/commit/a7923f01d396b015a8063d07ea44099ca7a9696a))
* support single-result limits and distinct queries ([40f0f35](https://github.com/Thejuampi/memris/commit/40f0f35e6c6735ccaaaa69c7b559dd6f4d2b5a62))


### Performance Improvements

* optimize hot paths with index-based loops ([7293962](https://github.com/Thejuampi/memris/commit/729396287ea3d4b893b1cf4a1402c9b9cef329b0))
* optimize type switching with pre-computed type codes ([234e920](https://github.com/Thejuampi/memris/commit/234e9202dcb0009d0f6f9b8dc11697374a346779))
* raise benchmark capacity and gate errorprone ([b2b1a0f](https://github.com/Thejuampi/memris/commit/b2b1a0f34e8100f30d15f705dc50f045c71ab9dd))
* **runtime:** Reduce allocation in HeapRuntimeKernel hot paths ([fbca58c](https://github.com/Thejuampi/memris/commit/fbca58c06c575fcf5fe34a85816aa958c7ce46ca))
* speed up built-in resolution ([075ef42](https://github.com/Thejuampi/memris/commit/075ef4269862abd2a8f8a61a9f47fb305c8beacc))
* **storage:** Add missing scan methods for range operations ([2400188](https://github.com/Thejuampi/memris/commit/2400188718c91bcc7eab3b5afcbf0f76c8730de3))
* **storage:** Optimize scanIn from O(n*m) to O(n) using HashSet ([c8f12f1](https://github.com/Thejuampi/memris/commit/c8f12f117589c842279e0e76625b0014b353b173))


### Documentation

* add ARCHITECTURE.md draft ([918ff23](https://github.com/Thejuampi/memris/commit/918ff238443758d2f0c50162caadc98e743e4bba))
* add comprehensive concurrency and implementation documentation ([252d98b](https://github.com/Thejuampi/memris/commit/252d98bb138da50378e6e9ae55dadd5f4372d484))
* add float/double precision handling to testing guidelines ([f2ee6a0](https://github.com/Thejuampi/memris/commit/f2ee6a05911b3c50b2d50cb0f4009e07d1fce0ef))
* add imports-only coding standard and fix Error Prone config ([6502764](https://github.com/Thejuampi/memris/commit/6502764c77159fc3523c8d511cc44ffac0c86aec))
* add testing guidelines for single AssertJ assertions ([e7d0fad](https://github.com/Thejuampi/memris/commit/e7d0fad0c124a7465ccb12f0117e61cad720ecaf))
* add TODO for pre-processed entity record in future iteration ([5643c87](https://github.com/Thejuampi/memris/commit/5643c87c689879c0c6f2a9e2b49171f3c99658ca))
* consolidate and streamline documentation ([c63ada3](https://github.com/Thejuampi/memris/commit/c63ada3799d8b9da0ef89509fa10e6cad22f5e6e))
* consolidate duplicate content between CLAUDE.md and README.md ([c451fb2](https://github.com/Thejuampi/memris/commit/c451fb2b6d22271d2a8050336a9e7a1eb0b882b5))
* document @Query annotation support ([791ff3f](https://github.com/Thejuampi/memris/commit/791ff3f464a37583c3314ee1a45cdccb10fdc62d))
* rewrite README with accurate repository usage examples and current feature status ([e79dd68](https://github.com/Thejuampi/memris/commit/e79dd68823cd20620a5d8e026f03f1eed3918819))
* standardize documentation and remove obsolete code ([d007f20](https://github.com/Thejuampi/memris/commit/d007f20abe86f4c83122ba3df86052e311852645))
* Update all documentation to reflect concurrency fixes and optimizations ([6deb92f](https://github.com/Thejuampi/memris/commit/6deb92ffa685079d10afb59311fe45d7044f4092))
* update build commands to use mvn.cmd for Windows ([625645f](https://github.com/Thejuampi/memris/commit/625645f376868efb87a6bca1f6212a50d462a48a))
* update documentation for RuntimeExecutorGenerator and Set return type support ([5ace6fc](https://github.com/Thejuampi/memris/commit/5ace6fcaa78e45c691023646f19dd5238d7943ff))
* update documentation to reflect current implementation status ([ed1fdc5](https://github.com/Thejuampi/memris/commit/ed1fdc549ea5cef5aad01a47b9f78fdac9fa2de8))
* update existing documentation for heap-based architecture ([d89ad29](https://github.com/Thejuampi/memris/commit/d89ad29d6e71421542697cae3e65c5ea651aa253))
* update float/double handling to use AssertJ's isCloseTo() ([ca144c5](https://github.com/Thejuampi/memris/commit/ca144c54d8ba07fdfb415593b02714e34392d550))
* update implementation status for relationships and enterprise features ([bf8c26e](https://github.com/Thejuampi/memris/commit/bf8c26eab7164ce8c3967514b527077464b297ae))
* update Maven commands to show warnings only ([e4dfe53](https://github.com/Thejuampi/memris/commit/e4dfe53f090744860d00f65fbe20a759b24c3eb4))
* update Maven commands to use -q -e for warnings/errors only ([0663c8e](https://github.com/Thejuampi/memris/commit/0663c8e626686c10a34935f6cf2180e740d2a987))
* update SRP architecture with domain package and corrected structure ([8c18f72](https://github.com/Thejuampi/memris/commit/8c18f7256c4c08705d161cb974c88963f93d76d7))


### Code Refactoring

* migrate from FFM to heap-based storage architecture ([7198297](https://github.com/Thejuampi/memris/commit/7198297ca2e06b9ecaa380a7834e4f773f31ad3b))
