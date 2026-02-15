package io.memris.runtime;

import io.memris.core.FloatEncoding;

import io.memris.core.ColumnAccessPlan;
import io.memris.core.EntityMetadata;
import io.memris.core.EntityMetadata.AuditField;
import io.memris.core.EntityMetadata.FieldMapping;
import io.memris.core.EntityMetadataProvider;
import io.memris.core.MemrisArena;
import io.memris.core.MemrisConfiguration;
import io.memris.core.MetadataExtractor;
import io.memris.repository.MemrisRepositoryFactory;
import io.memris.core.TypeCodes;
import io.memris.kernel.Predicate;
import io.memris.kernel.RowId;
import io.memris.core.converter.TypeConverter;
import io.memris.core.converter.TypeConverterRegistry;
import io.memris.index.CompositeHashIndex;
import io.memris.index.CompositeKey;
import io.memris.index.CompositeRangeIndex;
import io.memris.index.HashIndex;
import io.memris.index.RangeIndex;
import io.memris.index.StringPrefixIndex;
import io.memris.index.StringSuffixIndex;
import io.memris.query.CompiledQuery;
import io.memris.query.LogicalQuery;
import io.memris.storage.SimpleTable;
import io.memris.kernel.Column;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;
import io.memris.storage.SelectionImpl;
import io.memris.runtime.codegen.ConditionRowEvaluatorGenerator;
import io.memris.runtime.codegen.RuntimeExecutorGenerator;
import io.memris.runtime.dispatch.ConditionProgramCompiler;
import io.memris.runtime.dispatch.ConditionSelectionOrchestrator;
import io.memris.runtime.dispatch.CompositeIndexSelector;
import io.memris.runtime.dispatch.CompositeIndexProbeCompiler;
import io.memris.runtime.dispatch.DirectConditionExecutor;
import io.memris.runtime.dispatch.IndexProbeCompiler;
import io.memris.runtime.dispatch.IndexSelectionDispatcher;
import io.memris.runtime.dispatch.InIndexSelector;
import io.memris.runtime.dispatch.MultiColumnOrderCompiler;
import io.memris.runtime.dispatch.SingleOrderProgramCompiler;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;

import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Hot-path query execution engine.
 * <p>
 * RepositoryRuntime owns RepositoryPlan (built ONCE at repository creation)
 * and provides typed entrypoints for query execution.
 *
 * @param <T> the entity type
 */
public final class RepositoryRuntime<T> {

    private final RepositoryPlan<T> plan;
    private final MemrisRepositoryFactory factory;
    private final MemrisArena arena;
    private final EntityMaterializer<T> materializer;
    private final EntityMetadata<T> metadata;
    private final EntityMetadataProvider metadataProvider;
    private final Map<Class<?>, EntityMetadata<?>> relatedMetadata;
    private final EntitySaver<T, ?> entitySaver;
    private final java.util.concurrent.ConcurrentHashMap<Class<?>, EntityMetadata<?>> projectionMetadata;
    private final java.util.concurrent.ConcurrentHashMap<Class<?>, MethodHandle> projectionConstructors;
    private final java.util.concurrent.ConcurrentHashMap<CompiledQuery, ConditionExecutor[]> dynamicConditionExecutors;
    private final java.util.concurrent.ConcurrentHashMap<CompiledQuery, ProjectionExecutor> dynamicProjectionExecutors;
    private final Map<Class<?>, EntityRuntimeLayout> runtimeLayoutsByEntity;
    private final ColumnRuntimeDescriptor[] columnDescriptorsByColumn;
    private final int idColumnIndex;
    private final OneToManyPlan[] oneToManyPlans;
    private final ManyToManyPlan[] manyToManyPlans;
    private final IndexFieldReader[] indexFieldReaders;
    private final IndexPlan[] indexPlans;
    private final CompositeIndexSelector.CompositeIndexPlan[] compositeIndexPlans;
    private final Map<String, IndexPlan> indexPlansByField;
    private final AtomicLong idCounter;
    private final IdLookup idLookup;
    private final boolean[] primitiveNonNullByColumn;
    private final java.util.concurrent.ConcurrentHashMap<CompiledQuery.CompiledCondition, DirectConditionExecutor> directConditionExecutors;
    private final java.util.concurrent.ConcurrentHashMap<CompiledQuery.CompiledJoinPredicate, DirectConditionExecutor> joinDirectConditionExecutors;
    private final java.util.function.Predicate<RowId> rowValidator;
    private final java.util.concurrent.ConcurrentHashMap<Object, IndexProbeCompiler.IndexProbe> indexProbes;
    private final Map<CompiledQuery, CompositeIndexProbeCompiler.CompositeIndexProbe> compositeIndexProbesByQuery;
    private final Map<CompiledQuery, CompiledConditionProgram> compiledConditionProgramsByQuery;
    private final StorageValueReader[] storageReadersByColumn;
    private final ConditionExecutor[][] compiledConditionExecutorsByQuery;
    private final ProjectionExecutor[] compiledProjectionExecutorsByQuery;
    private final java.util.IdentityHashMap<CompiledQuery, Integer> compiledQueryIndexByInstance;
    private final boolean hasCompositeIndexPlans;
    private static final ThreadLocal<SortBuffers> SORT_BUFFERS = ThreadLocal.withInitial(SortBuffers::new);

    /**
     * Create a RepositoryRuntime from a RepositoryPlan.
     *
     * @param plan     the compiled repository plan
     * @param factory  the repository factory (for index queries)
     * @param metadata entity metadata for ID generation and field access
     */
    public RepositoryRuntime(RepositoryPlan<T> plan, MemrisRepositoryFactory factory, EntityMetadata<T> metadata) {
        this(plan, factory, null, metadata);
    }

    /**
     * Create a RepositoryRuntime from a RepositoryPlan with arena support.
     *
     * @param plan     the compiled repository plan
     * @param factory  the repository factory (for index queries)
     * @param arena    the arena (for arena-scoped index queries)
     * @param metadata entity metadata for ID generation and field access
     */
    public RepositoryRuntime(RepositoryPlan<T> plan, MemrisRepositoryFactory factory, MemrisArena arena,
            EntityMetadata<T> metadata) {
        if (plan == null) {
            throw new IllegalArgumentException("plan required");
        }
        this.plan = plan;
        this.factory = factory;
        this.arena = arena;
        this.metadata = metadata;
        if (metadata != null && plan.materializersByEntity() != null) {
            @SuppressWarnings("unchecked")
            EntityMaterializer<T> materializer = (EntityMaterializer<T>) plan.materializersByEntity()
                    .get(metadata.entityClass());
            this.materializer = materializer != null ? materializer : new EntityMaterializerImpl<>(metadata);
        } else {
            this.materializer = metadata != null ? new EntityMaterializerImpl<>(metadata) : null;
        }
        var config = factory != null ? factory.getConfiguration() : null;
        this.metadataProvider = config != null && config.entityMetadataProvider() != null
                ? config.entityMetadataProvider()
                : MetadataExtractor::extractEntityMetadata;
        var compiledArtifacts = RepositoryRuntimeCompiler.compile(plan, factory, arena, metadata,
                this.metadataProvider);
        this.storageReadersByColumn = compiledArtifacts.storageReadersByColumn();
        this.compiledConditionExecutorsByQuery = compiledArtifacts.conditionExecutorsByQuery();
        this.compiledProjectionExecutorsByQuery = compiledArtifacts.projectionExecutorsByQuery();
        this.compiledQueryIndexByInstance = compiledArtifacts.queryIndexByInstance();
        this.relatedMetadata = buildRelatedMetadata(metadata, this.metadataProvider);
        this.projectionMetadata = new java.util.concurrent.ConcurrentHashMap<>();
        this.dynamicConditionExecutors = new java.util.concurrent.ConcurrentHashMap<>();
        if (metadata != null) {
            this.projectionMetadata.put(metadata.entityClass(), metadata);
        }
        this.projectionMetadata.putAll(this.relatedMetadata);
        this.projectionConstructors = new java.util.concurrent.ConcurrentHashMap<>();
        this.dynamicProjectionExecutors = new java.util.concurrent.ConcurrentHashMap<>();
        this.entitySaver = plan.entitySaver();
        this.idCounter = new AtomicLong(1L);
        this.runtimeLayoutsByEntity = buildRuntimeLayouts(metadata, this.relatedMetadata);
        var rootLayout = metadata != null ? runtimeLayoutsByEntity.get(metadata.entityClass()) : null;
        this.columnDescriptorsByColumn = rootLayout != null
                ? rootLayout.descriptorsByColumn()
                : new ColumnRuntimeDescriptor[0];
        this.primitiveNonNullByColumn = buildPrimitiveNonNullByColumn(rootLayout);
        this.directConditionExecutors = new java.util.concurrent.ConcurrentHashMap<>();
        this.joinDirectConditionExecutors = new java.util.concurrent.ConcurrentHashMap<>();
        this.idColumnIndex = rootLayout != null ? rootLayout.idColumnIndex() : -1;
        this.oneToManyPlans = buildOneToManyPlans();
        this.manyToManyPlans = buildManyToManyPlans();
        this.indexPlans = buildIndexPlans(metadata);
        this.compositeIndexPlans = toCompositeIndexPlans(indexPlans);
        this.indexFieldReaders = buildIndexFieldReaders(metadata, indexPlans);
        this.indexPlansByField = buildIndexPlansByField(indexPlans);
        this.hasCompositeIndexPlans = hasCompositeIndexPlans(indexPlans);
        this.rowValidator = createRowValidator(plan.table());
        this.indexProbes = new java.util.concurrent.ConcurrentHashMap<>();
        this.compositeIndexProbesByQuery = buildCompositeIndexProbesByQuery(plan.queries());
        this.compiledConditionProgramsByQuery = buildCompiledConditionPrograms(plan.queries());
        this.idLookup = plan.idLookup();
        var queries = plan.queries();
        if (queries != null) {
            for (var query : queries) {
                var conditions = query.conditions();
                if (conditions == null) {
                    continue;
                }
                for (var condition : conditions) {
                    var primitiveNonNull = isPrimitiveNonNullColumn(condition.columnIndex());
                    directConditionExecutors.put(condition,
                            ConditionProgramCompiler.compile(condition, primitiveNonNull));
                }
                var joins = query.joins();
                if (joins == null) {
                    continue;
                }
                for (var join : joins) {
                    var predicates = join.predicates();
                    if (predicates == null) {
                        continue;
                    }
                    for (var predicate : predicates) {
                        var compiled = CompiledQuery.CompiledCondition.of(predicate.columnIndex(),
                                predicate.typeCode(),
                                predicate.operator(),
                                predicate.argumentIndex(),
                                predicate.ignoreCase());
                        joinDirectConditionExecutors.put(predicate, ConditionProgramCompiler.compile(compiled, false));
                    }
                }
            }
        }
    }

    public static ConditionExecutor[][] buildConditionExecutors(CompiledQuery[] queries, String[] columnNames,
            boolean[] primitiveNonNullColumns, Class<?> entityClass, boolean useIndex) {
        var executors = new ConditionExecutor[queries.length][];
        for (var i = 0; i < queries.length; i++) {
            var conditions = queries[i].conditions();
            if (conditions == null || conditions.length == 0) {
                executors[i] = new ConditionExecutor[0];
                continue;
            }
            var queryExecutors = new ConditionExecutor[conditions.length];
            for (var j = 0; j < conditions.length; j++) {
                queryExecutors[j] = buildConditionExecutor(conditions[j], columnNames, primitiveNonNullColumns,
                        entityClass, useIndex);
            }
            executors[i] = queryExecutors;
        }
        return executors;
    }

    public static OrderExecutor[] buildOrderExecutors(CompiledQuery[] queries, GeneratedTable table,
            boolean[] primitiveNonNullColumns) {
        var executors = new OrderExecutor[queries.length];
        for (var i = 0; i < queries.length; i++) {
            executors[i] = buildOrderExecutor(queries[i], table, primitiveNonNullColumns);
        }
        return executors;
    }

    public static ProjectionExecutor[] buildProjectionExecutors(CompiledQuery[] queries) {
        return buildProjectionExecutors(queries, io.memris.core.MetadataExtractor::extractEntityMetadata);
    }

    public static ProjectionExecutor[] buildProjectionExecutors(CompiledQuery[] queries,
            io.memris.core.EntityMetadataProvider metadataProvider) {
        var executors = new ProjectionExecutor[queries.length];
        for (var i = 0; i < queries.length; i++) {
            executors[i] = buildProjectionExecutor(queries[i], metadataProvider);
        }
        return executors;
    }

    private static ConditionExecutor buildConditionExecutor(CompiledQuery.CompiledCondition condition,
            String[] columnNames,
            boolean[] primitiveNonNullColumns,
            Class<?> entityClass,
            boolean useIndex) {
        var primitiveNonNull = isPrimitiveNonNullColumn(primitiveNonNullColumns, condition.columnIndex());
        var directExecutor = ConditionProgramCompiler.compile(condition, primitiveNonNull);
        var fallbackSelector = (ConditionExecutor.Selector) (runtime, args) -> directExecutor
                .execute(runtime.plan().table(), runtime.plan().kernel(), args);
        if (!useIndex || condition.ignoreCase()) {
            return new ConditionExecutor(condition.nextCombinator(), fallbackSelector);
        }
        var fieldName = (condition.columnIndex() >= 0 && condition.columnIndex() < columnNames.length)
                ? columnNames[condition.columnIndex()]
                : null;
        if (fieldName == null) {
            return new ConditionExecutor(condition.nextCombinator(), fallbackSelector);
        }
        var operator = condition.operator();
        return switch (operator) {
            case IN -> new ConditionExecutor(condition.nextCombinator(), (runtime, args) -> {
                if (condition.argumentIndex() < 0 || condition.argumentIndex() >= args.length) {
                    return fallbackSelector.execute(runtime, args);
                }
                Selection selection = InIndexSelector.select(
                        args[condition.argumentIndex()],
                        value -> runtime.queryIndex(entityClass, fieldName, Predicate.Operator.EQ, value),
                        runtime::selectionFromRows);
                return selection != null ? selection : fallbackSelector.execute(runtime, args);
            });
            case BETWEEN -> {
                var predicateOperator = operator.toPredicateOperator();
                yield new ConditionExecutor(condition.nextCombinator(), (runtime, args) -> {
                    if (condition.argumentIndex() + 1 >= args.length) {
                        return fallbackSelector.execute(runtime, args);
                    }
                    Object[] range = new Object[] { args[condition.argumentIndex()],
                            args[condition.argumentIndex() + 1] };
                    int[] rows = runtime.queryIndex(entityClass, fieldName, predicateOperator, range);
                    return rows != null ? runtime.selectionFromRows(rows) : fallbackSelector.execute(runtime, args);
                });
            }
            case EQ, GT, GTE, LT, LTE -> {
                var predicateOperator = operator.toPredicateOperator();
                yield new ConditionExecutor(condition.nextCombinator(), (runtime, args) -> {
                    if (condition.argumentIndex() < 0 || condition.argumentIndex() >= args.length) {
                        return fallbackSelector.execute(runtime, args);
                    }
                    var value = args[condition.argumentIndex()];
                    int[] rows = runtime.queryIndex(entityClass, fieldName, predicateOperator, value);
                    return rows != null ? runtime.selectionFromRows(rows) : fallbackSelector.execute(runtime, args);
                });
            }
            default -> new ConditionExecutor(condition.nextCombinator(), fallbackSelector);
        };
    }

    private static boolean isPrimitiveNonNullColumn(boolean[] primitiveNonNullColumns, int columnIndex) {
        return primitiveNonNullColumns != null
                && columnIndex >= 0
                && columnIndex < primitiveNonNullColumns.length
                && primitiveNonNullColumns[columnIndex];
    }

    private static boolean hasCompositeIndexPlans(IndexPlan[] plans) {
        if (plans == null) {
            return false;
        }
        for (var plan : plans) {
            if (plan != null && plan.columnPositions().length > 1) {
                return true;
            }
        }
        return false;
    }

    private static CompositeIndexSelector.CompositeIndexPlan[] toCompositeIndexPlans(IndexPlan[] plans) {
        if (plans == null || plans.length == 0) {
            return new CompositeIndexSelector.CompositeIndexPlan[0];
        }
        var count = 0;
        for (var plan : plans) {
            if (plan != null && plan.columnPositions().length > 1) {
                count++;
            }
        }
        if (count == 0) {
            return new CompositeIndexSelector.CompositeIndexPlan[0];
        }
        var compositePlans = new CompositeIndexSelector.CompositeIndexPlan[count];
        var index = 0;
        for (var plan : plans) {
            if (plan == null || plan.columnPositions().length <= 1) {
                continue;
            }
            compositePlans[index++] = new CompositeIndexSelector.CompositeIndexPlan(plan.indexName(),
                    plan.columnPositions());
        }
        return compositePlans;
    }

    private Map<CompiledQuery, CompositeIndexProbeCompiler.CompositeIndexProbe> buildCompositeIndexProbesByQuery(
            CompiledQuery[] queries) {
        if (queries == null || queries.length == 0 || compositeIndexPlans.length == 0 || arena == null) {
            return Map.of();
        }
        var indexes = entityIndexes();
        if (indexes == null || indexes.isEmpty()) {
            return Map.of();
        }

        var compiled = new HashMap<CompiledQuery, CompositeIndexProbeCompiler.CompositeIndexProbe>(queries.length * 2);
        for (var query : queries) {
            var conditions = query.conditions();
            if (conditions == null || conditions.length == 0) {
                continue;
            }
            var probe = CompositeIndexProbeCompiler.compile(compositeIndexPlans,
                    indexes,
                    conditions,
                    this::queryIndexFromObject,
                    this::selectionFromRows);
            compiled.put(query, probe);
        }
        return compiled.isEmpty() ? Map.of() : Map.copyOf(compiled);
    }

    private Map<CompiledQuery, CompiledConditionProgram> buildCompiledConditionPrograms(CompiledQuery[] queries) {
        if (queries == null || queries.length == 0 || metadata == null) {
            return Map.of();
        }

        var compiled = new HashMap<CompiledQuery, CompiledConditionProgram>(queries.length * 2);
        for (var query : queries) {
            var conditions = query.conditions();
            if (conditions == null || conditions.length == 0) {
                continue;
            }
            var groups = splitConditionGroups(conditions);
            var compiledGroups = new ArrayList<CompiledConditionGroup>(groups.size());
            var compilable = true;

            for (var group : groups) {
                var leadConditionIndex = findIndexedLeadConditionIndex(conditions, group.start(), group.end());
                if (leadConditionIndex < 0) {
                    compilable = false;
                    break;
                }
                var leadCondition = conditions[leadConditionIndex];
                var leadSelectionExecutor = compileLeadSelectionExecutor(leadCondition);
                if (leadSelectionExecutor == null) {
                    compilable = false;
                    break;
                }
                var residualMatchers = new ArrayList<ConditionRowEvaluatorGenerator.RowConditionEvaluator>(
                        Math.max(0, (group.end() - group.start()) - 1));

                for (var i = group.start(); i <= group.end(); i++) {
                    if (i == leadConditionIndex) {
                        continue;
                    }
                    var condition = conditions[i];
                    var matcher = ConditionRowEvaluatorGenerator.generate(
                            condition,
                            isPrimitiveNonNullColumn(condition.columnIndex()));
                    if (matcher == null) {
                        compilable = false;
                        break;
                    }
                    residualMatchers.add(matcher);
                }
                if (!compilable) {
                    break;
                }
                compiledGroups.add(new CompiledConditionGroup(
                        leadSelectionExecutor,
                        residualMatchers.toArray(new ConditionRowEvaluatorGenerator.RowConditionEvaluator[0])));
            }

            if (!compilable || compiledGroups.isEmpty()) {
                continue;
            }

            compiled.put(query, new CompiledConditionProgram(compiledGroups.toArray(new CompiledConditionGroup[0])));
        }

        return compiled.isEmpty() ? Map.of() : Map.copyOf(compiled);
    }

    private LeadSelectionExecutor compileLeadSelectionExecutor(CompiledQuery.CompiledCondition condition) {
        if (condition == null || metadata == null || condition.ignoreCase()) {
            return null;
        }
        var fieldName = resolveFieldName(condition.columnIndex());
        if (fieldName == null) {
            return null;
        }
        var argumentIndex = condition.argumentIndex();
        var operator = condition.operator();

        return switch (operator) {
            case IN -> args -> {
                if (argumentIndex < 0 || argumentIndex >= args.length) {
                    return null;
                }
                return selectWithIndexForIn(fieldName, args[argumentIndex]);
            };
            case BETWEEN -> args -> {
                if (argumentIndex + 1 >= args.length) {
                    return null;
                }
                var range = new Object[] { args[argumentIndex], args[argumentIndex + 1] };
                var rows = queryIndex(metadata.entityClass(), fieldName, Predicate.Operator.BETWEEN, range);
                return rows != null ? selectionFromRows(rows) : null;
            };
            case EQ, GT, GTE, LT, LTE, STARTING_WITH, ENDING_WITH -> {
                var predicateOperator = operator.toPredicateOperator();
                yield args -> {
                    if (argumentIndex < 0 || argumentIndex >= args.length) {
                        return null;
                    }
                    var rows = queryIndex(metadata.entityClass(), fieldName, predicateOperator,
                            args[argumentIndex]);
                    return rows != null ? selectionFromRows(rows) : null;
                };
            }
            default -> null;
        };
    }

    private List<ConditionGroupRange> splitConditionGroups(CompiledQuery.CompiledCondition[] conditions) {
        var groups = new ArrayList<ConditionGroupRange>();
        var start = 0;
        for (var i = 0; i < conditions.length; i++) {
            if (i == conditions.length - 1 || conditions[i].nextCombinator() == LogicalQuery.Combinator.OR) {
                groups.add(new ConditionGroupRange(start, i));
                start = i + 1;
            }
        }
        return groups;
    }

    private int findIndexedLeadConditionIndex(CompiledQuery.CompiledCondition[] conditions, int start, int end) {
        for (var i = start; i <= end; i++) {
            if (isIndexAddressable(conditions[i])) {
                return i;
            }
        }
        return -1;
    }

    private boolean isIndexAddressable(CompiledQuery.CompiledCondition condition) {
        if (condition.ignoreCase()) {
            return false;
        }
        var fieldName = resolveFieldName(condition.columnIndex());
        if (fieldName == null) {
            return false;
        }
        var indexPlan = indexPlansByField.get(fieldName);
        if (indexPlan == null || indexPlan.indexType() == null) {
            return false;
        }
        return supportsOperator(indexPlan.indexType(), condition.operator());
    }

    private static boolean supportsOperator(io.memris.core.Index.IndexType indexType, LogicalQuery.Operator operator) {
        return switch (indexType) {
            case HASH -> operator == LogicalQuery.Operator.EQ || operator == LogicalQuery.Operator.IN;
            case BTREE -> switch (operator) {
                case EQ, IN, GT, GTE, LT, LTE, BETWEEN, BEFORE, AFTER -> true;
                default -> false;
            };
            case PREFIX -> operator == LogicalQuery.Operator.STARTING_WITH;
            case SUFFIX -> operator == LogicalQuery.Operator.ENDING_WITH;
        };
    }

    private static OrderExecutor buildOrderExecutor(CompiledQuery query, GeneratedTable table,
            boolean[] primitiveNonNullColumns) {
        CompiledQuery.CompiledOrderBy[] orderBy = query.orderBy();
        if (orderBy == null || orderBy.length == 0) {
            return (runtime, rows) -> rows;
        }
        var limit = query.limit();
        if (orderBy.length == 1) {
            var single = orderBy[0];
            var typeCode = table.typeCodeAt(single.columnIndex());
            var primitiveNonNull = isPrimitiveNonNullColumn(primitiveNonNullColumns, single.columnIndex());
            return SingleOrderProgramCompiler.compile(single, limit, typeCode, primitiveNonNull);
        }

        var builders = MultiColumnOrderCompiler.compileBuilders(orderBy, table, primitiveNonNullColumns);
        return (runtime, rows) -> MultiColumnOrderCompiler.sortByCompiledBuilders(runtime.plan().table(), rows,
                builders);
    }

    private static ProjectionExecutor buildProjectionExecutor(CompiledQuery query,
            io.memris.core.EntityMetadataProvider metadataProvider) {
        var projection = query.projection();
        if (projection == null) {
            return null;
        }
        var items = projection.items();
        var itemExecutors = new ProjectionItemExecutor[items.length];
        for (var i = 0; i < items.length; i++) {
            itemExecutors[i] = buildProjectionItemExecutor(items[i], metadataProvider);
        }
        return new ProjectionExecutorImpl(projection, itemExecutors);
    }

    private static ProjectionItemExecutor buildProjectionItemExecutor(CompiledQuery.CompiledProjectionItem item,
            io.memris.core.EntityMetadataProvider metadataProvider) {
        var steps = new ProjectionStepExecutor[item.steps().length];
        for (var i = 0; i < item.steps().length; i++) {
            steps[i] = buildProjectionStepExecutor(item.steps()[i]);
        }
        EntityMetadata<?> fieldMetadata = metadataProvider.getMetadata(item.fieldEntity());
        TypeConverter<?, ?> converter = TypeConverterRegistry.getInstance()
                .getFieldConverter(item.fieldEntity(), item.fieldName());
        if (converter == null) {
            converter = fieldMetadata.converters().get(item.fieldName());
        }
        var reader = buildFieldValueReader(item.columnIndex(), item.typeCode(), converter);
        return new ProjectionItemExecutor(item.fieldEntity(), steps, reader);
    }

    private static ProjectionStepExecutor buildProjectionStepExecutor(CompiledQuery.CompiledProjectionStep step) {
        var fkReader = buildFkReader(step.fkTypeCode(), step.sourceColumnIndex());
        var resolver = buildTargetRowResolver(step);
        return new ProjectionStepExecutor(step.sourceEntity(), step.targetEntity(), fkReader, resolver);
    }

    private static FkReader buildFkReader(byte typeCode, int columnIndex) {
        return RuntimeExecutorGenerator.generateFkReader(columnIndex, typeCode)::read;
    }

    private static TargetRowResolver buildTargetRowResolver(CompiledQuery.CompiledProjectionStep step) {
        return RuntimeExecutorGenerator.generateTargetRowResolver(
                step.targetColumnIsId(), step.fkTypeCode(), step.targetColumnIndex())::resolve;
    }

    private static FieldValueReader buildFieldValueReader(int columnIndex, byte typeCode,
            TypeConverter<?, ?> converter) {
        return RuntimeExecutorGenerator.generateFieldValueReader(columnIndex, typeCode, converter)::read;
    }

    private static final class ProjectionExecutorImpl implements ProjectionExecutor {
        private final CompiledQuery.CompiledProjection projection;
        private final ProjectionItemExecutor[] itemExecutors;

        private ProjectionExecutorImpl(CompiledQuery.CompiledProjection projection,
                ProjectionItemExecutor[] itemExecutors) {
            this.projection = projection;
            this.itemExecutors = itemExecutors;
        }

        @Override
        public Object materialize(RepositoryRuntime<?> runtime, int rowIndex) {
            Object[] args = new Object[itemExecutors.length];
            for (int i = 0; i < itemExecutors.length; i++) {
                args[i] = itemExecutors[i].resolve(runtime, rowIndex);
            }
            MethodHandle ctor = runtime.projectionConstructor(projection.projectionType());
            try {
                return ctor.invokeWithArguments(args);
            } catch (Throwable e) {
                throw new RuntimeException("Failed to materialize projection", e);
            }
        }
    }

    private static final class SortBuffers {
        private int[] intKeys;
        private long[] longKeys;
        private float[] floatKeys;
        private double[] doubleKeys;
        private String[] stringKeys;
        private boolean[] present;

        private int[] intKeys(int size) {
            if (intKeys == null || intKeys.length < size) {
                intKeys = new int[size];
            }
            return intKeys;
        }

        private long[] longKeys(int size) {
            if (longKeys == null || longKeys.length < size) {
                longKeys = new long[size];
            }
            return longKeys;
        }

        private float[] floatKeys(int size) {
            if (floatKeys == null || floatKeys.length < size) {
                floatKeys = new float[size];
            }
            return floatKeys;
        }

        private double[] doubleKeys(int size) {
            if (doubleKeys == null || doubleKeys.length < size) {
                doubleKeys = new double[size];
            }
            return doubleKeys;
        }

        private String[] stringKeys(int size) {
            if (stringKeys == null || stringKeys.length < size) {
                stringKeys = new String[size];
            }
            return stringKeys;
        }

        private boolean[] present(int size) {
            if (present == null || present.length < size) {
                present = new boolean[size];
            }
            return present;
        }
    }

    private interface FieldValueReader {
        Object read(GeneratedTable table, int rowIndex);
    }

    private interface FkReader {
        Object read(GeneratedTable table, int rowIndex);
    }

    private interface TargetRowResolver {
        int resolve(GeneratedTable table, Object fkValue);
    }

    private static final class ProjectionStepExecutor {
        private final Class<?> sourceEntity;
        private final Class<?> targetEntity;
        private final FkReader fkReader;
        private final TargetRowResolver resolver;

        private ProjectionStepExecutor(Class<?> sourceEntity, Class<?> targetEntity, FkReader fkReader,
                TargetRowResolver resolver) {
            this.sourceEntity = sourceEntity;
            this.targetEntity = targetEntity;
            this.fkReader = fkReader;
            this.resolver = resolver;
        }
    }

    private static final class ProjectionItemExecutor {
        private final Class<?> fieldEntity;
        private final ProjectionStepExecutor[] steps;
        private final FieldValueReader reader;

        private ProjectionItemExecutor(Class<?> fieldEntity, ProjectionStepExecutor[] steps, FieldValueReader reader) {
            this.fieldEntity = fieldEntity;
            this.steps = steps;
            this.reader = reader;
        }

        private Object resolve(RepositoryRuntime<?> runtime, int rootRowIndex) {
            int currentRow = rootRowIndex;
            for (ProjectionStepExecutor step : steps) {
                GeneratedTable sourceTable = runtime.tableFor(step.sourceEntity);
                if (sourceTable == null || currentRow < 0) {
                    return null;
                }
                Object fkValue = step.fkReader.read(sourceTable, currentRow);
                if (fkValue == null) {
                    return null;
                }
                GeneratedTable targetTable = runtime.tableFor(step.targetEntity);
                if (targetTable == null) {
                    return null;
                }
                currentRow = step.resolver.resolve(targetTable, fkValue);
                if (currentRow < 0) {
                    return null;
                }
            }
            GeneratedTable fieldTable = runtime.tableFor(fieldEntity);
            if (fieldTable == null || currentRow < 0) {
                return null;
            }
            return reader.read(fieldTable, currentRow);
        }
    }

    public T saveOne(T entity) {
        return executeSaveOne(new Object[] { entity });
    }

    public List<T> saveAll(Iterable<T> entities) {
        return executeSaveAll(new Object[] { entities });
    }

    public Optional<T> findById(Object id) {
        return executeFindById(new Object[] { id });
    }

    public List<T> findAll() {
        return executeFindAll();
    }

    public Object find(CompiledQuery query, Object[] args) {
        return executeFind(query, args);
    }

    public long countFast(CompiledQuery query, Object[] args) {
        return executeCountFast(query, args);
    }

    public long countAll() {
        return executeCountAll();
    }

    public boolean existsFast(CompiledQuery query, Object[] args) {
        return executeExistsFast(query, args);
    }

    public boolean existsById(Object id) {
        return executeExistsById(new Object[] { id });
    }

    public void deleteOne(Object entity) {
        executeDeleteOne(new Object[] { entity });
    }

    public void deleteAll() {
        executeDeleteAll();
    }

    public void deleteById(Object id) {
        executeDeleteById(new Object[] { id });
    }

    public Object deleteQuery(CompiledQuery query, Object[] args) {
        return formatModifyingResult(executeDeleteQuery(query, args), query.returnKind());
    }

    public Object updateQuery(CompiledQuery query, Object[] args) {
        return formatModifyingResult(executeUpdateQuery(query, args), query.returnKind());
    }

    public long deleteAllById(Iterable<?> ids) {
        return executeDeleteAllById(new Object[] { ids });
    }

    private static Map<Class<?>, EntityMetadata<?>> buildRelatedMetadata(EntityMetadata<?> metadata,
            EntityMetadataProvider metadataProvider) {
        if (metadata == null) {
            return Map.of();
        }
        var related = new HashMap<Class<?>, EntityMetadata<?>>();
        for (var field : metadata.fields()) {
            if (field.isRelationship() && field.targetEntity() != null) {
                related.putIfAbsent(field.targetEntity(), metadataProvider.getMetadata(field.targetEntity()));
            }
        }
        return Map.copyOf(related);
    }

    private Map<Class<?>, EntityRuntimeLayout> buildRuntimeLayouts(EntityMetadata<T> rootMetadata,
            Map<Class<?>, EntityMetadata<?>> related) {
        var layouts = new HashMap<Class<?>, EntityRuntimeLayout>();
        if (rootMetadata != null) {
            layouts.put(rootMetadata.entityClass(), buildRuntimeLayout(rootMetadata));
        }
        for (var relatedMetadata : related.values()) {
            layouts.putIfAbsent(relatedMetadata.entityClass(), buildRuntimeLayout(relatedMetadata));
        }
        for (var entityClass : plan.tablesByEntity().keySet()) {
            if (!layouts.containsKey(entityClass)) {
                var entityMetadata = resolveEntityMetadata(entityClass);
                if (entityMetadata != null) {
                    layouts.put(entityClass, buildRuntimeLayout(entityMetadata));
                }
            }
        }
        return Map.copyOf(layouts);
    }

    private EntityRuntimeLayout buildRuntimeLayout(EntityMetadata<?> entityMetadata) {
        var maxColumnPos = entityMetadata.fields().stream()
                .filter(field -> field.columnPosition() >= 0)
                .mapToInt(FieldMapping::columnPosition)
                .max()
                .orElse(-1);
        if (maxColumnPos < 0) {
            return new EntityRuntimeLayout(
                    new FieldMapping[0],
                    new TypeConverter<?, ?>[0],
                    new ColumnRuntimeDescriptor[0],
                    -1);
        }
        var fieldsByColumn = new FieldMapping[maxColumnPos + 1];
        var convertersByColumn = new TypeConverter<?, ?>[maxColumnPos + 1];
        var descriptorsByColumn = new ColumnRuntimeDescriptor[maxColumnPos + 1];
        var plansByColumn = entityMetadata.columnAccessPlansByColumn();
        for (var field : entityMetadata.fields()) {
            if (field.columnPosition() < 0) {
                continue;
            }
            fieldsByColumn[field.columnPosition()] = field;
            var converter = resolveConverterAtSetup(entityMetadata, field.name());
            convertersByColumn[field.columnPosition()] = converter;
            ColumnAccessPlan plan = field.columnPosition() < plansByColumn.length
                    ? plansByColumn[field.columnPosition()]
                    : null;
            if (plan == null) {
                plan = entityMetadata.columnAccessPlan(field.name());
            }
            descriptorsByColumn[field.columnPosition()] = new ColumnRuntimeDescriptor(field, converter, plan);
        }
        var resolvedIdColumnIndex = entityMetadata.resolveColumnPosition(entityMetadata.idColumnName());
        return new EntityRuntimeLayout(fieldsByColumn, convertersByColumn, descriptorsByColumn, resolvedIdColumnIndex);
    }

    private TypeConverter<?, ?> resolveConverterAtSetup(EntityMetadata<?> entityMetadata, String fieldName) {
        var fieldConverter = TypeConverterRegistry.getInstance()
                .getFieldConverter(entityMetadata.entityClass(), fieldName);
        if (fieldConverter != null) {
            return fieldConverter;
        }
        return entityMetadata.converters().get(fieldName);
    }

    private boolean[] buildPrimitiveNonNullByColumn(EntityRuntimeLayout rootLayout) {
        if (rootLayout == null) {
            return new boolean[0];
        }
        var fields = rootLayout.fieldsByColumn();
        var flags = new boolean[fields.length];
        for (var i = 0; i < fields.length; i++) {
            var field = fields[i];
            if (field != null && field.javaType().isPrimitive()) {
                flags[i] = true;
            }
        }
        return flags;
    }

    private boolean isPrimitiveNonNullColumn(int columnIndex) {
        return columnIndex >= 0
                && columnIndex < primitiveNonNullByColumn.length
                && primitiveNonNullByColumn[columnIndex];
    }

    private EntityMetadata<?> resolveEntityMetadata(Class<?> entityClass) {
        if (metadata != null && metadata.entityClass().equals(entityClass)) {
            return metadata;
        }
        var related = relatedMetadata.get(entityClass);
        if (related != null) {
            return related;
        }
        var existingProjectionMetadata = projectionMetadata.get(entityClass);
        if (existingProjectionMetadata != null) {
            return existingProjectionMetadata;
        }
        var loaded = metadataProvider.getMetadata(entityClass);
        if (loaded != null) {
            projectionMetadata.put(entityClass, loaded);
        }
        return loaded;
    }

    private OneToManyPlan[] buildOneToManyPlans() {
        if (metadata == null) {
            return new OneToManyPlan[0];
        }
        var plans = new ArrayList<OneToManyPlan>();
        for (var field : metadata.fields()) {
            if (!field.isRelationship()
                    || !field.isCollection()
                    || field.relationshipType() != FieldMapping.RelationshipType.ONE_TO_MANY
                    || field.targetEntity() == null) {
                continue;
            }
            var targetMetadata = resolveEntityMetadata(field.targetEntity());
            if (targetMetadata == null) {
                continue;
            }
            var targetTable = plan.tablesByEntity().get(field.targetEntity());
            var targetKernel = plan.kernelsByEntity().get(field.targetEntity());
            var targetMaterializer = plan.materializersByEntity().get(field.targetEntity());
            var setter = metadata.fieldSetters().get(field.name());
            if (targetTable == null || targetKernel == null || targetMaterializer == null || setter == null) {
                continue;
            }
            var fkColumnIndex = targetMetadata.resolveColumnPosition(field.columnName());
            plans.add(new OneToManyPlan(
                    field.name(),
                    field.javaType(),
                    field.typeCode(),
                    fkColumnIndex,
                    targetTable,
                    targetKernel,
                    targetMaterializer,
                    targetMetadata.postLoadHandle(),
                    setter));
        }
        return plans.toArray(new OneToManyPlan[0]);
    }

    private ManyToManyPlan[] buildManyToManyPlans() {
        if (metadata == null) {
            return new ManyToManyPlan[0];
        }
        var plans = new ArrayList<ManyToManyPlan>();
        for (var field : metadata.fields()) {
            if (!field.isRelationship()
                    || !field.isCollection()
                    || field.relationshipType() != FieldMapping.RelationshipType.MANY_TO_MANY
                    || field.targetEntity() == null) {
                continue;
            }
            var targetMetadata = resolveEntityMetadata(field.targetEntity());
            if (targetMetadata == null) {
                continue;
            }
            var targetIdField = findIdField(targetMetadata);
            var targetKernel = plan.kernelsByEntity().get(field.targetEntity());
            var targetMaterializer = plan.materializersByEntity().get(field.targetEntity());
            var setter = metadata.fieldSetters().get(field.name());
            var joinInfo = resolveJoinTable(field, metadata, targetMetadata);
            if (targetIdField == null
                    || targetKernel == null
                    || targetMaterializer == null
                    || setter == null
                    || joinInfo == null
                    || joinInfo.table() == null
                    || joinInfo.targetTable() == null) {
                continue;
            }
            var persistEnabled = field.joinTable() != null && !field.joinTable().isBlank();
            plans.add(new ManyToManyPlan(
                    field.name(),
                    field.javaType(),
                    field.typeCode(),
                    joinInfo,
                    targetMetadata,
                    targetIdField,
                    targetKernel,
                    targetMaterializer,
                    targetMetadata.postLoadHandle(),
                    setter,
                    persistEnabled));
        }
        return plans.toArray(new ManyToManyPlan[0]);
    }

    public Object executeMethodIndex(int methodIndex, Object[] args) {
        RepositoryMethodExecutor[] executors = plan.executors();
        if (executors == null) {
            throw new IllegalStateException("RepositoryPlan executors not configured");
        }
        if (methodIndex < 0 || methodIndex >= executors.length) {
            throw new UnsupportedOperationException("Method index out of range: " + methodIndex);
        }
        return executors[methodIndex].execute(this, args);
    }

    private T executeSaveOne(Object[] args) {
        var entity = (T) args[0];
        var table = plan.table();

        // Use EntitySaver for ID extraction and field access
        // Cast to raw type to work around wildcard capture issues
        @SuppressWarnings("rawtypes")
        EntitySaver rawSaver = entitySaver;
        var currentId = rawSaver != null ? rawSaver.extractId(entity) : null;
        var isNew = (currentId == null) || isZeroId(currentId);

        var id = currentId;
        if (isNew) {
            id = generateNextId();
            if (rawSaver != null) {
                rawSaver.setId(entity, id);
            }
            invokeLifecycle(metadata != null ? metadata.prePersistHandle() : null, entity);
            applyAuditFields(entity, true);
        } else {
            invokeLifecycle(metadata != null ? metadata.preUpdateHandle() : null, entity);
            applyAuditFields(entity, false);
        }

        if (!isNew) {
            var existingRef = resolvePackedRefById(table, id);
            if (existingRef >= 0) {
                var existingRowIndex = io.memris.storage.Selection.index(existingRef);
                updateIndexesOnDelete(existingRowIndex);
                table.tombstone(existingRef);
            }
        }

        // EntitySaver handles all field extraction, converters, and relationships
        @SuppressWarnings("unchecked")
        var savedEntity = rawSaver != null ? (T) rawSaver.save(entity, table, id) : entity;

        // Look up row index by ID for index updates
        var rowIndex = resolveRowIndexById(table, id);

        // Update indexes after save - read values back from table
        if (metadata != null && rowIndex >= 0) {
            updateIndexesOnInsert(readIndexValues(rowIndex), rowIndex);
        }

        persistManyToMany(entity);

        return savedEntity;
    }

    private Long generateNextId() {
        return idCounter.getAndIncrement();
    }

    private void applyPostLoad(T entity) {
        invokeLifecycle(metadata != null ? metadata.postLoadHandle() : null, entity);
    }

    private void invokeLifecycle(MethodHandle handle, Object entity) {
        if (handle == null || entity == null) {
            return;
        }
        try {
            handle.invoke(entity);
        } catch (Throwable e) {
            throw new RuntimeException("Failed to invoke lifecycle method", e);
        }
    }

    private void applyAuditFields(T entity, boolean isNew) {
        if (metadata == null) {
            return;
        }
        var auditFields = metadata.auditFields();
        if (auditFields.isEmpty()) {
            return;
        }
        Object currentUser = currentAuditUser();
        for (AuditField auditField : auditFields) {
            Object value = null;
            switch (auditField.type()) {
                case CREATED_DATE -> {
                    if (!isNew) {
                        continue;
                    }
                    if (hasValue(entity, auditField.name())) {
                        continue;
                    }
                    value = nowForType(auditField.javaType());
                }
                case LAST_MODIFIED_DATE -> value = nowForType(auditField.javaType());
                case CREATED_BY -> {
                    if (!isNew || currentUser == null) {
                        continue;
                    }
                    if (hasValue(entity, auditField.name())) {
                        continue;
                    }
                    value = coerceUser(currentUser, auditField.javaType());
                }
                case LAST_MODIFIED_BY -> {
                    if (currentUser == null) {
                        continue;
                    }
                    value = coerceUser(currentUser, auditField.javaType());
                }
                default -> throw new IllegalArgumentException("Unsupported audit field type: " + auditField.type());
            }
            if (value == null) {
                continue;
            }
            setFieldValue(entity, auditField.name(), value);
        }
    }

    private Object currentAuditUser() {
        if (factory == null) {
            return null;
        }
        MemrisConfiguration config = factory.getConfiguration();
        if (config == null || config.auditProvider() == null) {
            return null;
        }
        return config.auditProvider().currentUser();
    }

    MemrisRepositoryFactory indexFactory() {
        return factory;
    }

    private boolean hasValue(Object entity, String fieldName) {
        MethodHandle getter = metadata.fieldGetters().get(fieldName);
        if (getter == null) {
            return false;
        }
        try {
            return getter.invoke(entity) != null;
        } catch (Throwable e) {
            return false;
        }
    }

    private void setFieldValue(Object entity, String fieldName, Object value) {
        MethodHandle setter = metadata.fieldSetters().get(fieldName);
        if (setter == null) {
            return;
        }
        try {
            setter.invoke(entity, value);
        } catch (Throwable e) {
            throw new RuntimeException("Failed to set audit field: " + fieldName, e);
        }
    }

    private Object nowForType(Class<?> type) {
        if (type == java.time.Instant.class) {
            return java.time.Instant.now();
        }
        if (type == java.time.LocalDate.class) {
            return java.time.LocalDate.now();
        }
        if (type == java.time.LocalDateTime.class) {
            return java.time.LocalDateTime.now();
        }
        if (type == java.util.Date.class) {
            return new java.util.Date();
        }
        if (type == long.class || type == Long.class) {
            return System.currentTimeMillis();
        }
        return null;
    }

    private Object coerceUser(Object user, Class<?> targetType) {
        if (user == null) {
            return null;
        }
        if (targetType.isAssignableFrom(user.getClass())) {
            return user;
        }
        if (targetType == String.class) {
            return user.toString();
        }
        return null;
    }

    private boolean isZeroId(Object id) {
        if (id == null)
            return true;
        return switch (id) {
            case Long l -> l == 0L;
            case Integer i -> i == 0;
            case Short s -> s == 0;
            case Byte b -> b == 0;
            case java.util.UUID uuid -> uuid.getMostSignificantBits() == 0 && uuid.getLeastSignificantBits() == 0;
            default -> false;
        };
    }

    private List<T> executeSaveAll(Object[] args) {
        var entities = (Iterable<T>) args[0];
        var saved = new ArrayList<T>();
        for (var entity : entities) {
            saved.add(executeSaveOne(new Object[] { entity }));
        }
        return saved;
    }

    private Optional<T> executeFindById(Object[] args) {
        var id = args[0];
        var table = plan.table();

        // Direct call to table methods - zero virtual dispatch
        long packedRef = resolvePackedRefById(table, id);

        if (packedRef < 0) {
            return Optional.empty();
        }

        var rowIndex = io.memris.storage.Selection.index(packedRef);
        var entity = table.readWithSeqLock(rowIndex, () -> materializer.materialize(table, rowIndex));
        applyPostLoad(entity);
        hydrateCollections(entity, rowIndex);
        return Optional.of(entity);
    }

    private List<T> executeFindAll() {
        var table = plan.table();
        int[] rowIndices = table.scanAll();

        var results = new ArrayList<T>(rowIndices.length);
        for (var rowIndex : rowIndices) {
            var entity = table.readWithSeqLock(rowIndex, () -> materializer.materialize(table, rowIndex));
            applyPostLoad(entity);
            hydrateCollections(entity, rowIndex);
            results.add(entity);
        }
        return results;
    }

    private Object executeFind(CompiledQuery query, Object[] args) {
        var selection = executeConditions(query, args);

        int[] rows = selection.toIntArray();
        if (query.distinct()) {
            rows = distinctRows(rows);
        }

        var limit = query.limit();
        var orderExecutor = plan.orderExecutorFor(query);
        if (orderExecutor != null) {
            rows = orderExecutor.apply(this, rows);
        }

        var max = (limit > 0 && limit < rows.length) ? limit : rows.length;

        var projection = query.projection();
        var projectionExecutor = projectionExecutorFor(query);
        if (projection != null) {
            if (projectionExecutor == null) {
                throw new IllegalStateException("Projection executor not configured for query");
            }
            var returnSet = query.returnKind() == LogicalQuery.ReturnKind.MANY_SET;
            List<Object> results = returnSet ? null : new ArrayList<>(max);
            java.util.Set<Object> resultSet = returnSet ? new java.util.LinkedHashSet<>(Math.max(16, max)) : null;
            for (var i = 0; i < max; i++) {
                var rowIndex = rows[i];
                var value = plan.table().readWithSeqLock(rowIndex,
                        () -> projectionExecutor.materialize(this, rowIndex));
                if (returnSet) {
                    resultSet.add(value);
                } else {
                    results.add(value);
                }
            }
            return switch (query.returnKind()) {
                case ONE -> results.isEmpty() ? null : results.get(0);
                case ONE_OPTIONAL -> results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
                case MANY_LIST -> results;
                case MANY_SET -> resultSet;
                default -> throw new IllegalStateException("Unexpected return kind for FIND: " + query.returnKind());
            };
        }

        var returnSet2 = query.returnKind() == LogicalQuery.ReturnKind.MANY_SET;
        var returnMap = query.returnKind() == LogicalQuery.ReturnKind.MANY_MAP;

        if (returnMap) {
            return buildMapResult(query, rows, max, args);
        }

        List results2 = returnSet2 ? null : new ArrayList<>(max);
        java.util.Set resultSet2 = returnSet2 ? new java.util.LinkedHashSet<>(Math.max(16, max)) : null;
        java.util.Set<DistinctKey> distinctKeys = query.distinct()
                ? new java.util.HashSet<>(Math.max(16, max))
                : null;
        var table = plan.table();
        for (var i = 0; i < max; i++) {
            var rowIndex = rows[i];
            var entity = table.readWithSeqLock(rowIndex, () -> materializer.materialize(table, rowIndex));
            applyPostLoad(entity);
            hydrateJoins(entity, rowIndex, query);
            hydrateCollections(entity, rowIndex);
            if (distinctKeys != null && !distinctKeys.add(DistinctKey.from(entity))) {
                continue;
            }
            if (returnSet2) {
                resultSet2.add(entity);
            } else {
                results2.add(entity);
            }
        }

        return switch (query.returnKind()) {
            case ONE -> results2.isEmpty() ? null : (T) results2.get(0);
            case ONE_OPTIONAL -> results2.isEmpty() ? Optional.empty() : Optional.of((T) results2.get(0));
            case MANY_LIST -> (List<T>) results2;
            case MANY_SET -> (java.util.Set<T>) resultSet2;
            default -> throw new IllegalStateException("Unexpected return kind for FIND: " + query.returnKind());
        };
    }

    private Map<Object, ?> buildMapResult(CompiledQuery query, int[] rows, int max, Object[] args) {
        CompiledQuery.CompiledGrouping grouping = query.grouping();
        if (grouping == null) {
            throw new IllegalStateException("MANY_MAP return kind requires grouping configuration");
        }

        int[] groupColumnIndices = grouping.columnIndices();
        byte[] groupTypeCodes = grouping.typeCodes();

        Map<Object, ?> result = switch (grouping.valueType()) {
            case LIST -> buildGroupList(query, rows, max, groupColumnIndices, groupTypeCodes);
            case SET -> buildGroupSet(query, rows, max, groupColumnIndices, groupTypeCodes);
            case COUNT -> buildGroupCount(query, rows, max, groupColumnIndices, groupTypeCodes);
        };
        CompiledQuery.CompiledCondition[] havingConditions = query.havingConditions();
        if (havingConditions == null || havingConditions.length == 0) {
            return result;
        }
        return applyHavingConditions(query, result, havingConditions, args);
    }

    private Map<Object, ?> applyHavingConditions(CompiledQuery query, Map<Object, ?> result,
            CompiledQuery.CompiledCondition[] havingConditions, Object[] args) {
        CompiledQuery.CompiledGrouping grouping = query.grouping();
        if (grouping == null) {
            return result;
        }
        boolean isCountGrouping = grouping.valueType() == LogicalQuery.Grouping.GroupValueType.COUNT;
        Map<Object, Object> filtered = new java.util.LinkedHashMap<>();
        for (Map.Entry<Object, ?> entry : result.entrySet()) {
            Object value = entry.getValue();
            if (!isCountGrouping) {
                throw new UnsupportedOperationException("HAVING is only supported for COUNT groupings");
            }
            long countValue = value instanceof Number number ? number.longValue() : 0L;
            if (matchesHaving(countValue, havingConditions, args)) {
                filtered.put(entry.getKey(), value);
            }
        }
        return filtered;
    }

    private boolean matchesHaving(long countValue, CompiledQuery.CompiledCondition[] havingConditions, Object[] args) {
        boolean current = true;
        for (int i = 0; i < havingConditions.length; i++) {
            CompiledQuery.CompiledCondition condition = havingConditions[i];
            boolean match = evaluateHavingCondition(condition, countValue, args);
            if (i == 0) {
                current = match;
            } else {
                LogicalQuery.Combinator combinator = havingConditions[i - 1].nextCombinator();
                current = combinator == LogicalQuery.Combinator.OR ? (current || match) : (current && match);
            }
        }
        return current;
    }

    private boolean evaluateHavingCondition(CompiledQuery.CompiledCondition condition, long countValue, Object[] args) {
        if (args == null || args.length == 0) {
            throw new UnsupportedOperationException("HAVING requires parameters");
        }
        int rhsIndex = condition.argumentIndex();
        if (rhsIndex < 0 || rhsIndex >= args.length || !(args[rhsIndex] instanceof Number rhsNumber)) {
            throw new UnsupportedOperationException("HAVING supports numeric comparisons only");
        }
        long lhs = countValue;
        long rhs = rhsNumber.longValue();
        return switch (condition.operator()) {
            case EQ -> lhs == rhs;
            case NE -> lhs != rhs;
            case GT -> lhs > rhs;
            case GTE -> lhs >= rhs;
            case LT -> lhs < rhs;
            case LTE -> lhs <= rhs;
            default ->
                throw new UnsupportedOperationException("HAVING operator not supported: " + condition.operator());
        };
    }

    private Map<Object, List<T>> buildGroupList(CompiledQuery query, int[] rows, int max, int[] groupColumnIndices,
            byte[] groupTypeCodes) {
        Map<Object, List<T>> resultMap = new java.util.LinkedHashMap<>();
        var table = plan.table();
        for (int i = 0; i < max; i++) {
            int rowIndex = rows[i];
            T entity = table.readWithSeqLock(rowIndex, () -> materializer.materialize(table, rowIndex));
            applyPostLoad(entity);
            hydrateJoins(entity, rowIndex, query);
            hydrateCollections(entity, rowIndex);
            Object key = readGroupingKey(query, groupColumnIndices, groupTypeCodes, rowIndex);
            resultMap.computeIfAbsent(key, k -> new ArrayList<>()).add(entity);
        }
        return resultMap;
    }

    private Map<Object, java.util.Set<T>> buildGroupSet(CompiledQuery query, int[] rows, int max,
            int[] groupColumnIndices,
            byte[] groupTypeCodes) {
        Map<Object, java.util.Set<T>> resultMap = new java.util.LinkedHashMap<>();
        var table = plan.table();
        for (int i = 0; i < max; i++) {
            int rowIndex = rows[i];
            T entity = table.readWithSeqLock(rowIndex, () -> materializer.materialize(table, rowIndex));
            applyPostLoad(entity);
            hydrateJoins(entity, rowIndex, query);
            hydrateCollections(entity, rowIndex);
            Object key = readGroupingKey(query, groupColumnIndices, groupTypeCodes, rowIndex);
            resultMap.computeIfAbsent(key, k -> new java.util.LinkedHashSet<>()).add(entity);
        }
        return resultMap;
    }

    private Map<Object, Long> buildGroupCount(CompiledQuery query, int[] rows, int max, int[] groupColumnIndices,
            byte[] groupTypeCodes) {
        Map<Object, Long> resultMap = new java.util.LinkedHashMap<>();
        for (int i = 0; i < max; i++) {
            int rowIndex = rows[i];
            Object key = plan.table().readWithSeqLock(rowIndex,
                    () -> readGroupingKey(query, groupColumnIndices, groupTypeCodes, rowIndex));
            resultMap.merge(key, 1L, Long::sum);
        }
        return resultMap;
    }

    private Object readGroupingKey(CompiledQuery query, int[] columnIndices, byte[] typeCodes, int rowIndex) {
        if (columnIndices.length == 1) {
            return readGroupingValue(columnIndices[0], typeCodes[0], rowIndex);
        }
        CompiledQuery.CompiledGrouping grouping = query != null ? query.grouping() : null;
        if (grouping == null || grouping.keyConstructor() == null) {
            throw new IllegalStateException("Grouping key constructor required for multi-field grouping");
        }
        Object[] args = new Object[columnIndices.length];
        for (int i = 0; i < columnIndices.length; i++) {
            args[i] = readGroupingValue(columnIndices[i], typeCodes[i], rowIndex);
        }
        try {
            return grouping.keyConstructor().invokeWithArguments(args);
        } catch (Throwable e) {
            throw new RuntimeException("Failed to construct grouping key", e);
        }
    }

    private Object readGroupingValue(int columnIndex, byte typeCode, int rowIndex) {
        var table = plan.table();
        return table.readWithSeqLock(rowIndex,
                () -> RuntimeExecutorGenerator.generateGroupingValueReader(columnIndex, typeCode).read(table,
                        rowIndex));
    }

    private int[] distinctRows(int[] rows) {
        if (rows.length < 2) {
            return rows;
        }
        java.util.Arrays.sort(rows);
        int unique = 1;
        for (int i = 1; i < rows.length; i++) {
            if (rows[i] != rows[i - 1]) {
                rows[unique++] = rows[i];
            }
        }
        if (unique == rows.length) {
            return rows;
        }
        int[] result = new int[unique];
        System.arraycopy(rows, 0, result, 0, unique);
        return result;
    }

    private record DistinctKey(Class<?> type, Object value) {
        private static DistinctKey from(Object entity) {
            if (entity == null) {
                return new DistinctKey(null, null);
            }
            return new DistinctKey(entity.getClass(), entity);
        }
    }

    private long executeCount(CompiledQuery query, Object[] args) {
        Selection selection = executeConditions(query, args);
        if (!query.distinct()) {
            return selection.size();
        }
        int[] rows = selection.toIntArray();
        return distinctRows(rows).length;
    }

    private long executeCountAll() {
        return plan.table().liveCount();
    }

    /**
     * Fast EXISTS that short-circuits without building full selections.
     * <p>
     * For AND groups: if any condition returns empty, immediately return false.
     * For OR groups: if any condition returns non-empty, immediately return true.
     */
    private boolean executeExistsFast(CompiledQuery query, Object[] args) {
        CompiledQuery.CompiledCondition[] conditions = query.conditions();
        ConditionExecutor[] executors = conditionExecutorsFor(query);

        if (conditions.length == 0) {
            // No conditions - check if table has any rows
            return plan.table().liveCount() > 0;
        }

        if (hasCompositeIndexPlans) {
            return executeConditions(query, args).size() > 0;
        }

        // For single condition, just check if it matches anything
        if (conditions.length == 1) {
            Selection selection = executors != null && executors.length == 1
                    ? executors[0].execute(this, args)
                    : selectWithIndex(conditions[0], args);
            return selection.size() > 0;
        }

        // Process conditions with short-circuiting
        // Track rows matched by current AND group
        IntAccumulator currentGroup = null;

        for (int i = 0; i < conditions.length; i++) {
            CompiledQuery.CompiledCondition condition = conditions[i];
            Selection next = executors != null ? executors[i].execute(this, args) : selectWithIndex(condition, args);
            int nextSize = next.size();

            if (currentGroup == null) {
                // First condition in this group
                if (nextSize == 0) {
                    // Empty result at start of group - check if this is standalone or AND group
                    // If next combinator is AND, the whole group will be empty
                    // If next combinator is OR, we can continue to next group
                    if (condition.nextCombinator() == LogicalQuery.Combinator.AND) {
                        // This AND group will be empty, skip to next OR group
                        // Fast-forward past all AND conditions
                        while (i < conditions.length - 1
                                && conditions[i].nextCombinator() == LogicalQuery.Combinator.AND) {
                            i++;
                        }
                        currentGroup = null;
                        continue;
                    }
                    // This is a standalone OR condition with empty result - continue
                    currentGroup = null;
                    continue;
                }
                // Non-empty result, start tracking this group
                currentGroup = new IntAccumulator(next.toIntArray());
            } else {
                // Intersect with current AND group
                currentGroup.intersect(next.toIntArray());
                if (currentGroup.isEmpty()) {
                    // AND group became empty - if followed by more ANDs, skip them
                    if (condition.nextCombinator() == LogicalQuery.Combinator.AND) {
                        while (i < conditions.length - 1
                                && conditions[i].nextCombinator() == LogicalQuery.Combinator.AND) {
                            i++;
                        }
                    }
                    currentGroup = null;
                    continue;
                }
            }

            LogicalQuery.Combinator combinator = executors != null ? executors[i].nextCombinator()
                    : condition.nextCombinator();
            if (combinator == LogicalQuery.Combinator.OR) {
                // End of AND group - if we have matches, return true
                if (currentGroup != null && !currentGroup.isEmpty()) {
                    return true;
                }
                currentGroup = null;
            }
        }

        // Check final group
        return currentGroup != null && !currentGroup.isEmpty();
    }

    /**
     * Fast COUNT that counts matches without building full selection arrays.
     * Uses a counter-based approach while still handling AND/OR logic correctly.
     */
    private long executeCountFast(CompiledQuery query, Object[] args) {
        if (query.distinct()) {
            return executeCount(query, args);
        }
        if (hasCompositeIndexPlans) {
            return executeCount(query, args);
        }
        CompiledQuery.CompiledCondition[] conditions = query.conditions();
        ConditionExecutor[] executors = conditionExecutorsFor(query);

        if (conditions.length == 0) {
            // No conditions - count all rows
            return plan.table().liveCount();
        }

        // For single condition, just return its count
        if (conditions.length == 1) {
            Selection selection = executors != null && executors.length == 1
                    ? executors[0].execute(this, args)
                    : selectWithIndex(conditions[0], args);
            return selection.size();
        }

        // Need to build proper intersection/union for accurate count
        // But we use IntAccumulator which is more memory-efficient than Selection
        IntAccumulator combined = null;
        IntAccumulator currentGroup = null;

        for (int i = 0; i < conditions.length; i++) {
            CompiledQuery.CompiledCondition condition = conditions[i];
            Selection next = executors != null ? executors[i].execute(this, args) : selectWithIndex(condition, args);
            int[] nextRows = next.toIntArray();

            if (currentGroup == null) {
                currentGroup = new IntAccumulator(nextRows);
            } else {
                currentGroup.intersect(nextRows);
            }

            LogicalQuery.Combinator combinator = executors != null ? executors[i].nextCombinator()
                    : condition.nextCombinator();
            if (combinator == LogicalQuery.Combinator.OR) {
                // Union this group with combined result
                if (combined == null) {
                    combined = currentGroup;
                } else {
                    combined.union(currentGroup);
                }
                currentGroup = null;
            }
        }

        // Handle final group
        if (currentGroup != null) {
            if (combined == null) {
                combined = currentGroup;
            } else {
                combined.union(currentGroup);
            }
        }

        return combined != null ? combined.size() : 0;
    }

    /**
     * Memory-efficient accumulator for row indices.
     * Used by fast EXISTS and COUNT to avoid creating full Selection objects.
     */
    private static final class IntAccumulator {
        private int[] rows;
        private int size;
        private java.util.BitSet bitSet; // Used for efficient intersection/union

        IntAccumulator(int[] initialRows) {
            if (initialRows.length <= 64) {
                // Small sets: use sorted array
                this.rows = initialRows.clone();
                java.util.Arrays.sort(this.rows);
                this.size = this.rows.length;
            } else {
                // Large sets: use BitSet for efficient operations
                this.bitSet = new java.util.BitSet();
                for (int row : initialRows) {
                    this.bitSet.set(row);
                }
                this.size = initialRows.length;
            }
        }

        boolean isEmpty() {
            return size == 0;
        }

        int size() {
            return size;
        }

        void intersect(int[] other) {
            if (size == 0) {
                return;
            }

            if (bitSet != null) {
                // BitSet intersection
                java.util.BitSet otherBits = new java.util.BitSet();
                for (int row : other) {
                    otherBits.set(row);
                }
                bitSet.and(otherBits);
                size = bitSet.cardinality();
            } else if (other.length <= 64) {
                // Array intersection (both small)
                java.util.Arrays.sort(other);
                int newSize = 0;
                int i = 0;
                int j = 0;
                while (i < size && j < other.length) {
                    if (rows[i] == other[j]) {
                        rows[newSize++] = rows[i];
                        i++;
                        j++;
                    } else if (rows[i] < other[j]) {
                        i++;
                    } else {
                        j++;
                    }
                }
                size = newSize;
            } else {
                // Other is large, convert to BitSet
                java.util.BitSet otherBits = new java.util.BitSet();
                for (int row : other) {
                    otherBits.set(row);
                }
                bitSet = new java.util.BitSet();
                for (int i = 0; i < size; i++) {
                    if (otherBits.get(rows[i])) {
                        bitSet.set(rows[i]);
                    }
                }
                rows = null;
                size = bitSet.cardinality();
            }
        }

        void union(IntAccumulator other) {
            if (other.size == 0) {
                return;
            }
            if (size == 0) {
                if (other.bitSet != null) {
                    bitSet = (java.util.BitSet) other.bitSet.clone();
                } else {
                    rows = other.rows.clone();
                    size = other.size;
                }
                return;
            }

            // Convert both to BitSet for union
            if (bitSet == null) {
                bitSet = new java.util.BitSet();
                for (int i = 0; i < size; i++) {
                    bitSet.set(rows[i]);
                }
                rows = null;
            }

            if (other.bitSet != null) {
                bitSet.or(other.bitSet);
            } else {
                for (int i = 0; i < other.size; i++) {
                    bitSet.set(other.rows[i]);
                }
            }
            size = bitSet.cardinality();
        }

        @SuppressWarnings("unused")
        int[] toIntArray() {
            if (bitSet != null) {
                int[] result = new int[size];
                int idx = 0;
                for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
                    result[idx++] = i;
                }
                return result;
            }
            int[] result = new int[size];
            System.arraycopy(rows, 0, result, 0, size);
            return result;
        }
    }

    private boolean executeExistsById(Object[] args) {
        Object id = args[0];
        GeneratedTable table = plan.table();

        // Direct call to table methods - zero virtual dispatch
        long packedRef = resolvePackedRefById(table, id);

        return packedRef >= 0;
    }

    private void executeDeleteOne(Object[] args) {
        T entity = (T) args[0];
        if (entity == null) {
            throw new IllegalArgumentException("Cannot delete null entity");
        }

        Object id;
        try {
            MethodHandle idGetter = metadata.fieldGetters().get(metadata.idColumnName());
            if (idGetter == null) {
                throw new IllegalStateException("No ID getter found for field: " + metadata.idColumnName());
            }
            id = idGetter.invoke(entity);
        } catch (Throwable e) {
            throw new RuntimeException("Failed to extract ID from entity", e);
        }

        if (id == null) {
            throw new IllegalArgumentException("Cannot delete entity with null ID");
        }

        GeneratedTable table = plan.table();
        // Direct call to table methods - zero virtual dispatch
        long packedRef = resolvePackedRefById(table, id);

        if (packedRef >= 0) {
            int rowIndex = io.memris.storage.Selection.index(packedRef);
            updateIndexesOnDelete(rowIndex);
            table.tombstone(packedRef);
        }
    }

    private void executeDeleteAll() {
        GeneratedTable table = plan.table();
        if (metadata != null && factory != null) {
            factory.clearIndexes(metadata.entityClass());
        }
        int[] rowIndices = table.scanAll();

        for (int rowIndex : rowIndices) {
            long packedRef = io.memris.storage.Selection.pack(rowIndex, table.rowGeneration(rowIndex));
            table.tombstone(packedRef);
        }
    }

    private void executeDeleteById(Object[] args) {
        Object id = args[0];
        GeneratedTable table = plan.table();

        // Direct call to table methods - zero virtual dispatch
        long packedRef = resolvePackedRefById(table, id);

        if (packedRef >= 0) {
            int rowIndex = io.memris.storage.Selection.index(packedRef);
            updateIndexesOnDelete(rowIndex);
            table.tombstone(packedRef);
        }
    }

    private long executeDeleteAllById(Object[] args) {
        @SuppressWarnings("unchecked")
        Iterable<Object> ids = (Iterable<Object>) args[0];
        GeneratedTable table = plan.table();

        long count = 0;
        for (Object id : ids) {
            // Direct call to table methods - zero virtual dispatch
            long packedRef = resolvePackedRefById(table, id);

            if (packedRef >= 0) {
                int rowIndex = io.memris.storage.Selection.index(packedRef);
                updateIndexesOnDelete(rowIndex);
                table.tombstone(packedRef);
                count++;
            }
        }
        return count;
    }

    private long executeDeleteQuery(CompiledQuery query, Object[] args) {
        Selection selection = executeConditions(query, args);
        GeneratedTable table = plan.table();

        long count = 0;
        long[] refs = selection.toRefArray();
        for (long packedRef : refs) {
            int rowIndex = io.memris.storage.Selection.index(packedRef);
            updateIndexesOnDelete(rowIndex);
            table.tombstone(packedRef);
            count++;
        }
        return count;
    }

    private long executeUpdateQuery(CompiledQuery query, Object[] args) {
        CompiledQuery.CompiledUpdateAssignment[] updates = query.updateAssignments();
        if (updates == null || updates.length == 0) {
            throw new IllegalArgumentException("@Query update requires SET assignments");
        }
        validateUpdateAssignments(updates, args);

        Selection selection = executeConditions(query, args);
        GeneratedTable table = plan.table();

        long count = 0;
        long[] refs = selection.toRefArray();
        for (long packedRef : refs) {
            int rowIndex = io.memris.storage.Selection.index(packedRef);
            Object[] values = buildRowValues(table, rowIndex);
            applyUpdateAssignments(values, updates, args);

            Object idValue = idColumnIndex >= 0 && idColumnIndex < values.length ? values[idColumnIndex] : null;
            updateIndexesOnDelete(rowIndex);
            table.tombstone(packedRef);

            table.insertFrom(values);
            int newRowIndex = resolveRowIndexById(table, idValue);
            if (newRowIndex >= 0) {
                updateIndexesOnInsert(readIndexValues(newRowIndex), newRowIndex);
            }
            count++;
        }

        return count;
    }

    private Object formatModifyingResult(long count, LogicalQuery.ReturnKind returnKind) {
        return switch (returnKind) {
            case MODIFYING_VOID -> null;
            case MODIFYING_INT -> (int) count;
            case MODIFYING_LONG, COUNT_LONG -> count;
            default -> count;
        };
    }

    private Object[] buildRowValues(GeneratedTable table, int rowIndex) {
        int columnCount = table.columnCount();
        Object[] values = new Object[columnCount];
        for (int i = 0; i < columnCount; i++) {
            if (isPrimitiveNonNullColumn(i)) {
                values[i] = readStorageValue(table, i, rowIndex);
                continue;
            }
            if (!table.isPresent(i, rowIndex)) {
                values[i] = null;
                continue;
            }
            values[i] = readStorageValue(table, i, rowIndex);
        }
        return values;
    }

    private void applyUpdateAssignments(Object[] values, CompiledQuery.CompiledUpdateAssignment[] updates,
            Object[] args) {
        for (var update : updates) {
            var columnIndex = update.columnIndex();
            Object value = resolveUpdateAssignmentValue(update, args);
            if (isPrimitiveNonNullColumn(columnIndex) && value == null) {
                throw new IllegalArgumentException("Null assigned to primitive column index: " + columnIndex);
            }
            values[columnIndex] = value;
        }
    }

    private void validateUpdateAssignments(CompiledQuery.CompiledUpdateAssignment[] updates, Object[] args) {
        for (var update : updates) {
            var columnIndex = update.columnIndex();
            if (!isPrimitiveNonNullColumn(columnIndex)) {
                continue;
            }
            Object value = resolveUpdateAssignmentValue(update, args);
            if (value == null) {
                throw new IllegalArgumentException("Null assigned to primitive column index: " + columnIndex);
            }
        }
    }

    private Object resolveUpdateAssignmentValue(CompiledQuery.CompiledUpdateAssignment update, Object[] args) {
        var columnIndex = update.columnIndex();
        Object value = update.argumentIndex() < args.length ? args[update.argumentIndex()] : null;
        var descriptor = descriptorForColumn(columnIndex);
        if (descriptor != null) {
            var converter = descriptor.converter();
            if (converter != null) {
                @SuppressWarnings("unchecked")
                var typed = (TypeConverter<Object, Object>) converter;
                value = typed.toStorage(value);
            }
        }
        return value;
    }

    private Object readStorageValue(GeneratedTable table, int columnIndex, int rowIndex) {
        return storageReadersByColumn[columnIndex].read(table, rowIndex);
    }

    private ColumnRuntimeDescriptor descriptorForColumn(int columnIndex) {
        if (columnIndex < 0 || columnIndex >= columnDescriptorsByColumn.length) {
            return null;
        }
        return columnDescriptorsByColumn[columnIndex];
    }

    private int resolveRowIndexById(GeneratedTable table, Object idValue) {
        if (idValue == null) {
            return -1;
        }
        long packedRef = resolvePackedRefById(table, idValue);
        return packedRef < 0 ? -1 : io.memris.storage.Selection.index(packedRef);
    }

    private long resolvePackedRefById(GeneratedTable table, Object idValue) {
        if (idValue == null) {
            return -1;
        }
        return idLookup.lookup(table, idValue);
    }

    private Object[] readIndexValues(int rowIndex) {
        if (indexFieldReaders == null || indexFieldReaders.length == 0) {
            return new Object[0];
        }
        Object[] indexValues = new Object[columnDescriptorsByColumn.length];
        GeneratedTable table = plan.table();
        for (var reader : indexFieldReaders) {
            indexValues[reader.columnIndex] = reader.reader.read(table, rowIndex);
        }
        return indexValues;
    }

    private void updateIndexesOnInsert(Object[] indexValues, int rowIndex) {
        if (metadata == null) {
            return;
        }
        var entityIndexes = entityIndexes();
        if (entityIndexes == null || indexPlans.length == 0) {
            return;
        }
        var rowId = RowId.fromLong(rowIndex);
        for (var plan : indexPlans) {
            var index = entityIndexes.get(plan.indexName());
            if (index == null) {
                continue;
            }
            var key = buildKeyFromValues(plan.columnPositions(), indexValues);
            if (key == null) {
                continue;
            }
            addIndexEntry(index, key, rowId);
        }
    }

    private void updateIndexesOnDelete(int rowIndex) {
        if (metadata == null) {
            return;
        }
        var entityIndexes = entityIndexes();
        if (entityIndexes == null || indexPlans.length == 0) {
            return;
        }
        var values = readIndexValues(rowIndex);
        var rowId = RowId.fromLong(rowIndex);
        for (var plan : indexPlans) {
            var index = entityIndexes.get(plan.indexName());
            if (index == null) {
                continue;
            }
            var key = buildKeyFromValues(plan.columnPositions(), values);
            if (key == null) {
                continue;
            }
            removeIndexEntry(index, key, rowId);
        }
    }

    private IndexFieldReader[] buildIndexFieldReaders(EntityMetadata<?> entityMetadata, IndexPlan[] plans) {
        if (entityMetadata == null) {
            return new IndexFieldReader[0];
        }
        if (plans == null || plans.length == 0) {
            return new IndexFieldReader[0];
        }
        var indexedColumns = new HashSet<Integer>();
        for (var indexPlan : plans) {
            for (var columnPosition : indexPlan.columnPositions()) {
                indexedColumns.add(columnPosition);
            }
        }
        if (indexedColumns.isEmpty()) {
            return new IndexFieldReader[0];
        }
        var layout = runtimeLayoutsByEntity.get(entityMetadata.entityClass());
        var descriptors = layout != null ? layout.descriptorsByColumn() : new ColumnRuntimeDescriptor[0];
        var readers = new ArrayList<IndexFieldReader>(indexedColumns.size());
        for (var field : entityMetadata.fields()) {
            if (field.columnPosition() < 0 || !indexedColumns.contains(field.columnPosition())) {
                continue;
            }
            TypeConverter<?, ?> converter = field.columnPosition() < descriptors.length && descriptors[field.columnPosition()] != null
                    ? descriptors[field.columnPosition()].converter()
                    : null;
            IndexValueReader reader = buildIndexValueReader(field, converter);
            readers.add(new IndexFieldReader(field.columnPosition(), field.name(), reader));
        }
        return readers.toArray(new IndexFieldReader[0]);
    }

    private IndexValueReader buildIndexValueReader(FieldMapping field, TypeConverter<?, ?> converter) {
        var compiledReader = RuntimeExecutorGenerator.generateFieldValueReader(
                field.columnPosition(),
                field.typeCode(),
                converter);
        return compiledReader::read;
    }

    private static final class IndexFieldReader {
        private final int columnIndex;
        private final String fieldName;
        private final IndexValueReader reader;

        private IndexFieldReader(int columnIndex, String fieldName, IndexValueReader reader) {
            this.columnIndex = columnIndex;
            this.fieldName = fieldName;
            this.reader = reader;
        }
    }

    private interface IndexValueReader {
        Object read(GeneratedTable table, int rowIndex);
    }

    private IndexPlan[] buildIndexPlans(EntityMetadata<?> entityMetadata) {
        if (entityMetadata == null) {
            return new IndexPlan[0];
        }
        var definitions = entityMetadata.indexDefinitions();
        var plans = new ArrayList<IndexPlan>(definitions != null ? definitions.size() + 1 : 1);
        if (definitions != null) {
            for (var def : definitions) {
                if (def.columnPositions().length == 0) {
                    continue;
                }
                plans.add(new IndexPlan(def.name(), def.fieldNames(), def.columnPositions(), def.indexType()));
            }
        }

        var idFieldName = entityMetadata.idColumnName();
        var hasIdPlan = plans.stream().anyMatch(plan -> plan.fieldNames().length == 1
                && idFieldName.equals(plan.fieldNames()[0]));
        if (!hasIdPlan) {
            var idPosition = entityMetadata.resolveColumnPosition(idFieldName);
            plans.add(new IndexPlan(
                    idFieldName,
                    new String[] { idFieldName },
                    new int[] { idPosition },
                    io.memris.core.Index.IndexType.HASH));
        }

        return plans.toArray(new IndexPlan[0]);
    }

    private Map<String, IndexPlan> buildIndexPlansByField(IndexPlan[] plans) {
        if (plans.length == 0) {
            return Map.of();
        }
        var byField = new HashMap<String, IndexPlan>(plans.length * 2);
        for (var plan : plans) {
            if (plan.fieldNames().length == 1) {
                byField.putIfAbsent(plan.fieldNames()[0], plan);
            }
        }
        return Map.copyOf(byField);
    }

    private Map<String, Object> entityIndexes() {
        if (arena == null || metadata == null) {
            return null;
        }
        return arena.getIndexes(metadata.entityClass());
    }

    private Object buildKeyFromValues(int[] columnPositions, Object[] values) {
        if (columnPositions.length == 1) {
            var value = values[columnPositions[0]];
            return value;
        }
        var parts = new Object[columnPositions.length];
        for (var i = 0; i < columnPositions.length; i++) {
            var value = values[columnPositions[i]];
            if (value == null) {
                return null;
            }
            parts[i] = value;
        }
        return CompositeKey.of(parts);
    }

    private void addIndexEntry(Object index, Object key, RowId rowId) {
        switch (index) {
            case HashIndex hashIndex when key != null -> hashIndex.add(key, rowId);
            case RangeIndex rangeIndex when key instanceof Comparable comp -> rangeIndex.add(comp, rowId);
            case StringPrefixIndex prefixIndex when key instanceof String value -> prefixIndex.add(value, rowId);
            case StringSuffixIndex suffixIndex when key instanceof String value -> suffixIndex.add(value, rowId);
            case CompositeHashIndex hashIndex when key instanceof CompositeKey value -> hashIndex.add(value, rowId);
            case CompositeRangeIndex rangeIndex when key instanceof CompositeKey value -> rangeIndex.add(value, rowId);
            default -> {
            }
        }
    }

    private void removeIndexEntry(Object index, Object key, RowId rowId) {
        switch (index) {
            case HashIndex hashIndex when key != null -> hashIndex.remove(key, rowId);
            case RangeIndex rangeIndex when key instanceof Comparable comp -> rangeIndex.remove(comp, rowId);
            case StringPrefixIndex prefixIndex when key instanceof String value -> prefixIndex.remove(value, rowId);
            case StringSuffixIndex suffixIndex when key instanceof String value -> suffixIndex.remove(value, rowId);
            case CompositeHashIndex hashIndex when key instanceof CompositeKey value -> hashIndex.remove(value, rowId);
            case CompositeRangeIndex rangeIndex when key instanceof CompositeKey value ->
                rangeIndex.remove(value, rowId);
            default -> {
            }
        }
    }

    private Selection executeConditions(CompiledQuery query, Object[] args) {
        var conditions = query.conditions();
        if (conditions.length == 0) {
            var allRows = plan.table().scanAll();
            return applyJoins(query, args, selectionFromRows(allRows));
        }
        var compiledProgram = compiledConditionProgramsByQuery.get(query);
        if (compiledProgram != null) {
            var optimized = executeCompiledConditionProgram(compiledProgram, args);
            if (optimized != null) {
                return applyJoins(query, args, optimized);
            }
        }
        var executors = conditionExecutorsFor(query);
        var compositeProbe = compositeIndexProbesByQuery.get(query);
        var combined = ConditionSelectionOrchestrator.execute(conditions,
                executors,
                args,
                (groupConditions, start, end, runtimeArgs, consumed) -> selectWithCompositeIndex(compositeProbe,
                        groupConditions, start, end, runtimeArgs, consumed),
                (compiled, compiledExecutors, index, runtimeArgs) -> compiledExecutors != null
                        && index < compiledExecutors.length
                                ? compiledExecutors[index].execute(this, runtimeArgs)
                                : selectWithIndex(compiled[index], runtimeArgs),
                () -> selectionFromRows(plan.table().scanAll()));
        return applyJoins(query, args, combined);
    }

    private Selection executeCompiledConditionProgram(CompiledConditionProgram program, Object[] args) {
        Selection combined = null;
        for (var group : program.groups()) {
            var groupSelection = executeCompiledConditionGroup(group, args);
            if (groupSelection == null) {
                return null;
            }
            combined = combined == null ? groupSelection : combined.union(groupSelection);
        }
        return combined != null ? combined : selectionFromRows(new int[0]);
    }

    private Selection executeCompiledConditionGroup(CompiledConditionGroup group, Object[] args) {
        var leadingSelection = group.leadSelectionExecutor().select(args);
        if (leadingSelection == null) {
            return null;
        }

        var residualMatchers = group.residualMatchers();
        if (residualMatchers.length == 0) {
            return leadingSelection;
        }

        var candidateRows = leadingSelection.toIntArray();
        var matched = new int[candidateRows.length];
        var matchedCount = 0;

        for (var rowIndex : candidateRows) {
            var accepted = true;
            for (var matcher : residualMatchers) {
                // Matchers might read from the table, so we must protect against torn reads
                // using the seqlock, similar to other read paths.
                var isMatch = plan.table().readWithSeqLock(rowIndex,
                        () -> matcher.matches(plan.table(), rowIndex, args));

                if (!isMatch) {
                    accepted = false;
                    break;
                }
            }
            if (accepted) {
                matched[matchedCount++] = rowIndex;
            }
        }

        if (matchedCount == candidateRows.length) {
            return leadingSelection;
        }
        return selectionFromRows(Arrays.copyOf(matched, matchedCount));
    }

    private Selection selectWithCompositeIndex(CompositeIndexProbeCompiler.CompositeIndexProbe probe,
            CompiledQuery.CompiledCondition[] conditions,
            int start,
            int end,
            Object[] args,
            boolean[] consumed) {
        if (compositeIndexPlans.length == 0 || arena == null || probe == null) {
            return null;
        }
        var indexes = entityIndexes();
        if (indexes == null) {
            return null;
        }
        return probe.select(conditions, start, end, args, consumed);
    }

    private Selection selectWithIndex(CompiledQuery.CompiledCondition condition, Object[] args) {
        var fallbackExecutor = directExecutorFor(condition);
        if (metadata == null || factory == null || condition.ignoreCase()) {
            return fallbackExecutor.execute(plan.table(), plan.kernel(), args);
        }

        var fieldName = resolveFieldName(condition.columnIndex());
        if (fieldName == null) {
            return fallbackExecutor.execute(plan.table(), plan.kernel(), args);
        }

        var rows = IndexSelectionDispatcher.selectRows(condition,
                args,
                (operator, value) -> queryIndex(metadata.entityClass(),
                        fieldName,
                        operator.toPredicateOperator(),
                        value),
                value -> {
                    var selection = selectWithIndexForIn(fieldName, value);
                    return selection == null ? null : selection.toIntArray();
                });

        return rows != null
                ? selectionFromRows(rows)
                : fallbackExecutor.execute(plan.table(), plan.kernel(), args);
    }

    private DirectConditionExecutor directExecutorFor(CompiledQuery.CompiledCondition condition) {
        return directConditionExecutors.computeIfAbsent(condition,
                cc -> ConditionProgramCompiler.compile(cc, isPrimitiveNonNullColumn(cc.columnIndex())));
    }

    private DirectConditionExecutor joinDirectExecutorFor(CompiledQuery.CompiledJoinPredicate predicate) {
        return joinDirectConditionExecutors.computeIfAbsent(predicate, p -> {
            var compiled = CompiledQuery.CompiledCondition.of(p.columnIndex(),
                    p.typeCode(),
                    p.operator(),
                    p.argumentIndex(),
                    p.ignoreCase());
            return ConditionProgramCompiler.compile(compiled, false);
        });
    }

    /**
     * Query index from either arena (if available) or factory.
     */
    private int[] queryIndex(Class<?> entityClass, String fieldName, Predicate.Operator operator, Object value) {
        if (arena != null) {
            // Use arena-scoped indexes
            var entityIndexes = arena.getIndexes(entityClass);
            if (entityIndexes != null) {
                Object index = entityIndexes.get(fieldName);
                if (index != null) {
                    return queryIndexFromObject(index, operator, value);
                }
            }
        }
        // Fall back to factory indexes
        return factory.queryIndex(entityClass, fieldName, operator, value);
    }

    /**
     * Query a specific index object based on its type.
     */
    private int[] queryIndexFromObject(Object index, Predicate.Operator operator, Object value) {
        var probe = indexProbes.computeIfAbsent(index, IndexProbeCompiler::compile);
        return probe.query(operator, value, rowValidator);
    }

    private static java.util.function.Predicate<RowId> createRowValidator(GeneratedTable table) {
        return rowId -> {
            int rowIndex = (int) rowId.value();
            long generation = table.rowGeneration(rowIndex);
            long ref = Selection.pack(rowIndex, generation);
            return table.isLive(ref);
        };
    }

    private Selection selectWithIndexForIn(String fieldName, Object value) {
        return InIndexSelector.select(
                value,
                item -> queryIndex(metadata.entityClass(), fieldName, Predicate.Operator.EQ, item),
                this::selectionFromRows);
    }

    private String resolveFieldName(int columnIndex) {
        // Use precomputed column names array for O(1) lookup
        String[] columnNames = plan.columnNames();
        if (columnIndex >= 0 && columnIndex < columnNames.length) {
            return columnNames[columnIndex];
        }
        return null;
    }

    private ConditionExecutor[] conditionExecutorsFor(CompiledQuery query) {
        var executors = plan.conditionExecutorsFor(query);
        if (executors != null) {
            return executors;
        }
        var index = compiledQueryIndexByInstance.get(query);
        if (index != null
                && index >= 0
                && index < compiledConditionExecutorsByQuery.length) {
            return compiledConditionExecutorsByQuery[index];
        }
        return dynamicConditionExecutors.computeIfAbsent(query, q -> {
            var columnNames = plan.columnNames() != null ? plan.columnNames() : new String[0];
            var entityClass = metadata != null ? metadata.entityClass() : plan.entityClass();
            var useIndex = (factory != null || arena != null) && entityClass != null;
            return buildConditionExecutors(
                    new CompiledQuery[] { q },
                    columnNames,
                    primitiveNonNullByColumn,
                    entityClass != null ? entityClass : Object.class,
                    useIndex)[0];
        });
    }

    private ProjectionExecutor projectionExecutorFor(CompiledQuery query) {
        var executor = plan.projectionExecutorFor(query);
        if (executor != null) {
            return executor;
        }
        var index = compiledQueryIndexByInstance.get(query);
        if (index != null
                && index >= 0
                && index < compiledProjectionExecutorsByQuery.length) {
            return compiledProjectionExecutorsByQuery[index];
        }
        if (query.projection() == null) {
            return null;
        }
        return dynamicProjectionExecutors.computeIfAbsent(query,
                q -> buildProjectionExecutor(q, metadataProvider));
    }

    Selection selectionFromRows(int[] rows) {
        return selectionFromRows(plan.table(), rows);
    }

    private static Selection selectionFromRows(GeneratedTable table, int[] rows) {
        long[] packed = new long[rows.length];
        int count = 0;
        for (int rowIndex : rows) {
            long generation = table.rowGeneration(rowIndex);
            long ref = io.memris.storage.Selection.pack(rowIndex, generation);
            if (table.isLive(ref)) {
                packed[count++] = ref;
            }
        }
        if (count == packed.length) {
            return new SelectionImpl(packed);
        }
        long[] trimmed = new long[count];
        System.arraycopy(packed, 0, trimmed, 0, count);
        return new SelectionImpl(trimmed);
    }

    private int[] limitRows(int[] rows, int limit) {
        if (limit >= rows.length) {
            return rows;
        }
        int[] limited = new int[limit];
        System.arraycopy(rows, 0, limited, 0, limit);
        return limited;
    }

    public int[] applyIntOrderCompiled(int[] rows,
            int columnIndex,
            boolean ascending,
            int limit,
            boolean primitiveNonNull) {
        if (limit > 0 && limit < rows.length) {
            return primitiveNonNull
                    ? topKIntNonNull(rows, columnIndex, ascending, limit)
                    : topKIntNullable(rows, columnIndex, ascending, limit);
        }
        return primitiveNonNull
                ? sortByIntColumnNonNull(rows, columnIndex, ascending)
                : sortByIntColumnNullable(rows, columnIndex, ascending);
    }

    public int[] applyLongOrderCompiled(int[] rows,
            int columnIndex,
            boolean ascending,
            int limit,
            boolean primitiveNonNull) {
        if (limit > 0 && limit < rows.length) {
            return primitiveNonNull
                    ? topKLongNonNull(rows, columnIndex, ascending, limit)
                    : topKLongNullable(rows, columnIndex, ascending, limit);
        }
        return primitiveNonNull
                ? sortByLongColumnNonNull(rows, columnIndex, ascending)
                : sortByLongColumnNullable(rows, columnIndex, ascending);
    }

    public int[] applyStringOrderCompiled(int[] rows, int columnIndex, boolean ascending, int limit) {
        if (limit > 0 && limit < rows.length) {
            return topKString(rows, columnIndex, ascending, limit);
        }
        return sortByStringColumn(rows, columnIndex, ascending);
    }

    public int[] applyFloatOrderCompiled(int[] rows,
            int columnIndex,
            boolean ascending,
            int limit,
            boolean primitiveNonNull) {
        var sorted = primitiveNonNull
                ? sortByFloatColumnNonNull(rows, columnIndex, ascending)
                : sortByFloatColumnNullable(rows, columnIndex, ascending);
        return limit > 0 && limit < sorted.length ? limitRows(sorted, limit) : sorted;
    }

    public int[] applyDoubleOrderCompiled(int[] rows,
            int columnIndex,
            boolean ascending,
            int limit,
            boolean primitiveNonNull) {
        var sorted = primitiveNonNull
                ? sortByDoubleColumnNonNull(rows, columnIndex, ascending)
                : sortByDoubleColumnNullable(rows, columnIndex, ascending);
        return limit > 0 && limit < sorted.length ? limitRows(sorted, limit) : sorted;
    }

    public int[] applySingleOrderFallback(int[] rows, CompiledQuery.CompiledOrderBy orderBy, int limit) {
        var sorted = sortBySingleColumn(rows, orderBy);
        return limit > 0 && limit < sorted.length ? limitRows(sorted, limit) : sorted;
    }

    private int[] topKIntNonNull(int[] rows, int columnIndex, boolean ascending, int k) {
        GeneratedTable table = plan.table();
        int[] result = rows.clone();
        SortBuffers buffers = SORT_BUFFERS.get();
        int[] keys = buffers.intKeys(result.length);
        for (int i = 0; i < result.length; i++) {
            final int ri = result[i];
            keys[i] = table.readWithSeqLock(ri, () -> table.readInt(columnIndex, ri));
        }
        topKHeapIntNonNull(result, keys, k, ascending);
        quickSortIntNonNull(result, keys, 0, k - 1, ascending);
        int[] trimmed = new int[k];
        System.arraycopy(result, 0, trimmed, 0, k);
        return trimmed;
    }

    private int[] topKIntNullable(int[] rows, int columnIndex, boolean ascending, int k) {
        GeneratedTable table = plan.table();
        int[] result = rows.clone();
        SortBuffers buffers = SORT_BUFFERS.get();
        int[] keys = buffers.intKeys(result.length);
        boolean[] present = buffers.present(result.length);
        for (int i = 0; i < result.length; i++) {
            final int idx = i;
            final int row = result[i];
            table.readWithSeqLock(row, () -> {
                present[idx] = table.isPresent(columnIndex, row);
                keys[idx] = table.readInt(columnIndex, row);
                return null;
            });
        }
        topKHeapInt(result, keys, present, k, ascending);
        quickSortInt(result, keys, present, 0, k - 1, ascending);
        int[] trimmed = new int[k];
        System.arraycopy(result, 0, trimmed, 0, k);
        return trimmed;
    }

    private void topKHeapInt(int[] rows, int[] keys, boolean[] present, int k, boolean ascending) {
        // Build a max-heap (if ascending) or min-heap (if descending) for the first k
        // elements
        // Then for remaining elements, compare with heap root and replace if better
        int n = rows.length;
        // Build heap for first k elements
        for (int i = k / 2 - 1; i >= 0; i--) {
            heapifyInt(rows, keys, present, i, k, ascending);
        }
        // Process remaining elements
        for (int i = k; i < n; i++) {
            int rootCompare = compareIntForTopK(keys[i], present[i], keys[0], present[0], ascending);
            if (rootCompare < 0) {
                // Current element is better than heap root, replace it
                rows[0] = rows[i];
                keys[0] = keys[i];
                present[0] = present[i];
                heapifyInt(rows, keys, present, 0, k, ascending);
            }
        }
    }

    private void heapifyInt(int[] rows, int[] keys, boolean[] present, int i, int size, boolean maxHeap) {
        int largest = i;
        int left = 2 * i + 1;
        int right = 2 * i + 2;

        if (left < size && compareIntForTopK(keys[left], present[left], keys[largest], present[largest], maxHeap) > 0) {
            largest = left;
        }
        if (right < size
                && compareIntForTopK(keys[right], present[right], keys[largest], present[largest], maxHeap) > 0) {
            largest = right;
        }

        if (largest != i) {
            swap(rows, i, largest);
            swap(keys, i, largest);
            swap(present, i, largest);
            heapifyInt(rows, keys, present, largest, size, maxHeap);
        }
    }

    private int compareIntForTopK(int keyA, boolean presentA, int keyB, boolean presentB, boolean ascending) {
        int cmp = compareNullable(presentA, presentB);
        if (cmp == 0) {
            cmp = Integer.compare(keyA, keyB);
        }
        return ascending ? cmp : -cmp;
    }

    private void topKHeapIntNonNull(int[] rows, int[] keys, int k, boolean ascending) {
        int n = rows.length;
        for (int i = k / 2 - 1; i >= 0; i--) {
            heapifyIntNonNull(rows, keys, i, k, ascending);
        }
        for (int i = k; i < n; i++) {
            int rootCompare = compareIntForTopKNonNull(keys[i], keys[0], ascending);
            if (rootCompare < 0) {
                rows[0] = rows[i];
                keys[0] = keys[i];
                heapifyIntNonNull(rows, keys, 0, k, ascending);
            }
        }
    }

    private void heapifyIntNonNull(int[] rows, int[] keys, int i, int size, boolean maxHeap) {
        int largest = i;
        int left = 2 * i + 1;
        int right = 2 * i + 2;

        if (left < size && compareIntForTopKNonNull(keys[left], keys[largest], maxHeap) > 0) {
            largest = left;
        }
        if (right < size && compareIntForTopKNonNull(keys[right], keys[largest], maxHeap) > 0) {
            largest = right;
        }

        if (largest != i) {
            swap(rows, i, largest);
            swap(keys, i, largest);
            heapifyIntNonNull(rows, keys, largest, size, maxHeap);
        }
    }

    private int compareIntForTopKNonNull(int keyA, int keyB, boolean ascending) {
        int cmp = Integer.compare(keyA, keyB);
        return ascending ? cmp : -cmp;
    }

    private int[] topKLongNonNull(int[] rows, int columnIndex, boolean ascending, int k) {
        GeneratedTable table = plan.table();
        int[] result = rows.clone();
        SortBuffers buffers = SORT_BUFFERS.get();
        long[] keys = buffers.longKeys(result.length);
        for (int i = 0; i < result.length; i++) {
            final int ri = result[i];
            keys[i] = table.readWithSeqLock(ri, () -> table.readLong(columnIndex, ri));
        }
        topKHeapLongNonNull(result, keys, k, ascending);
        quickSortLongNonNull(result, keys, 0, k - 1, ascending);
        int[] trimmed = new int[k];
        System.arraycopy(result, 0, trimmed, 0, k);
        return trimmed;
    }

    private int[] topKLongNullable(int[] rows, int columnIndex, boolean ascending, int k) {
        GeneratedTable table = plan.table();
        int[] result = rows.clone();
        SortBuffers buffers = SORT_BUFFERS.get();
        long[] keys = buffers.longKeys(result.length);
        boolean[] present = buffers.present(result.length);
        for (int i = 0; i < result.length; i++) {
            final int idx = i;
            final int row = result[i];
            table.readWithSeqLock(row, () -> {
                present[idx] = table.isPresent(columnIndex, row);
                keys[idx] = table.readLong(columnIndex, row);
                return null;
            });
        }
        topKHeapLong(result, keys, present, k, ascending);
        quickSortLong(result, keys, present, 0, k - 1, ascending);
        int[] trimmed = new int[k];
        System.arraycopy(result, 0, trimmed, 0, k);
        return trimmed;
    }

    private void topKHeapLong(int[] rows, long[] keys, boolean[] present, int k, boolean ascending) {
        int n = rows.length;
        for (int i = k / 2 - 1; i >= 0; i--) {
            heapifyLong(rows, keys, present, i, k, ascending);
        }
        for (int i = k; i < n; i++) {
            int rootCompare = compareLongForTopK(keys[i], present[i], keys[0], present[0], ascending);
            if (rootCompare < 0) {
                rows[0] = rows[i];
                keys[0] = keys[i];
                present[0] = present[i];
                heapifyLong(rows, keys, present, 0, k, ascending);
            }
        }
    }

    private void heapifyLong(int[] rows, long[] keys, boolean[] present, int i, int size, boolean maxHeap) {
        int largest = i;
        int left = 2 * i + 1;
        int right = 2 * i + 2;

        if (left < size
                && compareLongForTopK(keys[left], present[left], keys[largest], present[largest], maxHeap) > 0) {
            largest = left;
        }
        if (right < size
                && compareLongForTopK(keys[right], present[right], keys[largest], present[largest], maxHeap) > 0) {
            largest = right;
        }

        if (largest != i) {
            swap(rows, i, largest);
            swap(keys, i, largest);
            swap(present, i, largest);
            heapifyLong(rows, keys, present, largest, size, maxHeap);
        }
    }

    private int compareLongForTopK(long keyA, boolean presentA, long keyB, boolean presentB, boolean ascending) {
        int cmp = compareNullable(presentA, presentB);
        if (cmp == 0) {
            cmp = Long.compare(keyA, keyB);
        }
        return ascending ? cmp : -cmp;
    }

    private void topKHeapLongNonNull(int[] rows, long[] keys, int k, boolean ascending) {
        int n = rows.length;
        for (int i = k / 2 - 1; i >= 0; i--) {
            heapifyLongNonNull(rows, keys, i, k, ascending);
        }
        for (int i = k; i < n; i++) {
            int rootCompare = compareLongForTopKNonNull(keys[i], keys[0], ascending);
            if (rootCompare < 0) {
                rows[0] = rows[i];
                keys[0] = keys[i];
                heapifyLongNonNull(rows, keys, 0, k, ascending);
            }
        }
    }

    private void heapifyLongNonNull(int[] rows, long[] keys, int i, int size, boolean maxHeap) {
        int largest = i;
        int left = 2 * i + 1;
        int right = 2 * i + 2;

        if (left < size && compareLongForTopKNonNull(keys[left], keys[largest], maxHeap) > 0) {
            largest = left;
        }
        if (right < size && compareLongForTopKNonNull(keys[right], keys[largest], maxHeap) > 0) {
            largest = right;
        }

        if (largest != i) {
            swap(rows, i, largest);
            swap(keys, i, largest);
            heapifyLongNonNull(rows, keys, largest, size, maxHeap);
        }
    }

    private int compareLongForTopKNonNull(long keyA, long keyB, boolean ascending) {
        int cmp = Long.compare(keyA, keyB);
        return ascending ? cmp : -cmp;
    }

    private int[] topKString(int[] rows, int columnIndex, boolean ascending, int k) {
        GeneratedTable table = plan.table();
        int[] result = rows.clone();
        SortBuffers buffers = SORT_BUFFERS.get();
        String[] keys = buffers.stringKeys(result.length);
        for (int i = 0; i < result.length; i++) {
            final int ri = result[i];
            keys[i] = table.readWithSeqLock(ri, () -> table.readString(columnIndex, ri));
        }
        topKHeapString(result, keys, k, ascending);
        quickSortString(result, keys, 0, k - 1, ascending);
        int[] trimmed = new int[k];
        System.arraycopy(result, 0, trimmed, 0, k);
        return trimmed;
    }

    private void topKHeapString(int[] rows, String[] keys, int k, boolean ascending) {
        int n = rows.length;
        for (int i = k / 2 - 1; i >= 0; i--) {
            heapifyString(rows, keys, i, k, ascending);
        }
        for (int i = k; i < n; i++) {
            int rootCompare = compareStringForTopK(keys[i], keys[0], ascending);
            if (rootCompare < 0) {
                rows[0] = rows[i];
                keys[0] = keys[i];
                heapifyString(rows, keys, 0, k, ascending);
            }
        }
    }

    private void heapifyString(int[] rows, String[] keys, int i, int size, boolean maxHeap) {
        int largest = i;
        int left = 2 * i + 1;
        int right = 2 * i + 2;

        if (left < size && compareStringForTopK(keys[left], keys[largest], maxHeap) > 0) {
            largest = left;
        }
        if (right < size && compareStringForTopK(keys[right], keys[largest], maxHeap) > 0) {
            largest = right;
        }

        if (largest != i) {
            swap(rows, i, largest);
            swap(keys, i, largest);
            heapifyString(rows, keys, largest, size, maxHeap);
        }
    }

    private int compareStringForTopK(String keyA, String keyB, boolean ascending) {
        int cmp = compareStringValue(keyA, keyB);
        return ascending ? cmp : -cmp;
    }

    private int compareStringValue(String a, String b) {
        if (a == null && b == null)
            return 0;
        if (a == null)
            return 1; // nulls last
        if (b == null)
            return -1;
        return a.compareTo(b);
    }

    private int[] sortBySingleColumn(int[] rows, CompiledQuery.CompiledOrderBy orderBy) {
        int columnIndex = orderBy.columnIndex();
        boolean ascending = orderBy.ascending();
        byte typeCode = plan.table().typeCodeAt(columnIndex);

        return switch (typeCode) {
            case TypeCodes.TYPE_INT,
                    TypeCodes.TYPE_BOOLEAN,
                    TypeCodes.TYPE_BYTE,
                    TypeCodes.TYPE_SHORT,
                    TypeCodes.TYPE_CHAR ->
                sortByIntColumn(rows, columnIndex, ascending);
            case TypeCodes.TYPE_LONG,
                    TypeCodes.TYPE_INSTANT,
                    TypeCodes.TYPE_LOCAL_DATE,
                    TypeCodes.TYPE_LOCAL_DATE_TIME,
                    TypeCodes.TYPE_DATE ->
                sortByLongColumn(rows, columnIndex, ascending);
            case TypeCodes.TYPE_FLOAT -> sortByFloatColumn(rows, columnIndex, ascending);
            case TypeCodes.TYPE_DOUBLE -> sortByDoubleColumn(rows, columnIndex, ascending);
            case TypeCodes.TYPE_STRING,
                    TypeCodes.TYPE_BIG_DECIMAL,
                    TypeCodes.TYPE_BIG_INTEGER ->
                sortByStringColumn(rows, columnIndex, ascending);
            default -> throw new IllegalArgumentException("Unsupported type for sorting: " + typeCode);
        };
    }

    private int[] sortByIntColumn(int[] rows, int columnIndex, boolean ascending) {
        return isPrimitiveNonNullColumn(columnIndex)
                ? sortByIntColumnNonNull(rows, columnIndex, ascending)
                : sortByIntColumnNullable(rows, columnIndex, ascending);
    }

    private int[] sortByIntColumnNonNull(int[] rows, int columnIndex, boolean ascending) {
        int[] result = rows.clone();
        SortBuffers buffers = SORT_BUFFERS.get();
        int[] keys = buffers.intKeys(result.length);
        GeneratedTable table = plan.table();
        for (int i = 0; i < result.length; i++) {
            final int ri = result[i];
            keys[i] = table.readWithSeqLock(ri, () -> table.readInt(columnIndex, ri));
        }
        quickSortIntNonNull(result, keys, 0, result.length - 1, ascending);
        return result;
    }

    private int[] sortByIntColumnNullable(int[] rows, int columnIndex, boolean ascending) {
        int[] result = rows.clone();
        SortBuffers buffers = SORT_BUFFERS.get();
        int[] keys = buffers.intKeys(result.length);
        boolean[] present = buffers.present(result.length);
        GeneratedTable table = plan.table();
        for (int i = 0; i < result.length; i++) {
            final int idx = i;
            final int row = result[i];
            table.readWithSeqLock(row, () -> {
                present[idx] = table.isPresent(columnIndex, row);
                keys[idx] = table.readInt(columnIndex, row);
                return null;
            });
        }
        quickSortInt(result, keys, present, 0, result.length - 1, ascending);
        return result;
    }

    private int[] sortByFloatColumn(int[] rows, int columnIndex, boolean ascending) {
        return isPrimitiveNonNullColumn(columnIndex)
                ? sortByFloatColumnNonNull(rows, columnIndex, ascending)
                : sortByFloatColumnNullable(rows, columnIndex, ascending);
    }

    private int[] sortByFloatColumnNonNull(int[] rows, int columnIndex, boolean ascending) {
        int[] result = rows.clone();
        SortBuffers buffers = SORT_BUFFERS.get();
        float[] keys = buffers.floatKeys(result.length);
        GeneratedTable table = plan.table();
        for (int i = 0; i < result.length; i++) {
            final int ri = result[i];
            keys[i] = table.readWithSeqLock(ri, () -> FloatEncoding.sortableIntToFloat(table.readInt(columnIndex, ri)));
        }
        quickSortFloatNonNull(result, keys, 0, result.length - 1, ascending);
        return result;
    }

    private int[] sortByFloatColumnNullable(int[] rows, int columnIndex, boolean ascending) {
        int[] result = rows.clone();
        SortBuffers buffers = SORT_BUFFERS.get();
        float[] keys = buffers.floatKeys(result.length);
        boolean[] present = buffers.present(result.length);
        GeneratedTable table = plan.table();
        for (int i = 0; i < result.length; i++) {
            final int idx = i;
            final int row = result[i];
            table.readWithSeqLock(row, () -> {
                present[idx] = table.isPresent(columnIndex, row);
                keys[idx] = FloatEncoding.sortableIntToFloat(table.readInt(columnIndex, row));
                return null;
            });
        }
        quickSortFloat(result, keys, present, 0, result.length - 1, ascending);
        return result;
    }

    private int[] sortByLongColumn(int[] rows, int columnIndex, boolean ascending) {
        return isPrimitiveNonNullColumn(columnIndex)
                ? sortByLongColumnNonNull(rows, columnIndex, ascending)
                : sortByLongColumnNullable(rows, columnIndex, ascending);
    }

    private int[] sortByLongColumnNonNull(int[] rows, int columnIndex, boolean ascending) {
        int[] result = rows.clone();
        SortBuffers buffers = SORT_BUFFERS.get();
        long[] keys = buffers.longKeys(result.length);
        GeneratedTable table = plan.table();
        for (int i = 0; i < result.length; i++) {
            final int ri = result[i];
            keys[i] = table.readWithSeqLock(ri, () -> table.readLong(columnIndex, ri));
        }
        quickSortLongNonNull(result, keys, 0, result.length - 1, ascending);
        return result;
    }

    private int[] sortByLongColumnNullable(int[] rows, int columnIndex, boolean ascending) {
        int[] result = rows.clone();
        SortBuffers buffers = SORT_BUFFERS.get();
        long[] keys = buffers.longKeys(result.length);
        boolean[] present = buffers.present(result.length);
        GeneratedTable table = plan.table();
        for (int i = 0; i < result.length; i++) {
            final int idx = i;
            final int row = result[i];
            table.readWithSeqLock(row, () -> {
                present[idx] = table.isPresent(columnIndex, row);
                keys[idx] = table.readLong(columnIndex, row);
                return null;
            });
        }
        quickSortLong(result, keys, present, 0, result.length - 1, ascending);
        return result;
    }

    private int[] sortByDoubleColumn(int[] rows, int columnIndex, boolean ascending) {
        return isPrimitiveNonNullColumn(columnIndex)
                ? sortByDoubleColumnNonNull(rows, columnIndex, ascending)
                : sortByDoubleColumnNullable(rows, columnIndex, ascending);
    }

    private int[] sortByDoubleColumnNonNull(int[] rows, int columnIndex, boolean ascending) {
        int[] result = rows.clone();
        SortBuffers buffers = SORT_BUFFERS.get();
        double[] keys = buffers.doubleKeys(result.length);
        GeneratedTable table = plan.table();
        for (int i = 0; i < result.length; i++) {
            final int ri = result[i];
            keys[i] = table.readWithSeqLock(ri, () -> Double.longBitsToDouble(table.readLong(columnIndex, ri)));
        }
        quickSortDoubleNonNull(result, keys, 0, result.length - 1, ascending);
        return result;
    }

    private int[] sortByDoubleColumnNullable(int[] rows, int columnIndex, boolean ascending) {
        int[] result = rows.clone();
        SortBuffers buffers = SORT_BUFFERS.get();
        double[] keys = buffers.doubleKeys(result.length);
        boolean[] present = buffers.present(result.length);
        GeneratedTable table = plan.table();
        for (int i = 0; i < result.length; i++) {
            final int idx = i;
            final int row = result[i];
            table.readWithSeqLock(row, () -> {
                present[idx] = table.isPresent(columnIndex, row);
                keys[idx] = Double.longBitsToDouble(table.readLong(columnIndex, row));
                return null;
            });
        }
        quickSortDouble(result, keys, present, 0, result.length - 1, ascending);
        return result;
    }

    private int[] sortByStringColumn(int[] rows, int columnIndex, boolean ascending) {
        int[] result = rows.clone();
        SortBuffers buffers = SORT_BUFFERS.get();
        String[] keys = buffers.stringKeys(result.length);
        GeneratedTable table = plan.table();
        for (int i = 0; i < result.length; i++) {
            final int ri = result[i];
            keys[i] = table.readWithSeqLock(ri, () -> table.readString(columnIndex, ri));
        }
        quickSortString(result, keys, 0, result.length - 1, ascending);
        return result;
    }

    private static int compareNullable(boolean presentA, boolean presentB) {
        if (presentA == presentB) {
            return 0;
        }
        return presentA ? -1 : 1; // nulls last
    }

    private static void quickSortInt(int[] rows, int[] keys, boolean[] present, int low, int high, boolean ascending) {
        int i = low;
        int j = high;
        int pivotIndex = low + ((high - low) >>> 1);
        int pivot = keys[pivotIndex];
        boolean pivotPresent = present[pivotIndex];
        int pivotRow = rows[pivotIndex];

        while (i <= j) {
            while (compareInt(keys[i], present[i], rows[i], pivot, pivotPresent, pivotRow, ascending) < 0) {
                i++;
            }
            while (compareInt(keys[j], present[j], rows[j], pivot, pivotPresent, pivotRow, ascending) > 0) {
                j--;
            }
            if (i <= j) {
                swap(rows, i, j);
                swap(keys, i, j);
                swap(present, i, j);
                i++;
                j--;
            }
        }

        if (low < j)
            quickSortInt(rows, keys, present, low, j, ascending);
        if (i < high)
            quickSortInt(rows, keys, present, i, high, ascending);
    }

    private static void quickSortIntNonNull(int[] rows, int[] keys, int low, int high, boolean ascending) {
        int i = low;
        int j = high;
        int pivotIndex = low + ((high - low) >>> 1);
        int pivot = keys[pivotIndex];
        int pivotRow = rows[pivotIndex];

        while (i <= j) {
            while (compareIntNonNull(keys[i], rows[i], pivot, pivotRow, ascending) < 0) {
                i++;
            }
            while (compareIntNonNull(keys[j], rows[j], pivot, pivotRow, ascending) > 0) {
                j--;
            }
            if (i <= j) {
                swap(rows, i, j);
                swap(keys, i, j);
                i++;
                j--;
            }
        }

        if (low < j)
            quickSortIntNonNull(rows, keys, low, j, ascending);
        if (i < high)
            quickSortIntNonNull(rows, keys, i, high, ascending);
    }

    private static void quickSortFloat(int[] rows, float[] keys, boolean[] present, int low, int high,
            boolean ascending) {
        int i = low;
        int j = high;
        int pivotIndex = low + ((high - low) >>> 1);
        float pivot = keys[pivotIndex];
        boolean pivotPresent = present[pivotIndex];
        int pivotRow = rows[pivotIndex];

        while (i <= j) {
            while (compareFloat(keys[i], present[i], rows[i], pivot, pivotPresent, pivotRow, ascending) < 0) {
                i++;
            }
            while (compareFloat(keys[j], present[j], rows[j], pivot, pivotPresent, pivotRow, ascending) > 0) {
                j--;
            }
            if (i <= j) {
                swap(rows, i, j);
                swap(keys, i, j);
                swap(present, i, j);
                i++;
                j--;
            }
        }

        if (low < j)
            quickSortFloat(rows, keys, present, low, j, ascending);
        if (i < high)
            quickSortFloat(rows, keys, present, i, high, ascending);
    }

    private static void quickSortFloatNonNull(int[] rows, float[] keys, int low, int high, boolean ascending) {
        int i = low;
        int j = high;
        int pivotIndex = low + ((high - low) >>> 1);
        float pivot = keys[pivotIndex];
        int pivotRow = rows[pivotIndex];

        while (i <= j) {
            while (compareFloatNonNull(keys[i], rows[i], pivot, pivotRow, ascending) < 0) {
                i++;
            }
            while (compareFloatNonNull(keys[j], rows[j], pivot, pivotRow, ascending) > 0) {
                j--;
            }
            if (i <= j) {
                swap(rows, i, j);
                swap(keys, i, j);
                i++;
                j--;
            }
        }

        if (low < j)
            quickSortFloatNonNull(rows, keys, low, j, ascending);
        if (i < high)
            quickSortFloatNonNull(rows, keys, i, high, ascending);
    }

    private static void quickSortLong(int[] rows, long[] keys, boolean[] present, int low, int high,
            boolean ascending) {
        int i = low;
        int j = high;
        int pivotIndex = low + ((high - low) >>> 1);
        long pivot = keys[pivotIndex];
        boolean pivotPresent = present[pivotIndex];
        int pivotRow = rows[pivotIndex];

        while (i <= j) {
            while (compareLong(keys[i], present[i], rows[i], pivot, pivotPresent, pivotRow, ascending) < 0) {
                i++;
            }
            while (compareLong(keys[j], present[j], rows[j], pivot, pivotPresent, pivotRow, ascending) > 0) {
                j--;
            }
            if (i <= j) {
                swap(rows, i, j);
                swap(keys, i, j);
                swap(present, i, j);
                i++;
                j--;
            }
        }

        if (low < j)
            quickSortLong(rows, keys, present, low, j, ascending);
        if (i < high)
            quickSortLong(rows, keys, present, i, high, ascending);
    }

    private static void quickSortLongNonNull(int[] rows, long[] keys, int low, int high, boolean ascending) {
        int i = low;
        int j = high;
        int pivotIndex = low + ((high - low) >>> 1);
        long pivot = keys[pivotIndex];
        int pivotRow = rows[pivotIndex];

        while (i <= j) {
            while (compareLongNonNull(keys[i], rows[i], pivot, pivotRow, ascending) < 0) {
                i++;
            }
            while (compareLongNonNull(keys[j], rows[j], pivot, pivotRow, ascending) > 0) {
                j--;
            }
            if (i <= j) {
                swap(rows, i, j);
                swap(keys, i, j);
                i++;
                j--;
            }
        }

        if (low < j)
            quickSortLongNonNull(rows, keys, low, j, ascending);
        if (i < high)
            quickSortLongNonNull(rows, keys, i, high, ascending);
    }

    private static void quickSortDouble(int[] rows, double[] keys, boolean[] present, int low, int high,
            boolean ascending) {
        int i = low;
        int j = high;
        int pivotIndex = low + ((high - low) >>> 1);
        double pivot = keys[pivotIndex];
        boolean pivotPresent = present[pivotIndex];
        int pivotRow = rows[pivotIndex];

        while (i <= j) {
            while (compareDouble(keys[i], present[i], rows[i], pivot, pivotPresent, pivotRow, ascending) < 0) {
                i++;
            }
            while (compareDouble(keys[j], present[j], rows[j], pivot, pivotPresent, pivotRow, ascending) > 0) {
                j--;
            }
            if (i <= j) {
                swap(rows, i, j);
                swap(keys, i, j);
                swap(present, i, j);
                i++;
                j--;
            }
        }

        if (low < j)
            quickSortDouble(rows, keys, present, low, j, ascending);
        if (i < high)
            quickSortDouble(rows, keys, present, i, high, ascending);
    }

    private static void quickSortDoubleNonNull(int[] rows, double[] keys, int low, int high, boolean ascending) {
        int i = low;
        int j = high;
        int pivotIndex = low + ((high - low) >>> 1);
        double pivot = keys[pivotIndex];
        int pivotRow = rows[pivotIndex];

        while (i <= j) {
            while (compareDoubleNonNull(keys[i], rows[i], pivot, pivotRow, ascending) < 0) {
                i++;
            }
            while (compareDoubleNonNull(keys[j], rows[j], pivot, pivotRow, ascending) > 0) {
                j--;
            }
            if (i <= j) {
                swap(rows, i, j);
                swap(keys, i, j);
                i++;
                j--;
            }
        }

        if (low < j)
            quickSortDoubleNonNull(rows, keys, low, j, ascending);
        if (i < high)
            quickSortDoubleNonNull(rows, keys, i, high, ascending);
    }

    private static void quickSortString(int[] rows, String[] keys, int low, int high, boolean ascending) {
        int i = low;
        int j = high;
        int pivotIndex = low + ((high - low) >>> 1);
        String pivot = keys[pivotIndex];
        int pivotRow = rows[pivotIndex];

        while (i <= j) {
            while (compareString(keys[i], rows[i], pivot, pivotRow, ascending) < 0) {
                i++;
            }
            while (compareString(keys[j], rows[j], pivot, pivotRow, ascending) > 0) {
                j--;
            }
            if (i <= j) {
                swap(keys, i, j, rows);
                i++;
                j--;
            }
        }

        if (low < j)
            quickSortString(rows, keys, low, j, ascending);
        if (i < high)
            quickSortString(rows, keys, i, high, ascending);
    }

    private static int compareInt(int value, boolean present, int row,
            int pivot, boolean pivotPresent, int pivotRow,
            boolean ascending) {
        int cmp = compareNullable(present, pivotPresent);
        if (cmp == 0) {
            cmp = Integer.compare(value, pivot);
        }
        if (cmp == 0) {
            cmp = Integer.compare(row, pivotRow);
        }
        return ascending ? cmp : -cmp;
    }

    private static int compareIntNonNull(int value, int row, int pivot, int pivotRow, boolean ascending) {
        int cmp = Integer.compare(value, pivot);
        if (cmp == 0) {
            cmp = Integer.compare(row, pivotRow);
        }
        return ascending ? cmp : -cmp;
    }

    private static int compareFloat(float value, boolean present, int row,
            float pivot, boolean pivotPresent, int pivotRow,
            boolean ascending) {
        int cmp = compareNullable(present, pivotPresent);
        if (cmp == 0) {
            cmp = Float.compare(value, pivot);
        }
        if (cmp == 0) {
            cmp = Integer.compare(row, pivotRow);
        }
        return ascending ? cmp : -cmp;
    }

    private static int compareFloatNonNull(float value, int row,
            float pivot, int pivotRow,
            boolean ascending) {
        int cmp = Float.compare(value, pivot);
        if (cmp == 0) {
            cmp = Integer.compare(row, pivotRow);
        }
        return ascending ? cmp : -cmp;
    }

    private static int compareLong(long value, boolean present, int row,
            long pivot, boolean pivotPresent, int pivotRow,
            boolean ascending) {
        int cmp = compareNullable(present, pivotPresent);
        if (cmp == 0) {
            cmp = Long.compare(value, pivot);
        }
        if (cmp == 0) {
            cmp = Integer.compare(row, pivotRow);
        }
        return ascending ? cmp : -cmp;
    }

    private static int compareLongNonNull(long value, int row, long pivot, int pivotRow, boolean ascending) {
        int cmp = Long.compare(value, pivot);
        if (cmp == 0) {
            cmp = Integer.compare(row, pivotRow);
        }
        return ascending ? cmp : -cmp;
    }

    private static int compareDouble(double value, boolean present, int row,
            double pivot, boolean pivotPresent, int pivotRow,
            boolean ascending) {
        int cmp = compareNullable(present, pivotPresent);
        if (cmp == 0) {
            cmp = Double.compare(value, pivot);
        }
        if (cmp == 0) {
            cmp = Integer.compare(row, pivotRow);
        }
        return ascending ? cmp : -cmp;
    }

    private static int compareDoubleNonNull(double value, int row,
            double pivot, int pivotRow,
            boolean ascending) {
        int cmp = Double.compare(value, pivot);
        if (cmp == 0) {
            cmp = Integer.compare(row, pivotRow);
        }
        return ascending ? cmp : -cmp;
    }

    private static int compareString(String value, int row, String pivot, int pivotRow, boolean ascending) {
        int cmp;
        if (value == null && pivot == null) {
            cmp = 0;
        } else if (value == null) {
            cmp = 1;
        } else if (pivot == null) {
            cmp = -1;
        } else {
            cmp = value.compareTo(pivot);
        }
        if (cmp == 0) {
            cmp = Integer.compare(row, pivotRow);
        }
        return ascending ? cmp : -cmp;
    }

    private static void swap(int[] arr, int i, int j) {
        int tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }

    private static void swap(long[] arr, int i, int j) {
        long tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }

    private static void swap(double[] arr, int i, int j) {
        double tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }

    private static void swap(float[] arr, int i, int j) {
        float tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }

    private static void swap(boolean[] arr, int i, int j) {
        boolean tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }

    private static void swap(String[] arr, int i, int j, int[] rows) {
        String tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
        int row = rows[i];
        rows[i] = rows[j];
        rows[j] = row;
    }

    private static void swap(String[] arr, int i, int j) {
        String tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }

    private Selection applyJoins(CompiledQuery query, Object[] args, Selection baseSelection) {
        CompiledQuery.CompiledJoin[] joins = query.joins();
        if (joins == null || joins.length == 0) {
            return baseSelection;
        }

        Selection current = baseSelection;
        for (CompiledQuery.CompiledJoin join : joins) {
            Selection targetSelection = evaluateJoinPredicates(join, args);
            current = join.executor().filterJoin(plan.table(), join.targetTable(), current, targetSelection);
        }
        return current;
    }

    private GeneratedTable tableFor(Class<?> entityClass) {
        if (metadata != null && metadata.entityClass().equals(entityClass)) {
            return plan.table();
        }
        return plan.tablesByEntity().get(entityClass);
    }

    private MethodHandle projectionConstructor(Class<?> projectionType) {
        return projectionConstructors.computeIfAbsent(projectionType, type -> {
            if (!type.isRecord()) {
                throw new IllegalArgumentException("Projection type must be a record: " + type.getName());
            }
            RecordComponent[] components = type.getRecordComponents();
            Class<?>[] paramTypes = new Class<?>[components.length];
            for (int i = 0; i < components.length; i++) {
                paramTypes[i] = components[i].getType();
            }
            try {
                return MethodHandles.lookup().findConstructor(type, MethodType.methodType(void.class, paramTypes));
            } catch (NoSuchMethodException | IllegalAccessException e) {
                throw new RuntimeException("Failed to resolve projection constructor", e);
            }
        });
    }

    private Selection evaluateJoinPredicates(CompiledQuery.CompiledJoin join, Object[] args) {
        CompiledQuery.CompiledJoinPredicate[] predicates = join.predicates();
        if (predicates == null || predicates.length == 0) {
            return null;
        }

        var targetTable = join.targetTable();
        var targetKernel = join.targetKernel();
        if (targetTable == null || targetKernel == null) {
            return null;
        }

        Selection selection = null;
        for (CompiledQuery.CompiledJoinPredicate predicate : predicates) {
            var directExecutor = joinDirectExecutorFor(predicate);
            Selection next = directExecutor.execute(targetTable, targetKernel, args);
            selection = (selection == null) ? next : selection.intersect(next);
        }

        return selection;
    }

    private void hydrateJoins(T entity, int rowIndex, CompiledQuery query) {
        CompiledQuery.CompiledJoin[] joins = query.joins();
        if (joins == null) {
            return;
        }

        for (CompiledQuery.CompiledJoin join : joins) {
            join.materializer().hydrate(entity, rowIndex, plan.table(), join.targetTable(),
                    join.targetMaterializer());
        }
    }

    private void hydrateCollections(T entity, int rowIndex) {
        if (metadata == null) {
            return;
        }
        for (var plan : oneToManyPlans) {
            hydrateOneToMany(entity, rowIndex, plan);
        }
        for (var plan : manyToManyPlans) {
            hydrateManyToMany(entity, rowIndex, plan);
        }
    }

    private void hydrateOneToMany(T entity, int rowIndex, OneToManyPlan relationPlan) {
        long sourceId = readSourceId(plan.table(), idColumnIndex, relationPlan.sourceFkTypeCode(), rowIndex);
        int[] matches = switch (relationPlan.sourceFkTypeCode()) {
            case TypeCodes.TYPE_LONG ->
                relationPlan.targetTable().scanEqualsLong(relationPlan.fkColumnIndex(), sourceId);
            case TypeCodes.TYPE_INT, TypeCodes.TYPE_SHORT, TypeCodes.TYPE_BYTE ->
                relationPlan.targetTable().scanEqualsInt(relationPlan.fkColumnIndex(), (int) sourceId);
            default -> relationPlan.targetTable().scanEqualsLong(relationPlan.fkColumnIndex(), sourceId);
        };

        var collection = createCollection(relationPlan.collectionType(), matches.length);
        var targetTable = relationPlan.targetTable();
        for (var targetRow : matches) {
            var related = targetTable.readWithSeqLock(targetRow,
                    () -> relationPlan.targetMaterializer().materialize(targetTable, targetRow));
            invokeLifecycle(relationPlan.targetPostLoadHandle(), related);
            collection.add(related);
        }

        try {
            relationPlan.setter().invoke(entity, collection);
        } catch (Throwable e) {
            throw new RuntimeException("Failed to set collection field: " + relationPlan.fieldName(), e);
        }
    }

    private long readSourceId(GeneratedTable table, int idColumnIndex, byte typeCode, int rowIndex) {
        return table.readWithSeqLock(rowIndex, () -> switch (typeCode) {
            case TypeCodes.TYPE_LONG -> table.readLong(idColumnIndex, rowIndex);
            case TypeCodes.TYPE_INT,
                    TypeCodes.TYPE_SHORT,
                    TypeCodes.TYPE_BYTE ->
                (long) table.readInt(idColumnIndex, rowIndex);
            default -> table.readLong(idColumnIndex, rowIndex);
        });
    }

    private java.util.Collection<Object> createCollection(Class<?> fieldType, int expectedSize) {
        if (Set.class.isAssignableFrom(fieldType)) {
            return new LinkedHashSet<>(Math.max(expectedSize, 16));
        }
        return new ArrayList<>(Math.max(expectedSize, 10));
    }

    private void hydrateManyToMany(T entity, int rowIndex, ManyToManyPlan relationPlan) {
        var sourceIdValue = readSourceIdValue(plan.table(), idColumnIndex, relationPlan.sourceFkTypeCode(), rowIndex);
        if (sourceIdValue == null) {
            return;
        }

        var joinTable = relationPlan.joinInfo().table();
        var joinColumn = joinTable.column(relationPlan.joinInfo().joinColumn());
        var inverseColumn = joinTable.column(relationPlan.joinInfo().inverseJoinColumn());

        var rowCount = joinTable.rowCount();
        var collection = createCollection(relationPlan.collectionType(),
                (int) Math.min(rowCount, Integer.MAX_VALUE));

        for (int i = 0; i < rowCount; i++) {
            RowId rowId = new RowId(i >>> 16, i & 0xFFFF);
            Object joinValue = joinColumn.get(rowId);
            if (joinValue == null || !joinValue.equals(sourceIdValue)) {
                continue;
            }
            Object targetIdValue = inverseColumn.get(rowId);
            if (targetIdValue == null) {
                continue;
            }

            int[] matches = scanTargetById(relationPlan.joinInfo().targetTable(), relationPlan.targetIdField(),
                    targetIdValue);
            var targetTable = relationPlan.joinInfo().targetTable();
            for (int targetRow : matches) {
                Object related = targetTable.readWithSeqLock(targetRow,
                        () -> relationPlan.targetMaterializer().materialize(targetTable, targetRow));
                invokeLifecycle(relationPlan.targetPostLoadHandle(), related);
                collection.add(related);
            }
        }

        try {
            relationPlan.setter().invoke(entity, collection);
        } catch (Throwable e) {
            throw new RuntimeException("Failed to set many-to-many collection: " + relationPlan.fieldName(), e);
        }
    }

    private int[] scanTargetById(GeneratedTable targetTable,
            FieldMapping targetId,
            Object targetIdValue) {
        return switch (targetId.typeCode()) {
            case TypeCodes.TYPE_LONG -> targetTable.scanEqualsLong(
                    targetId.columnPosition(), ((Number) targetIdValue).longValue());
            case TypeCodes.TYPE_INT,
                    TypeCodes.TYPE_SHORT,
                    TypeCodes.TYPE_BYTE ->
                targetTable.scanEqualsInt(
                        targetId.columnPosition(), ((Number) targetIdValue).intValue());
            case TypeCodes.TYPE_STRING -> targetTable.scanEqualsString(
                    targetId.columnPosition(), targetIdValue.toString());
            default -> targetTable.scanEqualsLong(targetId.columnPosition(), ((Number) targetIdValue).longValue());
        };
    }

    private Object readSourceIdValue(GeneratedTable table, int idColumnIndex, byte typeCode,
            int rowIndex) {
        return table.readWithSeqLock(rowIndex, () -> switch (typeCode) {
            case TypeCodes.TYPE_LONG -> Long.valueOf(table.readLong(idColumnIndex, rowIndex));
            case TypeCodes.TYPE_INT -> Integer.valueOf(table.readInt(idColumnIndex, rowIndex));
            case TypeCodes.TYPE_SHORT -> Short.valueOf((short) table.readInt(idColumnIndex, rowIndex));
            case TypeCodes.TYPE_BYTE -> Byte.valueOf((byte) table.readInt(idColumnIndex, rowIndex));
            default -> Long.valueOf(table.readLong(idColumnIndex, rowIndex));
        });
    }

    private void persistManyToMany(T entity) {
        if (metadata == null) {
            return;
        }
        for (var relationPlan : manyToManyPlans) {
            if (!relationPlan.persistEnabled()) {
                continue;
            }
            Object collectionValue = readFieldValue(entity, metadata, relationPlan.fieldName());
            if (!(collectionValue instanceof Iterable<?> iterable)) {
                continue;
            }

            Object sourceIdValue = readFieldValue(entity, metadata, metadata.idColumnName());
            if (sourceIdValue == null) {
                continue;
            }

            Set<JoinKey> existing = loadJoinKeys(relationPlan.joinInfo());

            for (Object related : iterable) {
                if (related == null) {
                    continue;
                }
                Object targetIdValue = readFieldValue(related, relationPlan.targetMetadata(),
                        relationPlan.targetMetadata().idColumnName());
                if (targetIdValue == null) {
                    continue;
                }
                JoinKey key = new JoinKey(sourceIdValue, targetIdValue);
                if (existing.add(key)) {
                    relationPlan.joinInfo().table().insert(sourceIdValue, targetIdValue);
                }
            }
        }
    }

    private Set<JoinKey> loadJoinKeys(JoinTableInfo joinInfo) {
        Set<JoinKey> keys = new HashSet<>();
        SimpleTable table = joinInfo.table();
        Column<?> joinColumn = table.column(joinInfo.joinColumn());
        Column<?> inverseColumn = table.column(joinInfo.inverseJoinColumn());
        if (joinColumn == null || inverseColumn == null) {
            return keys;
        }
        long rowCount = table.rowCount();
        for (int i = 0; i < rowCount; i++) {
            RowId rowId = new RowId(i >>> 16, i & 0xFFFF);
            Object joinValue = joinColumn.get(rowId);
            Object inverseValue = inverseColumn.get(rowId);
            if (joinValue == null || inverseValue == null) {
                continue;
            }
            keys.add(new JoinKey(joinValue, inverseValue));
        }
        return keys;
    }

    private Object readFieldValue(Object entity, EntityMetadata<?> entityMetadata, String fieldName) {
        if (entity == null || entityMetadata == null) {
            return null;
        }
        MethodHandle getter = entityMetadata.fieldGetters().get(fieldName);
        if (getter != null) {
            try {
                return getter.invoke(entity);
            } catch (Throwable ignored) {
                return null;
            }
        }
        try {
            java.lang.reflect.Field field = entity.getClass().getDeclaredField(fieldName);
            if (!field.canAccess(entity)) {
                field.setAccessible(true);
            }
            return field.get(entity);
        } catch (ReflectiveOperationException ignored) {
            return null;
        }
    }

    private FieldMapping findIdField(EntityMetadata<?> targetMetadata) {
        for (var field : targetMetadata.fields()) {
            if (field.name().equals(targetMetadata.idColumnName())) {
                return field;
            }
        }
        return null;
    }

    private JoinTableInfo resolveJoinTable(FieldMapping field, EntityMetadata<?> sourceMetadata,
            EntityMetadata<?> targetMetadata) {
        if (field.joinTable() != null && !field.joinTable().isBlank()) {
            return buildJoinTableInfo(field, sourceMetadata, targetMetadata, false);
        }
        if (field.mappedBy() != null && !field.mappedBy().isBlank()) {
            var ownerField = findFieldByName(targetMetadata, field.mappedBy());
            if (ownerField == null || ownerField.joinTable() == null || ownerField.joinTable().isBlank()) {
                return null;
            }
            return buildJoinTableInfo(ownerField, targetMetadata, sourceMetadata, true);
        }
        return null;
    }

    private JoinTableInfo buildJoinTableInfo(FieldMapping ownerField,
            EntityMetadata<?> ownerMetadata,
            EntityMetadata<?> inverseMetadata,
            boolean inverseSide) {
        String joinTableName = ownerField.joinTable();
        SimpleTable joinTable = plan.joinTables().get(joinTableName);
        if (joinTable == null) {
            return null;
        }

        String joinColumn;
        String inverseJoinColumn;
        if (!inverseSide) {
            joinColumn = ownerField.columnName();
            if (joinColumn == null || joinColumn.isBlank()) {
                joinColumn = ownerMetadata.entityClass().getSimpleName().toLowerCase() + "_"
                        + ownerMetadata.idColumnName();
            }
            inverseJoinColumn = ownerField.referencedColumnName();
            if (inverseJoinColumn == null || inverseJoinColumn.isBlank()) {
                inverseJoinColumn = inverseMetadata.entityClass().getSimpleName().toLowerCase() + "_"
                        + inverseMetadata.idColumnName();
            }
            return new JoinTableInfo(joinTableName, joinColumn, inverseJoinColumn, joinTable,
                    plan.tablesByEntity().get(ownerField.targetEntity()));
        }

        joinColumn = ownerField.referencedColumnName();
        if (joinColumn == null || joinColumn.isBlank()) {
            joinColumn = inverseMetadata.entityClass().getSimpleName().toLowerCase() + "_"
                    + inverseMetadata.idColumnName();
        }
        inverseJoinColumn = ownerField.columnName();
        if (inverseJoinColumn == null || inverseJoinColumn.isBlank()) {
            inverseJoinColumn = ownerMetadata.entityClass().getSimpleName().toLowerCase() + "_"
                    + ownerMetadata.idColumnName();
        }
        return new JoinTableInfo(joinTableName, joinColumn, inverseJoinColumn, joinTable,
                plan.tablesByEntity().get(ownerMetadata.entityClass()));
    }

    private FieldMapping findFieldByName(EntityMetadata<?> targetMetadata,
            String name) {
        for (var field : targetMetadata.fields()) {
            if (field.name().equals(name)) {
                return field;
            }
        }
        return null;
    }

    private record IndexPlan(
            String indexName,
            String[] fieldNames,
            int[] columnPositions,
            io.memris.core.Index.IndexType indexType) {
    }

    private record EntityRuntimeLayout(
            FieldMapping[] fieldsByColumn,
            TypeConverter<?, ?>[] convertersByColumn,
            ColumnRuntimeDescriptor[] descriptorsByColumn,
            int idColumnIndex) {
    }

    private record ColumnRuntimeDescriptor(
            FieldMapping field,
            TypeConverter<?, ?> converter,
            ColumnAccessPlan plan) {
    }

    private record OneToManyPlan(
            String fieldName,
            Class<?> collectionType,
            byte sourceFkTypeCode,
            int fkColumnIndex,
            GeneratedTable targetTable,
            HeapRuntimeKernel targetKernel,
            EntityMaterializer<?> targetMaterializer,
            MethodHandle targetPostLoadHandle,
            MethodHandle setter) {
    }

    private record ManyToManyPlan(
            String fieldName,
            Class<?> collectionType,
            byte sourceFkTypeCode,
            JoinTableInfo joinInfo,
            EntityMetadata<?> targetMetadata,
            FieldMapping targetIdField,
            HeapRuntimeKernel targetKernel,
            EntityMaterializer<?> targetMaterializer,
            MethodHandle targetPostLoadHandle,
            MethodHandle setter,
            boolean persistEnabled) {
    }

    private record JoinTableInfo(
            String name,
            String joinColumn,
            String inverseJoinColumn,
            SimpleTable table,
            GeneratedTable targetTable) {
    }

    private record JoinKey(Object left, Object right) {
    }

    boolean hasCompiledConditionProgram(CompiledQuery query) {
        return compiledConditionProgramsByQuery.containsKey(query);
    }

    private record CompiledConditionProgram(
            CompiledConditionGroup[] groups) {
    }

    private record CompiledConditionGroup(
            LeadSelectionExecutor leadSelectionExecutor,
            ConditionRowEvaluatorGenerator.RowConditionEvaluator[] residualMatchers) {
    }

    private record ConditionGroupRange(int start, int end) {
    }

    @FunctionalInterface
    private interface LeadSelectionExecutor {
        Selection select(Object[] args);
    }

    public RepositoryPlan<T> plan() {
        return plan;
    }
}
