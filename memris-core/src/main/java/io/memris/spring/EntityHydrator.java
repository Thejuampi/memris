package io.memris.spring;

/**
 * Fast path entity materialization - zero reflection, zero maps.
 * <p>
 * Implementations are generated at repository creation time using ByteBuddy.
 * Each generated hydrator is specialized for a specific entity type and
 * contains unrolled per-field access code.
 * <p>
 * Example generated code structure:
 * <pre>{@code
 * public class PersonEntityHydrator implements EntityHydrator<Person> {
 *   private static final int NAME_COL = 0;
 *   private static final int AGE_COL = 1;
 *   private final FfmTable table;
 *
 *   public Person materializeRow(int row) {
 *     Person entity = new Person();
 *     entity.setName(table.getString(NAME_COL, row));  // direct call, no lookup
 *     entity.setAge(table.getInt(AGE_COL, row));
 *     return entity;
 *   }
 *
 *   public void materializeRows(int[] rows, List<Person> out) {
 *     for (int row : rows) {
 *       out.add(materializeRow(row));
 *     }
 *   }
 * }
 * }</pre>
 *
 * @param <T> the entity type
 */
public interface EntityHydrator<T> {

    /**
     * Fast path: create and populate one entity.
     * <p>
     * Generated implementation contains unrolled per-field access code:
     * - Direct table.getX(colId, row) calls (no string lookups)
     * - Direct setter invocations (no reflection)
     * - Type switch on byte typeCode (no string comparisons)
     *
     * @param row the row index to materialize
     * @return the populated entity instance
     */
    T materializeRow(int row);

    /**
     * Bulk path: materialize multiple rows into a pre-sized list.
     * <p>
     * This avoids intermediate allocations by using the list's known capacity.
     * <p>
     * Generated implementation contains a simple for-loop calling materializeRow
     * for each row index.
     *
     * @param rows the row indices to materialize
     * @param out  the list to populate (should be pre-sized with rows.length)
     */
    void materializeRows(int[] rows, java.util.List<T> out);
}
