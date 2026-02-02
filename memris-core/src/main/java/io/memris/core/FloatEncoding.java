package io.memris.core;

/**
 * Utility class for encoding/decoding floats and doubles to sortable integer
 * representations.
 * 
 * <p>
 * IEEE 754 floating-point bit patterns do not preserve numerical ordering when
 * compared
 * as integers. This is because negative numbers have the sign bit (MSB) set to
 * 1, making
 * their bit representation larger than positive values when treated as unsigned
 * integers.
 * 
 * <p>
 * This class provides methods to convert floats/doubles to a sortable format
 * where
 * integer comparison produces correct numerical ordering, including for
 * negative values.
 * 
 * <h3>Encoding Algorithm</h3>
 * <ul>
 * <li><b>Positive values</b>: XOR with 0x80000000 (flip sign bit only)</li>
 * <li><b>Negative values</b>: XOR with 0xFFFFFFFF (flip all bits)</li>
 * </ul>
 * 
 * <p>
 * This transformation ensures:
 * <ul>
 * <li>Negative values sort before zero</li>
 * <li>Zero sorts before positive values</li>
 * <li>Integer comparison matches numerical comparison</li>
 * <li>Special values (NaN, infinities) maintain proper ordering</li>
 * </ul>
 * 
 * @see Float#floatToRawIntBits(float)
 * @see Double#doubleToRawLongBits(double)
 */
public final class FloatEncoding {

    private FloatEncoding() {
        // Utility class - no instantiation
    }

    /**
     * Converts a float to a sortable integer representation.
     * 
     * <p>
     * The resulting integer can be compared using standard integer comparison
     * operators and will produce the same ordering as numerical float comparison.
     * 
     * @param value the float value to encode
     * @return sortable integer representation
     */
    public static int floatToSortableInt(float value) {
        var bits = Float.floatToRawIntBits(value);
        if (bits < 0) {
            return ~bits ^ 0x80000000;
        }
        return bits;
    }

    /**
     * Converts a sortable integer back to a float.
     * 
     * <p>
     * This is the inverse operation of {@link #floatToSortableInt(float)}.
     * 
     * @param sortable the sortable integer representation
     * @return the original float value
     */
    public static float sortableIntToFloat(int sortable) {
        int bits;
        if (sortable < 0) {
            bits = ~(sortable ^ 0x80000000);
        } else {
            bits = sortable;
        }
        return Float.intBitsToFloat(bits);
    }

    /**
     * Converts a double to a sortable long representation.
     * 
     * <p>
     * The resulting long can be compared using standard long comparison
     * operators and will produce the same ordering as numerical double comparison.
     * 
     * @param value the double value to encode
     * @return sortable long representation
     */
    public static long doubleToSortableLong(double value) {
        var bits = Double.doubleToRawLongBits(value);
        if (bits < 0) {
            return ~bits ^ 0x8000000000000000L;
        }
        return bits;
    }

    /**
     * Converts a sortable long back to a double.
     * 
     * <p>
     * This is the inverse operation of {@link #doubleToSortableLong(double)}.
     * 
     * @param sortable the sortable long representation
     * @return the original double value
     */
    public static double sortableLongToDouble(long sortable) {
        long bits;
        if (sortable < 0) {
            bits = ~(sortable ^ 0x8000000000000000L);
        } else {
            bits = sortable;
        }
        return Double.longBitsToDouble(bits);
    }
}
