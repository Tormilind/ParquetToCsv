import com.univocity.parsers.conversions.DoubleConversion;
import com.univocity.parsers.conversions.FloatConversion;
import com.univocity.parsers.conversions.LongConversion;
import org.apache.avro.generic.GenericData;

public class ParquetInputRecord implements InputRecord {

    private final GenericData.Record record;
    private final String[] header;

    private static final LongConversion longConversion = new LongConversion();
    private static final DoubleConversion doubleConversion = new DoubleConversion();
    private static final FloatConversion floatConversion = new FloatConversion();

    public ParquetInputRecord(String[] header, GenericData.Record record) {
        this.header = header;
        this.record = record;
    }

    @Override
    public String[] getValues() {
        String[] stringValues = new String[header.length];

        for (int i = 0; i < header.length; i++) {
            Object value = record.get(header[i]);

            stringValues[i] = convertToString(value);
        }

        return stringValues;
    }

    @Override
    public String getString(String column) {
        return convertToString(record.get(column));
    }

    @Override
    public Long getLong(String column) {
        return convertToLong(record.get(column));
    }

    @Override
    public Double getDouble(String column) {
        return convertToDouble(record.get(column));
    }

    @Override
    public Float getFloat(String column) {
        return convertToFloat(record.get(column));
    }


    private String convertToString(Object value) {
        if (value == null) {
            return null;
        } else {
            return String.valueOf(value);
        }
    }


    private Long convertToLong(Object value) {
        if (value == null) {
            return null;
        } else {
            return longConversion.execute(String.valueOf(value));
        }
    }

    private Double convertToDouble(Object value) {
        if (value == null) {
            return null;
        } else {
            return doubleConversion.execute(String.valueOf(value));
        }
    }

    private Float convertToFloat(Object value) {
        if (value == null) {
            return null;
        } else {
            return floatConversion.execute(String.valueOf(value));
        }
    }

}
