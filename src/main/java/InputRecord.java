public interface InputRecord {

    String[] getValues();

    String getString(String column);

    Long getLong(String column);

    Double getDouble(String column);

    Float getFloat(String column);
}
