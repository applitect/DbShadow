package dbshadow.table;

public interface ColumnTypeMap {
    String to(boolean pkey, String colType, DBType destDBType, int precision, int scale);
}
