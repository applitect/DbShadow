package dbshadow.table;


public class NoMapColumnTypeMap implements ColumnTypeMap {
    public String to(boolean pkey, String colType, DBType destDbType, int precision, int scale) {
        String newType = colType;

        if (precision <= 0 && scale <= 0)
            return newType;
        else if (precision > 0)
            return newType + "(" + precision + ")";
        else
            return newType + "(" + precision + ","	+ scale + ")";
    }
}
