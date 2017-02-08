package dbshadow.table;

import java.util.HashMap;
import java.util.Map;

public class DB2ColumnTypeMap implements ColumnTypeMap {
    private static Map<String, String> hsql =
            new HashMap<String, String>();
    private static Map<String, String> oracle =
        new HashMap<String, String>();
    static {
        oracle.put("CHAR", "CHAR");
        oracle.put("VARCHAR", "VARCHAR2");
        oracle.put("SMALLINT", "NUMBER");
        oracle.put("INTEGER", "NUMBER");
        oracle.put("FLOAT", "NUMBER");
        oracle.put("DOUBLE", "NUMBER");
        oracle.put("DECIMAL", "NUMBER");
        oracle.put("DATE", "DATE");
        oracle.put("TIMESTAMP", "TIMESTAMP");
        oracle.put("TIME", "TIMESTAMP");

        hsql.put("CHAR", "CHAR");
        hsql.put("VARCHAR", "VARCHAR2");
        hsql.put("SMALLINT", "NUMERIC");
        hsql.put("INTEGER", "INTEGER");
        hsql.put("FLOAT", "NUMERIC");
        hsql.put("DOUBLE", "NUMERIC");
        hsql.put("DECIMAL", "NUMERIC");
        hsql.put("DATE", "DATE");
        hsql.put("TIMESTAMP", "TIMESTAMP");
        hsql.put("TIME", "TIME");

        // TODO need to add other DB types if needed
    }

    public String to(boolean pkey, String colType, DBType destDbType, int precision, int scale) {
        String newType = colType;

        if (destDbType == DBType.Oracle) {
            newType = oracle.get(colType);
            if (newType.equals("TIMESTAMP") ||
                newType.equals("DATE"))
                return newType;
        } else if (destDbType == DBType.HSQL) {
            newType = hsql.get(colType);
            if (newType.equals("TIMESTAMP") ||
                    newType.equals("DATE"))
                    return newType;
        }

        if (precision <= 0 && scale <= 0)
            return newType;
        else if (precision > 0 && scale > 0)
            return newType + "(" + precision + ","	+ scale + ")";
        else
            return newType + "(" + precision + ")";

    }
}
