package dbshadow.table;

public class OracleColumnTypeMap implements ColumnTypeMap {
    public String to(boolean pkey, String colType, DBType destDbType, int precision, int scale) {
        String newType =  colType;

        if (destDbType == DBType.HSQL) {
            if (colType.equals("NUMBER"))
                newType = "NUMERIC";
            //else if (colType.equals("CHAR"))
                //newType = "VARCHAR";

            if (colType.equals("VARCHAR2"))
                newType = "VARCHAR";

            // These types don't require precision or scale.
            if (newType.equals("INTEGER") ||
                newType.equals("TIME") ||
                newType.equals("DATE") ||
                newType.equals("TIMESTAMP"))
                return newType;
        } else if (destDbType == DBType.MYSQL) {
            if (colType.equals("NUMBER"))
                newType = "NUMERIC";

            if (colType.equals("VARCHAR2"))
                newType = "VARCHAR";

            if (newType.equals("TIME") ||
                newType.equals("DATE") ||
                newType.equals("TIMESTAMP"))
                return newType;
        }

        if (precision <= 0 && scale <= 0)
            ;
        else if (precision > 0 && scale > 0)
            newType += "(" + precision + ","    + scale + ")";
        else
            newType += "(" + precision + ")";

        if (pkey && destDbType != DBType.HSQL && destDbType != DBType.MYSQL)
            newType += " PRIMARY KEY";
        return newType;
    }
}
