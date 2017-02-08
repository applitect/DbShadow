package dbshadow.table;

public class MysqlColumnTypeMap implements ColumnTypeMap {
    public String to(boolean pkey, String colType, DBType destDbType, int precision, int scale) {
        String newType =  colType;

        if (destDbType == DBType.HSQL) {
            if (colType.equals("NUMBER"))
                newType = "NUMERIC";
            //else if (colType.equals("CHAR"))
                //newType = "VARCHAR";

            // These types don't require precision or scale.
            if (newType.equals("INTEGER") ||
                newType.equals("TIME") ||
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

        if (pkey && destDbType != DBType.HSQL)
            newType += " PRIMARY KEY";
        return newType;
    }
}
