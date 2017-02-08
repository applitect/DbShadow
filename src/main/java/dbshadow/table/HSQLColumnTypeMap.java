package dbshadow.table;

public class HSQLColumnTypeMap implements ColumnTypeMap {
    public String to(boolean pkey, String colType, DBType destDbType, int precision, int scale) {
        String newType = colType;

        if (destDbType == DBType.Oracle) {
            if (colType.equals("INTEGER"))
                newType = "NUMBER";
        } else if (destDbType == DBType.MYSQL) {
            if (colType.equals("CHARACTER"))
                newType = "VARCHAR";
        }

        // These types don't require precision or scale.
        if (newType.equals("INTEGER") ||
            newType.equals("TIME") ||
            newType.equals("DATE") ||
            newType.equals("TIMESTAMP")) {

            if (pkey && destDbType != DBType.HSQL)
                newType += " PRIMARY KEY";
            return newType;
        }

        if (precision <= 0 && scale <= 0)
            ;
        else if (precision > 0 && scale > 0)
            newType += "(" + precision + ","	+ scale + ")";
        else
            newType += "(" + precision + ")";


        if (pkey && destDbType != DBType.HSQL)
            newType += " PRIMARY KEY";
        return newType;
    }
}
