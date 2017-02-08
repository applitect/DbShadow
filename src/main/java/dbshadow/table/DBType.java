package dbshadow.table;

/**
 * DBType is a first attempt at a database type conversion utility. It stores
 * a {@link ColumnTypeMap} for each DBType.
 * @author matt
 *
 */
public enum DBType {
    DB2(new DB2ColumnTypeMap()),
    Oracle(new OracleColumnTypeMap()),
    HSQL(new HSQLColumnTypeMap()),
    MYSQL(new MysqlColumnTypeMap()),
    Unknown(new NoMapColumnTypeMap());

    private ColumnTypeMap typeMap;

    private DBType(ColumnTypeMap typeMap) {
        this.typeMap = typeMap;
    }

    /**
     * Coverts the productName given by {@link java.sql.DatabaseMetaData}
     * to a DBType.
     * @param productName
     * @return
     */
    public static DBType getType(final String productName) {
        final String dbProduct = productName.toLowerCase();
        if (dbProduct.contains("hsql"))
            return DBType.HSQL;
        else if (dbProduct.contains("oracle"))
            return DBType.Oracle;
        else if (dbProduct.contains("db2"))
            return DBType.DB2;
        else if (dbProduct.contains("mysql"))
            return DBType.MYSQL;
        else
            assert false : "Unknown database type: " + dbProduct;
        return Unknown;
    }

    /**
     * Retrieve the {@link ColumnTypeMap} for this DBType.
     * @return
     */
    public ColumnTypeMap getTypeMap() {
        return typeMap;
    }
}
