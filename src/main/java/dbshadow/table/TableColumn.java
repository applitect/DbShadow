package dbshadow.table;

/**
 * TableColumn represents a single column of a table. This data is used by {@link MetaData}
 * to represent an entire table.
 * @author matt
 *
 */
public class TableColumn {
    private String name;
    private int jdbcType;
    private String type;
    private int precision;
    private int scale;
    private boolean pkey;

    TableColumn(String name, int jdbcType, String type, int precision, int scale, boolean pkey) {
        this.name = name;
        this.setJdbcType(jdbcType);
        this.type = type;
        this.precision = precision;
        this.scale = scale;
        this.setPkey(pkey);
    }

    /**
     * Returns the {@link String} representation of the column's type.
     *
     * Example: VARCHAR CHAR INTEGER BLOB etc...
     * @return
     */
    public String getType() {
        return type;
    }

    /**
     * Allows the ability to override what type the field is in order to
     * get a particular result from getTypeWithPrecision().
     * @param type
     */
    public void setType(String type) {
        this.type = type;
    }

    /**
     * Returns the column's name.
     * @return
     */
    public String getName() {
        return name;
    }

    /**
     * Allows the column name to be overriden.
     * @param name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Convert the current column type to the new database column type
     * by using the defined ColumnTypeMap for the dest db.
     * @see ColumnTypeMap
     * @see DBType
     * @param dest
     * @return
     */
    public String getTypeWithPrecision(DBType src, DBType dest) {
        return src.getTypeMap().to(pkey, type, dest, precision, scale);
    }

    public int getJdbcType() {
        return jdbcType;
    }

    public void setJdbcType(int jdbcType) {
        this.jdbcType = jdbcType;
    }

    public boolean isPkey() {
        return pkey;
    }

    public void setPkey(boolean pkey) {
        this.pkey = pkey;
    }
}
