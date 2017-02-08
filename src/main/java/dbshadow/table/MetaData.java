package dbshadow.table;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class MetaData {
    private String schemaName;
    private String tableName;
    private List<TableColumn> columns;
    private DBType type;
    private Set<String> pkeys;

    public MetaData(String table, ResultSetMetaData meta, DatabaseMetaData dbMeta)
      throws SQLException {
        type = DBType.getType(dbMeta.getDatabaseProductName());

        setSchemaName(meta.getSchemaName(1));
        if (type == DBType.Oracle)
            setSchemaName("COMPASS_DATA");
        assert getSchemaName() != null;

        setTableName(meta.getTableName(1));
        if (type == DBType.Oracle)
            if (tableName == null || tableName.length() == 0)
                tableName = table;
        assert getTableName() != null;

        columns = new ArrayList<TableColumn>();

        String catalog = meta.getCatalogName(1);
        if (type == DBType.Oracle)
            catalog = null;
        else if (type == DBType.HSQL)
            catalog = null;
        else if (type == DBType.DB2)
            catalog = null;

        // Store the primary keys at the Table level.
        pkeys = new TreeSet<String>();
        final ResultSet pkeysRs;

        if (type == DBType.Oracle)
            pkeysRs = dbMeta.getPrimaryKeys(catalog, null, getTableName());
        else
            pkeysRs = dbMeta.getPrimaryKeys(catalog, getSchemaName(), getTableName());

        while(pkeysRs.next())
            pkeys.add(pkeysRs.getString("COLUMN_NAME"));

        assert pkeys.size() > 0 : String.format(
            "No pkey result set catalog<%s> schema<%s> table<%s> type<%s>",
            catalog, getSchemaName(), getTableName(), type);

        for (int i = 1; i < meta.getColumnCount() + 1; i++)
            columns.add(
                new TableColumn(
                    meta.getColumnName(i),
                    meta.getColumnType(i),
                    meta.getColumnTypeName(i),
                    meta.getPrecision(i),
                    meta.getScale(i),
                    pkeys.contains(meta.getColumnName(i))));
    }

    public List<TableColumn> getColumns() {
        return columns;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public DBType getDBType() {
        return type;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public Set<String> getPkeys() {
        return pkeys;
    }

    public void setPkeys(Set<String> pkeys) {
        this.pkeys = pkeys;
    }
}
