package dbshadow.sync;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import dbshadow.log4j.XLevel;
import dbshadow.table.DBType;
import dbshadow.table.MetaData;
import dbshadow.table.TableColumn;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ResultSetEnhancer {
    private static final Logger log = LogManager.getLogger(ResultSetEnhancer.class);
    private DBType dbType;
    private MetaData meta;
    private PreparedStatement pstmt;
    private PreparedStatement delPstmt;
    private ResultSet rs;
    private int current = 0;
    private int fetchSize = 1000;

    public ResultSetEnhancer(DBType dbType, Connection conn, String tableName, MetaData meta)
      throws SQLException {
        this.dbType = dbType;

        // Oracle does some crappy stuff with ScrollableResultSets that force
        // us to work around them.
        final String selectSql;
        if (dbType == DBType.Oracle) {
            selectSql = genSelPrimKeyStmtOracle(tableName, meta);

            // Use a direct delete statement to oracle since we have to
            // maintain the row location directly.
            final String deleteSQL = DataSync.genDelWhereStmt(tableName, meta);
            log.log(XLevel.DEBUG2, "Dest Delete Stmt: " + deleteSQL);
            delPstmt = conn.prepareStatement(deleteSQL);

            this.meta = meta;
        } else {
            selectSql = genSelPrimKeyStmt(tableName, meta);
        }

        // Build the pstmt to query for the keys
        log.log(XLevel.DEBUG2, "Primary Key Fetch Stmt: " + selectSql);
        this.pstmt = conn.prepareStatement(selectSql,
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_UPDATABLE);

        fetch();

        if (dbType != DBType.Oracle)
            rs.first();
    }

    public boolean absolute(int i)
      throws SQLException {
        // Refetch if we are going beyond the boundary of Oracle.
        if (dbType == DBType.Oracle) {
            current = i;
            fetch();
            if (rs != null)
                return true;
            return false;
        }

        return rs.absolute(i);
    }

    public boolean first()
      throws SQLException {
        if (dbType == DBType.Oracle) {
            current = 1;
            fetch();
            if (rs != null)
                return true;
            return false;
        }

        return rs.first();
    }

    public boolean relative(int i)
      throws SQLException {
        // Refetch if we are going beyond the boundary of Oracle.
        if (dbType == DBType.Oracle) {
            current += i;
            if (i < 0 || (current) > (current - i + fetchSize)) {
                fetch();
                if (rs != null)
                    return true;
                return false;
            }
        }

        return rs.relative(i);
    }

    public void deleteRow()
      throws SQLException {
        if (dbType == DBType.Oracle) {
            delPstmt.clearParameters();
            int keyCount = 0;
            for (String column : meta.getPkeys()) {
                final Object o = rs.getObject(column);
                delPstmt.setObject(keyCount + 1, o);
                keyCount++;
            }
            delPstmt.execute();

            absolute(current - 1);
            return;
        }

        rs.deleteRow();
    }

    public boolean next()
      throws SQLException {
        // Refetch if we are going beyond the boundary of Oracle.
        if (dbType == DBType.Oracle) {
            current++;
            if ((current) > (current - 1 + fetchSize)) {
                fetch();
                if (rs != null)
                    return true;
                return false;
            }
        }
        return rs.next();
    }

    public int getRow()
      throws SQLException {
        if (dbType == DBType.Oracle)
            return current;
        return rs.getRow();
    }

    public Object getObject(String s)
      throws SQLException {
        return rs.getObject(s);
    }

    public void close()
      throws SQLException {
        if (rs != null)
            rs.close();
    }

    private void fetch()
      throws SQLException {
        if (dbType == DBType.Oracle) {
            pstmt.setInt(1, current);
            pstmt.setInt(2, current + fetchSize);
            rs = pstmt.executeQuery();
            if (rs != null)
                rs.first();
        } else
            rs = pstmt.executeQuery();
    }

    private String genSelPrimKeyStmtOracle(String tableName, MetaData meta)
      throws SQLException {
        StringBuilder sb = new StringBuilder();
        StringBuilder pk = new StringBuilder();

        int keyCount = 0;
        for (TableColumn column : meta.getColumns()) {
            if (column.isPkey()) {
                String name = column.getName();
                if (keyCount == meta.getPkeys().size() - 1)
                    pk.append("\t" + name + " \n");
                else
                    pk.append("\t" + name + ", \n");
                keyCount++;
            }
        }

        sb.append("SELECT ");
        sb.append(pk);

        sb.append("FROM ");
        sb.append("(");
        sb.append("SELECT " ).append(pk).append(",ROW_NUMBER() OVER (ORDER BY ").append(pk).append(") R FROM ");
        sb.append(tableName + ") WHERE R between ? and ? ");

        sb.append("\n");

        return sb.toString();
    }

    private static String genSelPrimKeyStmt(String tableName, MetaData meta)
            throws SQLException {
        StringBuilder sb = new StringBuilder();
        StringBuilder pk = new StringBuilder();

        int keyCount = 0;
        for (TableColumn column : meta.getColumns()) {
            if (column.isPkey()) {
                String name = "a." + column.getName();
                if (keyCount == meta.getPkeys().size() - 1)
                    pk.append("\t" + name + " \n");
                else
                    pk.append("\t" + name + ", \n");
                keyCount++;
            }
        }

        sb.append("SELECT ");
        sb.append(pk);

        if (meta.getSchemaName() != null && meta.getSchemaName().length() > 0)
            if (meta.getDBType() == DBType.Oracle)
                sb.append("FROM " + tableName + " ORDER BY " + pk);
            else if (meta.getDBType() == DBType.DB2
                    || meta.getDBType() == DBType.MYSQL)
                sb.append("FROM " + meta.getSchemaName() + "." + tableName
                        + " a ORDER BY " + pk);
            else
                sb.append("FROM " + meta.getSchemaName() + "." + tableName
                        + " a ");
        else if (meta.getDBType() == DBType.MYSQL)
            sb.append("FROM " + tableName + " a ORDER BY " + pk);
        else
            sb.append("FROM (select * from " + tableName + " order by "
                    + pk.toString().replaceAll("a.", "") + ") a");

        sb.append("\n");

        // TODO We need to parse the where stmt.
        // sb.append(genSelWhereStmt(tableName, meta) + "\n");
        // sb.append("ORDER BY " + pk);
//        log.log(XLevel.DEBUG2, sb.toString());
        return sb.toString();
    }
}
