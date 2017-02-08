package dbshadow.table;

import dbshadow.log4j.XLevel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class TableCreator {
    private static final Logger log = LogManager.getLogger(TableCreator.class);

    private static final String CREATE = "CREATE TABLE ";
    private static final String DROP = "DROP TABLE ";
    private static final String TRUNC = "TRUNCATE TABLE ";

    private MetaData meta;

    public TableCreator(MetaData meta) {
        this.meta = meta;
    }

    public void createTable(String tableName, Connection destConn)
      throws SQLException {
        PreparedStatement pstmtCreate = null;
        try {
            // Some database drivers require us to be in a transaction to
            // perform a commit.
            destConn.setAutoCommit(false);

            // Create the table
            log.info("Creating table " + tableName);
            final String createSQL = genCreateScript(
                tableName, meta, DBType.getType(destConn.getMetaData().getDatabaseProductName()));
            log.info("Preparing create statement");
            pstmtCreate = destConn.prepareStatement(createSQL);
            pstmtCreate.execute();
        } finally {
            // Commit the table creation to the database now. This would also
            // happen when the connection closed, but we allow the class that
            // passed the connection to close it.
            destConn.commit();
            destConn.setAutoCommit(true);

            try {
                if (pstmtCreate != null) pstmtCreate.close();
            } catch (SQLException ignored) {}
        }
    }

    public static void dropTable(String tableName, Connection con)
      throws SQLException {
        truncOrDropTable(tableName, con, true);
    }

    public static void truncTable(String tableName, Connection con)
      throws SQLException {
        truncOrDropTable(tableName, con, false);
    }

    // We have to pass the table name in since Oracle does not include it
    // in their implementation of the java api.
    public static void truncOrDropTable(String tableName, Connection con, boolean drop)
      throws SQLException {
        // Truncate the table if it exists before we add data.
        // This may not be the desired functionality and may have to be
        // removed.
        PreparedStatement pstmtTrunc = null;
        try {
            if (drop) {
                log.info("Dropping the existing table...");
                pstmtTrunc = con.prepareStatement(DROP + tableName);
            } else {
                log.info("Truncate the existing table...");
                pstmtTrunc = con.prepareStatement(TRUNC + tableName);
            }
            pstmtTrunc.execute();
        } finally {
            if (pstmtTrunc != null)
                pstmtTrunc.close();
        }
    }

    private String genCreateScript(String tableName, MetaData meta, DBType destType)
      throws SQLException {
        StringBuilder sb = new StringBuilder();

        if (destType == DBType.HSQL)
            sb.append("CREATE CACHED TABLE " + tableName + " (\n");
        else
            sb.append(CREATE + tableName + " (\n");

        int colCount = meta.getColumns().size();

        for (int i = 0; i < colCount; i++) {
            TableColumn column = meta.getColumns().get(i);
            sb.append("\t" + column.getName() + " " +
                      column.getTypeWithPrecision(meta.getDBType(), destType));
            sb.append(",\n");
        }

        boolean pkExist = false;
        for (TableColumn column : meta.getColumns()) {
            if (column.isPkey())
                pkExist = true;
        }

        if ((destType == DBType.Oracle ||
             destType == DBType.HSQL ||
             destType == DBType.MYSQL) && pkExist) {

            sb.append("\n");
            sb.append("PRIMARY KEY (");
            for (TableColumn column : meta.getColumns()) {
                if (column.isPkey())
                    sb.append(column.getName()).append(",");
            }
            sb.deleteCharAt(sb.lastIndexOf(","));
            sb.append(")");
        } else {
            sb.deleteCharAt(sb.lastIndexOf(","));
        }
        sb.append(")");

        log.log(XLevel.DEBUG2, sb.toString());
        return sb.toString();
    }
}
