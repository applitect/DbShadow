package dbshadow.table;

import dbshadow.log4j.XLevel;
import dbshadow.stats.StatCollector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class TableWriter {
    private static final Logger log = LogManager.getLogger(TableWriter.class);

    // How many rows do we insert before committing the data.
    private static final int COMMIT_COUNT = 10000;


    private final String sql;
    private final Connection sourceConn;
    private final MetaData meta;
    private StatCollector stat;

    public TableWriter(String sql, Connection sourceConn)
      throws SQLException, IOException {
        this.sql = sql;
        this.sourceConn = sourceConn;
        meta = MetaDataRetriever.getMeta(sql, sourceConn);
        stat = new StatCollector();
    }

    public TableWriter(String sql, Connection sourceConn, StatCollector stat)
      throws SQLException, IOException {
        this(sql, sourceConn);
        this.stat = stat;
    }

    public void writeDataCreate(String destTableName, Connection destConn)
      throws SQLException {
        try {
            TableCreator.dropTable(destTableName, destConn);
            stat.tableDropped();
        } catch (SQLException ignored) {
        }
        TableCreator creator = new TableCreator(meta);
        creator.createTable(destTableName, destConn);
        stat.tableCreated();

        writeData(destTableName, destConn);
    }

    public void writeDataDrop(String destTableName, Connection destConn)
      throws SQLException {
        TableCreator.dropTable(destTableName, destConn);
        stat.tableDropped();
        writeData(destTableName, destConn);
    }

    public void writeDataTrunc(String destTableName, Connection destConn)
      throws SQLException {
        TableCreator.truncTable(destTableName, destConn);
        stat.tableTruncated();
        writeData(destTableName, destConn);
    }

    public void writeData(String destTableName, Connection destConn)
            throws SQLException {
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            // To download the entire result set it is supposedly much
            // faster to do forward only. This means that we can only
            // call rs.next() and not rs.first(). So we must make sure
            // that rs.next() is not called before we start downloading
            // the table. This is called in populateTable.
            pstmt = sourceConn.prepareStatement(sql,
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

            log.log(XLevel.DEBUG2, "Querying <SOURCE> data...");
            pstmt.setFetchSize(10000);
            pstmt.setMaxRows(0);

            stat.startInitialFetch();
            rs = pstmt.executeQuery();

            // If no data was found quit.
            if (rs == null) {
                log.warn("No data found in <SOURCE> using sql statement:" + sql);
                return;
            }
            stat.completeInitialFetch();

            log.log(XLevel.DEBUG2, "Processing <SOURCE> data...");

            log.log(XLevel.DEBUG2, "Populating table with data...");
            populateTable(destTableName, rs, destConn);
        } finally {
            try {
                if (rs != null)
                    rs.close();
                if (pstmt != null)
                    pstmt.close();
            } catch (SQLException ignored) {
            }
        }

        stat.done();
    }

    private void populateTable(String destTableName, ResultSet rs,
            Connection destConn)
            throws SQLException {
        PreparedStatement pstmt = null;
        try {
            // We do not want to commit every row for performance reasons.
            destConn.setAutoCommit(false);

            // Prepare the insert stmt.
            pstmt = destConn.prepareStatement(genInsertStmt(destTableName,
                    meta.getColumns()));

            // Count the number of records that we have imported.
            int count = 0;

            // Run the population.
            System.out.print("Populating table...\n\t[");
            System.out.flush();

            // Since the recordSet is passed in we need to make sure that
            // we are starting at the first record.
            // If no records exist rs.first() will return false
            while (rs.next()) {
                // Clear any params that have been set in the pstmt.
                pstmt.clearParameters();

                // Populate the insert statement with data.
                for (int i = 0; i < meta.getColumns().size(); i++) {
                    Object o = rs.getObject(meta.getColumns().get(i).getName());

                    // TODO add a command line parameter to allow converting empty columns to NULL and trim. Useful between database
                    // types such as CHAR to VARCHAR.
//                    if (o != null && o instanceof String) {
//                        String val = (String) o;
//                        val = safeTrim(val);
//                        if (val.length() == 0)
//                            o = null;
//                        else
//                            o = val;
//                    }

                    // TODO get Oracle specific logic added back after linking to Oracle driver.
//                    if (o != null && o instanceof oracle.sql.TIMESTAMP)
//                        pstmt.setObject(i + 1, new java.sql.Timestamp(
//                            ((oracle.sql.TIMESTAMP)o).timestampValue().getTime()));
//                    else
                        pstmt.setObject(i + 1, o);
                }

                // Run the prepared statement to insert the row and
                // increment the appropriate counters.
                pstmt.executeUpdate();
                count++;
                stat.recordInserted();
                if ((count % 1000) == 0) {
                    System.out.print("#");
                    System.out.flush();
                }

                // If the count is evenly divisible by the COMMIT_COUNT
                // we need to commit the data. We do commits this way for
                // performance reasons on big loads.
                if ((count % COMMIT_COUNT) == 0) {
                    System.out.print("]    Committing " + COMMIT_COUNT + " of " + count
                            + " records.\n\t[");
                    System.out.flush();
                    destConn.commit();
                    stat.commit();
                }
            }

            // Run a final commit and return the connection's Auto Commit
            // parameter to true.
            System.out.println("]    Committed " + count + " records\n");
            destConn.commit();
            stat.commit();
            destConn.setAutoCommit(true);
        } finally {
            try {
                if (pstmt != null)
                    pstmt.close();
            } catch (SQLException ignored) {
            }
        }
    }

    private static String genInsertStmt(String tableName,
            List<TableColumn> columns) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO " + tableName + " (\n");

        log.log(XLevel.DEBUG2, "Generating Insert Statement");
        for (int i = 0; i < columns.size(); i++) {
            if (i == columns.size() - 1)
                sb.append("\t" + columns.get(i).getName() + "\n");
            else
                sb.append("\t" + columns.get(i).getName() + ",\n");
        }

        sb.append("\n)");

        sb.append("VALUES (\n");
        for (int i = 0; i < columns.size(); i++) {
            if (i == columns.size() - 1)
                sb.append("\t?\n");
            else
                sb.append("\t?,\n");
        }
        sb.append("\n)");
        log.log(XLevel.DEBUG2, "sql statement: " + sb);
        return sb.toString();
    }
}
