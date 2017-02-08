package dbshadow.sync;

import dbshadow.log4j.XLevel;
import dbshadow.stats.StatCollector;
import dbshadow.table.MetaData;
import dbshadow.table.MetaDataRetriever;
import dbshadow.table.TableColumn;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;

public class DataSync {
    private static final Logger log = LogManager.getLogger(DataSync.class);
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    private static final SimpleDateFormat sts = new SimpleDateFormat("HH:mm:ss");
//
//    // How many rows do we insert before committing the data.
    private static final int COMMIT_COUNT = 10000;
    private static final int FETCH_SIZE = 1000;
    private static final int MAX_CONSECUTIVE_DELETE_RECORDS = 500;
//
//    /**
//     * @param query
//     * @param destTableName
//     * @param source
//     * @param dest
//     * @param stats
//     * @param appendOnly
//     * @param procId - process id for the process history tracking tables. If
//     *                  value is 0, then no tracking will be done.
//     * @param rowCountError
//     * @return true if completes successfully, false otherwise.
//     * @throws SQLException
//     */
    public static boolean sync(String query, String destTableName,
                               Connection source, Connection dest,
                               boolean rowCountError, boolean simulate)
      throws SQLException {
        return DataSync.syncTable(query, destTableName, source, dest, rowCountError, simulate, new StatCollector());
    }

    private static boolean syncTable(String sql, String destTableName,
                                     Connection sourceCon, Connection destCon,
                                     boolean rowCountError, boolean simulate,
                                     StatCollector callback)
      throws SQLException {
        // Can't sync anything without a query.
        if (sql == null || sql.length() == 0)
            return false;

        ResultSet rsSource = null;

        PreparedStatement pstmtSrcSel = null;
        PreparedStatement pstmtInsert = null;
        PreparedStatement pstmtUpdate = null;
        PreparedStatement pstmtSelectDest = null;
        PreparedStatement pstmtCountSrc = null;
        PreparedStatement pstmtCountDest = null;

        // Needed to count number of records committed in the case when
        // there is a dw timeout.
        int updCommits = 0;
        int insCommits = 0;

        boolean success = true;
        try {
            final MetaData meta = MetaDataRetriever.getMeta(sql, sourceCon);
            assert meta != null;

            // Prepare the insert, update, and select stmts for use.
            pstmtSrcSel = sourceCon.prepareStatement(
                sql,ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_UPDATABLE);
            pstmtInsert = destCon.prepareStatement(
                genInsertStmt(destTableName, meta));
            pstmtUpdate = destCon.prepareStatement(
                genUpdateStmt(destTableName, meta));
            pstmtSelectDest = destCon.prepareStatement(
                genSelWhereStmt(destTableName, meta));
            pstmtCountSrc = sourceCon.prepareStatement(
                    genSelCountRowsStmt(meta.getTableName(), meta.getSchemaName()));
            pstmtCountDest = destCon.prepareStatement(
                    genSelCountRowsStmt(destTableName, ""));

            // We do not want to commit every row for performance reasons.
            destCon.setAutoCommit(false);

            // Count the number of records that we have imported.
            int updated = 0;
            int inserted = 0;
            boolean insertTick = false;
            boolean updateTick = false;
            boolean updateCount = false;
            boolean commitCalled = false;
            int count = 0;
            int tickCount = 0;

            log.info("Syncing table...");
            log.info("Querying the source database");
            log.log(XLevel.DEBUG2, sql);

            System.out.flush();
            System.out.print("\t[");

            StringBuilder srcRecordStr = new StringBuilder();

            pstmtSrcSel.setFetchSize(FETCH_SIZE);
            log.info("Executing SRC database query.");
            rsSource = pstmtSrcSel.executeQuery();
            log.log(XLevel.DEBUG2, "Finished executing SRC database query.");

            // Since the recordSet is passed in we need to make sure that
            // we are starting at the first record and we have some rows.
            if (rsSource.first())
            do {
                // Fill the where clause of the update stmt.
                String sourceKey = "";
                pstmtSelectDest.clearParameters();

                // Populate the where statements for both the select and update.
                int keyCount = 0;
                for (String key : meta.getPkeys()) {
                    Object o = rsSource.getObject(key);

                    // only trim varchar elements for primary key lookup
                    for (TableColumn column : meta.getColumns())
                        if (column.getName().equals(key)) {
                            o = trimDBString(o, column.getJdbcType());

                            sourceKey += o + ",";
                        }


                    // Set where statement to primary keys
                    pstmtSelectDest.setObject(keyCount + 1, o);
                    pstmtUpdate.setObject(keyCount + meta.getColumns().size() + 1, o);

                    log.log(XLevel.DEBUG4, "pstmt: arg[" + (keyCount + 1) + "] . " +
                            "'" + o + "'");

                    keyCount++;
                }

                // Run the prepared statement to query if there is a row
                // with the primary key.
                ResultSet rsDest = pstmtSelectDest.executeQuery();

                if (rsDest == null || !rsDest.next()) {
                    // Insert new record.
                    srcRecordStr.setLength(0);

                    for (int i = 0; i < meta.getColumns().size(); i++) {
                        TableColumn column = meta.getColumns().get(i);
                        Object o = rsSource.getObject(column.getName());

                        // Strings in db2 are not necessarily varchar
                        o = trimDBString(o, column.getJdbcType());

                        pstmtInsert.setObject(i+1, o);

                        if (log.getLevel() == XLevel.DEBUG3) {
                            if (i > 0)
                                srcRecordStr.append("::");

                            srcRecordStr.append(
                                fieldToString(o, column.getJdbcType()));
                        }
                    }

                    if (simulate) {
                        System.out.println("insert: " + srcRecordStr.toString());
                    } else {
                        pstmtInsert.executeUpdate();
                    }

                    log.log(XLevel.DEBUG3, "\nRecord: " +
                            srcRecordStr.toString());

                    log.log(XLevel.DEBUG2, "Inserting <" + sourceKey + ">");
                    inserted++;
                    insertTick = true;

                    pstmtInsert.clearParameters();

                } else {
                    // Update existing record.

                    // Blank out comparison string
                    srcRecordStr.setLength(0);

                    for (int i = 0; i < meta.getColumns().size(); i++) {
                        TableColumn column = meta.getColumns().get(i);
                        Object o = null;

                        if (i > 0)
                            srcRecordStr.append("::");

                        // Calling getObject is very expensive. Major speedups
                        // can be had by calling specific getXXX functions.

                        if (column.getJdbcType() == Types.CHAR ||
                            column.getJdbcType() == Types.VARCHAR) {
                            o = rsSource.getString(column.getName());
                            o = trimDBString(o, column.getJdbcType());
                            srcRecordStr.append((String)o);
                        } else if (column.getJdbcType() == Types.DATE) {
                            Date d = rsSource.getDate(column.getName());
                            o = d;
                            if (o == null)
                                srcRecordStr.append("null");
                            else
                                srcRecordStr.append(sdf.format(d));
                        } else {
                            o = rsSource.getObject(column.getName());
                            srcRecordStr.append(
                                    fieldToString(o, column.getJdbcType()));
                        }

                        pstmtUpdate.setObject(i+1, o);

                    }

                    // We did find a result set record. Look to see if the
                    // records are the same, if not then update.
                    StringBuilder destRecordStr = new StringBuilder();
                    for (int i = 0; i < meta.getColumns().size(); i++) {
                        TableColumn column = meta.getColumns().get(i);

                        Object o = rsDest.getObject(column.getName());
                        o = trimDBString(o, column.getJdbcType());

                        if (i > 0)
                            destRecordStr.append("::");

                        destRecordStr.append(
                            fieldToString(o, column.getJdbcType()));
                    }

                    log.log(XLevel.DEBUG4, "Record comparing---------");
                    log.log(XLevel.DEBUG4, destRecordStr.toString());
                    log.log(XLevel.DEBUG4, srcRecordStr.toString() + "\n");

                    if (!destRecordStr.toString().equalsIgnoreCase(
                            srcRecordStr.toString())) {
                        if (simulate) {
                            System.out.println("update: " + sourceKey);
                        } else {
                            if (pstmtUpdate.executeUpdate() != 1) {
                                count++;
                                updateCount = true;
                            } else {
                                updated++;
                                updateTick = true;
                            }
                        }

                        log.log(XLevel.DEBUG2, "Updating <" + sourceKey + ">");

                        log.log(XLevel.DEBUG3, "\nRecord: " +
                                srcRecordStr.toString());
                    } else {
                        count++;
                        updateCount = true;
                    }

                    pstmtUpdate.clearParameters();
                }

                // If we have updated or inserted COMMIT_COUNT records then
                // we need to commit all transactions.
                if ((updateTick || insertTick) &&
                        ((updated + inserted) % COMMIT_COUNT) == 0) {
                    commitCalled = true;
                    updCommits = updated;
                    insCommits = inserted;
                    destCon.commit();
                }

                if (updateCount && (count % 1000) == 0) {
                    System.out.print(".");
                    System.out.flush();
                    tickCount++;
                    updateCount = false;
                }

                if (updateTick && (updated % 1000) == 0) {
                    System.out.print("#");
                    System.out.flush();
                    tickCount++;
                    updateTick = false;
                }

                if (insertTick && (inserted % 1000) == 0) {
                    System.out.print("^");
                    System.out.flush();
                    tickCount++;
                    insertTick = false;
                }

                if (rsDest != null)
                    rsDest.close();

                if (tickCount > 9) {
                    System.out.print("] Worked on 10000 records.");
                    if (commitCalled) {
                        commitCalled = false;
                        System.out.print("  (Commited " + COMMIT_COUNT + " records) ");
                    }
                    System.out.print("\n\t[");
                    System.out.flush();
                    tickCount= 0;
                }

            }  while (rsSource.next());

            // Get rid of the source data since we don't need it anymore.
            rsSource.close();

            // Run a final commit and return the connection's Auto Commit
            // parameter to true.
            System.out.println("]\n      Updated " + updated + " records\n");
            System.out.println("     Inserted " + inserted + " records\n");
            destCon.commit();
            destCon.setAutoCommit(true);
/*
            CallableStatement cstmt = null;
            if (procId > 0) {
                try {
                    cstmt = stats.prepareCall(
                            "{call prcs_trck_support_pkg.insert_statistic_record(" +
                            "?,?,?)}");
                    cstmt.setInt(1, procId);
                    cstmt.setString(2, "Table Rows Updated");
                    cstmt.setInt(3, updated);
                    cstmt.execute();

                    cstmt.setString(2, "Table Rows Inserted");
                    cstmt.setInt(3, inserted);
                    cstmt.execute();

                } catch (SQLException e) {
                    log.error("Unable to add download statistic information",
                            e.fillInStackTrace());
                } finally {
                    cstmt.close();
                }
            }
*/
            // On an append, we want to warn if the # of table rows
            // doesn't match. Doesn't guarantee that the tables are
            // identical--but does give a heads up if the rows may have
            // been deleted.
            // On an sync, we want to warn if the # of table rows
            // doesn't match. Doesn't guarantee that the tables are
            // identical--but does give a heads up if at some point we
            // are missing rows.
            if (rowCountError) {
                log.info("Querying table row counts.");
                rsSource = pstmtCountSrc.executeQuery();
                if (rsSource != null && rsSource.next()) {
                    BigDecimal sCnt = rsSource.getBigDecimal(1);
                    rsSource = pstmtCountDest.executeQuery();
                    if (rsSource != null && rsSource.next()) {
                        BigDecimal dCnt = rsSource.getBigDecimal(1);
                        if (sCnt.compareTo(dCnt) != 0) {
                            log.info("The src/dest (" + meta.getTableName() + ", " +
                                     destTableName +
                                     ") tables are out of sync. Row " +
                                     "counts are source:" + sCnt.toString() +
                                     " destination:" + dCnt.toString());
                            // # of rows don't match
                            throw new Exception("Row counts don't match.");

                        }
                    }
                }
                log.info("Done querying table row counts.");
            }
        } catch (Exception e) {
            log.error("Unable to process sync.", e);
            success = false;
/*
            CallableStatement cstmt = null;
            if (procId > 0) {
                try {
                    if (insCommits > 0 || updCommits > 0 ) {
                        cstmt = stats.prepareCall(
                                "{call prcs_trck_support_pkg." +
                                "insert_statistic_record(?,?,?)}");
                        cstmt.setInt(1, procId);
                        cstmt.setString(2, "Table Rows Updated");
                        cstmt.setInt(3, updCommits);
                        cstmt.execute();
                        cstmt.setString(2, "Table Rows Inserted");
                        cstmt.setInt(3, insCommits);
                        cstmt.execute();
                        cstmt.close();
                    }

                    cstmt = stats.prepareCall(
                            "{call prcs_trck_support_pkg." +
                            "insert_message_record(?,?,?)}");
                    cstmt.setInt(1, procId);
                    cstmt.setString(2, "ERROR");
                    cstmt.setString(3, StringUtil.truncate(e.toString(),300));
                    cstmt.execute();
                } catch (SQLException e1) {
                    log.error("Unable to add download statistic information",
                            e1.fillInStackTrace());
                } finally {
                    cstmt.close();
                }
            }
            */
        } finally {
            try {
                if (pstmtSrcSel != null) pstmtSrcSel.close();
                if (pstmtInsert != null) pstmtInsert.close();
                if (pstmtUpdate != null) pstmtUpdate.close();
                if (pstmtCountSrc != null) pstmtCountSrc.close();
                if (pstmtCountDest != null) pstmtCountDest.close();
                log.log(XLevel.DEBUG3, "rs/PSTMT closed");
            } catch (SQLException ignored) {
            }
        }

        return success;
    }

    public static Boolean delete(String query, String destTableName,
            Connection source, Connection dest,
            boolean simulate, int partitionSize)
      throws SQLException, IOException {
//        return deleteRecords(query, destTableName, source, dest, new StatCollector(),
//                simulate, partitionSize);
        return null;
    }

    public static Boolean delete(String query, String destTableName,
            Connection source, Connection dest, StatCollector callback,
            boolean simulate,
            int partitionSize)
      throws SQLException, IOException {
        return null;
//        return deleteRecords(query, destTableName, source, dest,
//                callback, simulate, partitionSize);
    }
//
//    private static boolean deleteRecords(String sql, String destTableName,
//            Connection srcCon, Connection destCon, StatCollector callback,
//            boolean simulate,
//            int partitionSize)
//      throws SQLException, IOException {
//        log.info("Removing unknown records...");
//
//        // Need two ResultSetEnhancers, src and dest
//        // These allow us to delete even if the underlying database (ORACLE) doesn't allow fetch by rownum.
//        final MetaData srcMeta = MetaDataRetriever.getMeta(sql, srcCon);
//        final ResultSetEnhancer srcEnhancer =
//            new ResultSetEnhancer(srcMeta.getDBType(), srcCon, srcMeta.getTableName(), srcMeta);
//        final MetaData destMeta = MetaDataRetriever.getMeta("select * from " + destTableName + " where 1 = 2", destCon);
//        final ResultSetEnhancer destEnhancer =
//            new ResultSetEnhancer(destMeta.getDBType(), destCon, destTableName, destMeta);
//
//        // The point in being able to delete records is to locate records
//        // that exist in the destination database, but don't exist in the
//        // source database.
//
//        // To accomplish this goal, I believe we can fetch the records ordered
//        // by primary key in blocks and then compare then end. If the ends
//        // don't match, then fetch all the records in the block and determine
//        // which records should be deleted.
//        // If it happens that src has extra records, then we will report them
//        // out but will not add them to the destination. Typically the tables
//        // should be sync'd first before running this method so this shouldn't
//        // happen.
//        int deletes = 0;
//        try {
//            /*
//             * From here, we will progress down every partitionSize records and
//             * see how fast we can get through the two tables.
//             * Comparing at the end of each block.
//             */
//
//            int hopSize = partitionSize;
//            boolean hop = false;
//            boolean scanning = false;
//            int lastGoodRow = -1;
//            while (true) {
//                if (rowKeyMatch(srcMeta, srcEnhancer, destEnhancer)) {
//                    lastGoodRow = srcEnhancer.getRow();
//
//                    // If the hop size is less than 1 row we can't hop anymore
//                    if (hopSize <= 1)
//                            hop = false;
//
//                    boolean sNext = false;
//                    boolean dNext = false;
//                    if (hop) {
//                        int prevSrc = srcEnhancer.getRow();
//                        int prevDest = destEnhancer.getRow();
//                        sNext = srcEnhancer.relative(hopSize);
//                        dNext = destEnhancer.relative(hopSize);
//                        if (!sNext || !dNext) {
//                            sNext = srcEnhancer.absolute(prevSrc);
//                            dNext = destEnhancer.absolute(prevDest);
//                            if (hopSize > 1) {
//                                // In the case where the partition is larger than the table
//                                // we need to decrease the partiition so we don't keep returning
//                                // to the original size and having to backoff until we are below
//                                // the table size again.
//                                partitionSize /= 2;
//
//                                hopSize /= 5;
//
//                                // Avoid having to compare the row again.
//                                sNext = srcEnhancer.next();
//                                dNext = destEnhancer.next();
//                            }
//                        }
//                    } else {
//                        if (!scanning)
//                            hop = true;
//                        sNext = srcEnhancer.next();
//                        dNext = destEnhancer.next();
//                    }
//
//                    if (sNext && !dNext) {
//                        log.error("Dest is missing records");
//                        break;
//                    } else if (dNext && !sNext) {
//                        log.log(XLevel.DEBUG4, "Delete Dest");
//                        destEnhancer.deleteRow();
//                        break;
//                    } else if (!dNext && !sNext) {
//                        break;
//                    }
//                } else {
//                    if (hop) {
//                        if (!scanning) {
//                            hopSize /= 5;
//                            scanning = true;
//                        } else if (hopSize > 1) {
//                            hopSize /= 5;
//                        } else {
//                            hop = false;
//                        }
//
//                        srcEnhancer.absolute(lastGoodRow + 1);
//                        destEnhancer.absolute(lastGoodRow + 1 + deletes);
//                    } else {
//                        deletes++;
//
//                        // Delete the row from the actual target db.
//                        log.log(XLevel.DEBUG4, "Delete Dest");
//                        destEnhancer.deleteRow();
//
//                        // Since we have successfully deleted a row we can begin hopping again.
//                        scanning = false;
//                        hopSize = partitionSize;
//
//                        // The row only gets scheduled for deletion.
//                        if (!destEnhancer.next())
//                            log.warn("Unable to go next");
//                    }
//                }
//            }
//        } catch (SQLException e) {
//            log.error("SQLState: " + e.getSQLState());
//            log.error("VenderErrorCode: " + e.getErrorCode());
//            log.error("Message: " + e.getMessage());
//            return false;
//        } catch (Exception e) {
//            log.error("Unable to process sync.", e);
//            return false;
//        } finally {
//            try {
//                if (srcEnhancer != null) srcEnhancer.close();
//                if (destEnhancer != null) destEnhancer.close();
//                log.log(XLevel.DEBUG3, "Delete rs/PSTMT closed");
//            } catch (SQLException ignored) {
//            }
//        }
//
//        log.info("Delete Complete!");
//        log.info("Deleted " + deletes + " records");
//        return true;
//    }
//
    /**
     * Determines if the key values of the row are a match.
     * @param meta
     * @param srcEnhancer
     * @param destEnhancer
     * @return
     * @throws SQLException
     */
    private static boolean rowKeyMatch(MetaData meta, ResultSetEnhancer srcEnhancer,
                                    ResultSetEnhancer destEnhancer)
      throws SQLException {
        final StringBuilder srcBuilder = new StringBuilder();
        final StringBuilder destBuilder = new StringBuilder();

        boolean match = true;
        for (TableColumn column : meta.getColumns()) {
            if (column.isPkey()) {
                srcBuilder.append(String.format("<%s> ", srcEnhancer.getObject(column.getName()), srcEnhancer.getRow()));
                destBuilder.append(String.format("<%s> ", destEnhancer.getObject(column.getName()), destEnhancer.getRow()));
            }
            if (column.isPkey() &&
                !fieldToString(srcEnhancer.getObject(column.getName()), column.getJdbcType()).equals(
                   fieldToString(destEnhancer.getObject(column.getName()), column.getJdbcType()))) {

                match = false;
            }
        }

        if (match) {
            log.log(XLevel.DEBUG3, String.format("s(%s)" + srcBuilder.toString() + "\td(%s)" +
                destBuilder.toString(), srcEnhancer.getRow(), destEnhancer.getRow()));
        } else {
            int srcSize = srcBuilder.length();
            int destSize = destBuilder.length();

            destBuilder.insert(0, String.format("\td(%s)", destEnhancer.getRow()));
            for (int i = 0; i < srcSize; i++)
                destBuilder.insert(0, "-");
            log.log(XLevel.DEBUG3, destBuilder.toString());

            srcBuilder.append("\t");
            for (int i = 0; i < destSize; i++)
                srcBuilder.append("-");
            log.log(XLevel.DEBUG3, String.format("s(%s)" + srcBuilder.toString(), srcEnhancer.getRow()));
        }
        return match;
    }

    private static Object trimDBString(Object o, int colType) {
        Object ret = null;
        if (colType == Types.VARCHAR) {
            String val = (String) o;
            val = StringUtils.trimToEmpty(val);
            if (val.length() != 0)
                ret = val;
            return ret;
        } else if (colType == Types.CHAR) {
            // Only determine if it should be null
            if (StringUtils.trimToEmpty((String)o).length() == 0)
                return ret;
        }
        return o;
    }

    private static String fieldToString(Object o, int colType) {
        if (o == null)
            return "null";

        switch (colType) {
        case Types.CHAR:
            return o.toString().trim();
        case Types.DECIMAL:
            // Databases have different number formats so this
            // normalizes them.
            return "" + new Double(o.toString()).doubleValue();
        case Types.TIMESTAMP:
            try {
//                if (o instanceof oracle.sql.TIMESTAMP) {
//                     return ((oracle.sql.TIMESTAMP)o).
//                              timestampValue().toString();
//                }
            } catch (Exception e) {}
            break;
        case Types.TIME:
            try {
//                if (o instanceof oracle.sql.TIMESTAMP) {
//                    java.sql.Timestamp ts = ((oracle.sql.TIMESTAMP) o).timestampValue();
//                    log.log(XLevel.DEBUG3, "Converting oracle timestamp: " +
//                            ts.toString() + " : " + sts.format(ts));
//                    return sts.format(ts);
//                }
            } catch (Exception e) {}
            break;
        case Types.DATE:
            // Do not compare time stamp of date
            if (o instanceof java.util.Date)
                    return sdf.format((java.util.Date)o);

        case Types.BINARY:
            // to perform text comparison of binary we must convert
            if (o instanceof byte[])
                return Base64.getEncoder().encodeToString((byte[]) o);
        }

        return o.toString();
    }

    private static String genSelCountRowsStmt(String tableName, String schema) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT COUNT(*) FROM ");
        if (schema != null && schema.length() > 0)
            sb.append(schema + "." + tableName);
        else
            sb.append(tableName);
        return sb.toString();
    }

    public static String genSelWhereStmt(String tableName, MetaData meta)
      throws SQLException {
        return genSelWhereStmt(tableName, meta, true);
    }

    public static String genSelWhereStmt(String tableName, MetaData meta, boolean where)
      throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");

        for (int i = 0; i < meta.getColumns().size(); i++) {
            TableColumn column = meta.getColumns().get(i);
            if (i == meta.getColumns().size() - 1)
                sb.append("\t" + column.getName() + "\n");
            else
                sb.append("\t" + column.getName() + ",\n");
        }

        sb.append("FROM " + tableName + "\n");

        if (where) {
            sb.append("WHERE ");

            int keyCount = 0;
            for (String key : meta.getPkeys()) {
                if (keyCount == meta.getPkeys().size() - 1)
                    sb.append("\t" + key + " = ?\n");
                else
                    sb.append("\t" + key + " = ? AND \n");
                keyCount++;
            }

        }
        log.log(XLevel.DEBUG2, sb.toString());
        return sb.toString();
    }

    static String genDelWhereStmt(String tableName, MetaData meta)
      throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM ").append(tableName);
        sb.append(" WHERE ");

        int keyCount = 0;
        for (String key : meta.getPkeys()) {
            if (keyCount == meta.getPkeys().size() - 1)
                sb.append("\t" + key + " = ?\n");
            else
                sb.append("\t" + key + " = ? AND \n");
            keyCount++;
        }

        log.log(XLevel.DEBUG2, sb.toString());
        return sb.toString();
    }

    private static String genInsertStmt(String tableName, MetaData meta) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO " + tableName + " (\n");

        log.log(XLevel.DEBUG2, "Generating Insert Statement");
        for (int i = 0; i < meta.getColumns().size(); i++) {
            TableColumn column = meta.getColumns().get(i);

            if (i == meta.getColumns().size() - 1)
                sb.append("\t" + column.getName() + "\n");
            else
                sb.append("\t" + column.getName() + ",\n");
        }
        sb.append("\n)");

        sb.append("VALUES (\n");
        for (int i = 0; i < meta.getColumns().size(); i++) {
            if (i == meta.getColumns().size() - 1)
                sb.append("\t?\n");
            else
                sb.append("\t?,\n");
        }
        sb.append("\n)");
        log.log(XLevel.DEBUG2, sb.toString());
        return sb.toString();
    }

    private static String genUpdateStmt(String tableName, MetaData meta) {
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE " + tableName + " SET \n");

        log.log(XLevel.DEBUG2, "Generating Update Statement");

        for (int i = 0; i < meta.getColumns().size(); i++) {
            TableColumn column = meta.getColumns().get(i);
            if (i == meta.getColumns().size() - 1)
                sb.append("\t" + column.getName() + " = ?\n");
            else
                sb.append("\t" + column.getName() + " = ?,\n");
        }
        sb.append("\n");

        sb.append("WHERE \n");

        int keyCount = 0;
        for (String key : meta.getPkeys()) {
            if (keyCount == meta.getPkeys().size() - 1)
                sb.append("\t" + key + " = ?\n");
            else
                sb.append("\t" + key + " = ? AND \n");
            keyCount++;
        }

        sb.append("\n");
        log.log(XLevel.DEBUG2, sb.toString());
        return sb.toString();
    }
}
