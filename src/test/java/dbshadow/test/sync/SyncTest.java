package dbshadow.test.sync;
//
//import java.io.IOException;
//import java.math.BigDecimal;
//import java.sql.Connection;
//import java.sql.PreparedStatement;
//import java.sql.ResultSet;
//import java.sql.SQLException;
//import java.util.List;
//
//import org.hibernate.HibernateException;
//import org.hibernate.jdbc.Work;
//import dbshadow.sync.DataSync;
//import dbshadow.table.MetaData;
//import dbshadow.table.MetaDataRetriever;
//import dbshadow.table.TableColumn;
//import dbshadow.table.TableCreator;
//import dbshadow.table.TableWriter;
//import dbshadow.test.SrcDest;
//import org.testng.annotations.Test;
//
///**
// * @author matt
// *
// */
//public class SyncTest extends SrcDest {
//    @Test
//    public void freshSync() throws HibernateException, SQLException, IOException {
//        try {
//            TableWriter tableWriter = new TableWriter("select * from test", srcConn);
//            tableWriter.writeDataCreate("test", destConn);
//        } catch (IOException e) {
//            assert false : "Table creation failure";
//        }
//
//        assert DataSync.sync("select * from test", "test",
//            srcConn, destConn, false);
//
//        compareAll("test");
//    }
//
//    private void compareAll(String table)
//      throws SQLException, IOException {
//
//        MetaData meta = MetaDataRetriever.getMeta("select * from " + table, srcConn);
//        String sql = getSelectAll(meta, srcConn);
//
//        PreparedStatement p1 = srcConn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
//        ResultSet rsSrc = p1.executeQuery();
//        PreparedStatement p2 = destConn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
//        ResultSet rsDest = p2.executeQuery();
//
//        addCloseMe(p1);
//        addCloseMe(p2);
//        addCloseMe(rsSrc);
//        addCloseMe(rsDest);
//
//        compareAll(meta, rsSrc, rsDest);
//    }
//
//    private void compareAll(MetaData meta, ResultSet src, ResultSet dest)
//      throws SQLException {
//        int count = 0;
//        while (true) {
//            boolean srcNext = src.next();
//            boolean destNext = dest.next();
//
////            if (count++ % 100 == 0)
////                System.out.println("Comparing row: " + src.getRow());
//
//            if (!srcNext && !destNext)
//                break;
//            else if (srcNext ^ destNext)
//                assert false : "Data sets do not have the same number of rows.";
//
//            for (TableColumn col : meta.getColumns()) {
//                Object srcObj = src.getObject(col.getName());
//                Object destObj = dest.getObject(col.getName());
//
//                // DB2 compare fix... No nulls in there.
//                if (srcObj != null && srcObj.toString().trim().equals(""))
//                    srcObj = null;
//                if (destObj != null && destObj.toString().trim().equals(""))
//                    destObj = null;
//
//                if ((srcObj == null && destObj != null) || (srcObj != null && destObj == null))
//                    assert false : srcObj + " not equal to " + destObj;
//                else if (srcObj != destObj) {
//                    if (srcObj instanceof java.sql.Timestamp) {
//                        java.sql.Timestamp a = ((java.sql.Timestamp)srcObj);
//                        if (destObj instanceof java.sql.Timestamp) {
//                            java.sql.Timestamp b = ((java.sql.Timestamp)destObj);
//                            assert a.getTime() == b.getTime();
//                        } else if (destObj instanceof java.sql.Date) {
//                            java.sql.Date b = ((java.sql.Date)destObj);
//                            assert a.getTime() == b.getTime();
//                        } else {
//                            assert false : "Unable to compare " + srcObj.getClass() + " with " + destObj.getClass();
//                        }
//                    } else if (srcObj instanceof oracle.sql.TIMESTAMP) {
//                        java.sql.Timestamp a = ((oracle.sql.TIMESTAMP)srcObj).timestampValue();
//                        if (destObj instanceof java.sql.Timestamp) {
//                            java.sql.Timestamp b = ((java.sql.Timestamp)destObj);
//                            assert a.getTime() == b.getTime();
//                        } else if (destObj instanceof java.sql.Date) {
//                            java.sql.Date b = ((java.sql.Date)destObj);
//                            assert a.getTime() == b.getTime();
//                        } else {
//                            assert false : "Unable to compare " + srcObj.getClass() + " with " + destObj.getClass();
//                        }
//                    } else if (srcObj instanceof BigDecimal && destObj instanceof BigDecimal) {
//                        assert ((BigDecimal)srcObj).compareTo((BigDecimal)destObj) == 0;
//                    } else
//                        assert srcObj.equals(destObj) : src.getRow() + ":" + col.getName() + " '" + srcObj + "' not equal to '" + destObj + "' " + srcObj.getClass() + " " + destObj.getClass();
//                }
//            }
//        }
//    }
//
//    @Test(dependsOnMethods={"freshSync"})
//    public void notFreshSync() throws HibernateException, SQLException, IOException {
//        freshSync();
//
//        Connection dCon = dest.getConnection();
//        addCloseMe(dCon);
//
//        assert ((Integer)destWrap.tw(new DeleteRandom(dCon))) == 1;
//        List<Object[]> l = (List<Object[]>) destWrap.tw(new SelectAll("test", dCon));
//        assert l.size() == 7;
//
//        freshSync();
//
//        assert ((Integer)destWrap.tw(new DeleteRandom(dCon))) == 1;
//        assert ((Integer)destWrap.tw(new DeleteRandom(dCon))) == 1;
//        assert ((Integer)destWrap.tw(new DeleteRandom(dCon))) == 1;
//        l = (List<Object[]>) destWrap.tw(new SelectAll("test", dCon));
//        assert l.size() == 5;
//
//        freshSync();
//    }
//
//    @Test(dependsOnMethods={"notFreshSync"})
//    public void trunc() throws HibernateException, SQLException {
//        // Verify that data exists.
//        Connection dCon = dest.getConnection();
//        addCloseMe(dCon);
//        assert ((List)destWrap.tw(new SelectAll("test", dCon))).size() > 0;
//
//        try {
//            TableCreator.truncTable("test", destConn);
//            assert ((List)destWrap.tw(new SelectAll("test", dCon))).size() == 0;
//        } catch (SQLException e) {
//            assert false : "Table trunc failure " + e.getMessage();
//        }
//    }
//
//    @Test(dependsOnMethods={"trunc"})
//    public void drop() throws HibernateException, SQLException {
//        // Verify that the table exists.
//        Connection dCon = dest.getConnection();
//        addCloseMe(dCon);
//        destWrap.tw(new SelectAll("test", dCon));
//
//        try {
//            TableCreator.dropTable("test", destConn);
//        } catch (SQLException e) {
//            assert false : "Table drop failure " + e.getMessage();
//        }
//    }
//
//    @Test(dependsOnMethods={"drop"})
//    public void delete() throws HibernateException, SQLException, IOException {
//        freshSync();
//
//        srcWrap.tw(new DeleteId(10));
//        dataSyncDelete();
//        compareAll("test");
//
//        srcWrap.tw(new DeleteId(30));
//        dataSyncDelete();
//        compareAll("test");
//
//        srcWrap.tw(new DeleteId(20));
//        dataSyncDelete();
//        compareAll("test");
//
//        srcWrap.tw(new DeleteId(20));
//        dataSyncDelete();
//        compareAll("test");
//    }
//
//    public void dataSyncDelete() {
//        try {
//            assert DataSync.delete("select id, name, date, date2, date3  from test", "test", srcConn, destConn, true, 3);
//        } catch (Throwable e) {
//            e.printStackTrace();
//            assert false : e.getMessage();
//        }
//    }
//
//    private String getSelectAll(MetaData meta, Connection c)
//      throws SQLException, IOException {
//        StringBuilder sql = new StringBuilder();
//        sql.append(DataSync.genSelWhereStmt(
//            meta.getTableName(), meta, false));
//        sql.append("ORDER BY ");
//        for (String key : meta.getPkeys())
//            sql.append(key).append(",");
//        char ch = sql.charAt(sql.length() - 1);
//        if (ch == ',')
//            sql.deleteCharAt(sql.length() - 1);
//        return sql.toString();
//    }
//
//}
