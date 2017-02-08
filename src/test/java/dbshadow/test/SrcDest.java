package dbshadow.test;
//
//import java.sql.Connection;
//import java.sql.PreparedStatement;
//import java.sql.ResultSet;
//import java.sql.SQLException;
//import java.util.ArrayList;
//import java.util.Iterator;
//import java.util.List;
//
//import org.apache.log4j.Level;
//import org.apache.log4j.Logger;
//import org.hibernate.HibernateException;
//import org.hibernate.Session;
//
//@RunWith(JUnitPlatform.class)
//public class SrcDest {
//    private static final Logger log = Logger.getLogger(SrcDest.class);
//
//    public static HibernateDBConn srcDBConn;
//    public static HibernateDBConn destDBConn;
//
//    protected static Session src;
//    protected static Session dest;
//    protected static Connection srcConn;
//    protected static Connection destConn;
//    protected static tw srcWrap;
//    protected static tw destWrap;
//
//    private List<ResultSet> resultSets = new ArrayList<ResultSet>();
//    private List<PreparedStatement> pstmts = new ArrayList<PreparedStatement>();
//    private List<Connection> conns = new ArrayList<Connection>();
//
//    @BeforeSuite
//    public void startup() {
//        //Logger.getLogger("org.hibernate").setLevel(Level.DEBUG);
//        srcDBConn = new HibernateDBConn();
//        srcDBConn.setConfigFile("src.hibernate.cfg.xml");
//        srcDBConn.startup();
//        assert srcDBConn != null;
//
//        destDBConn = new HibernateDBConn();
//        destDBConn.setConfigFile("dest.hibernate.cfg.xml");
//        destDBConn.startup();
//        assert destDBConn != null;
//
//        log.setLevel(XLevel.DEBUG4);
//    }
//
//    @BeforeTest
//    public void before() throws SQLException {
//        src = srcDBConn.getSession();
//        assert src != null;
//        assert src.isConnected();
//
//        srcConn = srcDBConn.getConnection();
//        assert srcConn != null;
//        try {
//            assert !srcConn.isClosed();
//        } catch (SQLException e) {
//            assert false : e.getMessage();
//        }
//
//        srcWrap = new tw(srcDBConn);
//        assert srcWrap != null;
//
//        dest = destDBConn.getSession();
//        assert dest != null;
//        assert dest.isConnected();
//
//        destConn = destDBConn.getConnection();
//        assert destConn != null;
//        try {
//            assert !destConn.isClosed();
//        } catch (SQLException e) {
//            assert false : e.getMessage();
//        }
//
//        destWrap = new tw(destDBConn);
//        assert destWrap != null;
//
//        assert srcConn != destConn : "Database connections should not be the same!";
//    }
//
//    @AfterTest
//    public void after() {
//        Iterator<PreparedStatement> it = pstmts.iterator();
//        while (it.hasNext()) {
//            PreparedStatement p = it.next();
//            it.remove();
//
//            assert p != null;
//            try {
//                p.close();
//            } catch (SQLException e) {
//                assert false : e.getMessage();
//            }
//        }
//
//        Iterator<ResultSet> rit = resultSets.iterator();
//        while (rit.hasNext()) {
//            ResultSet rs = rit.next();
//            rit.remove();
//
//            assert rs != null;
//            try {
//                rs.close();
//            } catch (SQLException e) {
//                assert false : e.getMessage();
//            }
//        }
//
//        Iterator<Connection> cit = conns.iterator();
//        while (cit.hasNext()) {
//            Connection c = cit.next();
//            cit.remove();
//
//            assert c != null;
//            try {
//                c.close();
//            } catch (SQLException e) {
//                assert false : e.getMessage();
//            }
//        }
//
//        close(src, dest);
//
//        assert !src.isOpen();
//        assert !dest.isOpen();
//
//        srcWrap = null;
//        destWrap = null;
//        srcConn = null;
//        destConn = null;
//    }
//
//    @AfterSuite
//    public void shutdown() {
//        srcDBConn.shutdown();
//        destDBConn.shutdown();
//    }
//
//    private final void close(Session... sessions) {
//        for (Session s : sessions) {
//            try {
//                assert s.isOpen();
//            } catch (HibernateException ignored) {
//                assert false : ignored.getMessage();
//            }
//            if (s.isOpen())
//                s.close();
//        }
//    }
//
//    protected void addCloseMe(PreparedStatement p) {
//        pstmts.add(p);
//    }
//
//    protected void addCloseMe(ResultSet rs) {
//        resultSets.add(rs);
//    }
//
//    protected void addCloseMe(Connection c) {
//        conns.add(c);
//    }
//}
