package dbshadow;

import dbshadow.exception.IncompleteArgumentException;
import dbshadow.log4j.XLevel;
import dbshadow.sync.DataSync;
import dbshadow.table.TableWriter;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

public class DbShadow {
    private static final Logger log = LogManager.getLogger(DbShadow.class);

    private static SessionFactory srcSessionFactory;
    private static SessionFactory destSessionFactory;


    public static void main(String args[]) {

        DbShadowOpt dbo = null;
        try {
            dbo = new DbShadowOpt().getCommandLineArgs(args);
        } catch (IncompleteArgumentException | IOException | IllegalArgumentException e) {
            System.exit(1);
        }
        final DbShadowOpt opt = dbo;

        // Before doing anything let's make sure our data connections are closed at shutdown.
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                if (srcSessionFactory != null)
                    srcSessionFactory.close();
                if (destSessionFactory != null)
                    destSessionFactory.close();
            }
        });

        // Verbose
        if (opt.getVerbose() != null) {
            switch (opt.getVerbose()) {
            case "2":
                Configurator.setRootLevel(XLevel.DEBUG2);
                break;
            case "3":
                Configurator.setRootLevel(XLevel.DEBUG3);
                break;
            case "4":
                Configurator.setRootLevel(XLevel.DEBUG4);
                break;
            default:
                Configurator.setRootLevel(Level.DEBUG);
                break;
            }
        }

        log.log(XLevel.DEBUG3, "Arguments gathered starting Application...");

        log.info("Start processing.");

        log.log(XLevel.DEBUG3, "Init src database");
        File src = new File(opt.getSrcCfgFilename());
        srcSessionFactory = new Configuration().configure(src).buildSessionFactory();

        // Right now we can only have the destination be an oracle db.
        log.log(XLevel.DEBUG3, "Init dest database");
        File dest = new File(opt.getDestCfgFilename());
        destSessionFactory = new Configuration().configure(dest).buildSessionFactory();

        boolean error = false;
        final String sqlStmt = Optional.ofNullable(opt.getSqlStmt()).orElse("select * from " + opt.getSource());
        try {
            log.log(XLevel.DEBUG3, "sqlStmt: " + sqlStmt);

            switch (opt.getWorkType()) {
            case CREATE:
                log.log(XLevel.DEBUG3, "Creating Table");
                srcSessionFactory.openSession().doWork(srcConn ->
                    destSessionFactory.openSession().doWork(dstConn ->
                    {
                        try {
                            new TableWriter(sqlStmt, srcConn).writeDataCreate(opt.getDest(), dstConn);
                        } catch (IOException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    }));
                break;
            case TRUNC:
                log.log(XLevel.DEBUG3, "Truncating Table");
                srcSessionFactory.openSession().doWork(srcConn ->
                    destSessionFactory.openSession().doWork(dstConn ->
                    {
                        try {
                            new TableWriter(sqlStmt, srcConn).writeDataTrunc(opt.getDest(), dstConn);
                        } catch (IOException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    }));
                break;
            case ADD:
                log.log(XLevel.DEBUG3, "Adding Data");
                srcSessionFactory.openSession().doWork(srcConn ->
                    destSessionFactory.openSession().doWork(dstConn ->
                    {
                        try {
                            new TableWriter(sqlStmt, srcConn).writeData(opt.getDest(), dstConn);
                        } catch (IOException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    }));
                break;
            case SYNC:
                log.log(XLevel.DEBUG3, "Syncing Table");
                srcSessionFactory.openSession().doWork(srcConn ->
                    destSessionFactory.openSession().doWork(dstConn ->
                      DataSync.sync(sqlStmt, opt.getDest(), srcConn, dstConn, opt.getRowCountError(), opt.simulate())));
                break;
            case DELETE:
                log.log(XLevel.DEBUG3, "Mirroring Deleted Rows");
                srcSessionFactory.openSession().doWork(srcConn ->
                    destSessionFactory.openSession().doWork(dstConn ->
                        {
                            try {
                                DataSync.delete(sqlStmt, opt.getDest(), srcConn, dstConn, false,
                                                opt.getPartitionSize());
                            } catch (IOException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }
                        }));
                break;
            default:
                log.error("Ran with no valid work command.");
                break;
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e.fillInStackTrace());
            error = true;
        }

        if (error) {
            log.info("Finished with ERROR.");
            System.exit(-1);
        }

        log.info("Finished Successfully.");
        System.exit(0);
    }
}
