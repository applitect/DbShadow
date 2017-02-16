package dbshadow.test;

import dbshadow.log4j.XLevel;
import org.apache.logging.log4j.core.config.Configurator;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

@RunWith(JUnitPlatform.class)
public class SrcDest {
    public static SessionFactory srcSessionFactory;
    public static SessionFactory destSessionFactory;


    private List<ResultSet> resultSets = new ArrayList<ResultSet>();
    private List<PreparedStatement> pstmts = new ArrayList<PreparedStatement>();

    @BeforeAll
    public void startup() {
        //Logger.getLogger("org.hibernate").setLevel(Level.DEBUG);
        File src = new File("src.hibernate.cfg.xml");
        srcSessionFactory = new Configuration().configure(src).buildSessionFactory();
        
        File dest = new File("dest.hibernate.cfg.xml");
        destSessionFactory = new Configuration().configure(dest).buildSessionFactory();

        Configurator.setRootLevel(XLevel.DEBUG4);
    }

    @AfterAll
    public void shutdown() {
    	srcSessionFactory.close();
    	destSessionFactory.close();
    }
}
