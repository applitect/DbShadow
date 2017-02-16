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
import java.net.URISyntaxException;
import java.net.URL;

@RunWith(JUnitPlatform.class)
public class SrcDest {
    public static SessionFactory srcSessionFactory;
    public static SessionFactory destSessionFactory;

    @BeforeAll
    static void startup() throws URISyntaxException {
    	URL url = SrcDest.class.getClassLoader().getResource("src.hibernate.cfg.xml");
        File src = new File(url.toURI());
        srcSessionFactory = new Configuration().configure(src).buildSessionFactory();
        
    	url = SrcDest.class.getClassLoader().getResource("dest.hibernate.cfg.xml");
        File dest = new File(url.toURI());
        destSessionFactory = new Configuration().configure(dest).buildSessionFactory();

        Configurator.setRootLevel(XLevel.DEBUG4);
    }

    @AfterAll
    static void shutdown() {
    	srcSessionFactory.close();
    	destSessionFactory.close();
    }
}
