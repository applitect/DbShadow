package dbshadow.test;

import dbshadow.log4j.XLevel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.h2.tools.RunScript;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.net.URISyntaxException;
import java.net.URL;

@RunWith(JUnitPlatform.class)
public class SrcDest {
    private static final Logger log = LogManager.getLogger(SrcDest.class);

    public static SessionFactory srcSessionFactory;
    public static SessionFactory destSessionFactory;

    @BeforeAll
    static void startup() throws URISyntaxException {
    	URL url = SrcDest.class.getClassLoader().getResource("src.hibernate.cfg.xml");
        File src = new File(url.toURI());
        srcSessionFactory = new Configuration().configure(src).buildSessionFactory();
        
        // Script load order.
        final String[] scripts = {
            "import.sql"
        };

        // Pre-load data for testing
        srcSessionFactory.openSession().doWork(conn -> {
            for (String script : scripts) {
                try {
                	log.info("Loading script: " + script);
                	URL scriptUrl = SrcDest.class.getClassLoader().getResource("sql/" + script);
					RunScript.execute(conn, new FileReader(new File(scriptUrl.toURI())));
				} catch (URISyntaxException | FileNotFoundException e) {
					log.error("Unable to locate script file for loading: " + script);
					e.printStackTrace();
				}
            }        	
        });
        
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
