package dbshadow.test.tw;
//
//import java.sql.Connection;
//import java.util.List;
//import java.util.Random;
//
//import org.hibernate.Session;
//
//public class DeleteRandom {
// TODO switch to using doWork.
//	private Connection c;
//	public DeleteRandom(Connection c) {
//		this.c = c;
//	}
//
//	public Object run(Session s) {
//		List<Object[]> l = (List<Object[]>) new SelectAll("test", c).run(s);
//		assert l != null;
//		assert l.size() > 0;
//
//		int origSize = l.size();
//
//		Random r = new Random();
//		int deleteNum = r.nextInt(origSize);
//
//		deleteNum = (Integer)l.get(deleteNum)[0];
//
//		int x = s.createSQLQuery("delete from test where id = ?")
//			.setParameter(0, deleteNum).executeUpdate();
//		s.getTransaction().commit();
//		l = (List<Object[]>) new SelectAll("test", c).run(s);
//		assert l != null;
//		assert l.size() == origSize - 1 : "Delete random of " + deleteNum + " failed. Orig <" + origSize + ">";
//
//		return x;
//	}
//}
