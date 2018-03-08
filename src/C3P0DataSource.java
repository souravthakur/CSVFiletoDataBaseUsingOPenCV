import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;

import com.mchange.v2.c3p0.ComboPooledDataSource;


public class C3P0DataSource {
   private static C3P0DataSource dataSource;
   private ComboPooledDataSource comboPooledDataSource;

   private C3P0DataSource() {
      try {
         comboPooledDataSource = new ComboPooledDataSource();
         comboPooledDataSource
            .setDriverClass("oracle.jdbc.driver.OracleDriver");
         comboPooledDataSource
            .setJdbcUrl("jdbc:oracle:thin:@localhost:1521:xe");
         comboPooledDataSource.setUser("DBUSER");
         comboPooledDataSource.setPassword("1234");
      }
      catch (PropertyVetoException ex1) {
         ex1.printStackTrace();
      }
   }

   public static C3P0DataSource getInstance() {
      if (dataSource == null)
         dataSource = new C3P0DataSource();
      return dataSource;
   }

   public Connection getConnection() {
      Connection con = null;
      try {
         con = comboPooledDataSource.getConnection();
      } catch (SQLException e) {
         e.printStackTrace();
      }
      return con;
   }
}
