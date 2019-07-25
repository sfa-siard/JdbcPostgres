package ch.admin.bar.siard2.jdbc;

import java.sql.*;
import static org.junit.Assert.*;
import org.junit.*;
import ch.enterag.utils.*;
import ch.enterag.utils.base.*;
import ch.enterag.utils.jdbc.*;
import ch.admin.bar.siard2.jdbcx.*;

public class PostgresConnectionTester extends BaseConnectionTester
{
  private static final ConnectionProperties _cp = new ConnectionProperties();
  private static final String _sDB_URL = PostgresDriver.getUrl(_cp.getHost()+":"+_cp.getPort()+"/"+_cp.getCatalog());
  private static final String _sDB_USER = _cp.getUser();
  private static final String _sDB_PASSWORD = _cp.getPassword();

  @Before
  public void setUp()
  {
    try 
    { 
      PostgresDataSource dsPostgres = new PostgresDataSource();
      dsPostgres.setUrl(_sDB_URL);
      dsPostgres.setUser(_sDB_USER);
      dsPostgres.setPassword(_sDB_PASSWORD);
      PostgresConnection connPostgres = (PostgresConnection)dsPostgres.getConnection();
      connPostgres.setAutoCommit(false);
      setConnection(connPostgres);
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* setUp */
  
  @Test
  public void testClass()
  {
    assertEquals("Wrong connection class!", PostgresConnection.class, getConnection().getClass());
  } /* testClass */

} /* class PostgresConnectionTester */
