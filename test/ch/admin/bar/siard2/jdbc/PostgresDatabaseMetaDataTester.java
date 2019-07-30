package ch.admin.bar.siard2.jdbc;

import static org.junit.Assert.*;

import java.sql.SQLException;

import org.junit.*;

import ch.enterag.utils.*;
import ch.enterag.utils.base.*;
import ch.enterag.utils.jdbc.*;
import ch.admin.bar.siard2.jdbcx.*;

public class PostgresDatabaseMetaDataTester extends BaseDatabaseMetaDataTester
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
      PostgresDatabaseMetaData dmdPostgres = (PostgresDatabaseMetaData)connPostgres.getMetaData();
      setDatabaseMetaData(dmdPostgres);
    }
    catch(SQLException se) { fail(se.getClass().getName()+": "+se.getMessage()); }
  } /* setUp */
  
  @Test
  public void testClass()
  {
    assertEquals("Wrong database meta data class!", PostgresDatabaseMetaData.class, getDatabaseMetaData().getClass());
  } /* testClass */

  @Override
  @Test
  public void testGetTypeInfo()
  {
    enter();
    try { print(getDatabaseMetaData().getTypeInfo()); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  }

  @Override
  @Test
  public void testGetUDTs()
  {
    enter();
    try { print(getDatabaseMetaData().getUDTs(null, "%", "%", null)); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  }

}
