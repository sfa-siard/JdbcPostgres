package ch.admin.bar.siard2.jdbcx;

import java.sql.*;
import javax.sql.DataSource;

import static org.junit.Assert.*;
import org.junit.*;
import ch.admin.bar.siard2.jdbc.*;
import org.testcontainers.containers.PostgreSQLContainer;

public class PostgresDataSourceTester
{
  private static PostgreSQLContainer postgres;

  private PostgresDataSource _dsPostgres = null;
  private Connection _conn = null;

  @BeforeClass
  public static void startPostgres() {
    postgres = new PostgreSQLContainer();
    postgres.start();
  }
  @Before
  public void setUp()
  {
    _dsPostgres = new PostgresDataSource();
  } /* setUp */
  
  @After
  public void tearDown()
  {
    try
    {
      if ((_conn != null) && (!_conn.isClosed()))
        _conn.close();
    }
    catch(SQLException se) { fail(se.getClass().getName()+": "+se.getMessage()); }
  } /* tearDown */
  
  @Test
  public void testWrapper()
  {
    try
    {
      Assert.assertSame("Invalid wrapper!", true, _dsPostgres.isWrapperFor(DataSource.class));
      DataSource dsWrapped = _dsPostgres.unwrap(DataSource.class);
      assertSame("Invalid wrapped class!", org.postgresql.ds.PGSimpleDataSource.class, dsWrapped.getClass());
    }
    catch(SQLException se) { fail(se.getClass().getName()+": "+se.getMessage()); }
  } /* testWrapper */
  
  @Test
  public void testGetConnection()
  {
    _dsPostgres.setUrl(postgres.getJdbcUrl());
    _dsPostgres.setUser(postgres.getUsername());
    _dsPostgres.setPassword(postgres.getPassword());
    try 
    {
      _conn = _dsPostgres.getConnection();
      if (_conn.unwrap(Connection.class) instanceof PostgresConnection)
        fail("Double wrap!");
    }
    catch(SQLException se) { fail(se.getClass().getName()+": "+se.getMessage()); }
  } /* testConnection */
  
}
