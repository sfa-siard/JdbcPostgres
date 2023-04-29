package ch.admin.bar.siard2.jdbc;


import java.sql.*;
import java.util.*;
import static org.junit.Assert.*;
import org.junit.*;
import ch.enterag.utils.base.*;
import org.testcontainers.containers.PostgreSQLContainer;

public class PostgresDriverTester
{
  private static PostgreSQLContainer postgres;

  private static final String sDRIVER_CLASS = "ch.admin.bar.siard2.jdbc.PostgresDriver";
  private static final String sTEST_POSTGRES_URL = "jdbc:postgresql://localhost";
  private static final String sINVALID_POSTGRES_URL = "jdbc:oracle:thin:@//localhost:1521/orcl";;
  
  private Driver _driver = null;
  private Connection _conn = null;

  @BeforeClass
  public static void startPostgres() {
    postgres = new PostgreSQLContainer(PostgreSQLContainer.IMAGE);
    postgres.start();
  }

  @Before
  public void setUp()
  {
    try { Class.forName(sDRIVER_CLASS); }
    catch(ClassNotFoundException cnfe) { fail(cnfe.getClass().getName()+": "+cnfe.getMessage()); }
    try
    {
      _driver = DriverManager.getDriver(sTEST_POSTGRES_URL);
      _conn = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }
    catch(SQLException se) { fail(se.getClass().getName()+": "+se.getMessage()); }
  } /* setUp */
  
  @After
  public void tearDown()
  {
    try
    {
      if ((_conn != null) && (!_conn.isClosed()))
        _conn.close();
      else
        fail("Connection cannot be closed!");
    }
    catch(SQLException se) { fail(se.getClass().getName()+": "+se.getMessage()); }
  } /* tearDown */

  @Test
  public void testWrapping()
  {
    assertSame("Registration of driver wrapper failed!", PostgresDriver.class, _driver.getClass());
    assertSame("Choice of connection wrapper failed!", PostgresConnection.class, _conn.getClass());
  } /* testWrapping */
  
  @Test
  public void testCompliant()
  {
    assertSame("Postgres driver is suddenly JDBC compliant!", false, _driver.jdbcCompliant());
  } /* testCompliant */
  
  @Test
  public void testAcceptsURL()
  {
    try
    {
      assertSame("Valid Postgres URL not accepted!", true, _driver.acceptsURL(postgres.getJdbcUrl()));
      assertSame("Invalid Postgres URL accepted!", false, _driver.acceptsURL(sINVALID_POSTGRES_URL));
    }
    catch(SQLException se) { fail(se.getClass().getName()+": "+se.getMessage()); }
  } /* testAcceptsURL */
  
  @Test
  public void testVersion()
  {
    int iMajorVersion = _driver.getMajorVersion();
    int iMinorVersion = _driver.getMinorVersion();
    String sVersion = String.valueOf(iMajorVersion)+"."+String.valueOf(iMinorVersion);
    assertEquals("Wrong Postgres version "+sVersion+" found!", "42.2", sVersion);
  } /* testVersion */
  
  @Test
  public void testDriverProperties()
  {
    try
    {
      DriverPropertyInfo[] apropInfo = _driver.getPropertyInfo(postgres.getJdbcUrl(), new Properties());
      for (DriverPropertyInfo dpi: apropInfo)
        System.out.println(dpi.name+": "+dpi.value+" ("+String.valueOf(dpi.description)+")");
      assertSame("Unexpected driver properties!", 58, apropInfo.length);
    }
    catch(SQLException se) { fail(se.getClass().getName()+": "+se.getMessage()); }
  } /* testDriverProperties */

} /* class PostgresDriverTester */
