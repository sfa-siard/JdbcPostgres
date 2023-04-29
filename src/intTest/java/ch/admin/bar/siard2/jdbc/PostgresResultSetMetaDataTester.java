package ch.admin.bar.siard2.jdbc;

import static org.junit.Assert.*;

import java.io.*;
import java.sql.*;
import java.util.*;

import org.junit.*;

import ch.enterag.sqlparser.identifier.*;
import ch.enterag.utils.*;
import ch.enterag.utils.base.*;
import ch.enterag.utils.jdbc.*;
import ch.admin.bar.siard2.jdbcx.*;
import ch.admin.bar.siard2.postgres.*;
import org.testcontainers.containers.PostgreSQLContainer;

public class PostgresResultSetMetaDataTester
  extends BaseResultSetMetaDataTester
{

  private static PostgreSQLContainer postgres;

  private static String getTableQuery(QualifiedId qiTable, List<TestColumnDefinition> listCd)
  {
    StringBuilder sbSql = new StringBuilder("SELECT\r\n  ");
    for (int iColumn = 0; iColumn < listCd.size(); iColumn++)
    {
      if (iColumn > 0)
        sbSql.append(",\r\n  ");
      TestColumnDefinition tcd = listCd.get(iColumn);
      sbSql.append(tcd.getName());
    }
    sbSql.append("\r\nFROM ");
    sbSql.append(qiTable.format());
    return sbSql.toString();
  } /* getTableQuery */
  
  private static String _sNativeQuerySimple = getTableQuery(TestPostgresDatabase.getQualifiedSimpleTable(),TestPostgresDatabase._listCdSimple);
  private static String _sNativeQueryComplex = getTableQuery(TestPostgresDatabase.getQualifiedComplexTable(),TestPostgresDatabase._listCdComplex);
  private static String _sSqlQuerySimple = getTableQuery(TestSqlDatabase.getQualifiedSimpleTable(),TestSqlDatabase._listCdSimple);
  private static String _sSqlQueryComplex = getTableQuery(TestSqlDatabase.getQualifiedComplexTable(),TestSqlDatabase._listCdComplex);
  
  @BeforeClass
  public static void setUpClass() 
  {
    try 
    {
      postgres = new PostgreSQLContainer(PostgreSQLContainer.IMAGE);
      postgres.start();

      PostgresDataSource dsPostgres = new PostgresDataSource();
      dsPostgres.setUrl(postgres.getJdbcUrl());
      dsPostgres.setUser(postgres.getUsername());
      dsPostgres.setPassword(postgres.getPassword());
      PostgresConnection connPostgres = (PostgresConnection)dsPostgres.getConnection();
      /* drop and create the test databases */
      new TestSqlDatabase(connPostgres,postgres.getUsername());
      TestPostgresDatabase.grantSchemaUser(connPostgres, TestSqlDatabase._sTEST_SCHEMA, postgres.getUsername());
      new TestPostgresDatabase(connPostgres,postgres.getUsername());
      TestPostgresDatabase.grantSchemaUser(connPostgres, TestPostgresDatabase._sTEST_SCHEMA, postgres.getUsername());
      connPostgres.close();
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
    catch(IOException ie) { fail(EU.getExceptionMessage(ie)); }
  }
  
  private Connection closeResultSet()
    throws SQLException
  {
    Connection conn = null;
    ResultSet rs = getResultSet();
    if (rs != null)
    {
      if (!rs.isClosed())
      {
        Statement stmt = rs.getStatement();
        rs.close();
        setResultSetMetaData(null,null);
        if (!stmt.isClosed())
        {
          conn = stmt.getConnection();
          stmt.close();
        }
      }
    }
    return conn;
  } /* closeResultSet */

  private void openResultSet(Connection conn, String sQuery)
    throws SQLException
  {
    closeResultSet();
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery(sQuery);
    ResultSetMetaData rsmd = rs.getMetaData();
    setResultSetMetaData(rsmd,rs);
  } /* openResultSet */
  
  @Before
  public void setUp() {
    try 
    {
      PostgresDataSource dsPostgres = new PostgresDataSource();
      dsPostgres.setUrl(postgres.getJdbcUrl());
      dsPostgres.setUser(postgres.getUsername());
      dsPostgres.setPassword(postgres.getPassword());
      Connection conn = (PostgresConnection)dsPostgres.getConnection();
      conn.setAutoCommit(false);
      openResultSet(conn,_sNativeQuerySimple);
    }
    catch (SQLException se) { fail(se.getClass().getName() + ": " + se.getMessage()); }
  }

  @After
  @Override
  public void tearDown()
 {
    try
    {
      Connection conn = closeResultSet();
      if (conn != null)
      {
        if (!conn.isClosed())
        {
          conn.commit();
          conn.close();
        }
      }
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* tearDown */
  
  @Test
  public void testClass()
  {
    assertEquals("Wrong result set metadata class!", PostgresResultSetMetaData.class, getResultSetMetaData().getClass());
  } /* testClass */
  
  @Test
  public void testNativeSimple()
  {
    try
    {
      openResultSet(getResultSet().getStatement().getConnection(),_sNativeQuerySimple);
      super.testAll();
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testNativeSimple */

  @Test
  public void testNativeComplex()
  {
    try
    {
      openResultSet(getResultSet().getStatement().getConnection(),_sNativeQueryComplex);
      super.testAll();
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testNativeComplex */

  @Test
  public void testSqlSimple()
  {
    try
    {
      openResultSet(getResultSet().getStatement().getConnection(),_sSqlQuerySimple);
      super.testAll();
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testSqlSimple */

  @Test
  public void testSqlComplex()
  {
    try
    {
      openResultSet(getResultSet().getStatement().getConnection(),_sSqlQueryComplex);
      super.testAll();
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testSqlComplex */
  
}
