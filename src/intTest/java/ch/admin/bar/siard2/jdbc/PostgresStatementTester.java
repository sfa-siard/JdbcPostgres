package ch.admin.bar.siard2.jdbc;

import java.io.IOException;
import java.sql.*;

import org.junit.*;
import static org.junit.Assert.*;

import ch.enterag.utils.*;
import ch.enterag.utils.base.*;
import ch.enterag.utils.jdbc.*;
import ch.admin.bar.siard2.postgres.*;
import ch.admin.bar.siard2.jdbcx.*;
import org.testcontainers.containers.PostgreSQLContainer;

public class PostgresStatementTester extends BaseStatementTester
{
  private static PostgreSQLContainer postgres;

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
  } /* setUpClass */
  
  @Before
  public void setUp()
  {
    try 
    { 
      PostgresDataSource dsPostgres = new PostgresDataSource();
      dsPostgres.setUrl(postgres.getJdbcUrl());
      dsPostgres.setUser(postgres.getUsername());
      dsPostgres.setPassword(postgres.getPassword());
      PostgresConnection connPostgres = (PostgresConnection)dsPostgres.getConnection();
      connPostgres.setAutoCommit(false);
      PostgresStatement stmtPostgres = (PostgresStatement)connPostgres.createStatement();
      setStatement(stmtPostgres);
    }
    catch(SQLException se) { fail(se.getClass().getName()+": "+se.getMessage()); }
  } /* setUp */
  
  @Test
  public void testClass()
  {
    assertEquals("Wrong statement class!", PostgresStatement.class, getStatement().getClass());
  } /* testClass */

} /* class PostgresStatementTester */
