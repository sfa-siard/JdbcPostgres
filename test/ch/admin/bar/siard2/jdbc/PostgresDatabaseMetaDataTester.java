package ch.admin.bar.siard2.jdbc;

import java.sql.*;
import java.util.*;

import static org.junit.Assert.*;
import org.junit.*;

import ch.enterag.sqlparser.datatype.enums.PreType;
import ch.enterag.utils.*;
import ch.enterag.utils.base.*;
import ch.enterag.utils.jdbc.*;
import ch.admin.bar.siard2.jdbcx.*;
import ch.admin.bar.siard2.postgres.*;

public class PostgresDatabaseMetaDataTester extends BaseDatabaseMetaDataTester
{
  private static final ConnectionProperties _cp = new ConnectionProperties();
  private static final String _sDB_URL = PostgresDriver.getUrl(_cp.getHost()+":"+_cp.getPort()+"/"+_cp.getCatalog());
  private static final String _sDB_USER = _cp.getUser();
  private static final String _sDB_PASSWORD = _cp.getPassword();
  private static final String _sDBA_USER = _cp.getDbaUser();
  private static final String _sDBA_PASSWORD = _cp.getDbaPassword();
  private static Set<String> _setTestTables = new HashSet<String>(Arrays.asList(new String[] {
    TestPostgresDatabase.getQualifiedSimpleTable().getName().toLowerCase(), // irritating lower case for identifiers ...
    TestPostgresDatabase.getQualifiedComplexTable().getName().toLowerCase() /**,
    TestSqlDatabase.getQualifiedSimpleTable().getName(),
    TestSqlDatabase.getQualifiedComplexTable().getName()**/}));
  private static Set<String> _setTestViews = new HashSet<String>(Arrays.asList(new String[] {
    /** TestSqlDatabase.getQualifiedSimpleView().getName().toLowerCase()**/}));

  @BeforeClass
  public static void setUpClass()
  {
    try 
    { 
      PostgresDataSource dsPostgres = new PostgresDataSource();
      dsPostgres.setUrl(_sDB_URL);
      dsPostgres.setUser(_sDBA_USER);
      dsPostgres.setPassword(_sDBA_PASSWORD);
      PostgresConnection connPostgres = (PostgresConnection)dsPostgres.getConnection();
      /* drop and create the test databases */
      // new TestSqlDatabase(connMsSql);
      new TestPostgresDatabase(connPostgres,_sDB_USER);
      connPostgres.close();
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* setUpClass */
  
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
  public void testGetClientInfoProperties()
  {
    enter();
    try { print(getDatabaseMetaData().getClientInfoProperties()); } 
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  }

  /** getTypeInfo is not overridden with a version conforming to SQL:2008
   * because that would not result in useful information (just the
   * standard predefined types would be listed).  
   */
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
  public void testGetTableTypes()
  {
    enter();
    try { print(getDatabaseMetaData().getTableTypes()); } 
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  }

  @Test
  public void testGetTablesTable()
  {
    enter();
    try 
    {
      print(getDatabaseMetaData().getTables(null,null,"%",new String[] {"TABLE"}));
      Set<String>setTestTables = new HashSet<String>(_setTestTables);
      ResultSet rs = getDatabaseMetaData().getTables(null,null,"%",new String[] {"TABLE"});
      while (rs.next())
        setTestTables.remove(rs.getString("TABLE_NAME"));
      rs.close();
      assertTrue("Some test tables not found!",setTestTables.isEmpty());
    } 
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  }

  @Test
  public void testGetTablesView()
  {
    enter();
    try 
    { 
      print(getDatabaseMetaData().getTables(null,null,"%",new String[] {"VIEW"})); 
      Set<String>setTestViews = new HashSet<String>(_setTestViews);
      ResultSet rs = getDatabaseMetaData().getTables(null,null,"%",new String[] {"VIEW"});
      while (rs.next())
        setTestViews.remove(rs.getString("TABLE_NAME"));
      rs.close();
      assertTrue("Some test views not found!",setTestViews.isEmpty());
    } 
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  }

  /** determine set of user tables or views.
   * @return user tables.
   * @throws SQLException
   */
  private Set<String> getUserTablesViews()
    throws SQLException
  {
    Set<String>setTables = new HashSet<String>();
    ResultSet rsTable = getDatabaseMetaData().getTables(null,null,"%",new String[] {"TABLE","VIEW"});
    while (rsTable.next())
      setTables.add(rsTable.getString("TABLE_NAME"));
    rsTable.close();
    return setTables;
  } /* getTablesUser */
  
  /** list columns of user tables
   */
  @Test
  public void testGetUserColumns()
  {
    enter();
    try
    {
      Set<String> setUserTablesViews = getUserTablesViews();
      for (Iterator<String> iterTableView = setUserTablesViews.iterator(); iterTableView.hasNext(); )
      {
        String sTableView = iterTableView.next();
        System.out.println("\nTable/View: "+sTableView);
        print(getDatabaseMetaData().getColumns(null, null, sTableView, "%"));
        if (sTableView.equals(TestPostgresDatabase.getQualifiedSimpleTable().getName().toLowerCase()))
        {
          ResultSet rsColumns = getDatabaseMetaData().getColumns(null, null, sTableView, "%");
          while (rsColumns.next())
          {
            String sColumnName = rsColumns.getString("COLUMN_NAME");
            int iDataType = rsColumns.getInt("DATA_TYPE");
            String sTypeName = rsColumns.getString("TYPE_NAME");
            int iColumnSize = rsColumns.getInt("COLUMN_SIZE");
            int iDecimalDigits = rsColumns.getInt("DECIMAL_DIGITS");
            if (sColumnName.equalsIgnoreCase("CINTEGER"))
            {
              assertEquals("Data type "+String.valueOf(Types.INTEGER)+" expected for "+sColumnName+"!",Types.INTEGER,iDataType);
              assertEquals("Type name "+PreType.INTEGER.getKeyword()+" expected for "+sColumnName+"!",PreType.INTEGER.getKeyword(),sTypeName);
              assertEquals("Column size 10 expected for "+sColumnName,10,iColumnSize); // should really be 11 (for sign)?
              assertEquals("Decimal digits 0 expected for "+sColumnName,0,iDecimalDigits);
            }
            else if (sColumnName.equalsIgnoreCase("CSMALLINT"))
            {
              assertEquals("Data type "+String.valueOf(Types.SMALLINT)+" expected for "+sColumnName+"!",Types.SMALLINT,iDataType);
              assertEquals("Type name "+PreType.SMALLINT.getKeyword()+" expected for "+sColumnName+"!",PreType.SMALLINT.getKeyword(),sTypeName);
              assertEquals("Column size 10 expected for "+sColumnName,5,iColumnSize);
              assertEquals("Decimal digits 0 expected for "+sColumnName,0,iDecimalDigits);
            }
            else if (sColumnName.equalsIgnoreCase("CBIGINT"))
            {
              assertEquals("Data type "+String.valueOf(Types.BIGINT)+" expected for "+sColumnName+"!",Types.BIGINT,iDataType);
              assertEquals("Type name "+PreType.BIGINT.getKeyword()+" expected for "+sColumnName+"!",PreType.BIGINT.getKeyword(),sTypeName);
              assertEquals("Column size 19 expected for "+sColumnName,19,iColumnSize); // should really be 20 (for sign?)
              assertEquals("Decimal digits 0 expected for "+sColumnName,0,iDecimalDigits);
            }
            else if (sColumnName.equalsIgnoreCase("COID"))
            {
              assertEquals("Data type "+String.valueOf(Types.INTEGER)+" expected for "+sColumnName+"!",Types.INTEGER,iDataType);
              assertEquals("Type name "+PreType.INTEGER.getKeyword()+" expected for "+sColumnName+"!",PreType.INTEGER.getKeyword(),sTypeName);
              assertEquals("Column size 10 expected for "+sColumnName,10,iColumnSize); // is unsigned
              assertEquals("Decimal digits 0 expected for "+sColumnName,0,iDecimalDigits);
            }
          }
          rsColumns.close();
        }
      }
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testGetUserColumns */
    
  @Override
  @Test
  public void testGetUDTs()
  {
    enter();
    try { print(getDatabaseMetaData().getUDTs(null, "%", "%", null)); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  }

  /** determine set of schemas holding user tables.
   * @return schemas, holding user tables.
   * @throws SQLException
   */
  private Set<String> getUserSchemas()
    throws SQLException
  {
    Set<String>setSchemas = new HashSet<String>();
    ResultSet rsTable = getDatabaseMetaData().getTables(null,null,"%",new String[] {"TABLE","VIEW"});
    while (rsTable.next())
      setSchemas.add(rsTable.getString("table_schem"));
    rsTable.close();
    return setSchemas;
  } /* getUserSchemas */
  
  /** get procedures of schemas holding user tables
   */
  @Test
  public void testGetUserProcedures()
  {
    enter();
    try 
    { 
      Set<String>setSchemas = getUserSchemas();
      for (Iterator<String>iterSchema = setSchemas.iterator(); iterSchema.hasNext(); )
      {
        String sSchema = iterSchema.next();
        System.out.println("Schema "+sSchema);
        print(getDatabaseMetaData().getProcedures(null,sSchema,"%"));
      }
    } 
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  }

  /** get functions of schemas holding user tables
   */
  @Test
  public void testGetUserFunctions()
  {
    enter();
    try 
    {
      Set<String>setSchemas = getUserSchemas();
      for (Iterator<String>iterSchema = setSchemas.iterator(); iterSchema.hasNext(); )
      {
        String sSchema = iterSchema.next();
        System.out.println("Schema "+sSchema);
        print(getDatabaseMetaData().getFunctions(null,sSchema,"%"));
      }
    } 
    catch(SQLFeatureNotSupportedException sfnse) { System.out.println(EU.getExceptionMessage(sfnse)); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  }

}
