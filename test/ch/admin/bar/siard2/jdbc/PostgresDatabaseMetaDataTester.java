package ch.admin.bar.siard2.jdbc;

import java.io.*;
import java.sql.*;
import java.util.*;

import static org.junit.Assert.*;
import org.junit.*;

import ch.enterag.sqlparser.datatype.enums.*;
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
  private static Set<String> _setTestSchemas = new HashSet<String>(Arrays.asList(new String[] {
    TestPostgresDatabase._sTEST_SCHEMA.toLowerCase(), // irritating lower case for identifiers ...
    TestSqlDatabase._sTEST_SCHEMA.toLowerCase()}));
  private static Set<String> _setTestTables = new HashSet<String>(Arrays.asList(new String[] {
    TestPostgresDatabase.getQualifiedSimpleTable().getName().toLowerCase(), // irritating lower case for identifiers ...
    TestPostgresDatabase.getQualifiedComplexTable().getName().toLowerCase(),
    TestSqlDatabase.getQualifiedSimpleTable().getName().toLowerCase(),
    TestSqlDatabase.getQualifiedComplexTable().getName().toLowerCase()}));
  private static Set<String> _setTestViews = new HashSet<String>(Arrays.asList(new String[] {
    TestSqlDatabase.getQualifiedSimpleView().getName().toLowerCase()}));

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
      new TestSqlDatabase(connPostgres,_sDB_USER);
      TestPostgresDatabase.grantSchemaUser(connPostgres, TestSqlDatabase._sTEST_SCHEMA, _sDB_USER);
      new TestPostgresDatabase(connPostgres,_sDB_USER);
      TestPostgresDatabase.grantSchemaUser(connPostgres, TestPostgresDatabase._sTEST_SCHEMA, _sDB_USER);
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

  /** determine set of user tables or views for tests.
   * @return user tables.
   * @throws SQLException
   */
  private Set<String> getUserTablesViews()
    throws SQLException
  {
    Set<String>setTables = new HashSet<String>();
    ResultSet rsTable = getDatabaseMetaData().getTables(null,null,"%",new String[] {"TABLE","VIEW"});
    while (rsTable.next())
    {
      String sSchema = rsTable.getString("TABLE_SCHEM");
      if (_setTestSchemas.contains(sSchema))
        setTables.add(rsTable.getString("TABLE_NAME"));
    }
    rsTable.close();
    return setTables;
  } /* getTablesUserViews */
  
  /** compute size in characters of the int type with the given maximum.
   * N.B.: Postgres uses this value. The correct value would be
   * length(2*kMaxUnsigned+1)+1 for unsigned types with sign.
   * @param lMaxSigned maximum of signed int type.
   * @return size in characters
   */
  private int sizeFromMax(long lMaxSigned)
  {
    int iSize = String.valueOf(lMaxSigned).length();
    return iSize;
  }
  
  /** split type name from rest
   * @param sType type with qualifications.
   * @return array with type name as first element and possibly 
   *   qualifications as second element. 
   */
  
  private PostgresType parsePostgresType(String sType)
  {
    String[] asType = sType.split("\\(",2);
    PostgresType pgt = PostgresType.getByKeyword(asType[0]);
    if (pgt == null)
    {
      asType = sType.split("\\s",2);
      pgt = PostgresType.getByKeyword(asType[0]);
    } 
    return pgt;
  }
  
  /** split type name from rest and drop "N" (NATIONAL) from character types.
   * @param sType type with qualifications.
   * @return array with type name as first element and possibly 
   *   qualifications as second element. 
   */
  
  private PreType parsePreType(String sType)
  {
    String[] asType = sType.split("\\(",2);
    PreType pt = PreType.getByKeyword(asType[0]);
    if (pt == null)
    {
      asType = sType.split("\\s",2);
      pt = PreType.getByKeyword(asType[0]);
    }
    if (pt == PreType.NCHAR)
      pt = PreType.CHAR;
    else if (pt == PreType.NVARCHAR)
      pt = PreType.VARCHAR;
    else if (pt == PreType.NCLOB)
      pt = PreType.CLOB;
    else if (pt == PreType.DECIMAL)
      pt = PreType.NUMERIC;
    else if (pt == PreType.FLOAT)
      pt = PreType.DOUBLE;
    return pt;
  }
  
  /** extract precision from type in column definition.
   * @param sType type in column definition.
   * @return precision.
   */
  private int parsePrecision(String sType)
  {
    int iPrecision = 0;
    PostgresType pgt = parsePostgresType(sType);
    PreType pt = null;
    if (pgt != null)
      pt = pgt.getPreType();
    else
      pt = parsePreType(sType);
    
    if (pgt != null)
    {
      if (pgt == PostgresType.MONEY)
        iPrecision = Integer.MAX_VALUE;
      // we have mapped oid to bigint but currently it is just an int 
      else if (pgt == PostgresType.OID)
        iPrecision = sizeFromMax(Integer.MAX_VALUE);
      else if (pgt == PostgresType.TIMETZ)
        iPrecision = 21;
      else if (pgt == PostgresType.TIMESTAMPTZ)
        iPrecision = 29;
      else if (pgt == PostgresType.UUID)
        iPrecision = 16;
      else if (pgt == PostgresType.MACADDR)
        iPrecision = 6;
      else if (pgt == PostgresType.MACADDR8)
        iPrecision = 8;
      else if ((pgt == PostgresType.POINT) ||
               (pgt == PostgresType.LINE) ||
               (pgt == PostgresType.LSEG) ||
               (pgt == PostgresType.BOX) ||
               (pgt == PostgresType.PATH) ||
               (pgt == PostgresType.POLYGON) ||
               (pgt == PostgresType.CIRCLE))
        iPrecision = Integer.MAX_VALUE;
    }
    if (iPrecision == 0)
    {
      if (pt == PreType.INTEGER)
        iPrecision = sizeFromMax(Integer.MAX_VALUE);
      else if (pt == PreType.SMALLINT)
        iPrecision = sizeFromMax(Short.MAX_VALUE);
      else if (pt == PreType.BIGINT)
        iPrecision = sizeFromMax(Long.MAX_VALUE);
      else if (pt == PreType.DOUBLE)
        iPrecision = 17;
      else if (pt == PreType.FLOAT)
        iPrecision = 17;
      else if (pt == PreType.REAL)
        iPrecision = 8;
      else if (pt == PreType.BOOLEAN)
        iPrecision = 1;
      else if (pt == PreType.CLOB)
        iPrecision = Integer.MAX_VALUE;
      else if (pt == PreType.NCLOB)
        iPrecision = Integer.MAX_VALUE;
      else if (pt == PreType.XML)
        iPrecision = Integer.MAX_VALUE;
      else if (pt == PreType.BLOB)
        iPrecision = Integer.MAX_VALUE;
      else if (pt == PreType.DATE)
        iPrecision = 13;
      else if (pt == PreType.TIME)
        iPrecision = 15;
      else if (pt == PreType.TIMESTAMP)
        iPrecision = 29;
      else if (pt == PreType.INTERVAL)
        iPrecision = 49;
    }
    if (iPrecision == 0)
    {
      int iParen = sType.indexOf('(');
      if (iParen >= 0)
      {
        String sPrecision = sType.substring(iParen+1,sType.length()-1);
        int iComma = sPrecision.indexOf(',');
        if (iComma >= 0)
          sPrecision = sPrecision.substring(0,iComma).toUpperCase();
        int iMult = 1;
        if (sPrecision.endsWith("K") || sPrecision.endsWith("M") || sPrecision.endsWith("G"))
        {
          iMult = 1024;
          if (!sPrecision.endsWith("K"))
          {
            iMult = 1024*iMult;
            if (!sPrecision.endsWith("M"))
              iMult = 1024*iMult;
          }
          sPrecision = sPrecision.substring(0,sPrecision.length()-1);
        }
        iPrecision = iMult*Integer.valueOf(sPrecision);
        if ((pgt != null) && ((pgt == PostgresType.BIT) || (pgt == PostgresType.VARBIT)))
          iPrecision = (iPrecision + 7)/8;
      }
    }
    if (iPrecision == 0)
    {
      if (pt == PreType.VARCHAR)
        iPrecision = Integer.MAX_VALUE;
      else if (pt == PreType.VARBINARY)
        iPrecision = Integer.MAX_VALUE;
    }
    
    return iPrecision;
  } /* parsePrecision */
  
  private int parseScale(String sType)
  {
    int iScale = 0;
    PostgresType pgt = parsePostgresType(sType);
    PreType pt = null;
    if (pgt != null)
      pt = pgt.getPreType();
    else
      pt = parsePreType(sType);
    
    if (pt == PreType.DOUBLE)
      iScale = 17;
    else if (pt == PreType.REAL)
      iScale = 8;
    else if (pt == PreType.TIME)
      iScale = 6;
    else if (pt == PreType.TIMESTAMP)
      iScale = 6;

    if (iScale == 0)
    {
      int iRparen = sType.lastIndexOf(")");
      int iLparen = sType.lastIndexOf("(");
      if ((iLparen >= 0) && (iRparen == (sType.length()-1)))
      {
        String sPrecision = sType.substring(iLparen+1,iRparen);
        int iComma = sPrecision.indexOf(',');
        if (iComma >= 0)
        {
          String sScale = sPrecision.substring(iComma+1);
          iScale = Integer.valueOf(sScale);
        }
        else if (pt == PreType.INTERVAL)
          iScale = Integer.valueOf(sPrecision);
      }
    }
    return iScale;
  } /* parseScale */
  
  /** find column definition in list with matching column name.
   * @param sColumnName column name.
   * @param listCd list
   * @return column definition.
   */
  private TestColumnDefinition findTestColumnDefinition(String sColumnName, List<TestColumnDefinition> listCd)
  {
    TestColumnDefinition cdFound = null;
    for (Iterator<TestColumnDefinition> iterCd = listCd.iterator(); (cdFound == null) && iterCd.hasNext(); )
    {
      TestColumnDefinition cd = iterCd.next();
      if (cd.getName().equalsIgnoreCase(sColumnName))
        cdFound = cd;
    }
    return cdFound;
  } /* findTestColumnDefinition */
  
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
        ResultSet rsColumns = getDatabaseMetaData().getColumns(null, null, sTableView, "%");
        int iPosition = 0;
        while (rsColumns.next())
        {
          String sColumnName = rsColumns.getString("COLUMN_NAME");
          int iDataType = rsColumns.getInt("DATA_TYPE");
          String sTypeName = rsColumns.getString("TYPE_NAME");
          int iColumnSize = rsColumns.getInt("COLUMN_SIZE");
          int iDecimalDigits = rsColumns.getInt("DECIMAL_DIGITS");
          int iNumPrecRadix = rsColumns.getInt("NUM_PREC_RADIX");
          int iNullable = rsColumns.getInt("NULLABLE");
          int iCharOctetLength = rsColumns.getInt("CHAR_OCTET_LENGTH");
          int iOrdinalPosition = rsColumns.getInt("ORDINAL_POSITION");
          String sIsNullable = rsColumns.getString("IS_NULLABLE");
          String sIsAutoIncrement = rsColumns.getString("IS_AUTOINCREMENT");
          
          String sAutoIncrement = "NO";
          int iNulls = DatabaseMetaData.columnNullable;
          String sNullable = "YES";
          int iPrecision = Integer.MAX_VALUE;
          int iScale = 0;
          int iRadix = 10;
          int iType = Types.NULL;
          String sType = sTypeName;
          if ((sTableView.equals(TestPostgresDatabase.getQualifiedSimpleTable().getName().toLowerCase())) ||
              (sTableView.equals(TestPostgresDatabase.getQualifiedComplexTable().getName().toLowerCase())))
          {
            TestColumnDefinition cd = null;
            if (sTableView.equals(TestPostgresDatabase.getQualifiedSimpleTable().getName().toLowerCase()))
            {
              cd = findTestColumnDefinition(sColumnName,TestPostgresDatabase._listCdSimple);
              if (iPosition == TestPostgresDatabase._iPrimarySimple)
                iNulls = DatabaseMetaData.columnNoNulls;
            }
            else if (sTableView.equals(TestPostgresDatabase.getQualifiedComplexTable().getName().toLowerCase()))
            {
              cd = findTestColumnDefinition(sColumnName,TestPostgresDatabase._listCdComplex);
              if (iPosition == TestPostgresDatabase._iPrimaryComplex)
                iNulls = DatabaseMetaData.columnNoNulls;
            }
            if (sColumnName.equalsIgnoreCase("CINT_DOMAIN"))
            {
              iType = Types.DISTINCT;
              System.out.println("DISTINCT "+sTypeName);
            }
            else if (sColumnName.equalsIgnoreCase("CCOMPOSITE"))
            {
              iType = Types.STRUCT;
              System.out.println("STRUCT "+sTypeName);
            }
            else if (sColumnName.equalsIgnoreCase("CENUM_SUIT"))
              iType = Types.VARCHAR;
            else if (sColumnName.equalsIgnoreCase("CINT_BUILTIN"))
            {
              iType = Types.OTHER; // TODO: change to ARRAY
              System.out.println("BUILTIN "+sTypeName);
              System.out.println("  Size: "+String.valueOf(iColumnSize));
            }
            else if (sColumnName.equalsIgnoreCase("CSTRING_RANGE"))
            {
              iType = Types.OTHER; // TODO: change to ARRAY
              System.out.println("RANGE "+sTypeName);
              System.out.println("  Size: "+String.valueOf(iColumnSize));
            }
            else if (sColumnName.equalsIgnoreCase("CSTRING_ARRAY"))
            {
              iType = Types.ARRAY;
              sTypeName = "VARCHAR"; // could be parsed from list ...
            }
            else if (sColumnName.equalsIgnoreCase("CDOUBLE_MATRIX"))
            {
              iType = Types.ARRAY;
              sTypeName = "DOUBLE PRECISION"; // could be parsed from list ...
            }
            else
            {
              PostgresType pgt = parsePostgresType(cd.getType());
              iPrecision = parsePrecision(cd.getType());
              iScale = parseScale(cd.getType());
              iType = pgt.getPreType().getSqlType();
              sType = pgt.getPreType().getKeyword();
              if (cd.getType().indexOf("serial") >= 0)
                sAutoIncrement = "YES";
              if (sAutoIncrement.equals("YES"))
                iNulls = DatabaseMetaData.columnNoNulls;
              if (iNulls == DatabaseMetaData.columnNoNulls)
                sNullable = "NO";
              else if (iNulls == DatabaseMetaData.columnNullableUnknown)
                sNullable = "";
              if ((pgt == PostgresType.BIT) || (pgt == PostgresType.VARBIT))
                iRadix = 2;
            }
          }  
          else if ((sTableView.equals(TestSqlDatabase.getQualifiedSimpleTable().getName().toLowerCase())) ||
            (sTableView.equals(TestSqlDatabase.getQualifiedComplexTable().getName().toLowerCase())) ||
            (sTableView.equals(TestSqlDatabase.getQualifiedSimpleView().getName().toLowerCase())))
          {
            TestColumnDefinition tcd = null;
            if (sTableView.equals(TestSqlDatabase.getQualifiedSimpleTable().getName().toLowerCase()))
            {
              tcd = findTestColumnDefinition(sColumnName,TestSqlDatabase._listCdSimple);
              if (iPosition == TestSqlDatabase._iPrimarySimple)
                iNulls = DatabaseMetaData.columnNoNulls;
            }
            else if (sTableView.equals(TestSqlDatabase.getQualifiedComplexTable().getName().toLowerCase()))
            {
              tcd = findTestColumnDefinition(sColumnName,TestSqlDatabase._listCdComplex);
              if (iPosition == TestSqlDatabase._iPrimaryComplex)
                iNulls = DatabaseMetaData.columnNoNulls;
            }
            else if (sTableView.equals(TestSqlDatabase.getQualifiedSimpleView().getName().toLowerCase()))
              tcd = findTestColumnDefinition(sColumnName,TestSqlDatabase._listCdSimple);
            if (sColumnName.equalsIgnoreCase("CDISTINCT"))
            {
              iType = Types.DISTINCT;
              System.out.println("DISTINCT "+sTypeName);
            }
            else if (sColumnName.equalsIgnoreCase("CUDT"))
            {
              iType = Types.STRUCT;
              System.out.println("STRUCT "+sTypeName);
            }
            else if (sColumnName.equalsIgnoreCase("CARRAY"))
            {
              iType = Types.ARRAY;
              sTypeName = "VARCHAR"; // could be parsed from list ...
            }
            else
            {
              iPrecision = parsePrecision(tcd.getType());
              iScale = parseScale(tcd.getType());
              PreType pt = parsePreType(tcd.getType());
              iType = pt.getSqlType();
              sType = pt.getKeyword();
              if (iNulls == DatabaseMetaData.columnNoNulls)
                sNullable = "NO";
              else if (iNulls == DatabaseMetaData.columnNullableUnknown)
                sNullable = "";
              if ((pt == PreType.BINARY) || (pt == PreType.VARBINARY))
                iRadix = 2;
            }
          }
          assertEquals("Unexpected data type for "+sColumnName,iType,iDataType);
          assertEquals("Unexpected type name for "+sColumnName,sType,sTypeName);
          assertEquals("Unexpected column size for "+sColumnName,iPrecision,iColumnSize);
          assertEquals("Unexpected decimal digits for "+sColumnName,iScale,iDecimalDigits);
          assertEquals("Unexpected radix for "+sColumnName,iRadix,iNumPrecRadix);
          assertEquals("Unexpected nullable for "+sColumnName,iNulls,iNullable);
          assertEquals("Unexpected length for "+sColumnName,iPrecision,iCharOctetLength);        
          iPosition = iPosition + 1;
          assertEquals("Unexpected ordinal_position for "+sColumnName,iPosition,iOrdinalPosition);
          assertEquals("Unexpected is_nullable for "+sColumnName,sNullable,sIsNullable);
          assertEquals("Unexpected is_autoincrement for "+sColumnName,sAutoIncrement,sIsAutoIncrement);
        }
        rsColumns.close();
        // print(getDatabaseMetaData().getColumns(null, null, sTableView, "%"));
      }
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testGetUserColumns */
    
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
  
  @Test
  public void testGetUserUDTsDistinct()
  {
    enter();
    try 
    { 
      Set<String>setSchemas = getUserSchemas();
      for (Iterator<String>iterSchema = setSchemas.iterator(); iterSchema.hasNext(); )
      {
        String sSchema = iterSchema.next();
        System.out.println("Schema "+sSchema);
        print(getDatabaseMetaData().getUDTs(null,sSchema,"%", new int[] {Types.DISTINCT} ));
      }
    } 
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  }

  @Test
  public void testGetUserUDTsStruct()
  {
    enter();
    try 
    { 
      Set<String>setSchemas = getUserSchemas();
      for (Iterator<String>iterSchema = setSchemas.iterator(); iterSchema.hasNext(); )
      {
        String sSchema = iterSchema.next();
        System.out.println("Schema "+sSchema);
        print(getDatabaseMetaData().getUDTs(null,sSchema,"%", new int[] {Types.STRUCT} ));
      }
    } 
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  }

  @Test
  public void testGetUserAttributes()
  {
    enter();
    try 
    { 
      Set<String>setSchemas = getUserSchemas();
      for (Iterator<String>iterSchema = setSchemas.iterator(); iterSchema.hasNext(); )
      {
        String sSchema = iterSchema.next();
        System.out.println("Schema "+sSchema);
        print(getDatabaseMetaData().getAttributes(null,sSchema,"%", null));
      }
    } 
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  }

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
      print(getDatabaseMetaData().getProcedures(null,null,"int4range"));
    } 
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testGetUserProcedures */
  
  /** get parameters of procedures of schemas holding user tables
   */
  @Test
  public void testGetUserProcedureColumns()
  {
    enter();
    try 
    { 
      Set<String>setSchemas = getUserSchemas();
      for (Iterator<String>iterSchema = setSchemas.iterator(); iterSchema.hasNext(); )
      {
        String sSchema = iterSchema.next();
        System.out.println("Schema "+sSchema);
        print(getDatabaseMetaData().getProcedureColumns(null,sSchema,"%","%"));
      }
      print(getDatabaseMetaData().getProcedureColumns(null,null,"int4range","%"));
    } 
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testGetUserProcedureColumns */
  
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
