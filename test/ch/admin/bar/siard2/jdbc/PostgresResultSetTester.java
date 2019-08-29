package ch.admin.bar.siard2.jdbc;

import org.junit.*;
import static org.junit.Assert.*;

import java.io.*;
import java.math.*;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import javax.xml.datatype.*;

import ch.enterag.utils.*;
import ch.enterag.utils.base.*;
import ch.enterag.utils.jdbc.*;
import ch.enterag.sqlparser.*;
import ch.enterag.sqlparser.identifier.*;
import ch.admin.bar.siard2.jdbcx.*;
import ch.admin.bar.siard2.postgres.*;

public class PostgresResultSetTester
  extends BaseResultSetTester
{
  private static final ConnectionProperties _cp = new ConnectionProperties();
  private static final String _sDB_URL = PostgresDriver.getUrl(_cp.getHost()+":"+_cp.getPort()+"/"+_cp.getCatalog());
  private static final String _sDB_USER = _cp.getUser();
  private static final String _sDB_PASSWORD = _cp.getPassword();
  private static final String _sDBA_USER = _cp.getDbaUser();
  private static final String _sDBA_PASSWORD = _cp.getDbaPassword();

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

  @SuppressWarnings("deprecation")
  private static List<TestColumnDefinition> getListCdSimple()
  {
    List<TestColumnDefinition> listCdSimple = new ArrayList<TestColumnDefinition>();
    listCdSimple.add(new TestColumnDefinition("CCHAR_5","CHAR(5)","wxyZ"));
    listCdSimple.add(new TestColumnDefinition("CVARCHAR_255","VARCHAR(255)",TestUtils.getString(92)));
    listCdSimple.add(new TestColumnDefinition("CCLOB_2M","CLOB(2M)",TestUtils.getString(1000000)));
    listCdSimple.add(new TestColumnDefinition("CNCHAR_5","NCHAR(5)","Auää"));
    listCdSimple.add(new TestColumnDefinition("CNVARCHAR_127","NCHAR VARYING(127)",TestUtils.getNString(53)));
    listCdSimple.add(new TestColumnDefinition("CNCLOB_1M","NCLOB(1M)",TestUtils.getNString(500000)));
    listCdSimple.add(new TestColumnDefinition("CXML","XML","<a>foöäpwkfèégopàèwerkgv fviodsjv jdsjd idsjidsjsiudojiou operkv &lt; and &amp; ifjeifj</a>"));
    listCdSimple.add(new TestColumnDefinition("CBINARY_5","BINARY(5)",new byte[] {5,-4,3,-2} ));
    listCdSimple.add(new TestColumnDefinition("CVARBINARY_255","VARBINARY(255)",TestUtils.getBytes(76) ));
    listCdSimple.add(new TestColumnDefinition("CBLOB","BLOB",TestUtils.getBytes(500000)));
    listCdSimple.add(new TestColumnDefinition("CNUMERIC_31","NUMERIC(31)",BigInteger.valueOf(987654321098765432l)));
    listCdSimple.add(new TestColumnDefinition("CDECIMAL_15_5","DECIMAL(15,5)",new BigDecimal(BigInteger.valueOf(9876543210987l),5)));
    listCdSimple.add(new TestColumnDefinition("CSMALLINT","SMALLINT",Short.valueOf((short)23000)));
    listCdSimple.add(new TestColumnDefinition("CINTEGER","INTEGER",Integer.valueOf(987654321)));
    listCdSimple.add(new TestColumnDefinition("CBIGINT","BIGINT",Long.valueOf(-987654321098765432l)));
    listCdSimple.add(new TestColumnDefinition("CFLOAT_10","FLOAT(10)",Float.valueOf((float)Math.PI)));
    listCdSimple.add(new TestColumnDefinition("CREAL","REAL",Float.valueOf((float)Math.E)));
    listCdSimple.add(new TestColumnDefinition("CDOUBLE","DOUBLE PRECISION",Double.valueOf(Math.PI)));
    listCdSimple.add(new TestColumnDefinition("CBOOLEAN","BOOLEAN",Boolean.valueOf(false)));
    listCdSimple.add(new TestColumnDefinition("CDATE","DATE",new Date(2016-1900,12,2)));
    listCdSimple.add(new TestColumnDefinition("CTIME","TIME",new Time(14,24,12)));
    listCdSimple.add(new TestColumnDefinition("CTIMESTAMP","TIMESTAMP(9)",new Timestamp(2016-1900,12,2,14,24,12,987654321)));
    listCdSimple.add(new TestColumnDefinition("CINTERVAL_YEAR_3_MONTH","INTERVAL YEAR(3) TO MONTH",new Interval(1,3,6)));
    listCdSimple.add(new TestColumnDefinition("CINTERVAL_DAY_2_SECONDS_6","INTERVAL DAY(2) TO SECOND(6)",new Interval(1,0,17,54,23,123456000l)));
    return listCdSimple;
  }
  public static List<TestColumnDefinition> _listCdSimple = getListCdSimple();
  
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
    catch (SQLException se) { fail(se.getClass().getName() + ": " + se.getMessage()); }
  }

  private Connection _conn = null;
  
  private Connection closeResultSet()
    throws SQLException
  {
    ResultSet rs = getResultSet();
    if (rs != null)
    {
      if (!rs.isClosed())
      {
        Statement stmt = rs.getStatement();
        rs.close();
        setResultSet(null);
        if (!stmt.isClosed())
          stmt.close();
      }
    }
    _conn.commit();
    return _conn;
  } /* closeResultSet */
  
  private void openResultSet(String sQuery, int iType, int iConcurrency)
    throws SQLException
  {
    closeResultSet();
    int iHoldability = ResultSet.HOLD_CURSORS_OVER_COMMIT;
    Statement stmt = _conn.createStatement(iType, iConcurrency, iHoldability);
    ResultSet rs = stmt.executeQuery(sQuery);
    setResultSet(rs);
    rs.next();
  } /* openResultSet */
  
  @Before
  public void setUp() {
    try 
    {
      PostgresDataSource dsPostgres = new PostgresDataSource();
      dsPostgres.setUrl(_sDB_URL);
      dsPostgres.setUser(_sDB_USER);
      dsPostgres.setPassword(_sDB_PASSWORD);
      _conn = (PostgresConnection)dsPostgres.getConnection();
      _conn.setAutoCommit(false);
      openResultSet(_sSqlQuerySimple,ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_UPDATABLE);
    }
    catch (SQLException se) { fail(se.getClass().getName() + ": " + se.getMessage()); }
  }

  private TestColumnDefinition findColumnDefinition(List<TestColumnDefinition> listCd, String sName)
  {
    TestColumnDefinition tcd = null;
    for (Iterator<TestColumnDefinition> iterCd = listCd.iterator(); iterCd.hasNext(); )
    {
      TestColumnDefinition tcdTry = iterCd.next();
      if (sName.equals(tcdTry.getName()))
        tcd = tcdTry;
    }
    return tcd;
  } /* findColumnDefinition */
  
  @Test
  public void testClass() 
  {
    enter();
    assertEquals("Wrong result set class!", PostgresResultSet.class, getResultSet().getClass());
  } /* testClass */

  @Test
  @Override
  public void testAbsolute()
  {
    enter();
    try 
    { 
      openResultSet(_sSqlQuerySimple,ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
      getResultSet().absolute(1); 
    }
    catch(SQLFeatureNotSupportedException sfnse) { printExceptionMessage(sfnse); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testAbsolute */
  
  @Test
  @Override
  public void testRelative()
  {
    enter();
    try 
    { 
      openResultSet(_sSqlQuerySimple,ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
      getResultSet().relative(1);
    }
    catch(SQLFeatureNotSupportedException sfnse) { printExceptionMessage(sfnse); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testRelative */
  
  @Test
  @Override
  public void testFirst()
  {
    enter();
    try 
    { 
      openResultSet(_sSqlQuerySimple,ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
      getResultSet().first();
    }
    catch(SQLFeatureNotSupportedException sfnse) { printExceptionMessage(sfnse); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testFirst */
  
  @Test
  @Override
  public void testLast()
  {
    enter();
    try 
    { 
      openResultSet(_sSqlQuerySimple,ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
      getResultSet().last();
    }
    catch(SQLFeatureNotSupportedException sfnse) { printExceptionMessage(sfnse); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testLast */
  
  @Test
  @Override
  public void testPrevious()
  {
    enter();
    try 
    {
      openResultSet(_sSqlQuerySimple,ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
      getResultSet().next();
      getResultSet().previous();
    }
    catch(SQLFeatureNotSupportedException sfnse) { printExceptionMessage(sfnse); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testPrevious */
  
  @Test
  @Override
  public void testBeforeFirst()
  {
    enter();
    try 
    { 
      openResultSet(_sSqlQuerySimple,ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
      getResultSet().beforeFirst(); 
    }
    catch(SQLFeatureNotSupportedException sfnse) { printExceptionMessage(sfnse); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testBeforeFirst */
  
  @Test
  @Override
  public void testAfterLast()
  {
    enter();
    try 
    {
      openResultSet(_sSqlQuerySimple,ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
      getResultSet().afterLast(); 
    }
    catch(SQLFeatureNotSupportedException sfnse) { printExceptionMessage(sfnse); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testAfterLast */
  
  @Test
  public void testSetFetchDirection()
  {
    enter();
    try 
    { 
      openResultSet(_sSqlQuerySimple,ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
      getResultSet().setFetchDirection(ResultSet.FETCH_UNKNOWN); 
    }
    catch(SQLFeatureNotSupportedException sfnse) { printExceptionMessage(sfnse); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testSetFetchDirection */
  
  @Test
  @Override
  public void testFindColumn()
  {
    enter();
    try { getResultSet().findColumn("CCHAR_5"); }
    catch(SQLFeatureNotSupportedException sfnse) { printExceptionMessage(sfnse); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testFindColumn */
  
  @Test
  @Override
  public void testUpdateNull()
  {
    enter();
    try { getResultSet().updateNull("CCHAR_5"); }
    catch(SQLFeatureNotSupportedException sfnse) { printExceptionMessage(sfnse); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateNull */
  
  @Test
  @Override
  public void testGetString()
  {
    enter();
    try 
    {
      TestColumnDefinition tcd = findColumnDefinition(TestSqlDatabase._listCdSimple,"CCHAR_5");
      String s = getResultSet().getString(tcd.getName()); 
      assertEquals("Invalid string!",(String)tcd.getValue(),s);
    }
    catch(SQLFeatureNotSupportedException sfnse) { printExceptionMessage(sfnse); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testGetString */
  
  @Test
  @Override
  public void testUpdateString()
  {
    enter();
    try 
    { 
      TestColumnDefinition tcd = findColumnDefinition(_listCdSimple,"CVARCHAR_255");
      getResultSet().updateString(tcd.getName(),(String)tcd.getValue());
    }
    catch(SQLFeatureNotSupportedException sfnse) { printExceptionMessage(sfnse); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateString */
  
  @Test
  @Override
  public void testGetNString()
  {
    enter();
    try 
    { 
      TestColumnDefinition tcd = findColumnDefinition(TestSqlDatabase._listCdSimple,"CNCHAR_5");
      String s = getResultSet().getNString(tcd.getName());
      assertEquals("Invalid national string!",(String)tcd.getValue(),s);
      
    }
    catch(SQLFeatureNotSupportedException sfnse) { printExceptionMessage(sfnse); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testGetString */
  
  @Test
  @Override
  public void testUpdateNString()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(_listCdSimple,"CNVARCHAR_127");
      getResultSet().updateNString(tcd.getName(),(String)tcd.getValue());
    }
    catch(SQLFeatureNotSupportedException sfnse) { System.out.println(EU.getExceptionMessage(sfnse)); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateNString */
  
  @Test
  @Override
  public void testGetBoolean()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(TestSqlDatabase._listCdSimple,"CBOOLEAN");
      boolean b = getResultSet().getBoolean(tcd.getName());
      assertEquals("Invalid boolean!",((Boolean)tcd.getValue()).booleanValue(),b);
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testGetBoolean */
  
  @Test
  @Override
  public void testUpdateBoolean()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(_listCdSimple,"CBOOLEAN");
      getResultSet().updateBoolean(tcd.getName(),((Boolean)tcd.getValue()).booleanValue());
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateBoolean */
  
  @Test
  @Override
  public void testGetByte()
  {
    enter();
    try 
    { 
      TestColumnDefinition tcd = findColumnDefinition(TestSqlDatabase._listCdSimple,"CBOOLEAN");
      byte by = getResultSet().getByte(tcd.getName());
      assertEquals("Invalid boolean!",((Boolean)tcd.getValue()).booleanValue(),(boolean)(by != 0));
    }
    catch(SQLFeatureNotSupportedException sfnse) { printExceptionMessage(sfnse); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testGetByte */

  @Test
  @Override
  public void testUpdateByte()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(_listCdSimple,"CBOOLEAN");
      getResultSet().updateByte(tcd.getName(),((Boolean)tcd.getValue()).booleanValue()?(byte)1:(byte)0);
    }
    catch(SQLFeatureNotSupportedException sfnse) { printExceptionMessage(sfnse); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateByte */
  
  @Test
  @Override
  public void testGetShort()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(TestSqlDatabase._listCdSimple,"CSMALLINT");
      short sh = getResultSet().getShort(tcd.getName());
      assertEquals("Invalid short!",((Short)tcd.getValue()).shortValue(),sh);
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testGetShort */
  
  @Test
  @Override
  public void testUpdateShort()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(_listCdSimple,"CSMALLINT");
      getResultSet().updateShort(tcd.getName(),((Short)tcd.getValue()).shortValue());
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateShort */
  
  @Test
  @Override
  public void testGetInt()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(TestSqlDatabase._listCdSimple,"CINTEGER");
      int i = getResultSet().getInt(tcd.getName());
      assertEquals("Invalid int!",((Integer)tcd.getValue()).intValue(),i);
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testGetInt */
  
  @Test
  @Override
  public void testUpdateInt()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(_listCdSimple,"CINTEGER");
      getResultSet().updateInt(tcd.getName(),((Integer)tcd.getValue()).intValue());
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateInt */
  
  @Test
  @Override
  public void testGetLong()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(TestSqlDatabase._listCdSimple,"CBIGINT");
      long l = getResultSet().getLong(tcd.getName());
      assertEquals("Invalid long!",((Long)tcd.getValue()).longValue(),l);
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testGetLong */
  
  @Test
  @Override
  public void testUpdateLong()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(_listCdSimple,"CBIGINT");
      getResultSet().updateLong(tcd.getName(),((Long)tcd.getValue()).longValue());
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateLong */
  
  @Test
  @Override
  public void testGetFloat()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(TestSqlDatabase._listCdSimple,"CREAL");
      float f = getResultSet().getFloat(tcd.getName());
      assertEquals("Invalid float!",(Float)tcd.getValue(),Float.valueOf(f));
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testGetFloat */
  
  @Test
  @Override
  public void testUpdateFloat()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(_listCdSimple,"CREAL");
      getResultSet().updateFloat(tcd.getName(),((Float)tcd.getValue()).floatValue());
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateFloat */
  
  @Test
  @Override
  public void testGetDouble()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(TestSqlDatabase._listCdSimple,"CDOUBLE");
      double d = getResultSet().getDouble(tcd.getName());
      assertEquals("Invalid double!",(Double)tcd.getValue(),Double.valueOf(d));
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testGetDouble */
  
  @Test
  @Override
  public void testUpdateDouble()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(_listCdSimple,"CDOUBLE");
      getResultSet().updateDouble(tcd.getName(),((Double)tcd.getValue()).doubleValue());
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateDouble */
  
  @Test
  @Override
  public void testGetBigDecimal()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(TestSqlDatabase._listCdSimple,"CDECIMAL_15_5");
      BigDecimal bd = getResultSet().getBigDecimal(tcd.getName());
      assertTrue("Invalid BigDecimal!",bd.compareTo((BigDecimal)tcd.getValue()) == 0);
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testGetBigDecimal */
  
  @Test
  @Override
  public void testUpdateBigDecimal()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(_listCdSimple,"CDECIMAL_15_5");
      getResultSet().updateBigDecimal(tcd.getName(),(BigDecimal)tcd.getValue());
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateBigDecimal */
  
  @Test
  @Override
  @SuppressWarnings("deprecation")
  public void testGetBigDecimal_Int()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(TestSqlDatabase._listCdSimple,"CDECIMAL_15_5");
      BigDecimal bd = getResultSet().getBigDecimal(tcd.getName(),5);
      assertEquals("Invalid BigDecimal!",((BigDecimal)tcd.getValue()).setScale(5,RoundingMode.DOWN),bd);
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testGetBigDecimal_String_Int */
  
  @Test
  @Override
  public void testGetBytes()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(TestSqlDatabase._listCdSimple,"CBINARY_5");
      byte[] buf = getResultSet().getBytes(tcd.getName());
      assertTrue("Invalid byte array!",Arrays.equals((byte[])tcd.getValue(),buf));
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testGetBytes */

  @Test
  @Override
  public void testUpdateBytes()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(_listCdSimple,"CVARBINARY_255");
      getResultSet().updateBytes(tcd.getName(),(byte[])tcd.getValue());
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateBytes */
  
  @Test
  @Override
  public void testGetDate()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(TestSqlDatabase._listCdSimple,"CDATE");
      Date date = getResultSet().getDate(tcd.getName());
      assertEquals("Invalid Date!",(Date)tcd.getValue(),date);
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testGetDate */
  
  @Test
  @Override
  public void testUpdateDate()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(_listCdSimple,"CDATE");
      getResultSet().updateDate(tcd.getName(),(Date)tcd.getValue());
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateDate */

  @Test
  @Override
  public void testGetDate_Calendar()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(TestSqlDatabase._listCdSimple,"CDATE");
      Calendar cal = new GregorianCalendar();
      Date date = getResultSet().getDate(tcd.getName(),cal);
      assertEquals("Invalid Date!",(Date)tcd.getValue(),date);
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testGetDate_Calendar */

  @Test
  @Override
  public void testGetTime()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(TestSqlDatabase._listCdSimple,"CTIME");
      Time time = getResultSet().getTime(tcd.getName());
      assertEquals("Invalid Time!",(Time)tcd.getValue(),time);
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testGetTime */
  
  
  @Test
  @Override
  public void testUpdateTime()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(_listCdSimple,"CTIME");
      getResultSet().updateTime(tcd.getName(),(Time)tcd.getValue());
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateTime */
  
  @Test
  @Override
  public void testGetTime_Calendar()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(TestSqlDatabase._listCdSimple,"CTIME");
      Calendar cal = new GregorianCalendar();
      Time time = getResultSet().getTime(tcd.getName(),cal);
      assertEquals("Invalid Time!",(Time)tcd.getValue(),time);
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testGetTime_Calendar */
  
  @Test
  @Override
  public void testGetTimestamp()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(TestSqlDatabase._listCdSimple,"CTIMESTAMP");
      Timestamp ts = getResultSet().getTimestamp(tcd.getName());
      assertEquals("Invalid Timestamp!",(Timestamp)tcd.getValue(),ts);
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testGetTimestamp */

  @Test
  @Override
  public void testUpdateTimestamp()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(_listCdSimple,"CTIMESTAMP");
      getResultSet().updateTimestamp(tcd.getName(),(Timestamp)tcd.getValue());
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateTimestamp */

  @Test
  @Override
  public void testGetTimestamp_Calendar()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(TestSqlDatabase._listCdSimple,"CTIMESTAMP");
      Calendar cal = new GregorianCalendar();
      Timestamp ts = getResultSet().getTimestamp(tcd.getName(),cal);
      assertEquals("Invalid Timestamp!",(Timestamp)tcd.getValue(),ts);
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testGetTimestamp_Calendar */
  
  @Test
  @Override
  public void testGetDuration()
  {
    enter();
    try 
    {
      TestColumnDefinition tcd = findColumnDefinition(TestSqlDatabase._listCdSimple,"CINTERVAL_YEAR_3_MONTH");
      Duration duration = getBaseResultSet().getDuration(tcd.getName());
      assertEquals("Invalid Duration!",(Interval)tcd.getValue(),Interval.fromDuration(duration));
    }
    catch(SQLFeatureNotSupportedException sfnse) { printExceptionMessage(sfnse); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  }
  
  @Test
  @Override
  public void testUpdateDuration()
  {
    enter();
    try 
    {
      TestColumnDefinition tcd = findColumnDefinition(_listCdSimple,"CINTERVAL_DAY_2_SECONDS_6");
      getBaseResultSet().updateDuration(tcd.getName(), ((Interval)tcd.getValue()).toDuration());
    }
    catch(SQLFeatureNotSupportedException sfnse) { printExceptionMessage(sfnse); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateDuration */
  
  @Test
  @Override
  public void testGetAsciiStream()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(TestSqlDatabase._listCdSimple,"CVARCHAR_255");
      InputStream is = getResultSet().getAsciiStream(tcd.getName());
      byte[] buf = new byte[((String)tcd.getValue()).length()];
      is.read(buf);
      if (is.read() != -1)
        fail("Invalid length of ASCII stream!");
      is.close();
      String s = new String(buf);
      assertEquals("Invalid String!",(String)tcd.getValue(),s);
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
    catch(IOException ie) { fail(EU.getExceptionMessage(ie)); }
  } /* testGetAsciiStream */
  
  @Test
  @Override
  public void testUpdateAsciiStream()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(_listCdSimple,"CVARCHAR_255");
      InputStream is = new ByteArrayInputStream(((String)tcd.getValue()).getBytes());
      getResultSet().updateAsciiStream(tcd.getName(),is);
    }
    catch(SQLFeatureNotSupportedException sfnse) { System.out.println(EU.getExceptionMessage(sfnse)); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateAsciiStream */
  
  @Test
  @Override
  public void testUpdateAsciiStream_Int()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(_listCdSimple,"CVARCHAR_255");
      InputStream is = new ByteArrayInputStream(((String)tcd.getValue()).getBytes());
      getResultSet().updateAsciiStream(tcd.getName(),is,((String)tcd.getValue()).length());
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateAsciiStream_Int */
  
  @Test
  @Override
  public void testUpdateAsciiStream_Long()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(_listCdSimple,"CVARCHAR_255");
      InputStream is = new ByteArrayInputStream(((String)tcd.getValue()).getBytes());
      getResultSet().updateAsciiStream(tcd.getName(),is,(long)((String)tcd.getValue()).length());
    }
    catch(SQLFeatureNotSupportedException sfnse) { System.out.println(EU.getExceptionMessage(sfnse)); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateAsciiStream_Long */
  
  @Test
  @Override
  @SuppressWarnings("deprecation")
  public void testGetUnicodeStream()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(TestSqlDatabase._listCdSimple,"CNVARCHAR_127");
      Reader rdr = new InputStreamReader(getResultSet().getUnicodeStream(tcd.getName()),"UTF-8");
      char[] cbuf = new char[((String)tcd.getValue()).length()];
      int iLength = 0;
      for (int iRead = rdr.read(cbuf); (iRead != -1) && (iLength < cbuf.length); iRead = rdr.read(cbuf,iLength,cbuf.length-iLength))
        iLength = iLength + iRead;
      rdr.close();
      assertEquals("Invalid length of character stream!",cbuf.length,iLength);
      String s = new String(cbuf);
      assertEquals("Invalid String!",(String)tcd.getValue(),s);
    }
    catch(SQLFeatureNotSupportedException sfnse) { printExceptionMessage(sfnse); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
    catch(IOException ie) { fail(EU.getExceptionMessage(ie)); }
  } /* testGetUnicodeStream */
  
  @Test
  @Override
  public void testGetCharacterStream()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(TestSqlDatabase._listCdSimple,"CVARCHAR_255");
      Reader rdr = getResultSet().getCharacterStream(tcd.getName());
      char[] cbuf = new char[((String)tcd.getValue()).length()];
      rdr.read(cbuf);
      if (rdr.read() != -1)
        fail("Invalid length of character stream!");
      rdr.close();
      String s = new String(cbuf);
      assertEquals("Invalid String!",(String)tcd.getValue(),s);
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
    catch(IOException ie) { fail(EU.getExceptionMessage(ie)); }
  } /* testGetCharacterStream */
  
  @Test
  @Override
  public void testUpdateCharacterStream()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(_listCdSimple,"CVARCHAR_255");
      Reader rdr = new StringReader((String)tcd.getValue());
      getResultSet().updateCharacterStream(tcd.getName(),rdr);
    }
    catch(SQLFeatureNotSupportedException se) { System.out.println(EU.getExceptionMessage(se)); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateCharacterStream */
  
  @Test
  @Override
  public void testUpdateCharacterStream_Int()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(_listCdSimple,"CVARCHAR_255");
      Reader rdr = new StringReader((String)tcd.getValue());
      getResultSet().updateCharacterStream(tcd.getName(),rdr,((String)tcd.getValue()).length());
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateCharacterStream_Int */
  
  @Test
  @Override
  public void testUpdateCharacterStream_Long()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(
        _listCdSimple,"CVARCHAR_255");
      Reader rdr = new StringReader((String)tcd.getValue());
      getResultSet().updateCharacterStream(tcd.getName(),rdr,(long)((String)tcd.getValue()).length());
    }
    catch(SQLFeatureNotSupportedException sfnse) { System.out.println(EU.getExceptionMessage(sfnse)); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateCharacterStream_Long */
  
  @Test
  @Override
  public void testGetNCharacterStream()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(TestSqlDatabase._listCdSimple,"CNVARCHAR_127");
      Reader rdr = getResultSet().getNCharacterStream(tcd.getName());
      char[] cbuf = new char[((String)tcd.getValue()).length()];
      rdr.read(cbuf);
      if (rdr.read() != -1)
        fail("Invalid length of character stream!");
      rdr.close();
      String s = new String(cbuf);
      assertEquals("Invalid String!",(String)tcd.getValue(),s);
    }
    catch(SQLFeatureNotSupportedException sfnse) { fail(EU.getExceptionMessage(sfnse)); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
    catch(IOException ie) { fail(EU.getExceptionMessage(ie)); }
  } /* testGetNCharacterStream */
  
  @Test
  @Override
  public void testUpdateNCharacterStream()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(_listCdSimple,"CNVARCHAR_127");
      Reader rdr = new StringReader((String)tcd.getValue());
      getResultSet().updateCharacterStream(tcd.getName(),rdr);
    }
    catch(SQLFeatureNotSupportedException sfnse) { System.out.println(EU.getExceptionMessage(sfnse)); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateNCharacterStream */
  
  @Test
  @Override
  public void testUpdateNCharacterStream_Int()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(_listCdSimple,"CNVARCHAR_127");
      Reader rdr = new StringReader((String)tcd.getValue());
      getResultSet().updateCharacterStream(tcd.getName(),rdr,((String)tcd.getValue()).length());
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateNCharacterStream_String_Int */
  
  @Test
  @Override
  public void testUpdateNCharacterStream_Long()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(_listCdSimple,"CNVARCHAR_127");
      Reader rdr = new StringReader((String)tcd.getValue());
      getResultSet().updateCharacterStream(tcd.getName(),rdr,(long)((String)tcd.getValue()).length());
    }
    catch(SQLFeatureNotSupportedException sfnse) { System.out.println(EU.getExceptionMessage(sfnse)); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateNCharacterStream_String_Long */

  @Test
  @Override
  public void testGetBinaryStream()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(TestSqlDatabase._listCdSimple,"CVARBINARY_255");
      InputStream is = getResultSet().getBinaryStream(tcd.getName());
      byte[] buf = new byte[((byte[])tcd.getValue()).length];
      is.read(buf);
      if (is.read() != -1)
        fail("Invalid length of binary stream!");
      is.close();
      assertTrue("Invalid byte array!",Arrays.equals((byte[])tcd.getValue(),buf));
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
    catch(IOException ie) { fail(EU.getExceptionMessage(ie)); }
  } /* testGetBinaryStream */
  
  @Test
  @Override
  public void testUpdateBinaryStream()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(_listCdSimple,"CVARBINARY_255");
      InputStream is = new ByteArrayInputStream((byte[])tcd.getValue());
      getResultSet().updateBinaryStream(tcd.getName(),is);
    }
    catch(SQLFeatureNotSupportedException sfnse) { System.out.println(EU.getExceptionMessage(sfnse)); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateBinaryStream */
  
  @Test
  @Override
  public void testUpdateBinaryStream_Int()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(_listCdSimple,"CVARBINARY_255");
      InputStream is = new ByteArrayInputStream((byte[])tcd.getValue());
      getResultSet().updateBinaryStream(tcd.getName(),is,((byte[])tcd.getValue()).length);
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateBinaryStream_Int */
  
  @Test
  @Override
  public void testUpdateBinaryStream_Long()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(_listCdSimple,"CVARBINARY_255");
      InputStream is = new ByteArrayInputStream((byte[])tcd.getValue());
      getResultSet().updateBinaryStream(tcd.getName(),is,(long)((byte[])tcd.getValue()).length);
    }
    catch(SQLFeatureNotSupportedException sfnse) { System.out.println(EU.getExceptionMessage(sfnse)); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateBinaryStream_Long */
  
  @Test
  @Override
  public void testGetObject()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(TestSqlDatabase._listCdSimple,"CINTERVAL_YEAR_3_MONTH");
      Object o = getResultSet().getObject(tcd.getName());
      assertEquals("Invalid Interval!",((Interval)tcd.getValue()).toDuration(),(Duration)o);
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testGetObject */

  @Test
  @Override
  public void testUpdateObject()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(_listCdSimple,"CDATE");
      getResultSet().updateObject(tcd.getName(),tcd.getValue());
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateObject */
  
  @Test
  @Override
  public void testUpdateObject_Int()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(_listCdSimple,"CDECIMAL_15_5");
      getResultSet().updateObject(tcd.getName(),tcd.getValue(),3);
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateObject_Int */
  
  @Test
  @Override
  public void testGetObject_Map()
  {
    enter();
    try
    {
      openResultSet(_sSqlQueryComplex,ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
      TestColumnDefinition tcd = findColumnDefinition(TestSqlDatabase._listCdComplex,"CUDT");
      Map<String,Class<?>> map = new HashMap<String,Class<?>>();
      map.put(tcd.getType(), tcd.getValue().getClass());
      Object o = getResultSet().getObject(tcd.getName(),map);
      assertEquals("Invalid Udt!",tcd.getValue(),o);
    }
    catch(SQLFeatureNotSupportedException snse) { System.out.println("testGetObject_Map: "+EU.getExceptionMessage(snse)); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testGetObject_Map */
  
  @Test
  @Override
  public void testGetObject_Class()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(TestSqlDatabase._listCdSimple,"CDATE");
      Date date = getResultSet().getObject(tcd.getName(),Date.class);
      assertEquals("Invalid Date!",(Date)tcd.getValue(),date);
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testGetObject_Class */
  
  @Test
  @Override
  public void testGetRef()
  {
    enter();
  } /* testGetRef */
  
  @Test
  @Override
  public void testUpdateRef()
  {
    enter();
  } /* testUpdateRef */
  
  @Test
  @Override
  public void testGetBlob()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(TestSqlDatabase._listCdSimple,"CBLOB");
      Blob blob = getResultSet().getBlob(tcd.getName());
      byte[] buf = blob.getBytes(1l,(int)blob.length());
      assertTrue("Invalid Blob!",Arrays.equals((byte[])tcd.getValue(),buf));
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testGetBlob */
  
  @Test
  @Override
  public void testUpdateBlob()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(_listCdSimple,"CBLOB");
      Blob blob = getResultSet().getStatement().getConnection().createBlob();
      blob.setBytes(1, (byte[])tcd.getValue());
      getResultSet().updateBlob(tcd.getName(),blob);
    }
    catch(SQLFeatureNotSupportedException sfnse) { System.out.println(EU.getExceptionMessage(sfnse)); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateBlob */

  @Test
  @Override
  public void testUpdateBlob_InputStream()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(_listCdSimple,"CBLOB");
      InputStream is = new ByteArrayInputStream((byte[])tcd.getValue());
      getResultSet().updateBlob(tcd.getName(),is);
    }
    catch(SQLFeatureNotSupportedException sfnse) { System.out.println(EU.getExceptionMessage(sfnse)); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateBlob_InputStream */
  
  @Override
  @Test
  public void testUpdateBlob_InputStream_Long()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(_listCdSimple,"CBLOB");
      InputStream is = new ByteArrayInputStream((byte[])tcd.getValue());
      getResultSet().updateBlob(tcd.getName(),is,((byte[])tcd.getValue()).length);
    }
    catch(SQLFeatureNotSupportedException sfnse) { fail(EU.getExceptionMessage(sfnse)); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateBlob_String_InputStream_Long */
  
  @Test
  @Override
  public void testGetClob()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(TestSqlDatabase._listCdSimple,"CCLOB_2M");
      Clob clob = getResultSet().getClob(tcd.getName());
      String s = clob.getSubString(1l,(int)clob.length());
      assertEquals("Invalid Clob!",(String)tcd.getValue(),s);
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testGetClob */
  
  @Test
  @Override
  public void testUpdateClob()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(_listCdSimple,"CCLOB_2M");
      Clob clob = getResultSet().getStatement().getConnection().createClob();
      clob.setString(1l,(String)tcd.getValue());
      getResultSet().updateClob(tcd.getName(),clob);
    }
    catch(SQLFeatureNotSupportedException sfnse) { System.out.println(EU.getExceptionMessage(sfnse)); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateClob */

  @Test
  @Override
  public void testUpdateClob_Reader()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(_listCdSimple,"CCLOB_2M");
      Reader rdr = new StringReader((String)tcd.getValue());
      getResultSet().updateClob(tcd.getName(),rdr);
    }
    catch(SQLFeatureNotSupportedException sfnse) { System.out.println(EU.getExceptionMessage(sfnse)); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateClob_Reader */
  
  @Test
  @Override
  public void testUpdateClob_Reader_Long()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(_listCdSimple,"CCLOB_2M");
      Reader rdr = new StringReader((String)tcd.getValue());
      getResultSet().updateClob(tcd.getName(),rdr,((String)tcd.getValue()).length());
    }
    catch(SQLFeatureNotSupportedException sfnse) { System.out.println(EU.getExceptionMessage(sfnse)); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateClob_Reader_Long */
  
  @Test
  @Override
  public void testGetNClob()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(TestSqlDatabase._listCdSimple,"CNCLOB_1M");
      NClob nclob = getResultSet().getNClob(tcd.getName());
      String s = nclob.getSubString(1l,(int)nclob.length());
      assertEquals("Invalid NClob!",(String)tcd.getValue(),s);
    }
    catch(SQLFeatureNotSupportedException fnse) { System.out.println("testNClob: "+EU.getExceptionMessage(fnse)); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testGetNClob */
  
  @Test
  @Override
  public void testUpdateNClob()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(_listCdSimple,"CNCLOB_1M");
      NClob nclob = getResultSet().getStatement().getConnection().createNClob();
      nclob.setString(1l, (String)tcd.getValue());
      getResultSet().updateNClob(tcd.getName(),nclob);
    }
    catch(SQLFeatureNotSupportedException sfnse) { System.out.println(EU.getExceptionMessage(sfnse)); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateNClob */
  
  @Test
  @Override
  public void testUpdateNClob_Reader()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(_listCdSimple,"CNCLOB_1M");
      Reader rdr = new StringReader((String)tcd.getValue());
      getResultSet().updateNClob(tcd.getName(),rdr);
    }
    catch(SQLFeatureNotSupportedException sfnse) { System.out.println(EU.getExceptionMessage(sfnse)); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateNClob_Reader */
  
  @Test
  @Override
  public void testUpdateNClob_Reader_Long()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition( _listCdSimple,"CNCLOB_1M");
      Reader rdr = new StringReader((String)tcd.getValue());
      getResultSet().updateNClob(tcd.getName(),rdr,((String)tcd.getValue()).length());
    }
    catch(SQLFeatureNotSupportedException sfnse) { System.out.println(EU.getExceptionMessage(sfnse)); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateNClob_Reader_Long */
  
  @Test
  @Override
  public void testGetSqlXml()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(TestSqlDatabase._listCdSimple,"CXML");
      SQLXML sqlxml = getResultSet().getSQLXML(tcd.getName());
      String s = sqlxml.getString().replaceAll("\\n\\s*","");
      assertEquals("Invalid SQLXML!",(String)tcd.getValue(),s);
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testGetSqlXml */
  
  @Test
  @Override
  public void testUpdateSqlXml()
  {
    enter();
    try
    {
      TestColumnDefinition tcd = findColumnDefinition(_listCdSimple,"CXML");
      SQLXML sqlxml = getResultSet().getStatement().getConnection().createSQLXML();
      sqlxml.setString((String)tcd.getValue());
      getResultSet().updateSQLXML(tcd.getName(),sqlxml);
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateSqlXml */
  
  @Test
  @Override
  public void testGetArray()
  {
    enter();
    try 
    { 
      openResultSet(_sSqlQueryComplex,ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
      TestColumnDefinition tcd = findColumnDefinition(TestSqlDatabase._listCdComplex,"CARRAY");
      Array array = getResultSet().getArray(tcd.getName());
      if (array != null)
        array.free();
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testGetArray */
  
  @Test
  @Override
  public void testUpdateArray()
  {
    enter();
    try 
    { 
      openResultSet(_sSqlQueryComplex,ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_UPDATABLE);
      // TestColumnDefinition tcd = findColumnDefinition(_listCdComplex,"CARRAY");
      Array array = getResultSet().getArray(1);
      getResultSet().updateArray("CARRAY",array);
      array.free();
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateArray */
  
  @Test
  @Override
  public void testGetRowId()
  {
    enter();
    try { getResultSet().getRowId("CCHAR_5"); }
    catch(SQLFeatureNotSupportedException sfnse) { System.out.println(EU.getExceptionMessage(sfnse)); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testGetRowId */
  
  @Test
  @Override
  public void testUpdateRowId()
  {
    enter();
    try 
    { 
      RowId rowid = getResultSet().getRowId(1);
      getResultSet().updateRowId("CCHAR_5",rowid); 
    }
    catch(SQLFeatureNotSupportedException sfnse) { System.out.println(EU.getExceptionMessage(sfnse)); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testUpdateRowId */
  
  @Test
  @Override
  public void testGetUrl()
  {
    enter();
  } /* testGetUrl */
  
  @Test
  @Override
  public void testInsertRow() throws SQLException
  {
    enter();
    try 
    {
      openResultSet(_sSqlQuerySimple,ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_UPDATABLE);
      getResultSet().moveToInsertRow();
      TestColumnDefinition tcd = findColumnDefinition(
        _listCdSimple,"CINTEGER");
      getResultSet().updateInt(tcd.getName(),((Integer)tcd.getValue()).intValue());
      getResultSet().insertRow();
      // restore the database
      tearDown();
      setUpClass();
      setUp();
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testInsertRow */
  
  @Test
  @Override
  public void testDeleteRow() throws SQLException
  {
    enter();
    try 
    {
      // cannot delete row in simple table because of foreign key
      openResultSet(_sSqlQueryComplex,ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_UPDATABLE);
      getResultSet().deleteRow();
      // restore the database
      tearDown();
      setUpClass();
      setUp();
    }
    catch(Exception e) { fail(EU.getExceptionMessage(e)); }
  } /* testDeleteRow */
   
  @Test
  @Override
  public void testUpdateRow() throws SQLException
  {
    enter();
    try 
    { 
      openResultSet(_sSqlQuerySimple,ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_UPDATABLE);
      TestColumnDefinition tcd = findColumnDefinition(
        _listCdSimple,"CXML");
      SQLXML sqlxml = getResultSet().getStatement().getConnection().createSQLXML();
      sqlxml.setString("<a>Konrad Zuse</a>");
      getResultSet().updateSQLXML(tcd.getName(), sqlxml);
      getResultSet().updateRow(); 
      sqlxml.free();
      // restore the database
      tearDown();
      setUpClass();
      setUp();
    }
    catch(Exception e) { fail(EU.getExceptionMessage(e)); }
  } /* testUpdateRow */

}
