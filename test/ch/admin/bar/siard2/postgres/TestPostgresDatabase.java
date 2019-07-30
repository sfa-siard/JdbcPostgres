package ch.admin.bar.siard2.postgres;

import static org.junit.Assert.assertSame;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.sql.Date;
import java.util.*;

import ch.enterag.utils.*;
import ch.enterag.utils.base.*;
import ch.enterag.sqlparser.*;
import ch.enterag.sqlparser.identifier.*;
import ch.admin.bar.siard2.jdbc.*;

public class TestPostgresDatabase
{
  public static final String _sTEST_SCHEMA = "TESTPGSCHEMA";
  private static final String _sTEST_TABLE_SIMPLE = "TPGSIMPLE";
  public static QualifiedId getQualifiedSimpleTable() { return new QualifiedId(null,_sTEST_SCHEMA,_sTEST_TABLE_SIMPLE); }
  private static final String _sTEST_TABLE_COMPLEX = "TPGCOMPLEX";
  public static QualifiedId getQualifiedComplexTable() { return new QualifiedId(null,_sTEST_SCHEMA,_sTEST_TABLE_COMPLEX); }
  private static final String _sTEST_TYPE_DISTINCT = "TPGDIST";
  public static QualifiedId getQualifiedDistinctType() { return new QualifiedId(null,_sTEST_SCHEMA, _sTEST_TYPE_DISTINCT); }

  private static class ColumnDefinition extends TestColumnDefinition
  {
    @Override
    public String getValueLiteral()
    {
      String sValueLiteral = "NULL";
      if (_oValue != null)
      {
        if (_sType.equals("GEOMETRY") ||
            _sType.equals("POINT") ||
            _sType.equals("LINESTRING") ||
            _sType.equals("POLYGON") ||
            _sType.equals("MULTIPOINT") ||
            _sType.equals("MULTILINESTRING") ||
            _sType.equals("MULTIPOLYGON") ||
            _sType.equals("GEOMETRYCOLLECTION"))
          sValueLiteral = "GeomFromText("+super.getValueLiteral()+")";
        else
          sValueLiteral = super.getValueLiteral();
      }
      return sValueLiteral;
    }
    public ColumnDefinition(String sName, String sType, Object oValue)
    {
      super(sName,sType,oValue);
    }
  } /* class ColumnDefinition */
  
  public static int _iPrimarySimple = -1;
  public static int _iCandidateSimple = -1;
  @SuppressWarnings("deprecation")
  private static List<TestColumnDefinition> getCdSimple() 
  {
    List<TestColumnDefinition> listCdSimple = new ArrayList<TestColumnDefinition>();
    
    // Numeric Data Types: Integer Types (Exact Values)
    _iPrimarySimple = listCdSimple.size(); // next column will be primary key column 
    listCdSimple.add(new ColumnDefinition("CINTEGER",PostgresType.INTEGER.getKeyword(),Integer.valueOf(1000000)));
    listCdSimple.add(new ColumnDefinition("CSMALLINT",PostgresType.SMALLINT.getKeyword(),Short.valueOf((short)-32767)));
    listCdSimple.add(new ColumnDefinition("CBIGINT",PostgresType.BIGINT.getKeyword(),Long.valueOf(-2147483648L)));
    _iCandidateSimple = listCdSimple.size(); // next column will be candidate key column 
    listCdSimple.add(new ColumnDefinition("CSERIAL",PostgresType.SERIAL.getKeyword(),Integer.valueOf(-1000000)));
    listCdSimple.add(new ColumnDefinition("CSMALLSERIAL",PostgresType.SMALLSERIAL.getKeyword(),Short.valueOf((short)32767)));
    listCdSimple.add(new ColumnDefinition("CBIGSERIAL",PostgresType.BIGSERIAL.getKeyword(),Long.valueOf(2147483648L)));
    listCdSimple.add(new ColumnDefinition("CMONEY",PostgresType.MONEY.getKeyword(),BigDecimal.valueOf(12345678901234l, 2)));
    
    // Numeric Data Types: Fixed-Point Types (Exact Values)
    listCdSimple.add(new ColumnDefinition("CNUMERIC_5_2",PostgresType.NUMERIC.getKeyword()+"(5,2)",BigDecimal.valueOf(12345, 2)));
    listCdSimple.add(new ColumnDefinition("CDECIMAL_15_5",PostgresType.DECIMAL.getKeyword()+"(15,5)",new BigDecimal("123455679.12345")));
    // Numeric Data Types: Floating-Point Types (Approximate Values)
    listCdSimple.add(new ColumnDefinition("CDOUBLE_16_14",PostgresType.DOUBLE.getKeyword()+"(16,14)",Double.valueOf(Math.E)));
    listCdSimple.add(new ColumnDefinition("CREAL_9_7",PostgresType.REAL.getKeyword()+"(9,7)",new Float(Double.valueOf(Math.PI).floatValue())));
    listCdSimple.add(new ColumnDefinition("CBOOL",PostgresType.BOOLEAN.getKeyword(),Boolean.FALSE));

    // Date and Time Types
    listCdSimple.add(new ColumnDefinition("CDATE",PostgresType.DATE.getKeyword(),new Date(2016-1900, 10, 30)));
    listCdSimple.add(new ColumnDefinition("CTIME",PostgresType.TIME.getKeyword(),new Time(12, 34, 56)));
    listCdSimple.add(new ColumnDefinition("CTIMETZ",PostgresType.TIMETZ.getKeyword(),new Time(9, 34, 56)));
    listCdSimple.add(new ColumnDefinition("CTIMESTAMP",PostgresType.TIMESTAMP.getKeyword(),new Timestamp(2016-1900,10,30,12,34,56,0)));
    listCdSimple.add(new ColumnDefinition("CTIMESTAMPTZ",PostgresType.TIMESTAMP.getKeyword(),new Timestamp(2019-1900,07,29,9,34,56,0)));
    listCdSimple.add(new ColumnDefinition("CINTERVALYM",PostgresType.INTERVAL.getKeyword()+" year to month",new Interval(1,1,3)));
    listCdSimple.add(new ColumnDefinition("CINTERVALDM",PostgresType.INTERVAL.getKeyword()+" day to micro",new Interval(-1,1,11,23,34,456789000l)));

    // CHAR/VARCHAR
    listCdSimple.add(new ColumnDefinition("CCHAR_4",PostgresType.CHAR.getKeyword()+"(4)",TestUtils.getString(3)));
    listCdSimple.add(new ColumnDefinition("CVARCHAR_500",PostgresType.VARCHAR.getKeyword()+"(500)",TestUtils.getString(255)));
    listCdSimple.add(new ColumnDefinition("CTEXT",PostgresType.TEXT.getKeyword(),TestUtils.getString(5000)));
    listCdSimple.add(new ColumnDefinition("CJSON",PostgresType.JSON.getKeyword(),"{ \"name\":\"John\", \"age\":30, \"car\":null }"));
    listCdSimple.add(new ColumnDefinition("CJSONB",PostgresType.JSONB.getKeyword(),"{\r\n" + 
      "    \"glossary\": {\r\n" + 
      "        \"title\": \"example glossary\",\r\n" + 
      "    \"GlossDiv\": {\r\n" + 
      "            \"title\": \"S\",\r\n" + 
      "      \"GlossList\": {\r\n" + 
      "                \"GlossEntry\": {\r\n" + 
      "                    \"ID\": \"SGML\",\r\n" + 
      "          \"SortAs\": \"SGML\",\r\n" + 
      "          \"GlossTerm\": \"Standard Generalized Markup Language\",\r\n" + 
      "          \"Acronym\": \"SGML\",\r\n" + 
      "          \"Abbrev\": \"ISO 8879:1986\",\r\n" + 
      "          \"GlossDef\": {\r\n" + 
      "                        \"para\": \"A meta-markup language, used to create markup languages such as DocBook.\",\r\n" + 
      "            \"GlossSeeAlso\": [\"GML\", \"XML\"]\r\n" + 
      "                    },\r\n" + 
      "          \"GlossSee\": \"markup\"\r\n" + 
      "                }\r\n" + 
      "            }\r\n" + 
      "        }\r\n" + 
      "    }\r\n" + 
      "}"));
    listCdSimple.add(new ColumnDefinition("CXML",PostgresType.XML.getKeyword(),"<a>Ein schöööönes XML Fragment</a>"));
    listCdSimple.add(new ColumnDefinition("CTSVECTOR",PostgresType.TSVECTOR.getKeyword(),"a fat cat sat on a mat and ate a fat rat"));
    listCdSimple.add(new ColumnDefinition("CTSQUERY",PostgresType.TSQUERY.getKeyword(),"fat:ab & cat"));

    // BINARY/VARBINARY
    listCdSimple.add(new ColumnDefinition("CBIT_256",PostgresType.BIT.getKeyword()+"(256)",TestUtils.getBytes(32)));
    listCdSimple.add(new ColumnDefinition("CVARBIT_805",PostgresType.VARBIT.getKeyword()+"(805)",TestUtils.getBytes(101)));
    listCdSimple.add(new ColumnDefinition("CBYTEA",PostgresType.BYTEA.getKeyword()+"(5000)",TestUtils.getBytes(5000)));
    listCdSimple.add(new ColumnDefinition("CUUID",PostgresType.UUID.getKeyword(),UUID.randomUUID()));
    
    // BLOB
    listCdSimple.add(new ColumnDefinition("COID",PostgresType.OID.getKeyword(),TestUtils.getBytes(500)));
    listCdSimple.add(new ColumnDefinition("CMACADDR",PostgresType.MACADDR.getKeyword(),TestUtils.getBytes(6)));
    listCdSimple.add(new ColumnDefinition("CMACADDR8",PostgresType.MACADDR8.getKeyword(),TestUtils.getBytes(8)));
    
    /* spatial */
    listCdSimple.add(new ColumnDefinition("CGEOMETRY","GEOMETRY","POINT (1 2)"));
    listCdSimple.add(new ColumnDefinition("CPOINT","POINT","POINT (1 2)"));
    listCdSimple.add(new ColumnDefinition("CLINESTRING","LINESTRING","LINESTRING (0 0, 1 1, 2 2)"));
    listCdSimple.add(new ColumnDefinition("CPOLYGON","POLYGON","POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (5 5, 7 5, 7 7, 5 7, 5 5))"));
    listCdSimple.add(new ColumnDefinition("CMULTIPOINT","MULTIPOINT","MULTIPOINT (1 1, 2 2, 3 3)"));
    listCdSimple.add(new ColumnDefinition("CMULTILINESTRING","MULTILINESTRING","MULTILINESTRING ((10 10, 20 20), (15 15, 30 15))"));
    listCdSimple.add(new ColumnDefinition("CMULTIPOLYGON","MULTIPOLYGON","MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0)), ((5 5, 7 5, 7 7, 5 7, 5 5)))"));
    listCdSimple.add(new ColumnDefinition("CGEOMETRYCOLLECTION","GEOMETRYCOLLECTION","GEOMETRYCOLLECTION (POINT (1 1), LINESTRING (0 0, 1 1, 2 2, 3 3, 4 4))"));
    
    return listCdSimple;    
  }
  public static List<TestColumnDefinition> _listCdSimple = getCdSimple();
  
  private Connection _conn = null;
  private String _sDbUser = null;
  
  public TestPostgresDatabase(PostgresConnection connPostgres, String sDbUser)
    throws SQLException
  {
    _conn = connPostgres.unwrap(Connection.class);
    _sDbUser = sDbUser;
    _conn.setAutoCommit(false);
    drop();
    create();
  } /* constructor */

  private void drop()
  {
    deleteTables();
    dropTables();
    dropTypes();
    dropSchema();
  } /* drop */
  
  private void executeDrop(String sSql)
  {
    try
    {
      Statement stmt = _conn.createStatement();
      stmt.executeUpdate(sSql);
      stmt.close();
      _conn.commit();
    }
    catch(SQLException se) 
    { 
      System.out.println(EU.getExceptionMessage(se));
      /* terminate transaction */
      try { _conn.rollback(); }
      catch(SQLException seRollback) { System.out.println("Rollback failed with "+EU.getExceptionMessage(seRollback)); }
    }
  } /* executeDrop */

  private void deleteTables()
  {
    deleteTable(getQualifiedSimpleTable());
    deleteTable(getQualifiedComplexTable());
  } /* deleteTables */
  
  private void deleteTable(QualifiedId qiTable)
  {
    executeDrop("DELETE FROM "+qiTable.format());
  } /* deleteTable */
  
  private void dropTables()
  {
    dropTable(getQualifiedSimpleTable());
    dropTable(getQualifiedComplexTable());
  } /* dropTables */
  
  private void dropTable(QualifiedId qiTable)
  {
    executeDrop("DROP TABLE "+qiTable.format());
  } /* dropTable */

  private void dropTypes()
  {
    dropType(getQualifiedDistinctType());
  } /* dropTypes */
  
  private void dropType(QualifiedId qiType)
  {
    executeDrop("DROP TYPE "+qiType.format()); 
  } /* dropType */
  
  private void dropSchema()
  {
    executeDrop("DROP SCHEMA "+SqlLiterals.formatId(_sTEST_SCHEMA));
  } /* dropSchema */

  private void create()
    throws SQLException
  {
    createSchema();
    createTypes();
    createTables();
    insertTables();
  } /* create */

  private void executeCreate(String sSql)
    throws SQLException
  {
    Statement stmt = _conn.createStatement();
    stmt.executeUpdate(sSql);
    stmt.close();
    _conn.commit();
  } /* executeCreate */

  private void createSchema()
    throws SQLException
  {
    SchemaId sid = new SchemaId(null,_sTEST_SCHEMA);
    executeCreate("CREATE SCHEMA "+sid.format()+" AUTHORIZATION "+_sDbUser);
  } /* createSchema */
  
  private void createTypes()
    throws SQLException
  {
    /**
    createType(getQualifiedDistinctType(),_listBaseDistinct);
    **/
  } /* createTypes */

  private void createType(QualifiedId qiType, List<TestColumnDefinition> listBase)
    throws SQLException
  {
    TestColumnDefinition tcd = listBase.get(0);
    executeCreate("CREATE TYPE "+qiType.format()+" FROM "+tcd.getType());
  } /* createType */

  private void createTables()
    throws SQLException
  {
    createTable(getQualifiedSimpleTable(),_listCdSimple,
      Arrays.asList(new String[] {_listCdSimple.get(_iPrimarySimple).getName()}),
      Arrays.asList(new String[] {_listCdSimple.get(_iCandidateSimple).getName()}));
    /**
    createTable(getQualifiedComplexTable(),_listCdComplex,null,null);
    **/
  } /* createTables */
  
  private void createTable(QualifiedId qiTable, List<TestColumnDefinition> listCd,
    List<String> listPrimary, List<String> listUnique)
    throws SQLException
  {
    StringBuilder sbSql = new StringBuilder("CREATE TABLE ");
    sbSql.append(qiTable.format());
    sbSql.append("\r\n(\r\n  ");
    for (int iColumn = 0; iColumn < listCd.size(); iColumn++)
    {
      TestColumnDefinition tcd = listCd.get(iColumn); 
      if (iColumn > 0)
        sbSql.append(",\r\n  ");
      sbSql.append(tcd.getName());
      sbSql.append(" ");
      sbSql.append(tcd.getType());
    }
    if (listPrimary != null)
    {
      sbSql.append(",\r\n  CONSTRAINT PK");
      sbSql.append(qiTable.getName());
      sbSql.append(" PRIMARY KEY(");
      sbSql.append(SqlLiterals.formatIdentifierCommaList(listPrimary));
      sbSql.append(")");
    }
    if (listUnique != null)
    {
      sbSql.append(",\r\n  CONSTRAINT UK");
      sbSql.append(qiTable.getName());
      sbSql.append(" UNIQUE(");
      sbSql.append(SqlLiterals.formatIdentifierCommaList(listUnique));
      sbSql.append(")");
    }
    sbSql.append("\r\n)");
    executeCreate(sbSql.toString());
  } /* createTable */
  
  private void insertTables()
    throws SQLException
  {
    /**
    insertTable(getQualifiedSimpleTable(),_listCdSimple);
    insertTable(getQualifiedComplexTable(),_listCdComplex);
    **/
  } /* insertTables */
  
  private void insertTable(QualifiedId qiTable, List<TestColumnDefinition> listCd)
    throws SQLException
  {
    StringBuilder sbSql = new StringBuilder("INSERT INTO ");
    sbSql.append(qiTable.format());
    sbSql.append("\r\n(\r\n  ");
    for (int iColumn = 0; iColumn < listCd.size(); iColumn++)
    {
      TestColumnDefinition tcd = listCd.get(iColumn); 
      if (tcd.getValue() != null)
      {
        if (iColumn > 0)
          sbSql.append(",\r\n  ");
        sbSql.append(tcd.getName());
      }
    }
    sbSql.append("\r\n)\r\nVALUES\r\n(\r\n  ");
    List<Object> listLobs = new ArrayList<Object>();
    for (int iColumn = 0; iColumn < listCd.size(); iColumn++)
    {
      TestColumnDefinition tcd = listCd.get(iColumn);
      if (tcd.getValue() != null)
      {
        if (iColumn > 0)
          sbSql.append(",\r\n  ");
        String sLiteral = tcd.getValueLiteral();
        if (sLiteral.length() < 1000)
          sbSql.append(sLiteral);
        else
        {
          sbSql.append("?");
          listLobs.add(tcd.getValue());
        }
      }
    }
    sbSql.append("\r\n)");
    PreparedStatement pstmt = _conn.prepareStatement(sbSql.toString());
    for (int iLob = 0; iLob < listLobs.size(); iLob++)
    {
      Object o = listLobs.get(iLob);
      if (o instanceof String)
      {
        Reader rdrClob = new StringReader((String)o);
        pstmt.setCharacterStream(iLob+1, rdrClob);
        
      }
      else if (o instanceof byte[])
      {
        InputStream isBlob = new ByteArrayInputStream((byte[])o);
        pstmt.setBinaryStream(iLob+1, isBlob);
      }
      else
        throw new SQLException("Invalid LOB type "+o.getClass().getName()+"!");
    }
    int iResult = pstmt.executeUpdate();
    assertSame("Insert failed!",1,iResult);
    pstmt.close();
    _conn.commit();
  } /* insertTable */

}
