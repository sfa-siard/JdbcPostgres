package ch.admin.bar.siard2.postgres;

import static org.junit.Assert.assertSame;

import java.io.*;
import java.math.*;
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
  private static final String _sTEST_INTEGER_DOMAIN = "TPGDOMAIN";
  public static QualifiedId getQualifiedDomainType() { return new QualifiedId(null,_sTEST_SCHEMA, _sTEST_INTEGER_DOMAIN); }
  private static final String _sTEST_BUILTIN_RANGE = "TPGBUILTIN";
  public static QualifiedId getQualifiedBuiltinRange() { return new QualifiedId(null,_sTEST_SCHEMA, _sTEST_BUILTIN_RANGE); }
  private static final String _sTEST_TYPE_ENUM = "TPGENUM";
  public static QualifiedId getQualifiedEnumType() { return new QualifiedId(null,_sTEST_SCHEMA, _sTEST_TYPE_ENUM); }
  private static final String _sTEST_TYPE_COMP = "TPGCOMP";
  public static QualifiedId getQualifiedCompositeType() { return new QualifiedId(null,_sTEST_SCHEMA, _sTEST_TYPE_COMP); }
  private static final String _sTEST_TYPE_RANGE = "TPGRANGE";
  public static QualifiedId getQualifiedRangeType() { return new QualifiedId(null,_sTEST_SCHEMA, _sTEST_TYPE_RANGE); }
  private static final String _sTEST_STRING_ARRAY = "TPGARRAY";
  public static QualifiedId getQualifiedArrayType() { return new QualifiedId(null,_sTEST_SCHEMA, _sTEST_STRING_ARRAY); }
  private static final String _sTEST_DOUBLE_MATRIX = "TPGMATRIX";
  public static QualifiedId getQualifiedMatrixType() { return new QualifiedId(null,_sTEST_SCHEMA, _sTEST_DOUBLE_MATRIX); }

  /*------------------------------------------------------------------*/
  public static class ColumnDefinition extends TestColumnDefinition
  {
    @Override
    public String getValueLiteral()
    {
      String sValueLiteral = "NULL";
      if (_oValue != null)
      {
        if (getName().equals("CINT_BUILTIN") || getName().equals("CSTRING_RANGE"))
        {
          StringBuilder sb = new StringBuilder();
          @SuppressWarnings("unchecked")
          List<ColumnDefinition> listCd = (List<ColumnDefinition>)getValue();
          ColumnDefinition cd0 = listCd.get(0);
          ColumnDefinition cd1 = listCd.get(1);
          if (cd0.getName().equals("min"))
            sb.append("[");
          else if (cd0.getName().equals("inf"))
            sb.append("(");
          else
            throw new IllegalArgumentException("Invalid lower bound type of RANGE "+cd0.getName()+"!");
          sb.append(cd0.getValueLiteral());
          sb.append(", ");
          sb.append(cd1.getValueLiteral());
          if (cd1.getName().equals("max"))
            sb.append("]");
          else if (cd1.getName().equals("sup"))
            sb.append(")");
          else
            throw new IllegalArgumentException("Invalid upper bound type of RANGE "+cd1.getName()+"!");
          sValueLiteral = PostgresLiterals.formatStringLiteral(sb.toString());
        }
        else if (getName().equals("CINT_DOMAIN") || getName().equals("CENUM_SUIT"))
        {
          @SuppressWarnings("unchecked")
          List<ColumnDefinition> listCd = (List<ColumnDefinition>)getValue();
          ColumnDefinition cd = listCd.get(0);
          sValueLiteral = cd.getValueLiteral();
        }
        else if (getName().equals("CCOMPOSITE"))
        {
          StringBuilder sb = new StringBuilder("(");
          @SuppressWarnings("unchecked")
          List<ColumnDefinition> listCd = (List<ColumnDefinition>)getValue();
          for (int iAttribute = 0; iAttribute < listCd.size(); iAttribute++)
          {
            ColumnDefinition cd = listCd.get(iAttribute);
            if (iAttribute > 0)
              sb.append(", ");
            sb.append(cd.getValueLiteral());
          }
          sb.append(")");
          sValueLiteral = sb.toString();
        }
        else if (getName().equals("CSTRING_ARRAY"))
        {
          StringBuilder sb = new StringBuilder("{");
          @SuppressWarnings("unchecked")
          List<ColumnDefinition> listCd = (List<ColumnDefinition>)getValue();
          for (int iElement = 0; iElement < listCd.size(); iElement++)
          {
            ColumnDefinition cd = listCd.get(iElement);
            if (iElement > 0)
              sb.append(", ");
            sb.append(cd.getValueLiteral());
          }
          sb.append("}");
          sValueLiteral = PostgresLiterals.formatStringLiteral(sb.toString());
        }
        else if (getName().equals("CDOUBLE_MATRIX"))
        {
          StringBuilder sb = new StringBuilder("{");
          @SuppressWarnings("unchecked")
          List<ColumnDefinition> listRowCd = (List<ColumnDefinition>)getValue();
          for (int iRow = 0; iRow < listRowCd.size(); iRow++)
          {
            ColumnDefinition cdRow = listRowCd.get(iRow);
            if (iRow > 0)
              sb.append(", ");
            sb.append("{");
            @SuppressWarnings("unchecked")
            List<ColumnDefinition> listColumnCd = (List<ColumnDefinition>)cdRow.getValue();
            for (int iColumn = 0; iColumn < listColumnCd.size(); iColumn++)
            {
              ColumnDefinition cdColumn = listColumnCd.get(iColumn);
              if (iColumn > 0)
                sb.append(", ");
              sb.append(cdColumn.getValueLiteral());
            }
            sb.append("}");
          }
          sb.append("}");
          sValueLiteral = PostgresLiterals.formatStringLiteral(sb.toString());
        }
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
  /*------------------------------------------------------------------*/
  
  public static int _iPrimarySimple = -1;
  public static int _iCandidateSimple = -1;
  @SuppressWarnings("deprecation")
  private static List<ColumnDefinition> getCdSimple() 
  {
    List<ColumnDefinition> listCdSimple = new ArrayList<ColumnDefinition>();
    
    // Numeric Data Types: Integer Types (Exact Values)
    _iPrimarySimple = listCdSimple.size(); // next column will be primary key column 
    listCdSimple.add(new ColumnDefinition("CINTEGER",PostgresType.INTEGER.getKeyword(),Integer.valueOf(-1000000)));
    listCdSimple.add(new ColumnDefinition("CSMALLINT",PostgresType.SMALLINT.getKeyword(),Short.valueOf((short)-32767)));
    listCdSimple.add(new ColumnDefinition("CBIGINT",PostgresType.BIGINT.getKeyword(),Long.valueOf(-2147483648L)));
    listCdSimple.add(new ColumnDefinition("COID",PostgresType.OID.getKeyword(),Integer.valueOf(19)));
    _iCandidateSimple = listCdSimple.size(); // next column will be candidate key column 
    listCdSimple.add(new ColumnDefinition("CSERIAL",PostgresType.SERIAL.getKeyword(),Integer.valueOf(1000000)));
    listCdSimple.add(new ColumnDefinition("CSMALLSERIAL",PostgresType.SMALLSERIAL.getKeyword(),Short.valueOf((short)32767)));
    listCdSimple.add(new ColumnDefinition("CBIGSERIAL",PostgresType.BIGSERIAL.getKeyword(),Long.valueOf(2147483648L)));
    listCdSimple.add(new ColumnDefinition("CMONEY",PostgresType.MONEY.getKeyword(),BigDecimal.valueOf(12345678901234l, 2)));
    
    // Numeric Data Types: Fixed-Point Types (Exact Values)
    listCdSimple.add(new ColumnDefinition("CNUMERIC_5_2",PostgresType.NUMERIC.getKeyword()+"(5,2)",BigDecimal.valueOf(12345, 2)));
    listCdSimple.add(new ColumnDefinition("CDECIMAL_15_5",PostgresType.NUMERIC.getAliases().toArray()[0]+"(15,5)",new BigDecimal("123455679.12345")));
    // Numeric Data Types: Floating-Point Types (Approximate Values)
    listCdSimple.add(new ColumnDefinition("CDOUBLE",PostgresType.DOUBLE.getKeyword(),Double.valueOf(Math.E)));
    listCdSimple.add(new ColumnDefinition("CREAL",PostgresType.REAL.getKeyword(),new Float(Double.valueOf(Math.PI).floatValue())));
    listCdSimple.add(new ColumnDefinition("CBOOL",PostgresType.BOOLEAN.getKeyword(),Boolean.FALSE));

    // Date and Time Types
    listCdSimple.add(new ColumnDefinition("CDATE",PostgresType.DATE.getKeyword(),new Date(2016-1900, 10, 30)));
    listCdSimple.add(new ColumnDefinition("CTIME",PostgresType.TIME.getKeyword(),new Time(12, 34, 56)));
    listCdSimple.add(new ColumnDefinition("CTIMETZ",PostgresType.TIMETZ.getKeyword(),new Time(9, 34, 56)));
    listCdSimple.add(new ColumnDefinition("CTIMESTAMP",PostgresType.TIMESTAMP.getKeyword(),new Timestamp(2016-1900,10,30,12,34,56,0)));
    listCdSimple.add(new ColumnDefinition("CTIMESTAMPTZ",PostgresType.TIMESTAMP.getKeyword(),new Timestamp(2019-1900,07,29,9,34,56,0)));
    listCdSimple.add(new ColumnDefinition("CINTERVALYM",PostgresType.INTERVAL.getKeyword()+" year to month",new Interval(1,1,3)));
    listCdSimple.add(new ColumnDefinition("CINTERVALDM",PostgresType.INTERVAL.getKeyword()+" day to second(6)",new Interval(-1,1,11,23,34,456789000l)));

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
    listCdSimple.add(new ColumnDefinition("CBIT_256",PostgresType.BIT.getKeyword()+"(256)",PostgresLiterals.formatBitString(TestUtils.getBytes(32),256)));
    listCdSimple.add(new ColumnDefinition("CVARBIT_805",PostgresType.VARBIT.getKeyword()+"(805)",PostgresLiterals.formatBitString(TestUtils.getBytes(101),805)));
    listCdSimple.add(new ColumnDefinition("CBYTEA",PostgresType.BYTEA.getKeyword(),TestUtils.getBytes(5000)));
    listCdSimple.add(new ColumnDefinition("CUUID",PostgresType.UUID.getKeyword(),UUID.randomUUID()));
    listCdSimple.add(new ColumnDefinition("CMACADDR",PostgresType.MACADDR.getKeyword(),PostgresLiterals.formatMacAddr(TestUtils.getBytes(6))));
    listCdSimple.add(new ColumnDefinition("CMACADDR8",PostgresType.MACADDR8.getKeyword(),PostgresLiterals.formatMacAddr(TestUtils.getBytes(8))));
    
    // spatial
    listCdSimple.add(new ColumnDefinition("CPOINT",PostgresType.POINT.getKeyword(),"(1.5, 2.0)"));
    listCdSimple.add(new ColumnDefinition("CLINE",PostgresType.LINE.getKeyword(),"{0.5, -0.1, 1.0}"));
    listCdSimple.add(new ColumnDefinition("CLSEG",PostgresType.LSEG.getKeyword(),"[(1.2, 2.1), (4.8, 5.1)]"));
    listCdSimple.add(new ColumnDefinition("CBOX",PostgresType.BOX.getKeyword(),"((1, 1), (2, 2))"));
    listCdSimple.add(new ColumnDefinition("CPATH",PostgresType.PATH.getKeyword(),"[(0, 0), (10, 0), (10, 10), (0, 10)]"));
    listCdSimple.add(new ColumnDefinition("CPOLYGON",PostgresType.POLYGON.getKeyword(),"((0, 0), (10, 0), (10, 10), (0, 10))"));
    listCdSimple.add(new ColumnDefinition("CCIRCLE",PostgresType.CIRCLE.getKeyword(),"<(1.0, 0.0),5.0>"));
    
    return listCdSimple;    
  }
  public static List<ColumnDefinition> _listCdSimple = getCdSimple();
  
  /* complex type : builtin int4range */
  private static List<ColumnDefinition> getListBuiltinRange()
  {
    List<ColumnDefinition> listBuiltinRange = new ArrayList<ColumnDefinition>();
    listBuiltinRange.add(new ColumnDefinition("min","int4",Integer.valueOf(473)));
    listBuiltinRange.add(new ColumnDefinition("sup","int4",Integer.valueOf(5435)));
    return listBuiltinRange;
  }
  public static List<ColumnDefinition> _listBuiltinRange = getListBuiltinRange();
  
  /* complex type : domain */
  private static List<ColumnDefinition> getListBaseDomain()
  {
    List<ColumnDefinition> listBaseDomain = new ArrayList<ColumnDefinition>();
    listBaseDomain.add(new ColumnDefinition(getQualifiedDomainType().format(),"int4",Integer.valueOf(999)));
    return listBaseDomain;
  }
  public static List<ColumnDefinition> _listBaseDomain = getListBaseDomain();
  
  /* complex type : composite */
  private static List<ColumnDefinition> getListCompositeType()
  {
    List<ColumnDefinition> listCompositeType = new ArrayList<ColumnDefinition>();
    listCompositeType.add(new ColumnDefinition("F1","int4",Integer.valueOf(-25)));
    listCompositeType.add(new ColumnDefinition("F2","text",TestUtils.getString(511)));
    return listCompositeType;
  }
  public static List<ColumnDefinition> _listCompositeType = getListCompositeType();
  
  /* complex type : enum */
  private static List<ColumnDefinition> getListEnumType()
  {
    List<ColumnDefinition> listEnumType = new ArrayList<ColumnDefinition>();
    listEnumType.add(new ColumnDefinition(getQualifiedEnumType().format(),"ENUM ('clubs','spades','hearts','diamonds')","hearts"));
    return listEnumType;
  }
  public static List<ColumnDefinition> _listEnumType = getListEnumType();
  
  /* complex type : string RANGE */
  private static List<ColumnDefinition> getListRangeType()
  {
    List<ColumnDefinition> listRangeType = new ArrayList<ColumnDefinition>();
    listRangeType.add(new ColumnDefinition("min","text","b"));
    listRangeType.add(new ColumnDefinition("sup","text","c"));
    return listRangeType;
  }
  public static List<ColumnDefinition> _listRangeType = getListRangeType();
  
  /* complex type : string ARRAY */
  private static List<ColumnDefinition> getListStringArray()
  {
    List<ColumnDefinition> listStringArray = new ArrayList<ColumnDefinition>();
    listStringArray.add(new ColumnDefinition("CARRAY[1]","text","line1"));
    listStringArray.add(new ColumnDefinition("CARRAY[2]","text","line2"));
    listStringArray.add(new ColumnDefinition("CARRAY[3]","text","line3"));
    listStringArray.add(new ColumnDefinition("CARRAY[4]","text","line4"));
    return listStringArray;
  }
  public static List<ColumnDefinition> _listStringArray = getListStringArray();
  
  /* complex type : double MATRIX */
  private static List<ColumnDefinition> getListDoubleMatrix()
  {
    List<ColumnDefinition> listDoubleMatrix = new ArrayList<ColumnDefinition>();
    List<ColumnDefinition> listDoubleArray = new ArrayList<ColumnDefinition>();
    listDoubleArray.add(new ColumnDefinition("CMATRIX[1][1]","float8",Double.valueOf(0.1)));
    listDoubleArray.add(new ColumnDefinition("CMATRIX[1][2]","float8",Double.valueOf(0.0)));
    listDoubleArray.add(new ColumnDefinition("CMATRIX[1][3]","float8",Double.valueOf(0.5)));
    listDoubleArray.add(new ColumnDefinition("CMATRIX[1][4]","float8",Double.valueOf(2.0)));
    listDoubleMatrix.add(new ColumnDefinition("CMATRIX[1]","float8[]",listDoubleArray));
    listDoubleArray = new ArrayList<ColumnDefinition>();
    listDoubleArray.add(new ColumnDefinition("CMATRIX[2][1]","float8",Double.valueOf(10.0)));
    listDoubleArray.add(new ColumnDefinition("CMATRIX[2][2]","float8",null));
    listDoubleArray.add(new ColumnDefinition("CMATRIX[2][3]","float8",Double.valueOf(2.0)));
    listDoubleArray.add(new ColumnDefinition("CMATRIX[2][4]","float8",Double.valueOf(0.5)));
    listDoubleMatrix.add(new ColumnDefinition("CMATRIX[2]","float8[]",listDoubleArray));
    listDoubleArray = new ArrayList<ColumnDefinition>();
    listDoubleArray.add(new ColumnDefinition("CMATRIX[3][1]","float8",Double.valueOf(5.0)));
    listDoubleArray.add(new ColumnDefinition("CMATRIX[3][2]","float8",Double.valueOf(0.0)));
    listDoubleArray.add(new ColumnDefinition("CMATRIX[3][3]","float8",Double.valueOf(0.25)));
    listDoubleArray.add(new ColumnDefinition("CMATRIX[3][4]","float8",Double.valueOf(1.0)));
    listDoubleMatrix.add(new ColumnDefinition("CMATRIX[3]","float8[]",listDoubleArray));
    return listDoubleMatrix;
  }
  public static List<ColumnDefinition> _listDoubleMatrix = getListDoubleMatrix();
  
  /* all complex types */
  private static List<ColumnDefinition> getCdComplex() 
  {
    List<ColumnDefinition> listCdComplex = new ArrayList<ColumnDefinition>();
    listCdComplex.add(new ColumnDefinition("CINT_DOMAIN",getQualifiedDomainType().format(),_listBaseDomain));
    listCdComplex.add(new ColumnDefinition("CCOMPOSITE",getQualifiedCompositeType().format(),_listCompositeType));
    listCdComplex.add(new ColumnDefinition("CENUM_SUIT",getQualifiedEnumType().format(),_listEnumType));
    listCdComplex.add(new ColumnDefinition("CINT_BUILTIN",getQualifiedBuiltinRange().format(),_listBuiltinRange));
    listCdComplex.add(new ColumnDefinition("CSTRING_RANGE",getQualifiedRangeType().format(),_listRangeType));
    listCdComplex.add(new ColumnDefinition("CSTRING_ARRAY",getQualifiedRangeType().format(),_listStringArray));
    listCdComplex.add(new ColumnDefinition("CDOUBLE_MATRIX",getQualifiedRangeType().format(),_listDoubleMatrix));
    return listCdComplex;
  }
  public static List<ColumnDefinition> _listCdComplex = getCdComplex();
  
  
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
    dropType(getQualifiedDomainType());
    dropType(getQualifiedCompositeType());
    dropType(getQualifiedEnumType());
    dropType(getQualifiedRangeType());
  } /* dropTypes */
  
  private void dropType(QualifiedId qiType)
  {
    if (qiType.getName().equals(_sTEST_INTEGER_DOMAIN))
      executeDrop("DROP DOMAIN "+qiType.format());
    else
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
    executeCreate("ALTER DEFAULT PRIVILEGES IN SCHEMA "+sid.format()+" GRANT ALL ON TABLES TO "+_sDbUser);
    executeCreate("ALTER DEFAULT PRIVILEGES IN SCHEMA "+sid.format()+" GRANT ALL ON TYPES TO "+_sDbUser);
    executeCreate("ALTER DEFAULT PRIVILEGES IN SCHEMA "+sid.format()+" GRANT ALL ON SEQUENCES TO "+_sDbUser);
    executeCreate("ALTER DEFAULT PRIVILEGES IN SCHEMA "+sid.format()+" GRANT ALL ON FUNCTIONS TO "+_sDbUser);
  } /* createSchema */
  
  private void createTypes()
    throws SQLException
  {
    createType(getQualifiedDomainType(),_listBaseDomain);
    createType(getQualifiedCompositeType(),_listCompositeType);
    createType(getQualifiedEnumType(),_listEnumType);
    createType(getQualifiedRangeType(),_listRangeType);
  } /* createTypes */

  private void createType(QualifiedId qiType, List<ColumnDefinition> listAttributes)
    throws SQLException
  {
    if (qiType.getName().equals(_sTEST_INTEGER_DOMAIN))
    {
      ColumnDefinition cd = listAttributes.get(0);
      executeCreate("CREATE DOMAIN "+qiType.format()+" AS "+cd.getType());
    }
    else if (qiType.getName().equals(_sTEST_TYPE_COMP))
    {
      StringBuilder sb = new StringBuilder("CREATE TYPE ");
      sb.append(qiType.format());
      sb.append(" AS (");
      for (int iAttribute = 0; iAttribute < listAttributes.size(); iAttribute++)
      {
        ColumnDefinition cd = listAttributes.get(iAttribute);
        if (iAttribute > 0)
          sb.append(",");
        sb.append("\r\n  ");
        sb.append(PostgresLiterals.formatId(cd.getName()));
        sb.append(" ");
        sb.append(cd.getType());
      }
      sb.append("\r\n)");
      executeCreate(sb.toString());
    }
    else if (qiType.getName().equals(_sTEST_TYPE_ENUM))
    {
      ColumnDefinition cd = listAttributes.get(0);
      executeCreate("CREATE TYPE "+qiType.format()+" AS "+cd.getType());
    }
    else if (qiType.getName().equals(_sTEST_TYPE_RANGE))
    {
      executeCreate("CREATE TYPE "+qiType.format()+" AS RANGE (subtype=text)");
    }
  } /* createType */

  private void createTables()
    throws SQLException
  {
    createTable(getQualifiedSimpleTable(),_listCdSimple,
      Arrays.asList(new String[] {_listCdSimple.get(_iPrimarySimple).getName()}),
      Arrays.asList(new String[] {_listCdSimple.get(_iCandidateSimple).getName()}));
    createTable(getQualifiedComplexTable(),_listCdComplex,null,null);
  } /* createTables */
  
  private void createTable(QualifiedId qiTable, List<ColumnDefinition> listCd,
    List<String> listPrimary, List<String> listUnique)
    throws SQLException
  {
    StringBuilder sbSql = new StringBuilder("CREATE TABLE ");
    sbSql.append(qiTable.format());
    sbSql.append("\r\n(\r\n  ");
    for (int iColumn = 0; iColumn < listCd.size(); iColumn++)
    {
      ColumnDefinition cd = listCd.get(iColumn); 
      if (iColumn > 0)
        sbSql.append(",\r\n  ");
      sbSql.append(cd.getName());
      sbSql.append(" ");
      if (cd.getName().equals("CINT_BUILTIN"))
        sbSql.append("int4range");
      else if (cd.getName().equals("CSTRING_ARRAY"))
        sbSql.append("text[4]");
      else if (cd.getName().equals("CDOUBLE_MATRIX"))
        sbSql.append("float8[3][4]");
      else
        sbSql.append(cd.getType());
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
    insertTable(getQualifiedSimpleTable(),_listCdSimple);
    insertTable(getQualifiedComplexTable(),_listCdComplex);
  } /* insertTables */
  
  private void insertTable(QualifiedId qiTable, List<ColumnDefinition> listCd)
    throws SQLException
  {
    StringBuilder sbSql = new StringBuilder("INSERT INTO ");
    sbSql.append(qiTable.format());
    sbSql.append("\r\n(\r\n  ");
    for (int iColumn = 0; iColumn < listCd.size(); iColumn++)
    {
      ColumnDefinition cd = listCd.get(iColumn); 
      if (cd.getValue() != null)
      {
        if (iColumn > 0)
          sbSql.append(",\r\n  ");
        sbSql.append(cd.getName());
      }
    }
    sbSql.append("\r\n)\r\nVALUES\r\n(\r\n  ");
    List<Object> listLobs = new ArrayList<Object>();
    for (int iColumn = 0; iColumn < listCd.size(); iColumn++)
    {
      ColumnDefinition cd = listCd.get(iColumn);
      if (cd.getValue() != null)
      {
        if (iColumn > 0)
          sbSql.append(",\r\n  ");
        String sLiteral = cd.getValueLiteral();
        if (sLiteral.length() < 1000)
          sbSql.append(sLiteral);
        else
        {
          sbSql.append("?");
          listLobs.add(cd.getValue());
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