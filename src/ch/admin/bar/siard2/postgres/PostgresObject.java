package ch.admin.bar.siard2.postgres;

import java.math.*;
import java.sql.*;
import java.sql.Date;
import java.text.*;
import java.time.*;
import java.util.*;
import org.postgresql.jdbc.*;
import org.postgresql.util.*;
import org.postgresql.core.BaseConnection;

import ch.enterag.utils.database.*;
import ch.enterag.sqlparser.identifier.*;
import ch.admin.bar.siard2.jdbc.*;

public class PostgresObject
{
  private static char cCOMMA = ',';
  private PGtokenizer _pt = null;
  private QualifiedId _qiType = null;
  private PostgresConnection _pconn = null;
  
  private class AttributeDescription
  {
    private int _iDataType;
    public int getDataType() { return _iDataType; }
    private String _sTypeName;
    public String getTypeName() { return _sTypeName; } 
    public AttributeDescription(int iDataType, String sTypeName)
    {
      _iDataType = iDataType;
      _sTypeName = sTypeName;
    }
  } /* class AttributeDescription */
  
  private String stripQuotes(String s)
    throws ParseException
  {
    if (s != null)
    {
      if ((s.length() > 1) && (s.charAt(0) == '"') && (s.charAt(s.length()-1) == '"'))
      {
        s = s.substring(1,s.length()-1).
          replace("\"\"", "\"").
          replace("\\t","\t"). // TAB
          replace("\\b","\b"). // BACKSPACE
          replace("\\n","\n"). // NEW LINE
          replace("\\r","\r"). // CARRIAGE RETURN
          replace("\\f","\f"). // FORM FEED
          replace("\\'","'"). // escaped single quotes
          replace("\\\"","\""). // escaped double quotes
          replace("\\\\","\\"); // escaped escape
      }
      else
        throw new ParseException("Quotes could not be stripped from unquoted string!",0);
    }
    return s;
  } /* stripQuotes */
  
  public PostgresObject(String sValue, int iDataType, String sTypeName, PostgresConnection pconn)
    throws ParseException
  {
    _pt = new PGtokenizer(sValue, cCOMMA);
    if ((iDataType == Types.STRUCT) || (iDataType == Types.DISTINCT) || (iDataType == Types.ARRAY))
      _qiType = new QualifiedId(sTypeName);
    _pconn = pconn;
  } /* constructor */
  
  private AttributeDescription getBuiltinRangeDescription()
  {
    AttributeDescription ad = null;
    String sTypeName = _qiType.getName();
    if (sTypeName.equals("int4range"))
      ad = new AttributeDescription(Types.INTEGER,"int4");
    else if (sTypeName.equals("int8range"))
      ad = new AttributeDescription(Types.BIGINT,"int8");
    else if (sTypeName.equals("numrange"))
      ad = new AttributeDescription(Types.NUMERIC,"numeric");
    else if (sTypeName.equals("tsrange"))
      ad = new AttributeDescription(Types.TIMESTAMP,"timestamp");
    else if (sTypeName.equals("tstzrange"))
      ad = new AttributeDescription(Types.TIMESTAMP_WITH_TIMEZONE,"timestamptz");
    else if (sTypeName.equals("daterange"))
      ad = new AttributeDescription(Types.DATE,"date");
    return ad;
  } /* getBuiltinRangeDescription */
  
  private List<AttributeDescription> getAttributeDescriptions()
    throws SQLException
  {
    PostgresDatabaseMetaData pdmd = (PostgresDatabaseMetaData)_pconn.getMetaData();
    List<AttributeDescription> listAttributeDescription = new ArrayList<AttributeDescription>();
    ResultSet rsAttributes = pdmd.getAttributes(
      pdmd.toPattern(_qiType.getCatalog()),
      pdmd.toPattern(_qiType.getSchema()),
      pdmd.toPattern(_qiType.getName()),
      "%");
    while (rsAttributes.next())
    {
      int iDataType = rsAttributes.getInt("DATA_TYPE");
      String sTypeName = rsAttributes.getString("ATTR_TYPE_NAME");
      AttributeDescription ad = new AttributeDescription(
        iDataType,
        sTypeName);
      // String sAttributeName = rsAttributes.getString("ATTR_NAME");
      // System.out.println(sAttributeName+": "+String.valueOf(ad.getDataType())+" "+ad.getTypeName());
      listAttributeDescription.add(ad);
    }
    rsAttributes.close();
    return listAttributeDescription;
  } /* getAttributeDescriptions */
  
  private Struct parseStruct(String sToken)
    throws ParseException, SQLException
  {
    Struct struct = null;
    // strip quotes
    if (sToken.startsWith("\"") && sToken.endsWith("\""))
      sToken = sToken.substring(1,sToken.length()-1);
    // strip parentheses
    if ((sToken.startsWith("(") || sToken.startsWith("[")) && (sToken.endsWith(")") || sToken.endsWith("]")))
    {
      Object[] ao = null;
      List<AttributeDescription> listAttributeDescriptions = getAttributeDescriptions();
      sToken = sToken.substring(1,sToken.length()-1);
      PGtokenizer ptAttributes = new PGtokenizer(sToken, cCOMMA);
      if (ptAttributes.getSize() == listAttributeDescriptions.size())
      {
        ao = new Object[ptAttributes.getSize()];
        for (int iAttribute = 0; iAttribute < ptAttributes.getSize(); iAttribute++)
        {
          String sAttribute = ptAttributes.getToken(iAttribute);
          AttributeDescription ad = listAttributeDescriptions.get(iAttribute);
          if (sAttribute.length() > 0) // zero length strings cannot be tokenized
          {
            PostgresObject po = new PostgresObject(sAttribute,ad.getDataType(),ad.getTypeName(),_pconn);
            ao[iAttribute] = po.getObject(0, ad.getDataType());
          }
          else
            ao[iAttribute] = null;
        }
        struct = new PostgresStruct(_qiType.format(), ao); 
      }
      else
        throw new ParseException("Number of attributes does not match number of tokens when parsing "+_pt.toString()+"!",0);
    }
    else
      throw new ParseException("STRUCT must be in parentheses!",0);
    return struct;
  } /* parseStruct */
  
  public Object getObject(int iToken, int iDataType)
    throws SQLException, ParseException
  {
    Object o = null;
    String sToken = _pt.getToken(iToken); 
    BaseConnection bc = (BaseConnection)_pconn.unwrap(Connection.class);
    long lOid = -1;
    BigDecimal bd = null;
    switch(iDataType)
    {
      case Types.CHAR:
      case Types.VARCHAR:
      case Types.NCHAR:
      case Types.NVARCHAR:
        o = stripQuotes(sToken); 
        break;
      case Types.CLOB:
      case Types.NCLOB: 
        lOid = Long.parseLong(sToken);
        o = new PostgresClob(new PgClob(bc,lOid));
        break;
      case Types.SQLXML:
        o = new PgSQLXML(bc,stripQuotes(sToken)); 
        break;
      case Types.BINARY:
      case Types.VARBINARY:
        o = PostgresLiterals.parseBytesLiteral(stripQuotes(sToken));
        break;
      case Types.BLOB:
        lOid = Long.parseLong(sToken);
        o = new PostgresBlob(new PgBlob(bc,lOid));
        break;
      case Types.NUMERIC:
      case Types.DECIMAL:
        o = PostgresLiterals.parseExactLiteral(sToken);
        break;
      case Types.SMALLINT:
        bd = PostgresLiterals.parseExactLiteral(sToken);
        o = Short.valueOf(bd.shortValue());
        break;
      case Types.INTEGER: 
        bd = PostgresLiterals.parseExactLiteral(sToken);
        o = Integer.valueOf(bd.intValue());
        break;
      case Types.BIGINT:
        bd = PostgresLiterals.parseExactLiteral(sToken);
        o = Long.valueOf(bd.longValue());
        break;
      case Types.DOUBLE:
        bd = PostgresLiterals.parseExactLiteral(sToken);
        o = Double.valueOf(bd.doubleValue());
        break;
      case Types.REAL:
        bd = PostgresLiterals.parseExactLiteral(sToken);
        o = Float.valueOf(bd.floatValue());
        break;
      case Types.BOOLEAN: 
        o = sToken.equals("t")?Boolean.TRUE:Boolean.FALSE; 
        break;
      case Types.DATE:
        LocalDate ld = LocalDate.parse(sToken);
        o = Date.valueOf(ld);
        break;
      case Types.TIME:
        LocalTime lt = LocalTime.parse(sToken);
        o = Time.valueOf(lt);
        break;
      case Types.TIMESTAMP:
        LocalDateTime ldt = LocalDateTime.parse(stripQuotes(sToken).replace(' ', 'T'));
        o = Timestamp.valueOf(ldt);
        break;
      case Types.OTHER: 
        o = PostgresLiterals.parseInterval(stripQuotes(sToken)).toDuration();
        break;
      case Types.STRUCT:
        if (PostgresType.setBUILTIN_RANGES.contains(_qiType.format()))
        {
          Object[] ao = new Object[4];
          /* a[0] indicates, whether lower limit is included in range */
          if (sToken.startsWith("["))
            ao[0] = Boolean.TRUE;
          else if (sToken.startsWith("("))
            ao[0] = Boolean.FALSE;
          else
            throw new SQLException("Range must start with [ or (!");
          /* a[3] indicates, whether upper limit is included in range */
          if (sToken.endsWith("]"))
            ao[3] = Boolean.TRUE;
          else if (sToken.endsWith(")"))
            ao[3] = Boolean.FALSE;
          else
            throw new SQLException("Range must end with ] or )!");
          sToken = sToken.substring(1,sToken.length()-1);
          PGtokenizer ptLimits = new PGtokenizer(sToken, cCOMMA);
          if (ptLimits.getSize() == 2)
          {
            AttributeDescription adRange = getBuiltinRangeDescription();
            String sLowerLimit = ptLimits.getToken(0);
            PostgresObject poLower = new PostgresObject(sLowerLimit,adRange.getDataType(),adRange.getTypeName(),_pconn);
            ao[1] = poLower.getObject(0, adRange.getDataType());
            String sUpperLimit = ptLimits.getToken(1);
            PostgresObject poUpper = new PostgresObject(sUpperLimit,adRange.getDataType(),adRange.getTypeName(),_pconn);
            ao[1] = poUpper.getObject(0, adRange.getDataType());
          }
          else
            throw new SQLException("Range must have upper and lower range!");
        }
        else
          o = parseStruct(sToken);
        break;
      default:
        throw new SQLException("Invalid data type found: "+String.valueOf(iDataType)+" ("+SqlTypes.getTypeName(iDataType)+")!");
    }
    return o;
  } /* getObject */

} /* class PostgresObject */
