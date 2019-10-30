package ch.admin.bar.siard2.postgres;

import java.math.*;
import java.sql.*;
import java.sql.Date;
import java.text.*;
import java.time.*;
import java.util.*;
import org.postgresql.jdbc.*;
import org.postgresql.util.*;
import org.postgresql.core.*;

import ch.enterag.utils.database.*;
import ch.admin.bar.siard2.jdbc.*;
import ch.admin.bar.siard2.postgres.identifier.*;

public class PostgresObject
{
  private static char cCOMMA = ',';
  private PGtokenizer _pt = null;
  private PostgresQualifiedId _qiType = null;
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
  
  public static String stripQuotes(String s)
    throws ParseException
  {
    if (s != null)
    {
      if (s.length() > 1)
      {
        if (((s.charAt(0) == '"') && (s.charAt(s.length()-1) == '"')) ||
          ((s.charAt(0) == '\'') && (s.charAt(s.length()-1) == '\'')))
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
      }
    }
    return s;
  } /* stripQuotes */
  
  public PostgresObject(String sValue, int iDataType, String sTypeName, PostgresConnection pconn)
    throws ParseException
  {
    _pt = new PGtokenizer(sValue, cCOMMA);
    if ((iDataType == Types.STRUCT) || (iDataType == Types.DISTINCT) || (iDataType == Types.ARRAY))
    {
      _qiType = new PostgresQualifiedId(sTypeName);
    }
    _pconn = pconn;
  } /* constructor */
  
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
      String sAttributeTypeName = rsAttributes.getString("ATTR_TYPE_NAME");
      if ((iDataType == Types.OTHER) && (PostgresType.INTERVAL.getKeyword().equals(sAttributeTypeName)))
      {
        String sSchemaName = rsAttributes.getString("TYPE_SCHEM");
        String sTypeName = rsAttributes.getString("TYPE_NAME");
        String sAttributeName = rsAttributes.getString("ATTR_NAME");
        sAttributeTypeName = PostgresMetaColumns.getIntervalTypeName(_pconn, sSchemaName, sTypeName, sAttributeName, sAttributeTypeName);
      }
      AttributeDescription ad = new AttributeDescription(
        iDataType,
        sAttributeTypeName);
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
    String sStartMark = sToken.substring(0,1);
    String sEndMark = sToken.substring(sToken.length()-1,sToken.length());
    
    if ((sStartMark.equals("(") || sStartMark.equals("[")) && 
        (sEndMark.equals(")") || sEndMark.equals("]")))
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
      }
      else if ((ptAttributes.getSize() == 2) && (listAttributeDescriptions.size() == 3))
      {
        ao = new Object[3];
        
        String sAttributeStart = ptAttributes.getToken(0);
        AttributeDescription adStart = listAttributeDescriptions.get(0);
        PostgresObject poStart = new PostgresObject(sAttributeStart,adStart.getDataType(),adStart.getTypeName(),_pconn);
        ao[0] = poStart.getObject(0, adStart.getDataType());
        
        String sAttributeEnd = ptAttributes.getToken(1);
        AttributeDescription adEnd = listAttributeDescriptions.get(1);
        PostgresObject poEnd = new PostgresObject(sAttributeEnd,adEnd.getDataType(),adEnd.getTypeName(),_pconn);
        ao[1] = poEnd.getObject(0, adEnd.getDataType());
        
        ao[2] = sStartMark+sEndMark;
      }
      else
        throw new ParseException("Number of attributes does not match number of tokens when parsing "+_pt.toString()+"!",0);
      struct = new PostgresStruct(_qiType.format(), ao); 
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
          o = parseStruct(sToken);
        break;
      default:
        throw new SQLException("Invalid data type found: "+String.valueOf(iDataType)+" ("+SqlTypes.getTypeName(iDataType)+")!");
    }
    return o;
  } /* getObject */

} /* class PostgresObject */
