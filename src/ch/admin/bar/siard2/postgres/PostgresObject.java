package ch.admin.bar.siard2.postgres;

import java.math.*;
import java.sql.*;
import java.sql.Date;
import java.text.*;
import java.util.*;
import javax.xml.datatype.*;
import org.postgresql.jdbc.*;
import org.postgresql.util.*;
import org.postgresql.core.*;

import ch.enterag.utils.database.*;
import ch.enterag.sqlparser.*;
import ch.enterag.sqlparser.expression.enums.*;
import ch.admin.bar.siard2.jdbc.*;
import ch.admin.bar.siard2.postgres.identifier.*;

public class PostgresObject
{
  private static char cCOMMA = ',';
  private String _sValue = null;
  private Object _o = null;
  public Object getObject() { return _o; }
  public void setObject(Object o) { _o = o; }
  public String getValue() { return _sValue; }
  private PostgresQualifiedId _qiType = null;
  private PostgresConnection _pconn = null;
  private String _sIndent = null;
  
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
        if ((s.charAt(0) == '"') && (s.charAt(s.length()-1) == '"'))
        {
          s = s.substring(1,s.length()-1).
            replace("\"\"", "\""); // escaped double quotes
        }
        else if ((s.charAt(0) == '\'') && (s.charAt(s.length()-1) == '\''))
        {
          s = s.substring(1,s.length()-1).
            replace("''", "'"); // escaped single quotes
        }
        s = s.
          replace("\\t","\t"). // TAB
          replace("\\b","\b"). // BACKSPACE
          replace("\\n","\n"). // NEW LINE
          replace("\\r","\r"). // CARRIAGE RETURN
          replace("\\f","\f"). // FORM FEED
          replace("\\\\","\\"); // escaped escape
      }
    }
    return s;
  } /* stripQuotes */
  
  public static String addQuotes(String s, String sQuote)
  {
    if (s != null)
    {
      s = s.
        replace("\t","\\t"). // TAB
        replace("\b","\\b"). // BACKSPACE
        replace("\n","\\n"). // NEW LINE
        replace("\r","\\r"). // CARRIAGE RETURN
        replace("\f","\\f"). // FORM FEED
        replace("\\","\\\\"); // escaped escape
      if (sQuote.equals("\""))
        s = s.replace("\"", "\"\"");
      else if (sQuote.equals("'"))
        s = s.replace("'", "''");
      s = sQuote + s + sQuote;
    }
    return s;
  } /* addQuotes */
  
  public PostgresObject(String sValue, int iDataType, String sTypeName, PostgresConnection pconn, String sIndent)
    throws ParseException, SQLException
  {
    _sValue = sValue; 
    _sIndent = sIndent;
    // System.out.println(sIndent + _sValue);
    if ((iDataType == Types.STRUCT) || (iDataType == Types.DISTINCT) || (iDataType == Types.ARRAY))
      _qiType = new PostgresQualifiedId(sTypeName);
    _pconn = pconn;
    _o = fromString(sValue,iDataType);
  } /* constructor */
  
  public PostgresObject(Object o, int iDataType, String sTypeName, PostgresConnection pconn, String sIndent)
    throws ParseException, SQLException
  {
    _o = o;
    _sIndent = sIndent;
    if ((iDataType == Types.STRUCT) || (iDataType == Types.DISTINCT) || (iDataType == Types.ARRAY))
      _qiType = new PostgresQualifiedId(sTypeName);
    _pconn = pconn;
    _sValue = fromObject(o,iDataType);
    // System.out.println(sIndent + _sValue);
  }
  
  private List<AttributeDescription> getAttributeDescriptions()
    throws SQLException, ParseException
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

  private String formatStruct(Struct struct)
    throws SQLException, ParseException
  {
    StringBuilder sb = new StringBuilder();
    List<AttributeDescription> listAttributeDescriptions = getAttributeDescriptions();
    for (int iAttribute = 0; iAttribute < struct.getAttributes().length; iAttribute++)
    {
      if (iAttribute > 0)
        sb.append(",");
      Object o = struct.getAttributes()[iAttribute];
      if (o != null)
      {
        AttributeDescription ad = listAttributeDescriptions.get(iAttribute);
        PostgresObject po = new PostgresObject(o,ad.getDataType(),ad.getTypeName(),_pconn, _sIndent+"  ");
        if (ad.getDataType() == Types.STRUCT)
          sb.append("\"");
        sb.append(po.getValue());
        if (ad.getDataType() == Types.STRUCT)
          sb.append("\"");
      }
    }
    // add parentheses
    sb.insert(0, "(");
    sb.append(")");
    return sb.toString();
  } /* formatStruct */
  
  private String fromObject(Object o, int iDataType)
    throws SQLException, ParseException
  {
    String sValue = null;
    switch(iDataType)
    {
      case Types.CHAR:
      case Types.VARCHAR:
      case Types.NCHAR:
      case Types.NVARCHAR:
        String s = (String)o;
        sValue = addQuotes(s,"\"");
        break;
      case Types.CLOB:
        PostgresClob clob = (PostgresClob)o;
        sValue = String.valueOf(clob.getOid());
        break;
      case Types.NCLOB:
        PostgresNClob nclob = (PostgresNClob)o;
        sValue = String.valueOf(nclob.getOid());
        break;
      case Types.SQLXML:
        PgSQLXML sqlxml = (PgSQLXML)o;
        sValue = addQuotes(sqlxml.getString(),"\"");
        break;
      case Types.BINARY:
      case Types.VARBINARY:
        byte[] buf = (byte[])o;
        sValue = addQuotes(stripQuotes(PostgresLiterals.formatBytesLiteral(buf)),"\"");
        break;
      case Types.BLOB:
        PostgresBlob blob = (PostgresBlob)o;
        sValue = String.valueOf(blob.getOid());
        break;
      case Types.NUMERIC:
      case Types.DECIMAL:
        BigDecimal bd = (BigDecimal)o;
        sValue = PostgresLiterals.formatExactLiteral(bd);
        break;
      case Types.SMALLINT:
        Short sh = (Short)o;
        bd = BigDecimal.valueOf(sh);
        sValue = PostgresLiterals.formatExactLiteral(bd);
        break;
      case Types.INTEGER:
        Integer i = (Integer)o;
        bd = BigDecimal.valueOf(i);
        sValue = PostgresLiterals.formatExactLiteral(bd);
        break;
      case Types.BIGINT:
        Long l = (Long)o;
        bd = BigDecimal.valueOf(l);
        sValue = PostgresLiterals.formatExactLiteral(bd);
        break;
      case Types.DOUBLE:
        if (o instanceof Float)
        {
          Float f = (Float)o;
          sValue = String.valueOf(f.doubleValue());
        }
        else
        {
          Double d = (Double)o;
          sValue = String.valueOf(d.doubleValue());
        }
        break;
      case Types.REAL:
        Float f = (Float)o;
        sValue = String.valueOf(f.floatValue());
        break;
      case Types.BOOLEAN:
        Boolean b = (Boolean)o;
        sValue = b.booleanValue()?"t":"f";
        break;
      case Types.DATE:
        Date date = (Date)o;
        sValue = stripQuotes(PostgresLiterals.formatDateLiteral(date).substring(PostgresLiterals.sDATE_LITERAL_PREFIX.length()));
        break;
      case Types.TIME:
        Time time = (Time)o;
        sValue = stripQuotes(PostgresLiterals.formatTimeLiteral(time).substring(PostgresLiterals.sTIME_LITERAL_PREFIX.length()));
        break;
      case Types.TIMESTAMP:
        Timestamp ts = (Timestamp)o;
        sValue = PostgresLiterals.formatTimestampLiteral(ts).substring(PostgresLiterals.sTIMESTAMP_LITERAL_PREFIX.length()).replace("'","\"");
        break;
      case Types.OTHER:
        Interval iv = null;
        if (o instanceof Interval)
          iv = (Interval)o;
        else
        {
          Duration duration = (Duration)o;
          iv = Interval.fromDuration(duration);
        }
        StringBuilder sb = new StringBuilder();
        if (iv.getYears() > 0)
        {
          sb.append(String.valueOf(iv.getYears()));
          sb.append(" year");
          if (iv.getYears() > 1)
            sb.append("s");
        }
        if (iv.getMonths() > 0)
        {
          if (sb.length() > 0)
            sb.append(" ");
          sb.append(String.valueOf(iv.getMonths()));
          sb.append(" mon");
          if (iv.getMonths() > 1)
            sb.append("s");
        }
        if (iv.getDays() > 0)
        {
          if (sb.length() > 0)
            sb.append(" ");
          sb.append(String.valueOf(iv.getDays()));
          sb.append(" day");
          if (iv.getDays() > 1)
            sb.append("s");
        }
        if ((iv.getHours() > 0) || (iv.getMinutes() > 0) || (iv.getSeconds() > 0) || (iv.getNanoSeconds() > 0))
        {
          if (sb.length() > 0)
            sb.append(" ");
          s = String.valueOf(iv.getHours());
          if (s.length() == 1)
            sb.append(0);
          sb.append(s);
          sb.append(":");
          s = String.valueOf(iv.getMinutes());
          if (s.length() == 1)
            sb.append("0");
          sb.append(s);
          sb.append(":");
          s = String.valueOf(iv.getSeconds());
          if (s.length() == 1)
            sb.append("0");
          sb.append(s);
          sb.append(".");
          s = String.valueOf(iv.getNanoSeconds()/1000);
          for (i = s.length(); i < 6; i++)
            sb.append("0");
          sb.append(s);
        }
        sValue = addQuotes(sb.toString(),"\"");
        break;
      case Types.STRUCT:
        Struct struct = (Struct)o;
        sValue = formatStruct(struct);
    }
    return sValue;
  }
  
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
          if (sAttribute.length() > 0) // zero length strings cannot be tokenized - they represent NULL
          {
            PostgresObject po = new PostgresObject(sAttribute,ad.getDataType(),ad.getTypeName(),_pconn, _sIndent + "  ");
            ao[iAttribute] = po.getObject();
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
        PostgresObject poStart = new PostgresObject(sAttributeStart,adStart.getDataType(),adStart.getTypeName(),_pconn, _sIndent + "  ");
        ao[0] = poStart.getObject();
        
        String sAttributeEnd = ptAttributes.getToken(1);
        AttributeDescription adEnd = listAttributeDescriptions.get(1);
        PostgresObject poEnd = new PostgresObject(sAttributeEnd,adEnd.getDataType(),adEnd.getTypeName(),_pconn, _sIndent + "  ");
        ao[1] = poEnd.getObject();
        
        ao[2] = sStartMark+sEndMark;
      }
      else
        throw new ParseException("Number of attributes does not match number of tokens when parsing "+sToken+"!",0);
      struct = new PostgresStruct(_qiType.format(), ao); 
    }
    else
      throw new ParseException("STRUCT must be in parentheses!",0);
    return struct;
  } /* parseStruct */
  
  private Object fromString(String sValue, int iDataType)
    throws SQLException, ParseException
  {
    Object o = null;
    String sToken = sValue; 
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
        o = new PostgresClob(_pconn,lOid);
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
        o = new PostgresBlob(_pconn,lOid);
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
        o = Double.valueOf(sToken);
        break;
      case Types.REAL:
        bd = PostgresLiterals.parseExactLiteral(sToken);
        o = Float.valueOf(sToken);
        break;
      case Types.BOOLEAN:
        BooleanLiteral bl = PostgresLiterals.parseBooleanLiteral(sToken);
        Boolean b = null;
        if (bl != BooleanLiteral.UNKNOWN)
          b = (bl == BooleanLiteral.TRUE)?Boolean.TRUE:Boolean.FALSE;
        o = b;
        break;
      case Types.DATE:
        o = PostgresLiterals.parseDateLiteral(sToken);
        break;
      case Types.TIME:
        o = PostgresLiterals.parseTimeLiteral(sToken);
        break;
      case Types.TIMESTAMP:
        o = PostgresLiterals.parseTimestampLiteral(stripQuotes(sToken));
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
  } /* fromString */

} /* class PostgresObject */
