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
  
  public PostgresObject(String sValue, String sTypeName, PostgresConnection pconn)
    throws ParseException
  {
    _pt = new PGtokenizer(sValue, cCOMMA);
    _qiType = new QualifiedId(sTypeName);
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
      AttributeDescription ad = new AttributeDescription(
        rsAttributes.getInt("DATA_TYPE"),
        rsAttributes.getString("TYPE_NAME"));
      listAttributeDescription.add(ad);
    }
    rsAttributes.close();
    return listAttributeDescription;
  } /* getAttributeDescriptions */
  
  public Object getObject(int iToken, int iDataType)
    throws SQLException, ParseException
  {
    Object o = null;
    String sToken = _pt.getToken(iToken);
    if (sToken.length() > 0) // length 0 means NULL value
    {
      BaseConnection bc = (BaseConnection)_pconn.unwrap(Connection.class);
      long lOid = -1;
      BigDecimal bd = null;
      switch(iDataType)
      {
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.NCHAR:
        case Types.NVARCHAR:
          o = sToken.substring(1,sToken.length()-1); 
          break;
        case Types.CLOB:
        case Types.NCLOB: 
          lOid = Long.parseLong(sToken);
          o = new PostgresClob(new PgClob(bc,lOid));
          break;
        case Types.SQLXML:
          o = new PgSQLXML(bc,sToken.substring(1,sToken.length()-1)); 
          break;
        case Types.BINARY:
        case Types.VARBINARY:
          o = PostgresLiterals.parseBytesLiteral(sToken.substring(1,sToken.length()-1));
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
        case Types.BOOLEAN: 
          o = sToken.equals("t")?Boolean.TRUE:Boolean.FALSE; 
          break;
        case Types.DATE:
          LocalDate ld = LocalDate.parse(sToken.substring(1,sToken.length()-1));
          o = Date.valueOf(ld);
          break;
        case Types.TIME:
          LocalTime lt = LocalTime.parse(sToken.substring(1,sToken.length()-1));
          o = Time.valueOf(lt);
          break;
        case Types.TIMESTAMP:
          LocalDateTime ldt = LocalDateTime.parse(sToken.replace(' ', 'T').substring(1,sToken.length()-1));
          o = Timestamp.valueOf(ldt);
          break;
        case Types.OTHER: 
          o = PostgresLiterals.parseInterval(sToken.substring(1,sToken.length()-1));
          break;
        case Types.STRUCT:
          PGtokenizer ptAttributes = new PGtokenizer(sToken.substring(1,sToken.length()-1), cCOMMA);
          Object[] ao = null;
          List<AttributeDescription> listAttributeDescriptions = getAttributeDescriptions();
          if (ptAttributes.getSize() == listAttributeDescriptions.size())
          {
            ao = new Object[_pt.getSize()];
            for (int iAttribute = 0; iAttribute < _pt.getSize(); iAttribute++)
            {
              String sAttribute = ptAttributes.getToken(iAttribute);
              AttributeDescription ad = listAttributeDescriptions.get(iAttribute);
              PostgresObject pp = new PostgresObject(sAttribute,ad.getTypeName(),_pconn);
              ao[iAttribute] = pp.getObject(0, ad.getDataType());
            }
            o = new PostgresStruct(_qiType.format(), ao); 
          }
          else
            throw new ParseException("Number of attributes does not match number of tokens when parsing "+_pt.toString()+"!",iToken);
          break;
        default:
          throw new SQLException("Invalid data type found: "+String.valueOf(iDataType)+" ("+SqlTypes.getTypeName(iDataType)+")!");
      }
    }
    return o;
  } /* getObject */

} /* class PostgresObject */
