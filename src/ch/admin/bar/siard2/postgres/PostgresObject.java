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
  private int _iDataType = Types.NULL;
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
  
  public PostgresObject(String sValue, int iDataType, String sTypeName, PostgresConnection pconn)
    throws ParseException
  {
    _pt = new PGtokenizer(sValue, cCOMMA);
    _iDataType = iDataType;
    if ((iDataType == Types.STRUCT) || (iDataType == Types.DISTINCT) || (iDataType == Types.ARRAY))
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
      String sAttributeName = rsAttributes.getString("ATTR_NAME");
      AttributeDescription ad = new AttributeDescription(
        rsAttributes.getInt("DATA_TYPE"),
        rsAttributes.getString("ATTR_TYPE_NAME"));
      System.out.println(sAttributeName+": "+String.valueOf(ad.getDataType())+" "+ad.getTypeName());
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
        o = PostgresLiterals.parseBytesLiteral(sToken.substring(2,sToken.length()-1));
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
        LocalDate ld = LocalDate.parse(sToken);
        o = Date.valueOf(ld);
        break;
      case Types.TIME:
        LocalTime lt = LocalTime.parse(sToken);
        o = Time.valueOf(lt);
        break;
      case Types.TIMESTAMP:
        LocalDateTime ldt = LocalDateTime.parse(sToken.substring(1,sToken.length()-1).replace(' ', 'T'));
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
          o = new PostgresStruct(_qiType.format(), ao); 
        }
        else
          throw new ParseException("Number of attributes does not match number of tokens when parsing "+_pt.toString()+"!",iToken);
        break;
      default:
        throw new SQLException("Invalid data type found: "+String.valueOf(iDataType)+" ("+SqlTypes.getTypeName(iDataType)+")!");
    }
    return o;
  } /* getObject */

} /* class PostgresObject */
