/*======================================================================
PostgresMetaColumns implements data type mapping from Postgres to ISO SQL.
Application : SIARD2
Description : PostgresMetaColumns implements data type mapping from Postgres to ISO SQL.
Platform    : Java 8-10   
------------------------------------------------------------------------
Copyright  : 2019, Swiss Federal Archives, Berne, Switzerland
License    : CDDL 1.0
Created    : 09.08.2019, Hartwig Thomas, Enter AG, Rüti ZH, Switzerland
======================================================================*/
package ch.admin.bar.siard2.jdbc;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import ch.enterag.sqlparser.datatype.*;
import ch.enterag.sqlparser.datatype.enums.*;
import ch.enterag.sqlparser.identifier.*;
import ch.admin.bar.siard2.postgres.*;

/*====================================================================*/
/** PostgresMetaColumns implements data type mapping from Postgres to ISO SQL.
 * @author Hartwig Thomas
 */
public class PostgresMetaColumns
  extends PostgresResultSet
{
  private int _iCatalog = -1;
  private int _iSchema = -1;
  private int _iDataType = -1;
  private int _iTypeName = -1;
  private int _iPrecision = -1;
  private int _iLength = -1;
  private int _iScale = -1;
  
  private Connection _conn = null;
  
  private static Map<String,PreType> mapNAME_POSTGRES_TO_ISO = new HashMap<String,PreType>();
  static
  {
    mapNAME_POSTGRES_TO_ISO.put("bool",PreType.BOOLEAN);
    mapNAME_POSTGRES_TO_ISO.put("bytea",PreType.BINARY);
    mapNAME_POSTGRES_TO_ISO.put("char",PreType.CHAR);
    mapNAME_POSTGRES_TO_ISO.put("name",PreType.VARCHAR);
    mapNAME_POSTGRES_TO_ISO.put("int8",PreType.BIGINT);
    mapNAME_POSTGRES_TO_ISO.put("bigserial",PreType.BIGINT);
    mapNAME_POSTGRES_TO_ISO.put("int2",PreType.SMALLINT);
    mapNAME_POSTGRES_TO_ISO.put("int4",PreType.INTEGER);
    mapNAME_POSTGRES_TO_ISO.put("serial",PreType.INTEGER);
    mapNAME_POSTGRES_TO_ISO.put("text",PreType.VARCHAR);
    mapNAME_POSTGRES_TO_ISO.put("oid",PreType.INTEGER);
    mapNAME_POSTGRES_TO_ISO.put("json",PreType.VARCHAR);
    mapNAME_POSTGRES_TO_ISO.put("xml",PreType.XML);
    mapNAME_POSTGRES_TO_ISO.put("point",PreType.VARCHAR);
    mapNAME_POSTGRES_TO_ISO.put("lseg",PreType.VARCHAR);
    mapNAME_POSTGRES_TO_ISO.put("path",PreType.VARCHAR);
    mapNAME_POSTGRES_TO_ISO.put("box",PreType.VARCHAR);
    mapNAME_POSTGRES_TO_ISO.put("polygon",PreType.VARCHAR);
    mapNAME_POSTGRES_TO_ISO.put("line",PreType.VARCHAR);
    mapNAME_POSTGRES_TO_ISO.put("float4",PreType.REAL);
    mapNAME_POSTGRES_TO_ISO.put("float8",PreType.DOUBLE);
    mapNAME_POSTGRES_TO_ISO.put("circle",PreType.VARCHAR);
    mapNAME_POSTGRES_TO_ISO.put("money",PreType.DECIMAL);
    mapNAME_POSTGRES_TO_ISO.put("macaddr",PreType.BINARY);
    mapNAME_POSTGRES_TO_ISO.put("inet",PreType.VARBINARY);
    mapNAME_POSTGRES_TO_ISO.put("cidr",PreType.VARCHAR);
    mapNAME_POSTGRES_TO_ISO.put("macaddr8",PreType.BINARY);
    mapNAME_POSTGRES_TO_ISO.put("varchar",PreType.VARCHAR);
    mapNAME_POSTGRES_TO_ISO.put("date",PreType.DATE);
    mapNAME_POSTGRES_TO_ISO.put("time",PreType.TIME);
    mapNAME_POSTGRES_TO_ISO.put("timestamp",PreType.TIMESTAMP);
    mapNAME_POSTGRES_TO_ISO.put("timestamptz",PreType.TIMESTAMP);
    mapNAME_POSTGRES_TO_ISO.put("interval",PreType.INTERVAL);
    mapNAME_POSTGRES_TO_ISO.put("timetz",PreType.TIME);
    mapNAME_POSTGRES_TO_ISO.put("bit",PreType.BINARY);
    mapNAME_POSTGRES_TO_ISO.put("varbit",PreType.VARBINARY);
    mapNAME_POSTGRES_TO_ISO.put("numeric",PreType.NUMERIC);
    mapNAME_POSTGRES_TO_ISO.put("uuid",PreType.BINARY);
    mapNAME_POSTGRES_TO_ISO.put("tsvector",PreType.VARCHAR);
    mapNAME_POSTGRES_TO_ISO.put("tsquery",PreType.VARCHAR);
    mapNAME_POSTGRES_TO_ISO.put("jsonb",PreType.VARCHAR);
    mapNAME_POSTGRES_TO_ISO.put("txid_snapshot",PreType.VARCHAR);
  }
  
  /*------------------------------------------------------------------*/
  private String getTypeName(String sTypeName, int iColumnSize, int iDecimals,
    String sCatalogName, String sSchemaName)
    throws SQLException
  {
    PreType pt = mapNAME_POSTGRES_TO_ISO.get(sTypeName);
    if (pt != null)
    {
      PostgresSqlFactory psf = new PostgresSqlFactory();
      PredefinedType preType = psf.newPredefinedType();
      preType.initialize(pt.getSqlType(), iColumnSize, iDecimals);
      sTypeName = preType.format();
    }
    else
    {
      String sTypeSchema = null;
      PostgresDatabaseMetaData dmd = (PostgresDatabaseMetaData)_conn.getMetaData();
      ResultSet rs = dmd.getUDTs(null, null, dmd.toPattern(sTypeName), null);
      while ((!sSchemaName.equals(sTypeSchema)) && rs.next())
      {
        if (sCatalogName.equals(rs.getString("TYPE_CAT")))
          sTypeSchema = rs.getString("TYPE_SCHEM");
      }
      rs.close();
      QualifiedId qiType = new QualifiedId(null,null,sTypeName);
      if (!sSchemaName.equals(sTypeSchema))
        qiType.setSchema(sTypeSchema);
      sTypeName = qiType.format();
    }
    return sTypeName;
  } /* getTypeName */
  
  /*------------------------------------------------------------------*/
  static int getDataType(int iType, String sTypeName,
    Connection conn, String sCatalogName, String sSchemaName)
    throws SQLException
  {
    PreType pt = mapNAME_POSTGRES_TO_ISO.get(sTypeName);
    if (pt != null)
      iType = pt.getSqlType();
    return iType;
  } /* getDataType */
  
  /*------------------------------------------------------------------*/
  /** constructor
   * @param rsWrapped DatabaseMetaData.getColumns() result set to be wrapped.
   * @param stmt wrapped statment.
   * @param iCatalog catalog column in wrapped result set.
   * @param iSchema schema column in wrapped result set.
   * @param iDataType data type column in wrapped result set.
   * @param iTypeName type name column in wrapped result set.
   * @param iPrecision precision column in wrapped result set.
   * @param iLength length column in wrapped result set.
   * @param iScale scale column in wrapped result set.
   */
  public PostgresMetaColumns(ResultSet rsWrapped,Connection conn,
    int iCatalog, int iSchema, int iDataType, int iTypeName,
    int iPrecision, int iLength, int iScale)
    throws SQLException
  {
    super(rsWrapped, rsWrapped.getStatement());
    _conn = conn;
    _iCatalog = iCatalog;
    _iSchema = iSchema;
    _iDataType = iDataType;
    _iTypeName = iTypeName;
    _iPrecision = iPrecision;
    _iLength = iLength;
    _iScale = iScale;
  } /* constructor */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc}
   * Type name (mapped to ISO SQL) is returned in TYPE_NAME.
   * Original type name can be retrieved by using unwrap. 
   */
  @Override
  public String getString(int columnIndex) throws SQLException
  {
    String sResult = super.getString(columnIndex);
    if (columnIndex == _iTypeName)
    {
      int iLength = super.getInt(_iPrecision);
      if (iLength <= 0)
        iLength = super.getInt(_iLength);
      sResult = getTypeName(
        sResult, 
        iLength, 
        super.getInt(_iScale),
        super.getString(_iCatalog), 
        super.getString(_iSchema));
    }
    return sResult;
  } /* getString */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc}
   * Mapped java.sql.Types type is returned in DATA_TYPE.
   * Original java.sql.Types type can be retrieved by using unwrap. 
   */
  @Override
  public int getInt(int columnIndex) throws SQLException
  {
    int iResult = -1;
    if (columnIndex == _iDataType)
    {
      iResult = getDataType(
        super.getInt(_iDataType), 
        super.getString(_iTypeName),
        _conn,
        super.getString(_iCatalog), 
        super.getString(_iSchema));
    }
    else
      iResult = super.getInt(columnIndex);
    return iResult;
  } /* getInt */

} /* PostgresMetaColumns */
