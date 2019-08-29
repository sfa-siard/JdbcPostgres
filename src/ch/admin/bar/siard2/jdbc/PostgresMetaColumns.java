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

import ch.enterag.sqlparser.datatype.enums.*;
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

  @SuppressWarnings("unused")
  private Connection _conn;
  
  /*------------------------------------------------------------------*/
  private String getTypeName(String sTypeName, int iColumnSize, int iDecimals,
    String sCatalogName, String sSchemaName)
    throws SQLException
  {
    // internal names starting with _ are used for array elements
    if (sTypeName.startsWith("_"))
      sTypeName = sTypeName.substring(1);
    PostgresType pgt = PostgresType.getByKeyword(sTypeName);
    if (pgt != null)
    {
      PreType pt = pgt.getPreType();
      sTypeName = pt.getKeyword();
    }
    return sTypeName;
  } /* getTypeName */
  
  /*------------------------------------------------------------------*/
  private int getDataType(int iType, String sTypeName,
    String sCatalogName, String sSchemaName)
    throws SQLException
  {
    PostgresType pgt = PostgresType.getByKeyword(sTypeName);
    if (pgt != null)
    {
      PreType pt = pgt.getPreType();
      if (pt != null)
        iType = pt.getSqlType();
    }
    return iType;
  } /* getDataType */
  
  /*------------------------------------------------------------------*/
  private int getPrecision(int iPrecision, int iType, String sTypeName)
  {
    PostgresType pgt = PostgresType.getByKeyword(sTypeName);
    if (pgt != null)
    {
      if ((pgt == PostgresType.BIT) || (pgt == PostgresType.VARBIT))
        iPrecision = (iPrecision + 7) / 8;
      else if (pgt == PostgresType.UUID)
        iPrecision = 16;
      else if (pgt == PostgresType.MACADDR)
        iPrecision = 6;
      else if (pgt == PostgresType.MACADDR8)
        iPrecision = 8;
    }
    else if ((iType == Types.ARRAY) || (iType == Types.STRUCT) || (iType == Types.DISTINCT))
      iPrecision = Integer.MAX_VALUE;
    return iPrecision;
  } /* getPrecision */
  
  /*------------------------------------------------------------------*/
  private int getScale(int iScale, int iType, String sTypeName)
  {
    PostgresType pgt = PostgresType.getByKeyword(sTypeName);
    if (pgt != null)
    {
      if ((pgt == PostgresType.INTERVAL) && (iScale > 6))
        iScale = 0;
    }
    else if ((iType == Types.ARRAY) || (iType == Types.STRUCT) || (iType == Types.DISTINCT))
      iScale = 0;
    return iScale;
  } /* getPrecision */
  
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
    int iResult = super.getInt(columnIndex);
    if (columnIndex == _iDataType)
    {
      iResult = getDataType(
        iResult, 
        super.getString(_iTypeName),
        super.getString(_iCatalog), 
        super.getString(_iSchema));
    }
    else if (columnIndex == _iPrecision)
    {
      iResult = getPrecision(
        iResult,
        super.getInt(_iDataType),
        super.getString(_iTypeName));
    }
    else if (columnIndex == _iScale)
    {
      iResult = getScale(
        iResult,
        super.getInt(_iDataType),
        super.getString(_iTypeName));
    }
    else if (columnIndex == _iLength)
    {
      iResult = getPrecision(
        iResult,
        super.getInt(_iDataType),
        super.getString(_iTypeName));
    }
    return iResult;
  } /* getInt */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc}
   * Mapped java.sql.Types type is returned in DATA_TYPE.
   * Type name (mapped to ISO SQL) is returned in TYPE_NAME.
   */
  @Override
  public Object getObject(int columnIndex) throws SQLException
  {
    Object oResult = super.getObject(columnIndex);
    if (columnIndex == _iDataType)
    {
      int iResult = super.getInt(columnIndex); // maps null to 0
      oResult = getDataType(
        iResult, 
        super.getString(_iTypeName),
        super.getString(_iCatalog), 
        super.getString(_iSchema));
    }
    else if (columnIndex == _iPrecision)
    {
      int iResult = super.getInt(columnIndex); // maps null to 0
      oResult = getPrecision(
        iResult,
        super.getInt(_iDataType),
        super.getString(_iTypeName));
    }
    else if (columnIndex == _iScale)
    {
      int iResult = super.getInt(columnIndex); // maps null to 0
      oResult = getScale(
        iResult,
        super.getInt(_iDataType),
        super.getString(_iTypeName));
    }
    else if (columnIndex == _iLength)
    {
      int iResult = super.getInt(columnIndex); // maps null to 0
      oResult = getPrecision(
        iResult,
        super.getInt(_iDataType),
        super.getString(_iTypeName));
    }
    else if (columnIndex == _iTypeName)
    {
      int iLength = super.getInt(_iPrecision);
      if (iLength <= 0)
        iLength = super.getInt(_iLength);
      oResult = getTypeName(
        (String)oResult, 
        iLength, 
        super.getInt(_iScale),
        super.getString(_iCatalog), 
        super.getString(_iSchema));
    }
    return oResult;
  } /* getObject */

} /* PostgresMetaColumns */
