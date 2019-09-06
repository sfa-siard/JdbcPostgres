/*======================================================================
PostgresDatabaseMetaData implements wrapped Postgres DatabaseMetaData.
Application : SIARD2
Description : PostgresDatabaseMetaData implements wrapped Postgres DatabaseMetaData.
Platform    : Java 8-10   
------------------------------------------------------------------------
Copyright  : 2019, Swiss Federal Archives, Berne, Switzerland
License    : CDDL 1.0
Created    : 25.07.2019, Hartwig Thomas, Enter AG, Rüti ZH, Switzerland
======================================================================*/
package ch.admin.bar.siard2.jdbc;

import java.sql.*;

import ch.admin.bar.siard2.postgres.PostgresLiterals;
import ch.admin.bar.siard2.postgres.PostgresType;
import ch.enterag.sqlparser.*;
import ch.enterag.utils.jdbc.*;
import ch.enterag.utils.logging.*;

/*====================================================================*/
/** PostgresDatabaseMetaData implements wrapped Postgres DatabaseMetaData.
 * @author Hartwig Thomas
 */
public class PostgresDatabaseMetaData
  extends BaseDatabaseMetaData
  implements DatabaseMetaData
{
  /** logger */
  private static IndentLogger _il = IndentLogger.getIndentLogger(PostgresDatabaseMetaData.class.getName());
  Connection _conn = null;
  
  /*------------------------------------------------------------------*/
  /** constructor
   * @param dmdWrapped database meta data to be wrapped.
   */
  public PostgresDatabaseMetaData(DatabaseMetaData dmdWrapped, Connection conn)
  {
    super(dmdWrapped);
    _conn = conn;
  } /* constructor */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public Connection getConnection() throws SQLException
  {
    return _conn;
  } /* getConnection */
  
  /*------------------------------------------------------------------*/
  /** {@inheritDoc}
   * Use PostgresMetaColumn for data type translation.
   */
  @Override
  public ResultSet getColumns(String catalog, String schemaPattern,
    String tableNamePattern, String columnNamePattern)
    throws SQLException
  {
    DatabaseMetaData dmd = unwrap(DatabaseMetaData.class);
    ResultSet rs = dmd.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
    return new PostgresMetaColumns(rs,_conn,1,2,5,6,7,16,9,10);
  } /* getColumns */

  private String getTypeNameCase(String sDataType)
  {
    StringBuilder sbTypeNameCase = new StringBuilder("  CASE ");
    sbTypeNameCase.append(sDataType);
    sbTypeNameCase.append("\r\n");
    for (int i = 0; i < PostgresType.values().length; i++)
    {
      PostgresType pgt = PostgresType.values()[i];
      sbTypeNameCase.append("    WHEN ");
      sbTypeNameCase.append(PostgresLiterals.formatStringLiteral(pgt.getKeyword()));
      sbTypeNameCase.append(" THEN ");
      sbTypeNameCase.append(PostgresLiterals.formatStringLiteral(pgt.getPreType().getKeyword()));
      sbTypeNameCase.append("\r\n");
      for (String sAlias : pgt.getAliases())
      {
        sbTypeNameCase.append("    WHEN ");
        sbTypeNameCase.append(PostgresLiterals.formatStringLiteral(sAlias));
        sbTypeNameCase.append(" THEN ");
        sbTypeNameCase.append(PostgresLiterals.formatStringLiteral(pgt.getPreType().getKeyword()));
        sbTypeNameCase.append("\r\n");
      }
    }
    sbTypeNameCase.append("    WHEN ");
    sbTypeNameCase.append(PostgresLiterals.formatStringLiteral("USER-DEFINED"));
    sbTypeNameCase.append(" THEN ");
    sbTypeNameCase.append(PostgresLiterals.formatStringLiteral("\""));
    sbTypeNameCase.append(" || a.attribute_udt_schema || ");
    sbTypeNameCase.append(PostgresLiterals.formatStringLiteral("."));
    sbTypeNameCase.append(" || a.attribute_udt_name || ");
    sbTypeNameCase.append(PostgresLiterals.formatStringLiteral("\""));
    sbTypeNameCase.append("\r\n");
    sbTypeNameCase.append("END");
    return sbTypeNameCase.toString();
  }

  private String getDataTypeCase(String sDataType)
  {
    StringBuilder sbDataTypeCase = new StringBuilder("  CASE ");
    sbDataTypeCase.append(sDataType);
    sbDataTypeCase.append("\r\n");
    for (int i = 0; i < PostgresType.values().length; i++)
    {
      PostgresType pgt = PostgresType.values()[i];
      sbDataTypeCase.append("    WHEN ");
      sbDataTypeCase.append(PostgresLiterals.formatStringLiteral(pgt.getKeyword()));
      sbDataTypeCase.append(" THEN ");
      sbDataTypeCase.append(String.valueOf(pgt.getPreType().getSqlType()));
      sbDataTypeCase.append("\r\n");
      for (String sAlias : pgt.getAliases())
      {
        sbDataTypeCase.append("    WHEN ");
        sbDataTypeCase.append(PostgresLiterals.formatStringLiteral(sAlias));
        sbDataTypeCase.append(" THEN ");
        sbDataTypeCase.append(String.valueOf(pgt.getPreType().getSqlType()));
        sbDataTypeCase.append("\r\n");
      }
    }
    sbDataTypeCase.append("    WHEN ");
    sbDataTypeCase.append(PostgresLiterals.formatStringLiteral("USER-DEFINED"));
    sbDataTypeCase.append(" THEN ");
    sbDataTypeCase.append(String.valueOf(Types.STRUCT));
    sbDataTypeCase.append("\r\n");
    sbDataTypeCase.append("END");
    return sbDataTypeCase.toString();
  }
  
  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public ResultSet getAttributes(String catalog, String schemaPattern,
    String typeNamePattern, String attributeNamePattern)
    throws SQLException
  {
    _il.enter(catalog,schemaPattern,typeNamePattern,attributeNamePattern);
    ResultSet rsAttributes = null;
    StringBuilder sbCondition = new StringBuilder();
    sbCondition.append("a.udt_name LIKE ");
    sbCondition.append(SqlLiterals.formatStringLiteral(typeNamePattern));
    sbCondition.append(" ESCAPE ");
    sbCondition.append(SqlLiterals.formatStringLiteral(getSearchStringEscape()));
    sbCondition.append("\r\n");
    if (schemaPattern != null)
    {
      sbCondition.append("AND a.udt_schema LIKE ");
      sbCondition.append(SqlLiterals.formatStringLiteral(schemaPattern));
      sbCondition.append(" ESCAPE ");
      sbCondition.append(SqlLiterals.formatStringLiteral(getSearchStringEscape()));
      sbCondition.append("\r\n");
    }
      
    String sSql = "SELECT\r\n" +
      "  a.udt_catalog AS TYPE_CAT,\r\n" +
      "  a.udt_schema AS TYPE_SCHEM,\r\n" +
      "  a.udt_name AS TYPE_NAME,\r\n" +
      "  a.attribute_name AS ATTR_NAME,\r\n" +
      "  "+getDataTypeCase("a.data_type")+" AS DATA_TYPE,\r\n" +
      "  "+getTypeNameCase("a.data_type") + " AS ATTR_TYPE_NAME,\r\n" +
      "  COALESCE(a.numeric_precision, a.character_octet_length) AS ATTR_SIZE,\r\n" +
      "  a.numeric_scale AS DECIMAL_DIGITS,\r\n" +
      "  a.numeric_precision_radix AS NUM_PREC_RADIX,\r\n" +
      "  CASE a.is_nullable\r\n" +
      "    WHEN 'NO' THEN "+String.valueOf(attributeNoNulls)+"\r\n" +
      "    WHEN 'YES' THEN "+String.valueOf(attributeNullable)+"\r\n" +
      "    ELSE "+String.valueOf(attributeNullableUnknown)+"\r\n" +
      "  END AS NULLABLE,\r\n" +
      "  NULL AS REMARKS,\r\n" + // to be added: https://stackoverflow.com/questions/11493978/how-to-retrieve-the-comment-of-a-postgresql-database
      "  a.attribute_default AS ATTR_DEF,\r\n" +
      "  NULL AS SQL_DATA_TYPE,\r\n" +
      "  NULL AS SQL_DATETIME_SUB,\r\n" +
      "  a.character_octet_length as CHAR_OCTET_LENGTH,\r\n" +
      "  a.ordinal_position AS POSITION,\r\n" +
      "  a.is_nullable AS IS_NULLABLE,\r\n" +
      "  a.scope_catalog AS SCOPE_CATALOG,\r\n" +
      "  a.scope_schema AS SCOPE_SCHEMA,\r\n" +
      "  a.scope_name AS SCOPE_TABLE,\r\n" +
      "  NULL AS SOURCE_DATA_TYPE\r\n" + // nonsense: DISTINCT types have no attributes!
      "FROM information_schema.attributes a\r\n" +
      "WHERE " + sbCondition.toString() + 
      "ORDER BY TYPE_CAT, TYPE_SCHEM, TYPE_NAME, ORDINAL_POSITION";
    
    _il.event("Unwrapped query: "+sSql);
    Statement stmt = getConnection().createStatement().unwrap(Statement.class);
    rsAttributes = stmt.executeQuery(sSql);
    _il.exit(rsAttributes);
    return new PostgresResultSet(rsAttributes,stmt);
  } /* getAttributes */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public ResultSet getProcedureColumns(String catalog,
    String schemaPattern, String procedureNamePattern,
    String columnNamePattern) throws SQLException
  {
    return new PostgresMetaColumns(super.getProcedureColumns(catalog, schemaPattern,
      procedureNamePattern, columnNamePattern),_conn,1,2,6,7,8,9,10,-1);
  } /* getProcedureColumns */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public ResultSet getFunctionColumns(String catalog,
    String schemaPattern, String functionNamePattern,
    String columnNamePattern) throws SQLException
  {
    return new PostgresMetaColumns(super.getFunctionColumns(catalog, schemaPattern,
      functionNamePattern, columnNamePattern),_conn,1,2,6,7,8,9,10,-1);
  } /* getFunctionColumns */

} /* class PostgresDatabaseMetaData */
