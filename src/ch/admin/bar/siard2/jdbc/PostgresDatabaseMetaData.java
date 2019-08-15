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
import ch.enterag.utils.jdbc.*;

/*====================================================================*/
/** PostgresDatabaseMetaData implements wrapped Postgres DatabaseMetaData.
 * @author Hartwig Thomas
 */
public class PostgresDatabaseMetaData
  extends BaseDatabaseMetaData
  implements DatabaseMetaData
{
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
    return new PostgresMetaColumns(rs,_conn,1,2,5,6,7,16,9);
  } /* getColumns */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public ResultSet getAttributes(String catalog, String schemaPattern,
    String typeNamePattern, String attributeNamePattern)
    throws SQLException
  {
    return new PostgresMetaColumns(super.getAttributes(catalog, schemaPattern, 
      typeNamePattern, attributeNamePattern),_conn,1,2,5,6,7,15,8);
  } /* getAttributes */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public ResultSet getProcedureColumns(String catalog,
    String schemaPattern, String procedureNamePattern,
    String columnNamePattern) throws SQLException
  {
    return new PostgresMetaColumns(super.getProcedureColumns(catalog, schemaPattern,
      procedureNamePattern, columnNamePattern),_conn,1,2,6,7,8,9,10);
  } /* getProcedureColumns */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public ResultSet getFunctionColumns(String catalog,
    String schemaPattern, String functionNamePattern,
    String columnNamePattern) throws SQLException
  {
    return new PostgresMetaColumns(super.getFunctionColumns(catalog, schemaPattern,
      functionNamePattern, columnNamePattern),_conn,1,2,6,7,8,9,10);
  } /* getFunctionColumns */

} /* class PostgresDatabaseMetaData */
