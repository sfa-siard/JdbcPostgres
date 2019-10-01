/*======================================================================
PostgresResultSetMetaData implements wrapped PostgreSQL ResultSetMetaData.
Application : SIARD2
Description : PostgresResultSetMetaData implements wrapped PostgreSQL ResultSetMetaData.
Platform    : Java 8-10   
------------------------------------------------------------------------
Copyright  : 2019, Swiss Federal Archives, Berne, Switzerland
License    : CDDL 1.0
Created    : 09.08.2019, Hartwig Thomas, Enter AG, Rüti ZH, Switzerland
======================================================================*/
package ch.admin.bar.siard2.jdbc;

import java.sql.*;

import ch.admin.bar.siard2.postgres.PostgresType;
import ch.enterag.sqlparser.datatype.enums.PreType;
import ch.enterag.utils.jdbc.*;

/*====================================================================*/
/** PostgresResultSetMetaData implements wrapped PostgreSQL ResultSetMetaData.
 * @author Hartwig Thomas
 */
public class PostgresResultSetMetaData
  extends BaseResultSetMetaData
  implements ResultSetMetaData
{
  protected Statement _stmt = null;
  /*------------------------------------------------------------------*/
  /** constructor
   * @param rsWrapped result set to be wrapped.
   * @param wrapped statement.
   */
  public PostgresResultSetMetaData(ResultSetMetaData rsmdWrapped, Statement stmt)
  {
    super(rsmdWrapped);
    _stmt = stmt;
  } /* constructor */
  
  @Override
  public int getColumnType(int column) throws SQLException
  {
    int iType = super.getColumnType(column);
    String sTypeName = super.getColumnTypeName(column);
    String sTableName = super.getTableName(column);
    if ((sTableName != null) && (sTableName.length() > 0))
    {
      String sColumnName = super.getColumnName(column);
      String sSchemaName = super.getSchemaName(column);
      PostgresDatabaseMetaData pdmd = (PostgresDatabaseMetaData)_stmt.getConnection().getMetaData();
      ResultSet rsColumn = pdmd.getColumns(null, 
        pdmd.toPattern(sSchemaName), 
        pdmd.toPattern(sTableName),
        pdmd.toPattern(sColumnName));
      if (rsColumn.next())
        sTypeName = rsColumn.getString("TYPE_NAME");
      rsColumn.close();
    }
    PostgresType pgt = PostgresType.getByKeyword(sTypeName.toLowerCase());
    if (pgt != null)
    {
      PreType pt = pgt.getPreType();
      if (pt != null)
        iType = pt.getSqlType();
    }
    return iType;
  }

} /* PostgresResultSetMetaData */
