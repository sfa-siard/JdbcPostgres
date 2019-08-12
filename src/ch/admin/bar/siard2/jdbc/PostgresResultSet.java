/*======================================================================
PostgresResultSet implements a wrapped PostgreSQL ResultSet.
Application : SIARD2
Description : PostgresResultSet implements a wrapped PostgreSQL ResultSet.
Platform    : Java 8-10   
------------------------------------------------------------------------
Copyright  : 2019, Swiss Federal Archives, Berne, Switzerland
License    : CDDL 1.0
Created    : 09.08.2019, Hartwig Thomas, Enter AG, Rüti ZH, Switzerland
======================================================================*/
package ch.admin.bar.siard2.jdbc;

import java.sql.*;
import ch.enterag.utils.jdbc.*;

/*====================================================================*/
/** PostgresResultSet implements a wrapped PostgreSQL ResultSet.
 * @author Hartwig Thomas
 */
public class PostgresResultSet
extends BaseResultSet
implements ResultSet
{
  private Statement _stmt = null;
  private ResultSetMetaData _rsmd = null;
  
  /*------------------------------------------------------------------*/
  /** constructor
   * @param rsWrapped result set to be wrapped.
   * @param stmt wrapped statement.
   */
  public PostgresResultSet(ResultSet rsWrapped, Statement stmt)
    throws SQLException
  {
    super(rsWrapped);
    _stmt = stmt;
    _rsmd = new PostgresResultSetMetaData(super.getMetaData(),_stmt);
  } /* constructor */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public Statement getStatement() throws SQLException
  {
    return _stmt;
  } /* getStatement */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public ResultSetMetaData getMetaData() throws SQLException
  {
    return _rsmd;
  } /* getMetaData */

} /* class PostgresResultSet */
