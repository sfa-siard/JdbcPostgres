/*======================================================================
PostgresStatement implements a wrapped Postgres Statement.
Application : SIARD2
Description : PostgresStatement implements a wrapped Postgres Statement.
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
/** PostgresStatement implements a wrapped Postgres Statement.
 * @author Hartwig Thomas
 */
public class PostgresStatement
  extends BaseStatement
  implements Statement
{
  protected Connection _conn;
  
  /*------------------------------------------------------------------*/
  /** constructor
   * @param stmtWrapped statement to be wrapped.
   * @param conn wrapped connection.
   */
  public PostgresStatement(Statement stmtWrapped, Connection conn)
    throws SQLException
  {
    super(stmtWrapped);
    _conn = conn;
  } /* constructor */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public Connection getConnection() 
    throws SQLException
  {
    return _conn;
  } /* getConnection */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc}
   * Return MsSqlResultSet. 
   */
  @Override
  public ResultSet executeQuery(String sql) throws SQLException
  {
    return new PostgresResultSet(super.executeQuery(sql),this);
  } /* executeQuery */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc}
   * Return MsSqlResultSet. 
   */
  @Override
  public ResultSet getResultSet() throws SQLException
  {
    return new PostgresResultSet(super.getResultSet(),this);
  } /* getResultSet */

} /* class PostgresStatement */
