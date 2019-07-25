/*======================================================================
PostgresStatement implements a wrapped Postgres Statement.
Application : SIARD2
Description : PostgresStatement implements a wrapped Postgres Statement.
Platform    : Java 8-10   
------------------------------------------------------------------------
Copyright  : 2019, Swiss Federal Archives, Berne, Switzerland
License    : CDDL 1.0
Created    : 25.07.2017, Hartwig Thomas, Enter AG, Rüti ZH, Switzerland
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
  /*------------------------------------------------------------------*/
  /** constructor
   * @param stmtWrapped statement to be wrapped.
   */
  public PostgresStatement(Statement stmtWrapped)
    throws SQLException
  {
    super(stmtWrapped);
  } /* constructor */

} /* class PostgresStatement */
