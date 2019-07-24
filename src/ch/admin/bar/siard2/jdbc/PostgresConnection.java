/*======================================================================
PostgresConnection implements a wrapped PostgreSQL Connection.
Application : SIARD2
Description : PostgresConnection implements a wrapped PostgreSQL Connection.
Platform    : Java 8-10   
------------------------------------------------------------------------
Copyright  : 2019, Swiss Federal Archives, Berne, Switzerland
License    : CDDL 1.0
Created    : 22.07.2017, Hartwig Thomas, Enter AG, Rüti ZH, Switzerland
======================================================================*/
package ch.admin.bar.siard2.jdbc;

import java.sql.*;
import ch.enterag.utils.jdbc.BaseConnection;
import ch.enterag.utils.logging.*;

/*====================================================================*/
/** PostgresConnection implements a wrapped PostgreSQL Connection.
 * @author Hartwig Thomas
 */
public class PostgresConnection
extends BaseConnection
implements Connection
{
  /** logger */
  private static IndentLogger _il = IndentLogger.getIndentLogger(PostgresConnection.class.getName());
  
  /*------------------------------------------------------------------*/
  /** constructor
   * @param connWrapped connection to be wrapped.
   */
  public PostgresConnection(Connection connWrapped)
    throws SQLException
  {
    super(connWrapped);
  } /* constructor */
  

} /* class PostgresConnection */
