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

} /* PostgresResultSetMetaData */
