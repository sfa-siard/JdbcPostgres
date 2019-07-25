/*======================================================================
PostgresDatabaseMetaData implements wrapped Postgres DatabaseMetaData.
Application : SIARD2
Description : PostgresDatabaseMetaData implements wrapped Postgres DatabaseMetaData.
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
/** PostgresDatabaseMetaData implements wrapped Postgres DatabaseMetaData.
 * @author Hartwig Thomas
 */
public class PostgresDatabaseMetaData
  extends BaseDatabaseMetaData
  implements DatabaseMetaData
{
  /*------------------------------------------------------------------*/
  /** constructor
   * @param dmdWrapped database meta data to be wrapped.
   */
  public PostgresDatabaseMetaData(DatabaseMetaData dmdWrapped)
  {
    super(dmdWrapped);
  } /* constructor */


} /* class PostgresDatabaseMetaData */
