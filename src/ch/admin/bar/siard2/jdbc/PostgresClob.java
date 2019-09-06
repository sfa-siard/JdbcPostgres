package ch.admin.bar.siard2.jdbc;

import ch.enterag.utils.jdbc.*;
import org.postgresql.jdbc.*;

public class PostgresClob
  extends BaseClob
{
  /*------------------------------------------------------------------*/
  /** constructor
   * @param pgClob
   */
  PostgresClob(PgClob pgClob)
  {
    super(pgClob);
  } /* constructor PostgresClob */

} /* class PostgresClob */
