package ch.admin.bar.siard2.jdbc;

import java.sql.*;

import ch.admin.bar.siard2.postgres.PostgresType;
import ch.enterag.utils.jdbc.*;

public class PostgresArray
  extends BaseArray
{
  public PostgresArray(Array array)
  {
    super(array);
  } /* constructor */
  
  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public String getBaseTypeName() throws SQLException
  {
    String sTypeName = super.getBaseTypeName();
    PostgresType pgt = PostgresType.getByKeyword(sTypeName);
    if (pgt != null)
      sTypeName = pgt.getPreType().getKeyword();
    return sTypeName;
  } /* getBaseTypeName */

}
