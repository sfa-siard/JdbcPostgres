package ch.admin.bar.siard2.jdbc;

import java.sql.*;
import java.sql.Date;
import java.text.*;
import java.util.*;

import ch.enterag.utils.EU;
import ch.enterag.utils.database.SqlTypes;
import ch.enterag.utils.jdbc.*;
import ch.enterag.sqlparser.identifier.*;

import org.postgresql.util.*;

public class PostgresStruct
  extends BaseStruct
{
  private String _sType = null;
  private Object[] _ao = null;
  
  /*------------------------------------------------------------------*/
  /** constructor
   * @param sType type (qualified)
   * @param ao array of objects.
   */
  PostgresStruct(String sType, Object[] ao)
  {
    super(null);
    _sType = sType;
    _ao = ao;
  } /* constructor PostgresStruct */
  
  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public String getSQLTypeName() throws SQLException
  {
    return _sType;
  } /* getSQLTypeName */
  
  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public Object[] getAttributes() throws SQLException
  {
    return _ao;
  } /* getAttributes */

} /* class PostgresStruct */
