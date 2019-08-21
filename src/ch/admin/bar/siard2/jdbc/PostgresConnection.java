/*======================================================================
PostgresConnection implements a wrapped PostgreSQL Connection.
Application : SIARD2
Description : PostgresConnection implements a wrapped PostgreSQL Connection.
Platform    : Java 8-10   
------------------------------------------------------------------------
Copyright  : 2019, Swiss Federal Archives, Berne, Switzerland
License    : CDDL 1.0
Created    : 22.07.2019, Hartwig Thomas, Enter AG, Rüti ZH, Switzerland
======================================================================*/
package ch.admin.bar.siard2.jdbc;

import java.sql.*;
import ch.admin.bar.siard2.postgres.*;
import ch.enterag.sqlparser.*;
import ch.enterag.utils.jdbc.*;
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
  /*** meta data */
  private DatabaseMetaData _dmd = null;
  
  /*------------------------------------------------------------------*/
  /** constructor
   * @param connWrapped connection to be wrapped.
   */
  public PostgresConnection(Connection connWrapped)
    throws SQLException
  {
    super(connWrapped);
    DatabaseMetaData dmd = super.getMetaData();
    if (dmd != null)
    {
      if (dmd instanceof PostgresDatabaseMetaData)
        throw new SQLException("PostgresConnection() returned a wrapped meta data instance!");
      dmd = new PostgresDatabaseMetaData(dmd,this);
    }
    _dmd = dmd;    
  } /* constructor */
  
  /*------------------------------------------------------------------*/
  /** {@inheritDoc}
   * returns wrapped database meta data.
   */
  @Override
  public DatabaseMetaData getMetaData()
    throws SQLException
  {
    return _dmd;
  } /* getMetadata */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public String nativeSQL(String sql) 
    throws SQLException
  {
    _il.enter(sql);
    PostgresSqlFactory psf = new PostgresSqlFactory();
    psf.setConnection(this);
    SqlStatement ss = psf.newSqlStatement();
    ss.parse(sql);
    sql = ss.format();
    _il.exit(sql);
    return sql;
  } /* nativeSQL */

  /*------------------------------------------------------------------*/
  /**
   * {@inheritDoc} wraps statement.
   */
  @Override
  public Statement createStatement() throws SQLException 
  {
    Statement stmt = super.createStatement();
    return new PostgresStatement(stmt,this);
  } /* createStatement */

  /*------------------------------------------------------------------*/
  /**
   * {@inheritDoc} wraps statement.
   */
  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException 
  {
    Statement stmt = super.createStatement(resultSetType, resultSetConcurrency);
    return new PostgresStatement(stmt,this);
  } /* createStatement */
  
  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public Statement createStatement(int resultSetType,
    int resultSetConcurrency, int resultSetHoldability)
    throws SQLException
  {
    Statement stmt = super.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    return new PostgresStatement(stmt,this);
  } /* createStatement */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException 
  {
    String sNative = nativeSQL(sql);
    PreparedStatement ps = super.prepareStatement(sNative);
    return new PostgresPreparedStatement(ps,this);
  } /* prepareStatement */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException 
  {
    String sNative = nativeSQL(sql);
    PreparedStatement ps = super.prepareStatement(sNative, resultSetType, resultSetConcurrency);
    return new PostgresPreparedStatement(ps,this);
  } /* prepareStatement */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException
  {
    String sNative = nativeSQL(sql);
    PreparedStatement ps = super.prepareStatement(sNative, resultSetType, resultSetConcurrency, resultSetHoldability);
    return new PostgresPreparedStatement(ps,this);
  } /* prepareStatement */
  
  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException 
  {
    String sNative = nativeSQL(sql);
    PreparedStatement ps = super.prepareStatement(sNative, autoGeneratedKeys);
    return new PostgresPreparedStatement(ps,this);
  } /* prepareStatement */
  
  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException 
  {
    String sNative = nativeSQL(sql);
    PreparedStatement ps = super.prepareStatement(sNative, columnIndexes);
    return new PostgresPreparedStatement(ps,this);
  } /* prepareStatement */
  
  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException 
  {
    String sNative = nativeSQL(sql);
    PreparedStatement ps = super.prepareStatement(sNative, columnNames);
    return new PostgresPreparedStatement(ps,this);
  } /* prepareStatement */
  
  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public CallableStatement prepareCall(String sql) throws SQLException 
  {
    String sNative = nativeSQL(sql);
    CallableStatement cs = super.prepareCall(sNative);
    return cs;
  } /* prepareCall */
  
  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException 
  {
    String sNative = nativeSQL(sql);
    CallableStatement cs = super.prepareCall(sNative, resultSetType, resultSetConcurrency);
    return cs;
  } /* prepareCall */
  
  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException 
  {
    String sNative = nativeSQL(sql);
    CallableStatement cs = super.prepareCall(sNative, resultSetType, resultSetConcurrency, resultSetHoldability);
    return cs;
  } /* prepareCall */

} /* class PostgresConnection */
