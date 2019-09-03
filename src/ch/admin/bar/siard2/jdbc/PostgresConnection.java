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
import ch.enterag.sqlparser.datatype.enums.*;
import ch.enterag.utils.*;
import ch.enterag.utils.jdbc.*;
import ch.enterag.utils.logging.*;
import org.postgresql.jdbc.*;

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
  
  private void createDomain(String sDomain)
    throws SQLException
  {
    /* if it exists already, do not do anything */
    try
    {
      String sSql = "CREATE DOMAIN public."+sDomain+" AS "+PostgresType.OID;
      Statement stmt = super.createStatement();
      stmt.executeUpdate(sSql);
      stmt.close();
      super.commit();
      stmt = super.createStatement();
      sSql = "GRANT ALL ON public."+sDomain+" TO public";
      stmt.executeUpdate(sSql);
      super.commit();
    }
    catch(SQLException se)
    {
      /* terminate transaction */
      try { super.rollback(); }
      catch(SQLException seRollback) { throw new SQLException("Rollback failed with "+EU.getExceptionMessage(seRollback)); }
    }
  } /* executeCreate */

  /*------------------------------------------------------------------*/
  /** constructor
   * @param connWrapped connection to be wrapped.
   */
  public PostgresConnection(Connection connWrapped)
    throws SQLException
  {
    super(connWrapped);
    boolean bAutoCommit = super.getAutoCommit();
    super.setAutoCommit(false);
    /* create standard domains BLOB, CLOB and NCLOB for oid */
    createDomain(PreType.BLOB.getKeyword());
    createDomain(PreType.CLOB.getKeyword());
    DatabaseMetaData dmd = super.getMetaData();
    if (dmd != null)
    {
      if (dmd instanceof PostgresDatabaseMetaData)
        throw new SQLException("PostgresConnection() returned a wrapped meta data instance!");
      dmd = new PostgresDatabaseMetaData(dmd,this);
    }
    _dmd = dmd;
    super.setAutoCommit(bAutoCommit);
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
  
  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public Clob createClob()
    throws SQLException
  {
    return new PostgresClob((PgClob)super.createClob());
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public Blob createBlob()
    throws SQLException
  {
    return new PostgresBlob((PgBlob)super.createBlob());
  }

} /* class PostgresConnection */
