/*======================================================================
PostgresPreparedStatement implements a wrapped Postgres PreparedStatement.
Application : SIARD2
Description : PostgresPreparedStatement implements a wrapped Postgres PreparedStatement.
Platform    : Java 8-10   
------------------------------------------------------------------------
Copyright  : 2019, Swiss Federal Archives, Berne, Switzerland
License    : CDDL 1.0
Created    : 09.08.2019, Hartwig Thomas, Enter AG, Rüti ZH, Switzerland
======================================================================*/
package ch.admin.bar.siard2.jdbc;

import java.io.*;
import java.math.*;
import java.net.*;
import java.sql.*;
import java.util.Calendar;

/*====================================================================*/
/** PostgresPreparedStatement implements a wrapped Postgres PreparedStatement.
 * @author Hartwig Thomas
 */
public class PostgresPreparedStatement
  extends PostgresStatement
  implements PreparedStatement
{
  /** wrapped prepared statement */
  private PreparedStatement _pstmtWrapped = null;
  
  /*------------------------------------------------------------------*/
  /** constructor
   * @param stmtWrapped statement to be wrapped.
   * @param conn wrapped connection.
   */
  public PostgresPreparedStatement(PreparedStatement pstmtWrapped, Connection conn)
    throws SQLException
  {
    super(pstmtWrapped,conn);
    _pstmtWrapped = pstmtWrapped;
  } /* constructor */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public ResultSetMetaData getMetaData() throws SQLException
  {
    return new PostgresResultSetMetaData(_pstmtWrapped.getMetaData(),this);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException
  {
    return _pstmtWrapped.getParameterMetaData();
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public ResultSet executeQuery() throws SQLException
  {
    return new PostgresResultSet(_pstmtWrapped.executeQuery(),this);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public int executeUpdate() throws SQLException
  {
    return _pstmtWrapped.executeUpdate();
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public boolean execute() throws SQLException
  {
    return _pstmtWrapped.execute();
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void addBatch() throws SQLException
  {
    _pstmtWrapped.addBatch();
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void clearParameters() throws SQLException
  {
    _pstmtWrapped.clearParameters();
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setNull(int parameterIndex, int sqlType)
    throws SQLException
  {
    _pstmtWrapped.setNull(parameterIndex, sqlType);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName)
    throws SQLException
  {
    _pstmtWrapped.setNull(parameterIndex, sqlType, typeName);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setDate(int parameterIndex, Date x) throws SQLException
  {
    _pstmtWrapped.setDate(parameterIndex, x);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setDate(int parameterIndex, Date x, Calendar cal)
    throws SQLException
  {
    _pstmtWrapped.setDate(parameterIndex, x, cal);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setTime(int parameterIndex, Time x) throws SQLException
  {
    _pstmtWrapped.setTime(parameterIndex, x);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setTime(int parameterIndex, Time x, Calendar cal)
    throws SQLException
  {
    _pstmtWrapped.setTime(parameterIndex, x, cal);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setTimestamp(int parameterIndex, Timestamp x)
    throws SQLException
  {
    _pstmtWrapped.setTimestamp(parameterIndex, x);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setTimestamp(int parameterIndex, Timestamp x,
    Calendar cal) throws SQLException
  {
    _pstmtWrapped.setTimestamp(parameterIndex, x, cal);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setBoolean(int parameterIndex, boolean x)
    throws SQLException
  {
    _pstmtWrapped.setBoolean(parameterIndex, x);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setByte(int parameterIndex, byte x) throws SQLException
  {
    _pstmtWrapped.setByte(parameterIndex, x);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setShort(int parameterIndex, short x) throws SQLException
  {
    _pstmtWrapped.setShort(parameterIndex, x);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setInt(int parameterIndex, int x) throws SQLException
  {
    _pstmtWrapped.setInt(parameterIndex, x);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setLong(int parameterIndex, long x) throws SQLException
  {
    _pstmtWrapped.setLong(parameterIndex, x);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setFloat(int parameterIndex, float x) throws SQLException
  {
    _pstmtWrapped.setFloat(parameterIndex, x);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setDouble(int parameterIndex, double x)
    throws SQLException
  {
    _pstmtWrapped.setDouble(parameterIndex, x);
  }
  
  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x)
    throws SQLException
  {
    _pstmtWrapped.setBigDecimal(parameterIndex, x);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setString(int parameterIndex, String x)
    throws SQLException
  {
    _pstmtWrapped.setString(parameterIndex, x);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setNString(int parameterIndex, String value)
    throws SQLException
  {
    _pstmtWrapped.setNString(parameterIndex, value);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setClob(int parameterIndex, Clob x) throws SQLException
  {
    _pstmtWrapped.setClob(parameterIndex, x);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setClob(int parameterIndex, Reader reader)
    throws SQLException
  {
    _pstmtWrapped.setClob(parameterIndex, reader);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setClob(int parameterIndex, Reader reader, long length)
    throws SQLException
  {
    _pstmtWrapped.setClob(parameterIndex, reader, length);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setNClob(int parameterIndex, NClob value)
    throws SQLException
  {
    _pstmtWrapped.setNClob(parameterIndex, value);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setNClob(int parameterIndex, Reader reader)
    throws SQLException
  {
    _pstmtWrapped.setNClob(parameterIndex, reader);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setNClob(int parameterIndex, Reader reader, long length)
    throws SQLException
  {
    _pstmtWrapped.setNClob(parameterIndex, reader, length);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setAsciiStream(int parameterIndex, InputStream x)
    throws SQLException
  {
    _pstmtWrapped.setAsciiStream(parameterIndex, x);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setAsciiStream(int parameterIndex, InputStream x,
    int length) throws SQLException
  {
    _pstmtWrapped.setAsciiStream(parameterIndex, x, length);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setAsciiStream(int parameterIndex, InputStream x,
    long length) throws SQLException
  {
    _pstmtWrapped.setAsciiStream(parameterIndex, x, length);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @SuppressWarnings("deprecation")
  @Override
  public void setUnicodeStream(int parameterIndex, InputStream x,
    int length) throws SQLException
  {
    _pstmtWrapped.setUnicodeStream(parameterIndex, x, length);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setCharacterStream(int parameterIndex, Reader reader)
    throws SQLException
  {
    _pstmtWrapped.setCharacterStream(parameterIndex, reader);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setCharacterStream(int parameterIndex, Reader reader,
    int length) throws SQLException
  {
    _pstmtWrapped.setCharacterStream(parameterIndex, reader, length);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setCharacterStream(int parameterIndex, Reader reader,
    long length) throws SQLException
  {
    _pstmtWrapped.setCharacterStream(parameterIndex, reader, length);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setNCharacterStream(int parameterIndex, Reader value)
    throws SQLException
  {
    _pstmtWrapped.setNCharacterStream(parameterIndex, value);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setNCharacterStream(int parameterIndex, Reader value,
    long length) throws SQLException
  {
    _pstmtWrapped.setNCharacterStream(parameterIndex, value, length);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setSQLXML(int parameterIndex, SQLXML xmlObject)
    throws SQLException
  {
    _pstmtWrapped.setSQLXML(parameterIndex, xmlObject);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setURL(int parameterIndex, URL x) throws SQLException
  {
    _pstmtWrapped.setURL(parameterIndex, x);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setRowId(int parameterIndex, RowId x) throws SQLException
  {
    _pstmtWrapped.setRowId(parameterIndex, x);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setRef(int parameterIndex, Ref x) throws SQLException
  {
    _pstmtWrapped.setRef(parameterIndex, x);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setBytes(int parameterIndex, byte[] x) throws SQLException
  {
    _pstmtWrapped.setBytes(parameterIndex, x);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setBlob(int parameterIndex, Blob x) throws SQLException
  {
    _pstmtWrapped.setBlob(parameterIndex, x);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setBlob(int parameterIndex, InputStream inputStream)
    throws SQLException
  {
    _pstmtWrapped.setBlob(parameterIndex, inputStream);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setBlob(int parameterIndex, InputStream inputStream,
    long length) throws SQLException
  {
    _pstmtWrapped.setBlob(parameterIndex, inputStream, length);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setBinaryStream(int parameterIndex, InputStream x)
    throws SQLException
  {
    _pstmtWrapped.setBinaryStream(parameterIndex, x);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setBinaryStream(int parameterIndex, InputStream x,
    int length) throws SQLException
  {
    _pstmtWrapped.setBinaryStream(parameterIndex, x, length);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setBinaryStream(int parameterIndex, InputStream x,
    long length) throws SQLException
  {
    _pstmtWrapped.setBinaryStream(parameterIndex, x, length);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setObject(int parameterIndex, Object x)
    throws SQLException
  {
    _pstmtWrapped.setObject(parameterIndex, x);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setArray(int parameterIndex, Array x) throws SQLException
  {
    _pstmtWrapped.setArray(parameterIndex, x);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType)
    throws SQLException
  {
    _pstmtWrapped.setObject(parameterIndex, x, targetSqlType);
  }

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType,
    int scaleOrLength) throws SQLException
  {
    _pstmtWrapped.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
  }

} /* PostgresPreparedStatement */
