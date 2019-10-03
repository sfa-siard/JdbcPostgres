/*======================================================================
PostgresResultSet implements a wrapped PostgreSQL ResultSet.
Application : SIARD2
Description : PostgresResultSet implements a wrapped PostgreSQL ResultSet.
Platform    : Java 8-10   
------------------------------------------------------------------------
Copyright  : 2019, Swiss Federal Archives, Berne, Switzerland
License    : CDDL 1.0
Created    : 09.08.2019, Hartwig Thomas, Enter AG, Rüti ZH, Switzerland
======================================================================*/
package ch.admin.bar.siard2.jdbc;

import java.io.*;
import java.sql.*;
import java.text.*;
import java.util.*;
import javax.xml.datatype.*;
import org.postgresql.jdbc.*;
import org.postgresql.util.*;
import ch.enterag.utils.*;
import ch.enterag.utils.jdbc.*;
import ch.admin.bar.siard2.postgres.*;

/*====================================================================*/
/** PostgresResultSet implements a wrapped PostgreSQL ResultSet.
 * @author Hartwig Thomas
 */
public class PostgresResultSet
extends BaseResultSet
implements ResultSet
{
  private static final int iBUFSIZ = 8192;
  private Statement _stmt = null;
  private ResultSetMetaData _rsmd = null;
  
  /*------------------------------------------------------------------*/
  /** constructor
   * @param rsWrapped result set to be wrapped.
   * @param stmt wrapped statement.
   */
  public PostgresResultSet(ResultSet rsWrapped, Statement stmt)
    throws SQLException
  {
    super(rsWrapped);
    _stmt = stmt;
    _rsmd = new PostgresResultSetMetaData(super.getMetaData(),_stmt);
  } /* constructor */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public Statement getStatement() throws SQLException
  {
    return _stmt;
  } /* getStatement */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public ResultSetMetaData getMetaData() throws SQLException
  {
    return _rsmd;
  } /* getMetaData */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public byte[] getBytes(int columnIndex) throws SQLException
  {
    byte[] buf = null;
    ResultSetMetaData rsmd = getMetaData();
    int iColumnType = rsmd.getColumnType(columnIndex);
    if ((iColumnType == Types.BIT) ||
        (iColumnType == Types.OTHER))
    {
      String sBitString = super.getString(columnIndex);
      buf = PostgresLiterals.parseBitString(sBitString);
    }
    else
      buf = super.getBytes(columnIndex);
    _bWasNull = super.wasNull();
    return buf;
  } /* getBytes */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void updateBytes(int columnIndex, byte[] x)
    throws SQLException
  {
    ResultSetMetaData rsmd = getMetaData();
    int iColumnType = rsmd.getColumnType(columnIndex);
    if ((iColumnType == Types.BIT) ||
        (iColumnType == Types.OTHER))
      super.updateString(columnIndex, PostgresLiterals.formatBitString(x, 8*x.length));
    else
      super.updateBytes(columnIndex, x);
  } /* updateBytes */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public InputStream getBinaryStream(int columnIndex)
    throws SQLException
  {
    InputStream is = null;
    ResultSetMetaData rsmd = getMetaData();
    int iColumnType = rsmd.getColumnType(columnIndex);
    if ((iColumnType == Types.BIT) ||
      (iColumnType == Types.OTHER))
      is = new ByteArrayInputStream(getBytes(columnIndex));
    else
      is = super.getBinaryStream(columnIndex);
    return is;
  } /* getBinaryStream */

  /*------------------------------------------------------------------*/
  private byte[] getByteArray(InputStream is)
    throws SQLException
  {
    try
    {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      byte[] buf = new byte[iBUFSIZ];
      for (int iRead = is.read(buf); iRead != -1; iRead = is.read(buf))
        baos.write(buf,0,iRead);
      baos.close();
      is.close();
      return baos.toByteArray();
    }
    catch(IOException ie) { throw new SQLException("UpdateBinaryStream failed for data type BIT ("+EU.getExceptionMessage(ie)+")!"); }
  } /* getByteArray */
  
  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void updateBinaryStream(int columnIndex, InputStream x)
    throws SQLException
  {
    ResultSetMetaData rsmd = getMetaData();
    int iColumnType = rsmd.getColumnType(columnIndex);
    if ((iColumnType == Types.BIT) ||
      (iColumnType == Types.OTHER))
      updateBytes(columnIndex,getByteArray(x));
    else
      super.updateBinaryStream(columnIndex, x);
  } /* updateBinaryStream */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void updateBinaryStream(int columnIndex, InputStream x,
    int length) throws SQLException
  {
    updateBinaryStream(columnIndex, x, (long)length);
  } /* updateBinaryStream */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void updateBinaryStream(int columnIndex, InputStream x,
    long length) throws SQLException
  {
    ResultSetMetaData rsmd = getMetaData();
    int iColumnType = rsmd.getColumnType(columnIndex);
    if ((iColumnType == Types.BIT) ||
      (iColumnType == Types.OTHER))
    {
      byte[] buf = getByteArray(x);
      if (buf.length == (int)length)
        updateBytes(columnIndex, buf);
      else
        throw new SQLException("Invalid length of binary stream in updateBinaryStream!");
    }
    else
      super.updateBinaryStream(columnIndex, x, length);
  } /* updateBinaryStream */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public Blob getBlob(int columnIndex) throws SQLException
  {
    Blob blob = new PostgresBlob((PgBlob)super.getBlob(columnIndex));
    return blob;
  } /* getBlob */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException
  {
    // TODO: String sSql = "GRANT ALL ON LARGE OBJECT "+String.valueOf(loid)+" TO PUBLIC";
    super.updateBlob(columnIndex, x);
  } /* updateBlob */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public Clob getClob(int columnIndex) throws SQLException
  {
    Clob clob = new PostgresClob((PgClob)super.getClob(columnIndex));
    return clob;
  } /* getClob */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException
  {
    // TODO: String sSql = "GRANT ALL ON LARGE OBJECT "+String.valueOf(loid)+" TO PUBLIC";
    super.updateClob(columnIndex, x);
  } /* updateClob */

  private Duration getDuration(PGInterval pgi)
    throws SQLException
  {
    Duration duration = null;
    try
    {
      DatatypeFactory df = DatatypeFactory.newInstance();
      boolean bPositive = true;
      if ((pgi.getDays() == 0) && 
          (pgi.getMinutes() == 0) && 
          (pgi.getSeconds() == 0.0))
      {
        int iYears = pgi.getYears();
        int iMonths = pgi.getMonths();
        if ((iYears < 0) || (iMonths < 0))
        {
          bPositive = false;
          iYears = -iYears;
          iMonths = -iMonths;
        }
        duration = df.newDurationYearMonth(bPositive, iYears, iMonths);
      }
      else
      {
        int iDays = pgi.getDays();
        int iHours = pgi.getHours();
        int iMinutes = pgi.getMinutes();
        double dSeconds = pgi.getSeconds();
        if ((iDays < 0) || (iHours < 0) || (iMinutes < 0) || (dSeconds < 0.0))
        {
          bPositive = false;
          iDays = -iDays;
          iHours = -iHours;
          iMinutes = -iMinutes;
          dSeconds = -dSeconds;
        }
        int iSeconds = (int)Math.round(dSeconds);
        int iMilliSeconds = (int)Math.round(1000*(dSeconds-iSeconds));  
        long lMilliSeconds = iDays;
        lMilliSeconds = 24*lMilliSeconds+iHours;
        lMilliSeconds = 60*lMilliSeconds+iMinutes;
        lMilliSeconds = 60*lMilliSeconds+iSeconds;
        lMilliSeconds = 1000*lMilliSeconds+iMilliSeconds;
        if (!bPositive)
          lMilliSeconds = -lMilliSeconds;
        duration = df.newDurationDayTime(lMilliSeconds);
      }
    }
    catch(DatatypeConfigurationException dcfe){}
    return duration;
  } /* getDuration */
  
  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public Object getObject(int columnIndex) throws SQLException
  {
    Object o = null;
    int iType = getMetaData().getColumnType(columnIndex);
    if (iType == Types.CLOB)
      o = getClob(columnIndex);
    else if (iType == Types.BLOB)
      o = getBlob(columnIndex);
    else if (iType == Types.OTHER)
    {
      o = super.getObject(columnIndex);
      if (o instanceof PGInterval)
        o = getDuration((PGInterval)o);
      else
        o = getBytes(columnIndex);
    }
    else if (iType == Types.BIT)
      o = getBytes(columnIndex);
    else if (iType == Types.SMALLINT)
      o = getShort(columnIndex);
    else if (iType == Types.DOUBLE)
    {
      try { o = super.getObject(columnIndex,Double.class); }
      catch(SQLException se) // bad JDBC implementation of MONEY data type
      {
        // most likely Postgres JDBC uses the client locale for formatting the string ...
        String s = super.getString(columnIndex);
        // in JAVA 8 the Locale de_CH uses apostrophes rather than right single quotes!
        Locale locDefault = Locale.getDefault();
        DecimalFormatSymbols dfs = new DecimalFormatSymbols(locDefault);
        DecimalFormat df = new DecimalFormat("#,##0.0#",dfs);
        df.setParseBigDecimal(true);
        s = s.substring(df.getCurrency().getCurrencyCode().length()).trim();
        s = s.replace('\u2019', '\'');
        try { o = df.parse(s); }
        catch(ParseException pe) { throw new SQLException("Error parsing string ("+EU.getExceptionMessage(pe)+")!"); }
      }
    }
    else if (iType == Types.TIME)
    {
      String s = super.getString(columnIndex);
      if (s.length() > 8)
        s = s.substring(0,8); // ignore offset, return local time
      o = Time.valueOf(s);
    }
    else if (iType == Types.VARCHAR)
      o = super.getString(columnIndex);
    else 
      o = super.getObject(columnIndex);
    return o;
  } /* getObject */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map)
    throws SQLException
  {
    Object o = getObject(columnIndex);
    return o;
  } /* getObject */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @SuppressWarnings("unchecked")
  @Override
  public <T> T getObject(int columnIndex, Class<T> type)
    throws SQLException
  {
    T o = (T)getObject(columnIndex);
    return o;
  } /* getObject */

} /* class PostgresResultSet */
