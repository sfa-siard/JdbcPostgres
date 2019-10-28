package ch.admin.bar.siard2.jdbc;

import java.sql.*;
import java.text.*;

import ch.admin.bar.siard2.postgres.*;
import ch.enterag.utils.*;
import ch.enterag.utils.jdbc.*;

public class PostgresArray
  extends BaseArray
{
  private Object[] _ao = null;
  private int _iBaseType = Types.NULL;
  private String _sBaseTypeName = null;
  private int _iFinalBaseType = Types.NULL;
  private String _sFinalBaseTypeName = null;
  
  private Object getElement(Object oElement, PostgresConnection conn)
    throws SQLException
  {
    try
    {
      if (oElement != null)
      {
        if (oElement.getClass().isArray())
        {
          oElement = (Object)new PostgresArray((Object[])oElement, _iFinalBaseType, _sFinalBaseTypeName, conn);
          _iBaseType = Types.ARRAY;
          _sBaseTypeName = "array";
        }
        else
        {
          switch(_iBaseType)
          {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
              oElement = (Object)PostgresObject.stripQuotes((String)oElement); 
              break;
          }
        }
      }
    }
    catch(ParseException pe) { throw new SQLException("Could not parse array element "+String.valueOf(oElement)+" ("+EU.getExceptionMessage(pe)+")!"); }
    return oElement;
  }
  
  public PostgresArray(Array array, PostgresConnection conn)
    throws SQLException
  {
    super(array);
    _ao = (Object[])super.getArray();
    _iFinalBaseType = super.getBaseType();
    _iBaseType = _iFinalBaseType;
    _sFinalBaseTypeName = super.getBaseTypeName();
    _sBaseTypeName = _sFinalBaseTypeName;
    Object[] ao = new Object[_ao.length];
    for (int iElement = 0; iElement < _ao.length; iElement++)
      ao[iElement] = getElement(_ao[iElement],conn);
    _ao = ao;
  } /* constructor */
  
  public PostgresArray(Object[] ao, int iFinalBaseType, String sFinalBaseTypeName, PostgresConnection conn)
    throws SQLException
  {
    super(null);
    _ao = ao;
    _iFinalBaseType = iFinalBaseType;
    _iBaseType = _iFinalBaseType;
    _sFinalBaseTypeName = sFinalBaseTypeName;
    _sBaseTypeName = _sFinalBaseTypeName;
    for (int iElement = 0; iElement < _ao.length; iElement++)
      _ao[iElement] = getElement(_ao[iElement],conn);
  }
  
  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public String getBaseTypeName() throws SQLException
  {
    return _sBaseTypeName;
  } /* getBaseTypeName */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public int getBaseType() throws SQLException
  {
    return _iBaseType;
  } /* getBaseType */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public Object getArray() throws SQLException
  {
    return _ao;
  } /* getArray */

}
