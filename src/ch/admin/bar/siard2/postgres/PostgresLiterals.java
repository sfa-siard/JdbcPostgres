/*======================================================================
PostgresLiterals extends SqlLiterals.
Application : SIARD2
Description : PostgresLiterals extends SqlLiterals. 
Platform    : Java 8-10   
------------------------------------------------------------------------
Copyright  : 2016, Enter AG, Rüti ZH, Switzerland
Created    : 30.10.2016, Simon Jutz
======================================================================*/
package ch.admin.bar.siard2.postgres;

import ch.enterag.sqlparser.*;
import ch.enterag.utils.*;

/* =============================================================================== */
/** PostgresLiterals extends SqlLiterals.
 * @author Hartwig Thomas
 */
public abstract class PostgresLiterals extends SqlLiterals
{
  /*------------------------------------------------------------------*/
  /** quote an identifier in normal form (upper case, regular).
   * @param sIdentifier identifier in normal form.
   * @return identifier suitably delimited.
   */
  public static String formatId(String sIdentifier)
  {
    String sDelimited = null;
    if (sIdentifier != null)
    {
      if ((sIdentifier.length() >= iMIN_IDENTIFIER_LENGTH) && 
          (sIdentifier.length() <= iMAX_IDENTIFIER_LENGTH))
      {
        if ((!sIdentifier.equals(sIdentifier.toUpperCase())) || (!isRegular(sIdentifier)))
          sDelimited = sQUOTE + sIdentifier.replace(sQUOTE, sDOUBLE_QUOTE) + sQUOTE;
        else
          sDelimited = sIdentifier.toLowerCase();
      }
      else
        throw new IllegalArgumentException("Identifier length ("+sIdentifier+") " +
          "must be at least " + String.valueOf(iMIN_IDENTIFIER_LENGTH) + " " + 
          "and at most " + String.valueOf(iMAX_IDENTIFIER_LENGTH) + "!");
    }
    else
      throw new NullPointerException("Identifier must not be null!");
    return sDelimited;
  } /* formatId */
  
  /*------------------------------------------------------------------*/
  /** format a bit string
   * @param buffer bytes for bit string.
   * @param iMaxBits maximum number of bits.
   * @return formatted bit string.
   */
  public static String formatBitString(byte[] buffer, int iMaxBits)
  {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < buffer.length; i++)
    {
      int iByte = buffer[i];
      if (iByte < 0)
        iByte = iByte + 256;
      for (int iMask = 128; iMask > 0; iMask = iMask >> 1)
      {
        if (sb.length() < iMaxBits)
        {
          if ((iMask & iByte) != 0)
            sb.append('1');
          else
            sb.append('0');
        }
      }
    }
    return sb.toString();
  } /* formatBitString */
  
  /*------------------------------------------------------------------*/
  /** format a binary MAC address as a string.
   * @param buffer binary MAC address.
   * @return string representation.
   */
  public static String formatMacAddr(byte[] buffer)
  {
    String[] as = new String[buffer.length];
    for (int i = 0; i < buffer.length; i++)
      as[i] = BU.toHex(buffer[i]);
    return String.join(":", as);
  } /* formatMacAddr */
  
} /* class PostgresLiterals */
