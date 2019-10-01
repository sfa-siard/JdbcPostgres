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

import java.nio.ByteBuffer;
import java.util.UUID;

import ch.enterag.sqlparser.*;
import ch.enterag.sqlparser.datatype.enums.IntervalField;
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
  /** format byte buffer value.
   * @param bufValue byte buffer value to be formatted.
   * @return byte string literal.
   */
  public static String formatBytesLiteral(byte[] bufValue)
  {
    String sFormatted = sNULL;
    if (bufValue != null)
      sFormatted = formatStringLiteral("\\x"+BU.toHex(bufValue));
    return sFormatted;
  } /* formatBytesLiteral */
  
  /*------------------------------------------------------------------*/
  /** format an interval value
   * @param ivValue interval value to be formatted.
   * @return interval literal.
   */
  public static String formatIntervalLiteral(Interval ivValue)
  {
    String sFormatted = sNULL;
    if (ivValue != null)
    {
      IntervalField ifStart = null;
      IntervalField ifEnd = null;
      String sValue = null;
      int iSecondsPrecision = -1;
      if (ivValue.getYears() != 0)
      {
        ifStart = IntervalField.YEAR;
        sValue = String.valueOf(ivValue.getYears());
      }
      if (ivValue.getMonths() != 0)
      {
        if (ifStart == null)
        {
          ifStart = IntervalField.MONTH;
          sValue = String.valueOf(ivValue.getMonths());
        }
        else
        {
          ifEnd = IntervalField.MONTH;
          sValue = sValue + sHYPHEN + String.valueOf(ivValue.getMonths()); 
        }
      }
      if (ivValue.getDays() != 0)
      {
        ifStart = IntervalField.DAY;
        sValue = String.valueOf(ivValue.getDays());
      }
      if (ivValue.getHours() != 0)
      {
        if (ifStart == null)
        {
          ifStart = IntervalField.HOUR;
          sValue = String.valueOf(ivValue.getHours());
        }
        else
        {
          ifEnd = IntervalField.HOUR;
          sValue = sValue + sSP + String.valueOf(ivValue.getHours());
        }
      }
      if (ivValue.getMinutes() != 0)
      {
        if (ifStart == null)
        {
          ifStart = IntervalField.MINUTE;
          sValue = String.valueOf(ivValue.getMinutes());
        }
        else
        {
          ifEnd = IntervalField.MINUTE;
          sValue = sValue + sCOLON + String.valueOf(ivValue.getMinutes());
        }
      }
      if ((ivValue.getSeconds() != 0) || (ivValue.getNanoSeconds() != 0))
      {
        if (ifStart == null)
        {
          ifStart = IntervalField.SECOND;
          sValue = String.valueOf(ivValue.getSeconds());
        }
        else
        {
          ifEnd = IntervalField.SECOND;
          sValue = sValue + sCOLON + String.valueOf(ivValue.getSeconds());
        }
        if (ivValue.getNanoSeconds() != 0)
        {
          String sNanos = String.valueOf(ivValue.getNanoSeconds());
          while (sNanos.length() < 9)
            sNanos = sZERO + sNanos;
          while (sNanos.endsWith(sZERO))
            sNanos = sNanos.substring(0,sNanos.length()-1);
          sValue = sValue + sPERIOD + sNanos;
          if (sNanos.length() > 6)
            iSecondsPrecision = sNanos.length();
        }
      }
      sFormatted = sINTERVAL_LITERAL_PREFIX + sSP;
      if (ivValue.getSign() < 0)
        sValue = sMINUS + sSP + sValue;
      sFormatted = sFormatted + formatStringLiteral(sValue) + 
        sSP + ifStart.getKeywords();
      if ((ifStart == IntervalField.SECOND) && (iSecondsPrecision >= 0))
      {
        if ((ifStart == IntervalField.SECOND) && (iSecondsPrecision >= 0))
          sFormatted = sFormatted + sCOMMA + sSP + String.valueOf(iSecondsPrecision);
        sFormatted = sFormatted + sRIGHT_PAREN;
      }
      if (ifEnd != null)
        sFormatted = sFormatted + sSP + K.TO.getKeyword() + sSP + ifEnd.getKeywords();
      if ((ifEnd == IntervalField.SECOND) && (iSecondsPrecision >= 0))
        sFormatted = sFormatted + sLEFT_PAREN + String.valueOf(iSecondsPrecision) + sRIGHT_PAREN;
    }
    return sFormatted;
  } /* formatIntervalLiteral */
  
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
  /** parse a bit string
   * @param sBitString bit string of 0s and 1s.
   * @param bFiller filler byte at the end.
   * @return bytes for bit string.
   */
  public static byte[] parseBitString(String sBitString)
  {
    int iBytes = (sBitString.length()+7)/8;
    byte[] buf = new byte[iBytes];
    int iBits = 0;
    for (int iByteIndex = 0; iByteIndex < iBytes; iByteIndex++)
    {
      int iByte = 0;
      int iMask = 0x00000080;
      for (int iBitIndex = 0; (iBits < sBitString.length()) && (iBitIndex < 8); iBitIndex++)
      {
        if (sBitString.charAt(iBits) == '1')
          iByte = iByte | iMask;
        iMask = iMask >> 1;
        iBits++;
      }
      if (iByte > 0x0000007F)
        iByte = iByte - 256;
      buf[iByteIndex] = (byte)iByte;
    }
    return buf;
  }
  
  /*------------------------------------------------------------------*/
  /** convert UUID to big-endian byte buffer.
   * @param uuid UUID
   * @return byte buffer.
   */
  public static byte[] convertUuidToByteArray(UUID uuid)
  {
    ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
    bb.putLong(uuid.getMostSignificantBits());
    bb.putLong(uuid.getLeastSignificantBits());
    byte[] buf = bb.array();
    // System.out.println(BU.toHex(buf));
    return buf;
  }
  
  /*------------------------------------------------------------------*/
  /** convert a big-endian byte buffer into a UUID.
   * taking the first 4 bytes as a little-endian integer,
   * the next 2 bytes as a little-endian word,
   * the next 2 bytes as a little-endian word,
   * and the final 8 bytes into a straight (big endian) byte buffer. 
   * @param buf big-endian byte buffer
   * @return UUID
   */
  public static UUID convertByteArrayToUuid(byte[] buf)
  {
    byte[] b = new byte[16];
    for (int i = 0; i < 4; i++)
      b[i] = buf[3-i];
    for (int i = 0; i < 2; i++)
      b[4+i] = buf[5-i];
    for (int i = 0; i < 2; i++)
      b[6+i] = buf[7-i];
    for (int i = 0; i < 8; i++)
      b[8+i] = buf[8+i];
    ByteBuffer bb = ByteBuffer.wrap(b);
    UUID uuid = new UUID(bb.getLong(),bb.getLong());
    return uuid;
  }
  
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
