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

import java.math.*;
import java.nio.*;
import java.text.*;
import java.time.*;
import java.util.*;

import ch.enterag.sqlparser.*;
import ch.enterag.sqlparser.datatype.enums.*;
import ch.enterag.utils.*;

/* =============================================================================== */
/** PostgresLiterals extends SqlLiterals.
 * @author Hartwig Thomas
 */
public abstract class PostgresLiterals extends SqlLiterals
{
  public static final String sPOSTGRES_BYTE_LITERAL_PREFIX = "\\x";
  private static int iNANOS_PER_SECOND = 1000000000;
  private static int iSECONDS_PER_MINUTE = 60;
  private static int iMINUTES_PER_HOUR = 60;
  private static int iHOURS_PER_DAY = 24;
  private static int iMONTHS_PER_YEAR = 12;
  static long lNANOS_PER_DAY = ((long)iNANOS_PER_SECOND)*iSECONDS_PER_MINUTE*iMINUTES_PER_HOUR*iHOURS_PER_DAY;
  private static String[] asTYPE = new String [] {"years", "mons", "days"};

  private static int skipWhite(String s, int iIndex)
  {
    for (; (iIndex < s.length()) && Character.isWhitespace(s.charAt(iIndex)); iIndex++) {}
    return iIndex;    
  } /* skipWhite */
  
  static long getNanos(LocalTime lt)
  {
    long lNanos = lt.getHour();
    lNanos = lNanos*iMINUTES_PER_HOUR + lt.getMinute();
    lNanos = lNanos*iSECONDS_PER_MINUTE + lt.getSecond();
    lNanos = lNanos*iNANOS_PER_SECOND + lt.getNano();
    return lNanos;
  } /* getNanos */
  
  static LocalTime getLocalTime(long lNanos)
  {
    LocalTime lt = null;
    int iNanos = (int)(lNanos % iNANOS_PER_SECOND);
    lNanos = lNanos / iNANOS_PER_SECOND;
    int iSeconds = (int)(lNanos % iSECONDS_PER_MINUTE);
    lNanos = lNanos /60;
    int iMinutes = (int)(lNanos % iSECONDS_PER_MINUTE);
    lNanos = lNanos / iMINUTES_PER_HOUR;
    int iHours = (int)(lNanos % iHOURS_PER_DAY);
    lNanos = lNanos / iHOURS_PER_DAY;
    int iDays = (int)lNanos;
    if (iDays == 0)
      lt = LocalTime.of(iHours, iMinutes, iSeconds, iNanos);
    return lt;
  } /* getLocalTime */
  
  /*------------------------------------------------------------------*/
  /** test a prefix and detach it.
   * @param s string to be tested.
   * @param sPrefix prefix to be detached.
   * @return string without prefix.
   */
  protected static String cutPrefix(String s, String sPrefix)
  {
    String sResult = null;
    if (s.toLowerCase().startsWith(sPrefix))
      sResult = s.substring(sPrefix.length()).trim();
    return sResult;
  } /* cutPrefix */
  
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
      sFormatted = formatStringLiteral(sPOSTGRES_BYTE_LITERAL_PREFIX+BU.toHex(bufValue));
    return sFormatted;
  } /* formatBytesLiteral */
  
  /*------------------------------------------------------------------*/
  /** parse a byte string literal.
   * @param sByteString byte string literal.
   * @return byte buffer value.
   * @throws ParseException if the input is not a valid byte string literal.
   */
  public static byte[] parseBytesLiteral(String sByteString)
    throws ParseException
  {
    byte[] bufParsed = null;
    String sParsed = cutPrefix(sByteString,sPOSTGRES_BYTE_LITERAL_PREFIX);
    if (sParsed != null)
      bufParsed = BU.fromHex(sParsed);
    else
      throw new ParseException("Byte character string literal must start with "+sPOSTGRES_BYTE_LITERAL_PREFIX+"!",0);
    return bufParsed;
  } /* parseBytesLiteral */
  
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
        if (ivValue.getSign() < 0)
          sValue = sMINUS + sSP + sValue;
      }
      if (ivValue.getHours() != 0)
      {
        String sHours = String.valueOf(ivValue.getHours());
        if (ivValue.getSign() < 0)
          sHours = sMINUS + sSP + sHours;
        if (ifStart == null)
        {
          ifStart = IntervalField.HOUR;
          sValue = sHours;
        }
        else
        {
          ifEnd = IntervalField.HOUR;
          sValue = sValue + sSP + sHours;
        }
      }
      if (ivValue.getMinutes() != 0)
      {
        String sMinutes = String.valueOf(ivValue.getMinutes());
        if (ifStart == null)
        {
          ifStart = IntervalField.MINUTE;
          sValue = sMinutes;
          if (ivValue.getSign() < 0)
            sValue = sMINUS + sSP + sValue;
        }
        else
        {
          ifEnd = IntervalField.MINUTE;
          sValue = sValue + sCOLON + sMinutes;
        }
      }
      if ((ivValue.getSeconds() != 0) || (ivValue.getNanoSeconds() != 0))
      {
        String sSeconds = String.valueOf(ivValue.getSeconds());
        if (ifStart == null)
        {
          ifStart = IntervalField.SECOND;
          sValue = sSeconds;
          if (ivValue.getSign() < 0)
            sValue = sMINUS + sSP + sValue;
        }
        else
        {
          ifEnd = IntervalField.SECOND;
          sValue = sValue + sCOLON + sSeconds;
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
  /** parse an interval literal.
   * See https://www.postgresql.org/docs/11/datatype-datetime.html#DATATYPE-INTERVAL-INPUT
   * @param sIntervalLiteral interval literal.
   * @return interval value.
   * @throws ParseException if the input is not a valid interval literal.
   */
  public static Interval parseInterval(String sIntervalLiteral)
      throws ParseException
  {
    Interval interval = null;
    DecimalFormat df = new DecimalFormat();
    df.setParseBigDecimal(true);
    int iYears = 0;
    int iMonths = 0;
    int iDays = 0;
    long lNanos = 0;
    int iSign = 1;
    ParsePosition pp = new ParsePosition(0);
    for (int iIndex = 0; iIndex < sIntervalLiteral.length(); )
    {
      iIndex = skipWhite(sIntervalLiteral,iIndex);
      // get number
      pp.setIndex(iIndex);
      BigDecimal bdValue = (BigDecimal)df.parse(sIntervalLiteral,pp);
      if (sIntervalLiteral.charAt(pp.getIndex()) != ':')
      {
        iIndex = pp.getIndex();
        int iValue = bdValue.intValue();
        iIndex = skipWhite(sIntervalLiteral,iIndex);
        String sTruncated = sIntervalLiteral.substring(iIndex);
        String sType = null;
        int iType = 0;
        for (; (iType < asTYPE.length) && (sType == null); iType++) 
        {
          if (sTruncated.startsWith(asTYPE[iType]))
            sType = asTYPE[iType];
        }
        switch (iType)
        {
          case 1: iYears = iValue; break;
          case 2: iMonths = iValue; break;
          case 3: iDays = iValue; break;
        }
        iIndex = iIndex + sType.length();
      }
      else
      {
        // iIndex points to [-] HH.mm.ss.SSSSSSSSS
        int iSignNanos = 1;
        if (sIntervalLiteral.charAt(iIndex) == '-')
        {
          iSignNanos = -1;
          iIndex = iIndex + 1;
        }
        LocalTime lt = LocalTime.parse(sIntervalLiteral.substring(iIndex));
        lNanos = getNanos(lt)*iSignNanos;
        iIndex = sIntervalLiteral.length();
      }
    }
    
    if ((iYears != 0) ||(iMonths != 0))
    {
      while ((iYears > 0) && (iMonths < 0))
      {
        iYears = iYears - 1;
        iMonths = iMonths + iMONTHS_PER_YEAR;
      }
      while ((iYears < 0) && (iMonths > 0))
      {
        iYears = iYears + 1;
        iMonths = iMonths - iMONTHS_PER_YEAR;
      }
      if ((iYears < 0) || (iMonths < 0))
      {
        iSign = -1;
        iYears = -iYears;
        iMonths = -iMonths;
      }
      interval = new Interval(iSign, iYears, iMonths);
    }
    else
    {
      while ((iDays > 0) && (lNanos < 0))
      {
        iDays = iDays - 1;
        lNanos = lNanos + lNANOS_PER_DAY; 
      }
      while ((iDays < 0) && (lNanos > 0))
      {
        iDays = iDays + 1;
        lNanos = lNanos - lNANOS_PER_DAY; 
      }
      if ((iDays < 0) || (lNanos < 0))
      {
        iSign = -1;
        iDays = -iDays;
        lNanos = -lNanos;
      }
      LocalTime lt = getLocalTime(lNanos);
      interval = new Interval(iSign, iDays, lt.getHour(), lt.getMinute(), lt.getSecond(), lt.getNano());
    }
    return interval;
  } /* parseInterval */
  
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
