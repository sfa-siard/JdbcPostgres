package ch.admin.bar.siard2.postgres;

import java.math.*;
import java.text.*;
import static org.junit.Assert.*;
import org.junit.*;
import org.postgresql.util.*;

import ch.enterag.utils.EU;

public class ParserTester
{

  @Test
  public void testSqlComplex()
  {
    try
    {
      // String sTypeName = "\"testsqlschema\".\"typsqlcomplex\"";
      String sStruct = "(1,\"(1,92148,92149)\")";
      PGtokenizer pt = new PGtokenizer(sStruct, ',');
      // as it is a struct, we remove parentheses
      pt.removePara();
      // there is a single token inside double quotes
      pt = pt.tokenizeToken(0,',');
      // remove double quotes from single token
      pt.remove("\"", "\"");
      for (int iToken = 0; iToken < pt.getSize(); iToken++)
      {
        // the first token is an INTEGER
        if (iToken == 0)
        {
          BigDecimal bd = PostgresLiterals.parseExactLiteral(pt.getToken(0));
          System.out.println(String.valueOf(iToken)+": "+String.valueOf(bd));
        }
        // the second token is nested struct
        else if (iToken == 1)
        {
          PGtokenizer ptNested = pt.tokenizeToken(1, ',');
          ptNested.removePara();
          ptNested = ptNested.tokenizeToken(0,',');
          for (int iNested = 0; iNested < ptNested.getSize(); iNested++)
          {
            BigDecimal bd = PostgresLiterals.parseExactLiteral(ptNested.getToken(iNested));
            // they are all three integers, the last two oids referncing LOBs
            System.out.println(String.valueOf("  "+iNested)+": "+String.valueOf(bd));
          }
          System.out.println(String.valueOf(iToken)+": \""+pt.getToken(iToken)+"\"");
        }
      }
    }
    catch(ParseException pe) { fail(EU.getExceptionMessage(pe)); }
  }
}
