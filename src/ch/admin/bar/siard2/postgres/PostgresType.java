/*======================================================================
PostgresType enum lists all native predefined Postgres data types we 
expect to encounter as types of columns in database tables.
Application : SIARD2
Description : PostgresType enum lists all native predefined Postgres data types.
Platform    : Java 8-10   
------------------------------------------------------------------------
Copyright  : 2019, Swiss Federal Archives, Berne, Switzerland
License    : CDDL 1.0
Created    : 29.07.2017, Hartwig Thomas, Enter AG, Rüti ZH, Switzerland
======================================================================*/
package ch.admin.bar.siard2.postgres;

import java.util.*;
import ch.enterag.sqlparser.datatype.enums.*;

/*====================================================================*/
/** PostgresType enum lists all native predefined Postgres data types.
 * See also: https://www.postgresql.org/docs/11/datatype.html
 * @author Hartwig Thomas
 */
public enum PostgresType
{
  INTEGER("integer", new HashSet<String>(Arrays.asList(new String[]{"int","int4"})), PreType.INTEGER),
  SMALLINT("smallint", new HashSet<String>(Arrays.asList(new String[]{"int2"})), PreType.SMALLINT),
  BIGINT("bigint", new HashSet<String>(Arrays.asList(new String[]{"int8"})), PreType.BIGINT),
  OID("oid", new HashSet<String>(), PreType.INTEGER),
  SERIAL("serial", new HashSet<String>(Arrays.asList(new String[]{"serial4"})), PreType.INTEGER),
  SMALLSERIAL("smallserial", new HashSet<String>(Arrays.asList(new String[]{"serial2"})), PreType.SMALLINT),
  BIGSERIAL("bigserial", new HashSet<String>(Arrays.asList(new String[]{"serial8"})), PreType.BIGINT),
  MONEY("money", new HashSet<String>(), PreType.DECIMAL),
  NUMERIC("numeric", new HashSet<String>(Arrays.asList(new String[]{"decimal"})), PreType.NUMERIC),
  DOUBLE("double precision", new HashSet<String>(Arrays.asList(new String[]{"float8"})), PreType.DOUBLE),
  REAL("real", new HashSet<String>(Arrays.asList(new String[]{"float4"})), PreType.REAL),
  BOOLEAN("boolean", new HashSet<String>(Arrays.asList(new String[]{"bool"})), PreType.BOOLEAN),
  DATE("date", new HashSet<String>(), PreType.DATE),
  TIME("time", new HashSet<String>(), PreType.TIME),
  TIMETZ("time with time zone", new HashSet<String>(Arrays.asList(new String[]{"timetz"})), PreType.TIME),
  TIMESTAMP("timestamp", new HashSet<String>(), PreType.TIMESTAMP),
  TIMESTAMPTZ("timestamp with time zone", new HashSet<String>(Arrays.asList(new String[]{"timestamptz"})), PreType.TIMESTAMP),
  INTERVAL("interval", new HashSet<String>(), PreType.INTERVAL),
  CHAR("character", new HashSet<String>(Arrays.asList(new String[]{"char","bpchar"})), PreType.CHAR),
  VARCHAR("character varying", new HashSet<String>(Arrays.asList(new String[]{"varchar"})), PreType.VARCHAR),
  TEXT("text", new HashSet<String>(), PreType.CLOB),
  JSON("json", new HashSet<String>(), PreType.CLOB),
  JSONB("jsonb", new HashSet<String>(), PreType.CLOB),
  XML("xml", new HashSet<String>(), PreType.XML),
  TSVECTOR("tsvector", new HashSet<String>(), PreType.CLOB), // // https://www.postgresql.org/docs/11/datatype-textsearch.html
  TSQUERY("tsquery", new HashSet<String>(), PreType.CLOB), // https://www.postgresql.org/docs/11/datatype-textsearch.html
  BIT("bit", new HashSet<String>(), PreType.BINARY),
  VARBIT("bit varying", new HashSet<String>(Arrays.asList(new String[]{"varbit"})), PreType.VARBINARY),
  BYTEA("bytea", new HashSet<String>(), PreType.BLOB),
  UUID("uuid", new HashSet<String>(), PreType.BINARY), // length 16
  MACADDR("macaddr", new HashSet<String>(), PreType.BINARY), // length 6
  MACADDR8("macaddr8", new HashSet<String>(), PreType.BINARY), // length 8
  POINT("point", new HashSet<String>(), PreType.VARCHAR),
  LINE("line", new HashSet<String>(), PreType.VARCHAR),
  LSEG("lseg", new HashSet<String>(), PreType.VARCHAR),
  BOX("box", new HashSet<String>(), PreType.VARCHAR),
  PATH("path", new HashSet<String>(), PreType.VARCHAR),
  POLYGON("polygon", new HashSet<String>(), PreType.VARCHAR),
  CIRCLE("circle", new HashSet<String>(), PreType.VARCHAR),
  CIDR("cidr", new HashSet<String>(), PreType.VARCHAR), // https://www.postgresql.org/docs/11/datatype-net-types.html#DATATYPE-CIDR
  INET("inet", new HashSet<String>(), PreType.VARBINARY), // length 4 (IPv4) or 16 (IPv6)
  NAME("name", new HashSet<String>(), PreType.VARCHAR), // length 63
  TXID("txid_snapshot", new HashSet<String>(), PreType.VARCHAR); // https://www.postgresql.org/docs/11/functions-info.html#FUNCTIONS-TXID-SNAPSHOT-PARTS
  
  private String _sKeyword = null;
  public String getKeyword() { return _sKeyword; }
  private Set<String> _setAliases = null;
  public Set<String> getAliases() { return _setAliases; }
  private PreType _pt = null;
  public PreType getPreType() { return _pt; }
  private PostgresType(String sKeyword, Set<String> setAliases, PreType pt)
  {
    _sKeyword = sKeyword;
    _setAliases = setAliases;
    _pt = pt;
  } /* constructor */
  public static PostgresType getByKeyword(String sKeyword)
  {
    PostgresType pgt  = null;
    for (int i = 0; (pgt == null) && (i < PostgresType.values().length); i++)
    {
      PostgresType dt = PostgresType.values()[i];
      if (sKeyword.equals(dt.getKeyword()) || dt.getAliases().contains(sKeyword))
        pgt = dt;
    }
    return pgt;
  } /* getByKeyword */
  public static PostgresType getByPreType(PreType pt)
  {
    PostgresType pgt = null;
    switch(pt)
    {
      case CHAR: pgt = CHAR; break;
      case VARCHAR: pgt = VARCHAR; break;
      case CLOB: pgt = TEXT; break;
      case NCHAR: pgt = CHAR; break;
      case NVARCHAR: pgt = VARCHAR; break;
      case NCLOB: pgt = TEXT; break;
      case XML: pgt = XML; break;
      case BINARY: pgt = BYTEA; break;
      case VARBINARY: pgt = BYTEA; break;
      case BLOB: pgt = BYTEA; break;
      case NUMERIC: pgt = NUMERIC; break;
      case DECIMAL: pgt = NUMERIC; break;
      case SMALLINT: pgt = SMALLINT; break;
      case INTEGER: pgt = INTEGER; break;
      case BIGINT: pgt = BIGINT; break;
      case FLOAT: pgt = DOUBLE; break;
      case REAL: pgt = REAL; break;
      case DOUBLE: pgt = DOUBLE; break;
      case BOOLEAN: pgt = BOOLEAN; break;
      case DATE: pgt = DATE; break;
      case TIME: pgt = TIME; break;
      case TIMESTAMP: pgt = TIMESTAMP; break;
      case INTERVAL: pgt = INTERVAL; break;
      default:
    }
    return pgt;
  } /* getByPreType */
} /* enum PostgresType */
