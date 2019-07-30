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

import ch.enterag.sqlparser.datatype.enums.*;

/*====================================================================*/
/** PostgresType enum lists all native predefined Postgres data types.
 * See also: https://www.postgresql.org/docs/11/datatype.html
 * @author Hartwig Thomas
 */
public enum PostgresType
{
  BIGINT("int8", PreType.BIGINT),
  BIGSERIAL("serial8", PreType.BIGINT),
  BIT("bit", PreType.BINARY),
  VARBIT("varbit", PreType.VARBINARY),
  BOOLEAN("bool", PreType.BOOLEAN),
  BOX("box", PreType.VARCHAR), // WKT
  BYTEA("bytea", PreType.BINARY),
  CHAR("char", PreType.CHAR),
  VARCHAR("varchar", PreType.VARCHAR),
  CIDR("cidr", PreType.VARCHAR), // https://www.postgresql.org/docs/11/datatype-net-types.html#DATATYPE-CIDR
  CIRCLE("circle", PreType.VARCHAR), // WKT
  DATE("date", PreType.DATE),
  DECIMAL("decimal", PreType.DECIMAL),
  DOUBLE("float8", PreType.DOUBLE),
  INET("inet", PreType.VARBINARY), // length 4 (IPv4) or 16 (IPv6)
  INTEGER("int4", PreType.INTEGER),
  INTERVAL("interval", PreType.INTERVAL),
  JSON("json", PreType.VARCHAR),
  JSONB("jsonb", PreType.VARCHAR),
  LINE("line", PreType.VARCHAR), // WKT
  LSEG("lseg", PreType.VARCHAR), // WKT
  MACADDR("macaddr", PreType.BINARY), // length 6
  MACADDR8("macaddr8", PreType.BINARY), // length 8
  MONEY("money", PreType.DECIMAL),
  NAME("name", PreType.VARCHAR), // length 63
  NUMERIC("numeric", PreType.DECIMAL),
  OID("oid", PreType.BLOB), // https://www.postgresql.org/docs/current/lo-interfaces.html
  PATH("path", PreType.VARCHAR), // WKT
  POINT("point", PreType.VARCHAR), // WKT
  POLYGON("polygon", PreType.VARCHAR), // WKT
  REAL("float4", PreType.REAL),
  SMALLINT("int2", PreType.SMALLINT),
  SMALLSERIAL("serial2", PreType.SMALLINT),
  SERIAL("serial", PreType.INTEGER),
  TEXT("text", PreType.CLOB),
  TIME("time", PreType.TIME),
  TIMETZ("timetz", PreType.TIME),
  TIMESTAMP("timestamp", PreType.TIMESTAMP),
  TIMESTAMPTZ("timestamptz", PreType.TIMESTAMP),
  TSQUERY("tsquery", PreType.VARCHAR), // https://www.postgresql.org/docs/11/datatype-textsearch.html
  TSVECTOR("tsvector", PreType.VARCHAR), // // https://www.postgresql.org/docs/11/datatype-textsearch.html
  TXID("txid_snapshot", PreType.VARCHAR), // https://www.postgresql.org/docs/9.4/functions-info.html#FUNCTIONS-TXID-SNAPSHOT-PARTS
  UUID("uuid", PreType.BINARY), // length 16
  XML("xml", PreType.XML);
  
  private String _sKeyword = null;
  public String getKeyword() { return _sKeyword; }
  private PreType _pt = null;
  public PreType getPreType() { return _pt; }
  private PostgresType(String sKeyword, PreType pt)
  {
    _sKeyword = sKeyword;
    _pt = pt;
  } /* constructor */

} /* enum PostgresType */
