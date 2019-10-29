/*======================================================================
PostgresDatabaseMetaData implements wrapped Postgres DatabaseMetaData.
Application : SIARD2
Description : PostgresDatabaseMetaData implements wrapped Postgres DatabaseMetaData.
Platform    : Java 8-10   
------------------------------------------------------------------------
Copyright  : 2019, Swiss Federal Archives, Berne, Switzerland
License    : CDDL 1.0
Created    : 25.07.2019, Hartwig Thomas, Enter AG, Rüti ZH, Switzerland
======================================================================*/
package ch.admin.bar.siard2.jdbc;

import java.sql.*;
import java.util.Iterator;

import ch.admin.bar.siard2.postgres.*;
import ch.enterag.sqlparser.datatype.enums.*;
import ch.enterag.utils.jdbc.*;
import ch.enterag.utils.logging.*;

/*====================================================================*/
/** PostgresDatabaseMetaData implements wrapped Postgres DatabaseMetaData.
 * @author Hartwig Thomas
 */
public class PostgresDatabaseMetaData
  extends BaseDatabaseMetaData
  implements DatabaseMetaData
{
  public static final String sRANGE_START = "range_start";
  public static final String sRANGE_END = "range_end";
  public static final String sRANGE_SIGNATURE = "range_signature";
  
  /** logger */
  private static IndentLogger _il = IndentLogger.getIndentLogger(PostgresDatabaseMetaData.class.getName());
  private static int iMAX_VAR_SIZE = 10*1024*1024;
  Connection _conn = null;
  
  /*------------------------------------------------------------------*/
  /** constructor
   * @param dmdWrapped database meta data to be wrapped.
   */
  public PostgresDatabaseMetaData(DatabaseMetaData dmdWrapped, Connection conn)
  {
    super(dmdWrapped);
    _conn = conn;
  } /* constructor */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public Connection getConnection() throws SQLException
  {
    return _conn;
  } /* getConnection */
  
  /*------------------------------------------------------------------*/
  /** {@inheritDoc}
   * Use PostgresMetaColumn for data type translation.
   */
  @Override
  public ResultSet getColumns(String catalog, String schemaPattern,
    String tableNamePattern, String columnNamePattern)
    throws SQLException
  {
    DatabaseMetaData dmd = unwrap(DatabaseMetaData.class);
    ResultSet rs = dmd.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
    PostgresStatement stmt = new PostgresStatement(rs.getStatement(),_conn);
    return new PostgresMetaColumns(new PostgresResultSet(rs,stmt),_conn,1,2,5,6,7,16,9,10);
  } /* getColumns */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public ResultSet getProcedureColumns(String catalog,
    String schemaPattern, String procedureNamePattern,
    String columnNamePattern) throws SQLException
  {
    DatabaseMetaData dmd = unwrap(DatabaseMetaData.class);
    ResultSet rs = dmd.getProcedureColumns(catalog, schemaPattern, procedureNamePattern, columnNamePattern);
    PostgresStatement stmt = new PostgresStatement(rs.getStatement(),_conn);
    return new PostgresMetaColumns(new PostgresResultSet(rs,stmt),_conn,1,2,6,7,8,9,10,-1);
  } /* getProcedureColumns */

  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public ResultSet getFunctionColumns(String catalog,
    String schemaPattern, String functionNamePattern,
    String columnNamePattern) throws SQLException
  {
    DatabaseMetaData dmd = unwrap(DatabaseMetaData.class);
    ResultSet rs = dmd.getFunctionColumns(catalog, schemaPattern, functionNamePattern, columnNamePattern);
    PostgresStatement stmt = new PostgresStatement(rs.getStatement(),_conn);
    return new PostgresMetaColumns(new PostgresResultSet(rs,stmt),_conn,1,2,6,7,8,9,10,-1);
  } /* getFunctionColumns */
  
  /*------------------------------------------------------------------*/
  /** return CASE statement evaluating predefined type of expression.
   * @param sTypename expression.
   * @return predefined type
   */
  private String getCasePredefinedType(String sTypeName)
  {
    StringBuilder sb = new StringBuilder();
    sb.append("\r\n    CASE ");
    sb.append(sTypeName);
    for (int iType = 0; iType < PostgresType.values().length; iType++)
    {
      PostgresType pgt = PostgresType.values()[iType];
      PreType pt = pgt.getPreType();
      String sKeyword = pgt.getKeyword();
      sb.append("\r\n      WHEN ");
      sb.append(PostgresLiterals.formatStringLiteral(sKeyword));
      sb.append(" THEN ");
      sb.append(String.valueOf(pt.getSqlType()));
      for (Iterator<String> iterAliases = pgt.getAliases().iterator(); iterAliases.hasNext(); )
      {
        String sAlias = iterAliases.next();
        sb.append("\r\n      WHEN ");
        sb.append(PostgresLiterals.formatStringLiteral(sAlias));
        sb.append(" THEN ");
        sb.append(String.valueOf(pt.getSqlType()));
      }
    }
    sb.append("\r\n      ELSE ");
    sb.append(String.valueOf(Types.NULL));
    sb.append("\r\n    END");
    return sb.toString();
  } /* getCasePredefinedType */
  
  /*------------------------------------------------------------------*/
  /** return the base type of domains, enums and ranges.
   * @return base type.
   */
  private String getCaseBaseType(String sPgType, String sPgBaseType)
  {
    StringBuilder sb = new StringBuilder();
    sb.append("\r\n  CASE ");
    sb.append(sPgType);
    sb.append(".typtype");
    sb.append("\r\n   WHEN ");
    sb.append(PostgresLiterals.formatStringLiteral("e"));
    sb.append(" THEN ");
    sb.append(String.valueOf(Types.VARCHAR));
    sb.append("\r\n   WHEN ");
    sb.append(PostgresLiterals.formatStringLiteral("d"));
    sb.append(" THEN ");
    sb.append(getCasePredefinedType(sPgBaseType+".typname"));
    sb.append("\r\n  ELSE ");
    sb.append(String.valueOf(Types.NULL));
    sb.append("\r\n  END");
    return sb.toString();
  } /* getCaseBaseType */
  
  /*------------------------------------------------------------------*/
  private String getCaseDataType(String sDataType)
  {
    StringBuilder sbDataTypeCase = new StringBuilder("  CASE ");
    sbDataTypeCase.append(sDataType);
    sbDataTypeCase.append("\r\n");
    for (int i = 0; i < PostgresType.values().length; i++)
    {
      PostgresType pgt = PostgresType.values()[i];
      sbDataTypeCase.append("    WHEN ");
      sbDataTypeCase.append(PostgresLiterals.formatStringLiteral(pgt.getKeyword()));
      sbDataTypeCase.append(" THEN ");
      sbDataTypeCase.append(String.valueOf(pgt.getPreType().getSqlType()));
      sbDataTypeCase.append("\r\n");
      for (String sAlias : pgt.getAliases())
      {
        sbDataTypeCase.append("    WHEN ");
        sbDataTypeCase.append(PostgresLiterals.formatStringLiteral(sAlias));
        sbDataTypeCase.append(" THEN ");
        sbDataTypeCase.append(String.valueOf(pgt.getPreType().getSqlType()));
        sbDataTypeCase.append("\r\n");
      }
    }
    sbDataTypeCase.append("    ELSE ");
    sbDataTypeCase.append(String.valueOf(Types.STRUCT));
    sbDataTypeCase.append("\r\n");
    sbDataTypeCase.append("END");
    return sbDataTypeCase.toString();
  } /* getCaseDataType */
  
  /*------------------------------------------------------------------*/
  /** return class name for UDT.
   * (Could be improved by basing it on the base type for DISTINCT ...)
   * @return
   */
  private String getCaseClassName(String sTypExpression)
  {
    StringBuilder sb = new StringBuilder();
    sb.append("\r\n  CASE ");
    sb.append(sTypExpression);
    sb.append(".typcategory");
    sb.append("\r\n    WHEN ");
    sb.append(PostgresLiterals.formatStringLiteral("A"));
    sb.append(" THEN ");
    sb.append(PostgresLiterals.formatStringLiteral(java.sql.Array.class.getName()));
    sb.append("\r\n    WHEN ");
    sb.append(PostgresLiterals.formatStringLiteral("B"));
    sb.append(" THEN ");
    sb.append(PostgresLiterals.formatStringLiteral(java.lang.Boolean.class.getName()));
    sb.append("\r\n    WHEN ");
    sb.append(PostgresLiterals.formatStringLiteral("C"));
    sb.append(" THEN ");
    sb.append(PostgresLiterals.formatStringLiteral(java.sql.Struct.class.getName()));
    sb.append("\r\n    WHEN ");
    sb.append(PostgresLiterals.formatStringLiteral("D"));
    sb.append(" THEN ");
    sb.append(PostgresLiterals.formatStringLiteral(java.sql.Timestamp.class.getName()));
    sb.append("\r\n    WHEN ");
    sb.append(PostgresLiterals.formatStringLiteral("E"));
    sb.append(" THEN ");
    sb.append(PostgresLiterals.formatStringLiteral(java.lang.String.class.getName()));
    sb.append("\r\n    WHEN ");
    sb.append(PostgresLiterals.formatStringLiteral("G"));
    sb.append(" THEN ");
    sb.append(PostgresLiterals.formatStringLiteral(java.lang.String.class.getName()));
    sb.append("\r\n    WHEN ");
    sb.append(PostgresLiterals.formatStringLiteral("I"));
    sb.append(" THEN ");
    sb.append(PostgresLiterals.formatStringLiteral((new byte[] {}).getClass().getName()));
    sb.append("\r\n    WHEN ");
    sb.append(PostgresLiterals.formatStringLiteral("N"));
    sb.append(" THEN ");
    sb.append(PostgresLiterals.formatStringLiteral(java.math.BigDecimal.class.getName()));
    sb.append("\r\n    WHEN ");
    sb.append(PostgresLiterals.formatStringLiteral("S"));
    sb.append(" THEN ");
    sb.append(PostgresLiterals.formatStringLiteral(java.lang.String.class.getName()));
    sb.append("\r\n    WHEN ");
    sb.append(PostgresLiterals.formatStringLiteral("T"));
    sb.append(" THEN ");
    sb.append(PostgresLiterals.formatStringLiteral(java.time.Duration.class.getName()));
    sb.append("\r\n    WHEN ");
    sb.append(PostgresLiterals.formatStringLiteral("U"));
    sb.append(" THEN ");
    sb.append(PostgresLiterals.formatStringLiteral(java.sql.Struct.class.getName()));
    sb.append("\r\n    WHEN ");
    sb.append(PostgresLiterals.formatStringLiteral("V"));
    sb.append(" THEN ");
    sb.append(PostgresLiterals.formatStringLiteral(java.lang.String.class.getName()));
    sb.append("\r\n    ELSE ");
    sb.append(PostgresLiterals.formatStringLiteral(java.lang.Object.class.getName()));
    sb.append("\r\n  END");
    return sb.toString();
  } /* getCaseClassName */
  
  /*------------------------------------------------------------------*/
  /** return the data type of the UDT.
   * @return STRUCT or DISTINCT.
   */
  private String getCaseUdtDataType(String sPgType)
  {
    StringBuilder sb = new StringBuilder();
    sb.append("\r\n  CASE ");
    sb.append(sPgType);
    sb.append(".typtype");
    sb.append("\r\n   WHEN ");
    sb.append(PostgresLiterals.formatStringLiteral("c"));
    sb.append(" THEN ");
    sb.append(String.valueOf(Types.STRUCT));
    sb.append("\r\n   WHEN ");
    sb.append(PostgresLiterals.formatStringLiteral("r"));
    sb.append(" THEN ");
    sb.append(String.valueOf(Types.STRUCT));
    sb.append("\r\n  ELSE ");
    sb.append(String.valueOf(Types.DISTINCT));
    sb.append("\r\n  END");
    return sb.toString();
  } /* getCaseUdtDataType */
  
  /*------------------------------------------------------------------*/
  /** return the FROM tables for the UDT query.
   * @return FROM tables.
   */
  private String getUdtFromTables(String sPgType, String sPgNamespaceType,
    String sPgClass, String sPgBaseType, String sPgDescription)
  {
    StringBuilder sb = new StringBuilder();
    sb.append("\r\n  (pg_type ");
    sb.append(sPgType);
    sb.append(" JOIN pg_namespace ");
    sb.append(sPgNamespaceType);
    sb.append(" ON (");
    sb.append(sPgType);
    sb.append(".typnamespace = ");
    sb.append(sPgNamespaceType);
    sb.append(".oid))");
    
    sb.append("\r\n  LEFT JOIN pg_class ");
    sb.append(sPgClass);
    sb.append(" ON (");
    sb.append(sPgType);
    sb.append(".oid = ");
    sb.append(sPgClass);
    sb.append(".reltype)");
    
    sb.append("\r\n  LEFT JOIN pg_type ");
    sb.append(sPgBaseType);
    sb.append(" ON (");
    sb.append(sPgType);
    sb.append(".typbasetype = ");
    sb.append(sPgBaseType);
    sb.append(".oid)");
    
    sb.append("\r\n  LEFT JOIN pg_description ");
    sb.append(sPgDescription);
    sb.append(" ON (");
    sb.append(sPgType);
    sb.append(".oid = ");
    sb.append(sPgDescription);
    sb.append(".objoid)");
    return sb.toString();
  } /* getUdtFromTables */

  /*------------------------------------------------------------------*/
  private String getUdtCondition(String sPgType, String sPgNamespace, String sPgClass,
    String catalog, String schemaPattern, String typeNamePattern, int[] types)
  {
    StringBuilder sb = new StringBuilder();
    sb.append("\r\n  ");
    sb.append(sPgType);
    sb.append(".typisdefined");
    if (types == null)
      types = new int[] {Types.STRUCT, Types.DISTINCT};
    StringBuilder sbTypes = new StringBuilder();
    String sPgTypeType = sPgType + ".typtype"; 
    for (int i = 0; i < types.length; i++)
    {
      String sTypeCondition = null; 
      if (types[i] == Types.DISTINCT)
      {
        sTypeCondition = sPgTypeType+" = 'd' OR "+sPgTypeType+" = 'e'";
      }
      else if (types[i] == Types.STRUCT)
        sTypeCondition = "("+sPgTypeType+" = 'c' AND ("+sPgClass+".relkind = 'c')) OR "+sPgTypeType+" = 'r'";
      else
        sTypeCondition = "FALSE";
      if ((sTypeCondition != null) && (sbTypes.length() > 0))
        sbTypes.append(" OR ");
      sbTypes.append(sTypeCondition);
    }
    if (sbTypes.length() > 0)
    {
      sb.append(" AND\r\n  (");
      sb.append(sbTypes.toString());
      sb.append(")");
    }
    if (catalog != null)
    {
      sb.append(" AND\r\n  ");
      sb.append(PostgresLiterals.formatStringLiteral("postgres"));
      sb.append(" = ");
      sb.append(PostgresLiterals.formatStringLiteral(catalog));
    }
    if (schemaPattern != null)
    {
      sb.append(" AND\r\n  ");
      sb.append(sPgNamespace);
      sb.append(".nspname LIKE ");
      sb.append(PostgresLiterals.formatStringLiteral(schemaPattern));
    }
    if (typeNamePattern != null)
    {
      sb.append(" AND\r\n  ");
      sb.append(sPgType);
      sb.append(".typname LIKE ");
      sb.append(PostgresLiterals.formatStringLiteral(typeNamePattern));
    }
    return sb.toString();
  } /* getUdtCondition */
  
  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public ResultSet getUDTs(String catalog, String schemaPattern,
    String typeNamePattern, int[] types) throws SQLException
  {
    String sPgType = "t";
    String sPgNamespace = "nt";
    String sPgClass = "c";
    String sPgBaseType = "bt";
    String sPgDescription = "d";
    
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT");
    sb.append("\r\n  ");
    sb.append(PostgresLiterals.formatStringLiteral("postgres"));
    sb.append(" AS TYPE_CAT,");
    sb.append("\r\n  ");
    sb.append(sPgNamespace);
    sb.append(".nspname AS TYPE_SCHEM,");
    sb.append("\r\n  ");
    sb.append(sPgType);
    sb.append(".typname AS TYPE_NAME,");
    sb.append(getCaseClassName(sPgType));
    sb.append(" AS CLASS_NAME,");
    sb.append(getCaseUdtDataType(sPgType));
    sb.append(" AS DATA_TYPE,");
    sb.append("\r\n  ");
    sb.append(sPgDescription);
    sb.append(".description AS REMARKS,");
    sb.append(getCaseBaseType(sPgType,sPgBaseType));
    sb.append(" AS BASE_TYPE");
    sb.append("\r\nFROM ");
    sb.append(getUdtFromTables(sPgType,sPgNamespace,sPgClass,sPgBaseType,sPgDescription));
    sb.append("\r\nWHERE");
    sb.append(getUdtCondition(sPgType,sPgNamespace,sPgClass,catalog,schemaPattern,typeNamePattern, types));
    sb.append("\r\nORDER BY 1 ASC, 2 ASC, 3 ASC");
    Statement stmt = getConnection().createStatement().unwrap(Statement.class);
    ResultSet rsUdts = stmt.executeQuery(sb.toString());
    return rsUdts;
  } /* getUDTs */

  /*------------------------------------------------------------------*/
  private String getAttributesFrom(String sPgTypeParent, String sPgTypeNamespace, String sPgClass, 
    String sPgAttribute, String sPgTypeAttribute, String sPgTypeAttributeBase, String sPgDescription,
    String sPgRange, String sPgTypeRange, String sPgTypeRangeBase, String sPgValues )
  {
    StringBuilder sb = new StringBuilder();
    sb.append("(pg_type ");
    sb.append(sPgTypeParent);
    sb.append(" JOIN pg_namespace ");
    sb.append(sPgTypeNamespace);
    sb.append(" ON ");
    sb.append(sPgTypeParent);
    sb.append(".typnamespace = ");
    sb.append(sPgTypeNamespace);
    sb.append(".oid)");
    
    sb.append("\r\n  LEFT JOIN");
    sb.append("\r\n  (pg_class ");
    sb.append(sPgClass);
    sb.append("\r\n    JOIN pg_attribute ");
    sb.append(sPgAttribute);
    sb.append(" ON ");
    sb.append(sPgClass);
    sb.append(".oid = ");
    sb.append(sPgAttribute);
    sb.append(".attrelid");
    sb.append("\r\n    JOIN pg_type ");
    sb.append(sPgTypeAttribute);
    sb.append(" ON ");
    sb.append(sPgAttribute);
    sb.append(".atttypid = ");
    sb.append(sPgTypeAttribute);
    sb.append(".oid");
    sb.append("\r\n    LEFT JOIN pg_type ");
    sb.append(sPgTypeAttributeBase);
    sb.append(" ON ");
    sb.append(sPgTypeAttribute);
    sb.append(".typbasetype = ");
    sb.append(sPgTypeAttributeBase);
    sb.append(".oid");
    sb.append("\r\n    LEFT JOIN pg_description ");
    sb.append(sPgDescription);
    sb.append(" ON (");
    sb.append(sPgClass);
    sb.append(".oid = ");
    sb.append(sPgDescription);
    sb.append(".objoid)");
    
    sb.append("\r\n  ) ON ");
    sb.append(sPgTypeParent);
    sb.append(".oid = ");
    sb.append(sPgClass);
    sb.append(".reltype");
    
    sb.append("\r\n  LEFT JOIN");
    sb.append("\r\n  (pg_range ");
    sb.append(sPgRange);
    sb.append("\r\n    JOIN pg_type ");
    sb.append(sPgTypeRange);
    sb.append(" ON ");
    sb.append(sPgRange);
    sb.append(".rngsubtype = ");
    sb.append(sPgTypeRange);
    sb.append(".oid");
    sb.append("\r\n    LEFT JOIN pg_type ");
    sb.append(sPgTypeRangeBase);
    sb.append(" ON ");
    sb.append(sPgTypeRange);
    sb.append(".typbasetype = ");
    sb.append(sPgTypeRangeBase);
    sb.append("\r\n    .oid");
    sb.append("\r\n    JOIN (VALUES");
    sb.append("\r\n      ('"+sRANGE_START+"',NULL,NULL,1,NULL,NULL),");
    sb.append("\r\n      ('"+sRANGE_END+"',NULL,NULL,2,NULL,NULL),");
    sb.append("\r\n      ('"+sRANGE_SIGNATURE+"','char',2,3,'NO','NULL')");
    sb.append("\r\n   ) ");
    sb.append(sPgValues);
    sb.append(" (attname,typname,typlen,attnum,attnotnull,typbasetypename) ON TRUE");
    sb.append("\r\n   ) ON ");
    sb.append(sPgTypeParent);
    sb.append(".oid = ");
    sb.append(sPgRange);
    sb.append(".rngtypid");
    return sb.toString();
  } /* getAttributesFrom */

  /*------------------------------------------------------------------*/
  private String getAttributesCondition(String sPgTypeParent, String sPgTypeNamespace, 
    String sPgClass, String sPgAttribute, String sPgValues, 
    String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern)
  {
    StringBuilder sb = new StringBuilder();
    sb.append("\r\n  ");
    sb.append(sPgTypeParent);
    sb.append(".typisdefined");
    String sPgTypeType = sPgTypeParent + ".typtype";
    sb.append(" AND ((");
    sb.append(sPgTypeType);
    sb.append(" = 'c' AND ");
    sb.append(sPgClass);
    sb.append(".relkind = 'c') OR (");
    sb.append(sPgTypeType);
    sb.append(" = 'r'))");
    if (catalog != null)
    {
      sb.append(" AND\r\n  ");
      sb.append(PostgresLiterals.formatStringLiteral("postgres"));
      sb.append(" = ");
      sb.append(PostgresLiterals.formatStringLiteral(catalog));
    }
    if (schemaPattern != null)
    {
      sb.append(" AND\r\n  ");
      sb.append(sPgTypeNamespace);
      sb.append(".nspname LIKE ");
      sb.append(PostgresLiterals.formatStringLiteral(schemaPattern));
    }
    if (typeNamePattern != null)
    {
      sb.append(" AND\r\n  ");
      sb.append(sPgTypeParent);
      sb.append(".typname LIKE ");
      sb.append(PostgresLiterals.formatStringLiteral(typeNamePattern));
    }
    if (attributeNamePattern != null)
    {
      sb.append(" AND\r\n  ");
      sb.append(getCaseAttributeName(sPgAttribute,sPgValues));
      sb.append(" LIKE ");
      sb.append(PostgresLiterals.formatStringLiteral(attributeNamePattern));
    }
    return sb.toString();
  } /* getAttributesCondition */
  
  private String getCaseAttributeName(String sPgAttribute, String sPgValues)
  {
    StringBuilder sb = new StringBuilder();
    sb.append("CASE WHEN ");
    sb.append(sPgAttribute);
    sb.append(".attname IS NULL THEN ");
    sb.append(sPgValues);
    sb.append(".attname ELSE ");
    sb.append(sPgAttribute);
    sb.append(".attname END");
    return sb.toString();
  } /* getCaseAttributeName */
  
  /*------------------------------------------------------------------*/
  private String getCaseTypeName(String sPgTypeAttribute, String sPgTypeRange, String sPgValues)
  {
    StringBuilder sb = new StringBuilder();
    sb.append("CASE");
    sb.append("\r\n    WHEN ");
    sb.append(sPgTypeAttribute);
    sb.append(".typname IS NULL THEN ");
    sb.append("\r\n      CASE");
    sb.append("\r\n        WHEN ");
    sb.append(sPgValues);
    sb.append(".typname IS NULL THEN ");
    sb.append(sPgTypeRange);
    sb.append(".typname");
    sb.append("\r\n        ELSE ");
    sb.append(sPgValues);
    sb.append(".typname");
    sb.append("\r\n      END");
    sb.append("\r\n    ELSE ");
    sb.append(sPgTypeAttribute);
    sb.append(".typname");
    sb.append("\r\n  END");
    return sb.toString();
  } /* getCaseTypeName */
  
  private String getCaseAttrSize(String sPgTypeAttribute, String sPgTypeRange, String sPgValues)
  {
    StringBuilder sb = new StringBuilder();
    sb.append("CASE");
    sb.append("\r\n    WHEN ");
    sb.append(sPgTypeRange);
    sb.append(".typlen IS NULL THEN");
    sb.append("\r\n      CASE ");
    sb.append(sPgTypeAttribute);
    sb.append(".typlen");
    sb.append("\r\n        WHEN -1 THEN ");
    sb.append(String.valueOf(iMAX_VAR_SIZE));
    sb.append("\r\n        ELSE ");
    sb.append(sPgTypeAttribute);
    sb.append(".typlen");
    sb.append("\r\n      END");
    sb.append("\r\n    ELSE");
    sb.append("\r\n      CASE");
    sb.append("\r\n        WHEN ");
    sb.append(sPgValues);
    sb.append(".typlen IS NULL THEN");
    sb.append("\r\n          CASE ");
    sb.append(sPgTypeRange);
    sb.append(".typlen");
    sb.append("\r\n            WHEN -1 THEN ");
    sb.append(String.valueOf(iMAX_VAR_SIZE));
    sb.append("\r\n            ELSE ");
    sb.append(sPgTypeRange);
    sb.append(".typlen");
    sb.append("\r\n          END");
    sb.append("\r\n        ELSE ");
    sb.append(sPgValues);
    sb.append(".typlen");
    sb.append("\r\n      END");
    sb.append("\r\n  END");
    return sb.toString();
  } /* getCaseAttrSize */
  
  private String getCaseOrdinalPosition(String sPgAttribute, String sPgValues)
  {
    StringBuilder sb = new StringBuilder();
    sb.append("CASE");
    sb.append("\r\n    WHEN ");
    sb.append(sPgValues);
    sb.append(".attnum IS NULL THEN ");
    sb.append(sPgAttribute);
    sb.append(".attnum");
    sb.append("\r\n    ELSE ");
    sb.append(sPgValues);
    sb.append(".attnum");
    sb.append("\r\n  END");
    return sb.toString();
  } /* getCaseOrdinalPosition */
  
  private String getCaseNullable(String sPgClass, String sPgAttribute, String sPgTypeAttribute, String sPgTypeRange, String sPgValues)
  {
    StringBuilder sb = new StringBuilder();
    sb.append("CASE");
    sb.append("\r\n    WHEN ");
    sb.append(sPgClass);
    sb.append(".oid IS NULL THEN ");
    sb.append("\r\n      CASE");
    sb.append("\r\n        WHEN ");
    sb.append(sPgValues);
    sb.append(".attnotnull IS NULL THEN");
    sb.append("\r\n          CASE ");
    sb.append(sPgTypeAttribute);
    sb.append(".typnotnull");
    sb.append("\r\n            WHEN TRUE THEN ");
    sb.append(String.valueOf(DatabaseMetaData.attributeNoNulls));
    sb.append("\r\n            ELSE ");
    sb.append(String.valueOf(DatabaseMetaData.attributeNullable));
    sb.append("\r\n          END");
    sb.append("\r\n        ELSE");
    sb.append("\r\n          CASE ");
    sb.append(sPgValues);
    sb.append(".attnotnull");
    sb.append("                WHEN 'YES' THEN ");
    sb.append(String.valueOf(DatabaseMetaData.attributeNoNulls));
    sb.append("\r\n            ELSE ");
    sb.append(String.valueOf(DatabaseMetaData.attributeNullable));
    sb.append("\r\n          END");
    sb.append("\r\n      END");
    sb.append("\r\n    ELSE");
    sb.append("\r\n      CASE");
    sb.append("\r\n        WHEN (");
    sb.append(sPgAttribute);
    sb.append(".attnotnull OR ((");
    sb.append(sPgTypeAttribute);
    sb.append(".typtype = 'd') AND ");
    sb.append(sPgTypeAttribute);
    sb.append(".typnotnull)) THEN ");
    sb.append(String.valueOf(DatabaseMetaData.attributeNoNulls));
    sb.append("\r\n        ELSE ");
    sb.append(String.valueOf(DatabaseMetaData.attributeNullable));
    sb.append("\r\n      END");
    sb.append("\r\n  END");
    return sb.toString();    
  } /* getCaseNullable */
  
  private String getCaseIsNullable(String sPgClass, String sPgAttribute, String sPgTypeAttribute, String sPgTypeRange, String sPgValues)
  {
    StringBuilder sb = new StringBuilder();
    sb.append("CASE");
    sb.append("\r\n    WHEN ");
    sb.append(sPgClass);
    sb.append(".oid IS NULL THEN ");
    sb.append("\r\n      CASE");
    sb.append("\r\n        WHEN ");
    sb.append(sPgValues);
    sb.append(".attnotnull IS NULL THEN");
    sb.append("\r\n          CASE ");
    sb.append(sPgTypeAttribute);
    sb.append(".typnotnull");
    sb.append("\r\n            WHEN TRUE THEN 'NO'");
    sb.append("\r\n            ELSE 'YES'");
    sb.append("\r\n          END");
    sb.append("\r\n        ELSE ");
    sb.append(sPgValues);
    sb.append(".attnotnull");
    sb.append("\r\n      END");
    sb.append("\r\n    ELSE");
    sb.append("\r\n      CASE");
    sb.append("\r\n        WHEN (");
    sb.append(sPgAttribute);
    sb.append(".attnotnull OR ((");
    sb.append(sPgTypeAttribute);
    sb.append(".typtype = 'd') AND ");
    sb.append(sPgTypeAttribute);
    sb.append(".typnotnull)) THEN 'NO'");
    sb.append("\r\n        ELSE 'YES'");
    sb.append("\r\n      END");
    sb.append("\r\n  END");
    return sb.toString();    
  } /* getCaseNullable */
  
  private String getCaseSourceTypeName(String sPgClass, String sPgValues,
    String sPgTypeAttributeBase, String sPgTypeRangeBase)
  {
    StringBuilder sb = new StringBuilder();
    sb.append("CASE");
    sb.append("\r\n    WHEN ");
    sb.append(sPgClass);
    sb.append(".oid IS NULL THEN");
    sb.append("\r\n      CASE");
    sb.append("\r\n        WHEN ");
    sb.append(sPgValues);
    sb.append(".typbasetypename IS NULL THEN ");
    sb.append(sPgTypeRangeBase);
    sb.append(".typname");
    sb.append("\r\n        ELSE ");
    sb.append(sPgValues);
    sb.append(".typbasetypename");
    sb.append("\r\n      END");
    sb.append("\r\n    ELSE ");
    sb.append(sPgTypeAttributeBase);
    sb.append(".typname");
    sb.append("\r\n  END");
    return sb.toString();
  } /* getCaseSourceTypeName */
  
  /*------------------------------------------------------------------*/
  /** {@inheritDoc} */
  @Override
  public ResultSet getAttributes(String catalog, String schemaPattern,
    String typeNamePattern, String attributeNamePattern)
    throws SQLException
  {
    _il.enter(catalog,schemaPattern,typeNamePattern,attributeNamePattern);
    String sPgTypeParent = "pt"; 
    String sPgTypeNamespace = "npt";
    String sPgClass = "c"; 
    String sPgAttribute = "a"; 
    String sPgTypeAttribute = "at"; 
    String sPgTypeAttributeBase = "abt";
    String sPgRange = "r"; 
    String sPgTypeRange = "rt"; 
    String sPgTypeRangeBase = "rbt"; 
    String sPgValues = "v";
    String sPgDescription = "d";

    StringBuilder sb = new StringBuilder();
    sb.append("SELECT");
    sb.append("\r\n  current_database() AS TYPE_CAT,");
    sb.append("\r\n  ");
    sb.append(sPgTypeNamespace);
    sb.append(".nspname AS TYPE_SCHEM,");
    sb.append("\r\n  ");
    sb.append(sPgTypeParent);
    sb.append(".typname AS TYPE_NAME,");
    sb.append("\r\n  ");
    sb.append(getCaseAttributeName(sPgAttribute,sPgValues));
    sb.append(" AS ATTR_NAME,");
    sb.append("\r\n  ");
    sb.append(getCaseDataType(getCaseTypeName(sPgTypeAttribute,sPgTypeRange,sPgValues)));
    sb.append(" AS DATA_TYPE,");
    sb.append("\r\n  ");
    sb.append(getCaseTypeName(sPgTypeAttribute,sPgTypeRange,sPgValues));
    sb.append(" AS ATTR_TYPE_NAME,");
    sb.append("\r\n  ");
    sb.append(getCaseAttrSize(sPgTypeAttribute,sPgTypeRange,sPgValues));
    sb.append(" AS ATTR_SIZE,");
    sb.append("\r\n  NULL AS DECIMAL_DIGITS,");
    sb.append("\r\n  10 AS NUM_PREC_RADIX,");
    sb.append("\r\n  ");
    sb.append(getCaseNullable(sPgClass, sPgAttribute, sPgTypeAttribute, sPgTypeRange, sPgValues));
    sb.append(" AS NULLABLE,");
    sb.append("\r\n  ");
    sb.append(sPgDescription);
    sb.append(".description AS REMARKS,");
    sb.append("\r\n  NULL AS ATTR_DEF,");
    sb.append("\r\n  NULL AS SQL_DATA_TYPE,");
    sb.append("\r\n  NULL AS SQL_DATETIME_SUB,");
    sb.append("\r\n  ");
    sb.append(String.valueOf(iMAX_VAR_SIZE));
    sb.append(" AS CHAR_OCTET_LENGTH,");
    sb.append("\r\n  ");
    sb.append(getCaseOrdinalPosition(sPgAttribute, sPgValues));
    sb.append(" AS ORDINAL_POSITION,");
    sb.append("\r\n  ");
    sb.append(getCaseIsNullable(sPgClass, sPgAttribute, sPgTypeAttribute, sPgTypeRange, sPgValues));
    sb.append(" AS IS_NULLABLE,");
    sb.append("  NULL AS SCOPE_CATALOG,");
    sb.append("  NULL AS SCOPE_SCHEMA,");
    sb.append("  NULL AS SCOPE_TABLE,");
    sb.append("\r\n  ");
    sb.append(getCaseDataType(getCaseSourceTypeName(sPgClass, sPgValues, sPgTypeAttributeBase, sPgTypeRangeBase)));
    sb.append(" AS SOURCE_DATA_TYPE");
    sb.append("\r\nFROM\r\n  ");
    sb.append(getAttributesFrom(sPgTypeParent, sPgTypeNamespace, sPgClass,
      sPgAttribute, sPgTypeAttribute, sPgTypeAttributeBase, sPgDescription,
      sPgRange, sPgTypeRange, sPgTypeRangeBase, sPgValues));
    sb.append("\r\nWHERE\r\n  ");
    sb.append(getAttributesCondition(sPgTypeParent, sPgTypeNamespace, sPgClass, sPgAttribute, sPgValues,
      catalog, schemaPattern, typeNamePattern, attributeNamePattern));
    sb.append("\r\nORDER BY 1 ASC, 2 ASC, 3 ASC, 16 ASC");
    
    Statement stmt = getConnection().createStatement().unwrap(Statement.class);
    ResultSet rsAttributes = stmt.executeQuery(sb.toString());
    return rsAttributes;
  } /* getAttributes */

} /* class PostgresDatabaseMetaData */
