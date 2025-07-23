package ch.admin.bar.siard2;

/**
 * Represents a database column, identified by its schema name,
 * table name, and column name.
 */
public class ColumnIdentifier {
    private String schemaName;
    private String tableName;
    private String columnName;

    public ColumnIdentifier(String schemaName, String tableName, String columnName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columnName = columnName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }
}
