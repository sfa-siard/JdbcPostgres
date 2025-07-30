package ch.admin.bar.siard2.jdbc;

import ch.admin.bar.siard2.jdbcx.PostgresDataSource;
import ch.admin.bar.siard2.postgres.TestPostgresDatabase;
import ch.admin.bar.siard2.postgres.TestSqlDatabase;
import ch.enterag.sqlparser.identifier.QualifiedId;
import ch.enterag.utils.EU;
import ch.enterag.utils.base.TestColumnDefinition;
import ch.enterag.utils.jdbc.BaseResultSetMetaDataTester;
import org.junit.*;
import org.testcontainers.containers.PostgreSQLContainer;

import java.io.IOException;
import java.sql.*;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Ignore
public class PostgresResultSetMetaDataTester extends BaseResultSetMetaDataTester {

    @ClassRule
    public static PostgreSQLContainer db = new PostgreSQLContainer("postgres:latest").withUsername("postgres")
                                                                                     .withPassword("postgres")
                                                                                     .withPassword("postgres");

    private static String getTableQuery(QualifiedId qiTable, List<TestColumnDefinition> listCd) {
        StringBuilder sbSql = new StringBuilder("SELECT\r\n  ");
        for (int iColumn = 0; iColumn < listCd.size(); iColumn++) {
            if (iColumn > 0)
                sbSql.append(",\r\n  ");
            TestColumnDefinition tcd = listCd.get(iColumn);
            sbSql.append(tcd.getName());
        }
        sbSql.append("\r\nFROM ");
        sbSql.append(qiTable.format());
        return sbSql.toString();
    } /* getTableQuery */

    private static String _sNativeQuerySimple = getTableQuery(TestPostgresDatabase.getQualifiedSimpleTable(), TestPostgresDatabase._listCdSimple);
    private static String _sNativeQueryComplex = getTableQuery(TestPostgresDatabase.getQualifiedComplexTable(), TestPostgresDatabase._listCdComplex);
    private static String _sSqlQuerySimple = getTableQuery(TestSqlDatabase.getQualifiedSimpleTable(), TestSqlDatabase._listCdSimple);
    private static String _sSqlQueryComplex = getTableQuery(TestSqlDatabase.getQualifiedComplexTable(), TestSqlDatabase._listCdComplex);

    @BeforeClass
    public static void setUpClass() throws SQLException, IOException {
        PostgresDataSource dsPostgres = new PostgresDataSource();
        dsPostgres.setUrl(db.getJdbcUrl());
        dsPostgres.setUser("postgres");
        dsPostgres.setPassword("postgres");
        PostgresConnection connPostgres = (PostgresConnection) dsPostgres.getConnection();
        /* drop and create the test databases */
        new TestSqlDatabase(connPostgres, db.getUsername());
        TestPostgresDatabase.grantSchemaUser(connPostgres, TestSqlDatabase._sTEST_SCHEMA, db.getUsername());
        new TestPostgresDatabase(connPostgres, db.getUsername());
        TestPostgresDatabase.grantSchemaUser(connPostgres, TestPostgresDatabase._sTEST_SCHEMA, db.getUsername());
        connPostgres.close();
    }

    private Connection closeResultSet()
            throws SQLException {
        Connection conn = null;
        ResultSet rs = getResultSet();
        if (rs != null) {
            if (!rs.isClosed()) {
                Statement stmt = rs.getStatement();
                rs.close();
                setResultSetMetaData(null, null);
                if (!stmt.isClosed()) {
                    conn = stmt.getConnection();
                    stmt.close();
                }
            }
        }
        return conn;
    } /* closeResultSet */

    private void openResultSet(Connection conn, String sQuery)
            throws SQLException {
        closeResultSet();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sQuery);
        ResultSetMetaData rsmd = rs.getMetaData();
        setResultSetMetaData(rsmd, rs);
    } /* openResultSet */

    @Before
    public void setUp() {
        try {
            PostgresDataSource dsPostgres = new PostgresDataSource();
            dsPostgres.setUrl(db.getJdbcUrl());
            dsPostgres.setUser(db.getUsername());
            dsPostgres.setPassword(db.getPassword());
            Connection conn = (PostgresConnection) dsPostgres.getConnection();
            conn.setAutoCommit(false);
            openResultSet(conn, _sNativeQuerySimple);
        } catch (SQLException se) {
            fail(se.getClass()
                   .getName() + ": " + se.getMessage());
        }
    }

    @After
    @Override
    public void tearDown() {
        try {
            Connection conn = closeResultSet();
            if (conn != null) {
                if (!conn.isClosed()) {
                    conn.commit();
                    conn.close();
                }
            }
        } catch (SQLException se) {
            fail(EU.getExceptionMessage(se));
        }
    } /* tearDown */

    @Test
    public void testClass() {
        assertEquals("Wrong result set metadata class!", PostgresResultSetMetaData.class, getResultSetMetaData().getClass());
    } /* testClass */

    @Test
    public void testNativeSimple() {
        try {
            openResultSet(getResultSet().getStatement()
                                        .getConnection(), _sNativeQuerySimple);
            super.testAll();
        } catch (SQLException se) {
            fail(EU.getExceptionMessage(se));
        }
    } /* testNativeSimple */

    @Test
    public void testNativeComplex() {
        try {
            openResultSet(getResultSet().getStatement()
                                        .getConnection(), _sNativeQueryComplex);
            super.testAll();
        } catch (SQLException se) {
            fail(EU.getExceptionMessage(se));
        }
    } /* testNativeComplex */

    @Test
    public void testSqlSimple() {
        try {
            openResultSet(getResultSet().getStatement()
                                        .getConnection(), _sSqlQuerySimple);
            super.testAll();
        } catch (SQLException se) {
            fail(EU.getExceptionMessage(se));
        }
    } /* testSqlSimple */

    @Test
    public void testSqlComplex() {
        try {
            openResultSet(getResultSet().getStatement()
                                        .getConnection(), _sSqlQueryComplex);
            super.testAll();
        } catch (SQLException se) {
            fail(EU.getExceptionMessage(se));
        }
    } /* testSqlComplex */

}
