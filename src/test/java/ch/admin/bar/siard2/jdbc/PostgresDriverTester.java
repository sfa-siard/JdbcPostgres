package ch.admin.bar.siard2.jdbc;

import java.sql.*;
import java.util.*;

import static org.junit.Assert.*;

import org.junit.*;
import org.testcontainers.containers.PostgreSQLContainer;

public class PostgresDriverTester {

    @Rule
    public PostgreSQLContainer db = new PostgreSQLContainer("postgres:latest");

    private static final String DRIVER_CLASS_NAME = "ch.admin.bar.siard2.jdbc.PostgresDriver";
    private static final String TEST_PG_URL = "jdbc:postgresql://localhost";
    private static final String TEST_PG_INVALID_URL = "jdbc:oracle:thin:@//localhost:1521/orcl";
    ;

    private Driver driver;
    private Connection connection;

    @Before
    public void setUp() throws ClassNotFoundException, SQLException {
        Class.forName(DRIVER_CLASS_NAME);
        driver = DriverManager.getDriver(TEST_PG_URL);
        connection = DriverManager.getConnection(db.getJdbcUrl(), db.getUsername(), db.getPassword());
    }

    @After
    public void tearDown() throws SQLException {
        if ((connection != null) && (!connection.isClosed())) connection.close();
    }

    @Test
    public void testWrapping() {
        assertSame("Registration of driver wrapper failed!", PostgresDriver.class, driver.getClass());
        assertSame("Choice of connection wrapper failed!", PostgresConnection.class, connection.getClass());
    }

    @Test
    public void testCompliant() {
        assertSame("Postgres driver is suddenly JDBC compliant!", false, driver.jdbcCompliant());
    }

    @Test
    public void testAcceptsURL() throws SQLException {
        assertSame("Valid Postgres URL not accepted!", true, driver.acceptsURL(db.getJdbcUrl()));
        assertSame("Invalid Postgres URL accepted!", false, driver.acceptsURL(TEST_PG_INVALID_URL));
    }

    @Test
    public void testVersion() {
        int iMajorVersion = driver.getMajorVersion();
        int iMinorVersion = driver.getMinorVersion();
        String sVersion = String.valueOf(iMajorVersion) + "." + String.valueOf(iMinorVersion);
        assertEquals("Wrong Postgres version " + sVersion + " found!", "42.2", sVersion);
    }

    @Test
    public void testDriverProperties() throws SQLException {
        DriverPropertyInfo[] apropInfo = driver.getPropertyInfo(db.getJdbcUrl(), new Properties());
        for (DriverPropertyInfo dpi : apropInfo)
            System.out.println(dpi.name + ": " + dpi.value + " (" + String.valueOf(dpi.description) + ")");
        assertSame("Unexpected driver properties!", 58, apropInfo.length);
    }
}
