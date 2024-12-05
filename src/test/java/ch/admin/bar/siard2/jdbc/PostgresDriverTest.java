package ch.admin.bar.siard2.jdbc;


import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.*;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

public class PostgresDriverTest {
    private static final String INVALID_POSTGRES_URL = "jdbc:oracle:thin:@//localhost:1521/orcl";

    private static Driver driver = null;
    private static Connection connection = null;

    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            "postgres:17-alpine"
    );


    @BeforeAll
    static void beforeAll() throws ClassNotFoundException, SQLException {
        postgres.start();
        Class.forName("ch.admin.bar.siard2.jdbc.PostgresDriver");
        driver = DriverManager.getDriver(postgres.getJdbcUrl());
        connection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    @AfterAll
    public static void tearDown() throws SQLException {
        connection.close();
    }

    @Test
    public void testWrapping() {
        assertAll(() -> {
            assertEquals(PostgresDriver.class, driver.getClass());
            assertEquals(PostgresConnection.class, connection.getClass());
        });

    }

    @Test
    public void testCompliant() {
        assertFalse(driver.jdbcCompliant());
    }

    @Test
    public void testAcceptsURL() throws SQLException {
        assertTrue(driver.acceptsURL(postgres.getJdbcUrl()));
        assertFalse(driver.acceptsURL(INVALID_POSTGRES_URL));
    }

    @Test
    public void testVersion() {
        int iMajorVersion = driver.getMajorVersion();
        int iMinorVersion = driver.getMinorVersion();
        String sVersion = iMajorVersion + "." + iMinorVersion;
        assertEquals("42.7", sVersion);
    }

    @Test
    public void testDriverProperties() throws SQLException {
        DriverPropertyInfo[] apropInfo = driver.getPropertyInfo(postgres.getJdbcUrl(), new Properties());
        for (DriverPropertyInfo dpi : apropInfo) {
            System.out.println(dpi.name + ": " + dpi.value + " (" + dpi.description + ")");
        }
        assertEquals(78, apropInfo.length);
    }

}
