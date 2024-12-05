package ch.admin.bar.siard2.jdbc;

import java.sql.*;


import ch.enterag.utils.jdbc.*;
import ch.admin.bar.siard2.jdbcx.*;
import lombok.SneakyThrows;
import org.junit.*;
import org.postgresql.util.PSQLException;
import org.testcontainers.containers.PostgreSQLContainer;

import static org.junit.Assert.assertEquals;

public class PostgresConnectionTest extends BaseConnectionTester {

    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            "postgres:17-alpine"
    );


    @Before
    public void setUp() throws SQLException {
        postgres.start();
        PostgresDataSource dsPostgres = new PostgresDataSource();
        dsPostgres.setUrl(postgres.getJdbcUrl());
        dsPostgres.setUser(postgres.getUsername());
        dsPostgres.setPassword(postgres.getPassword());
        PostgresConnection connPostgres = (PostgresConnection) dsPostgres.getConnection();
        connPostgres.setAutoCommit(false);
        setConnection(connPostgres);
    }

    @Test
    public void testClass() {
        assertEquals(PostgresConnection.class, getConnection().getClass());
    }

    @SneakyThrows
    @Override
    @Test
    public void testCreateArrayOf() {
        Array array = getConnection().createArrayOf("VARCHAR", new String[]{"a", "bc", "abc"});

        Object[] result = (Object[]) array.getArray();

        assertEquals("a", result[0]);
        assertEquals("bc", result[1]);
        assertEquals("abc", result[2]);
        array.free();
    }

    @SneakyThrows
    @Override
    @Test(expected = PSQLException.class)
    public void testPrepareStatement_String_AInt() {
        /* should really throw feature not supported */
        getConnection().prepareStatement(_sSQL, new int[]{1, 2});
    }
}
