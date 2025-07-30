package ch.admin.bar.siard2.jdbc;

import java.sql.*;

import ch.enterag.utils.*;
import ch.enterag.utils.jdbc.*;
import ch.admin.bar.siard2.jdbcx.*;
import lombok.SneakyThrows;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.postgresql.util.PSQLException;
import org.testcontainers.containers.PostgreSQLContainer;

import static org.junit.Assert.assertEquals;


public class PostgresConnectionTester extends BaseConnectionTester {

    @Rule
    public PostgreSQLContainer PG_CONTAINER = new PostgreSQLContainer("postgres:latest");


    @Before
    public void beforeAll() throws SQLException {
        PostgresDataSource dsPostgres = new PostgresDataSource();
        dsPostgres.setUrl(PG_CONTAINER.getJdbcUrl());
        dsPostgres.setUser(PG_CONTAINER.getUsername());
        dsPostgres.setPassword(PG_CONTAINER.getPassword());

        PostgresConnection connection = (PostgresConnection) dsPostgres.getConnection();
        connection.setAutoCommit(false);
        setConnection(connection);
    }

    @Test
    public void testClass() {
        assertEquals(PostgresConnection.class, getConnection().getClass());
    }

    @SneakyThrows
    @Override
    @Test
    public void testCreateArrayOf() {
        /* For some reason VARCHAR is OK, but VARCHAR(255) is not. */
        Array array = getConnection().createArrayOf("VARCHAR", new String[]{"a", "bc", "abc"});
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
