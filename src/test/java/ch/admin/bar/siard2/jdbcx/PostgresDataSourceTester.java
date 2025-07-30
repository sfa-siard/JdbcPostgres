package ch.admin.bar.siard2.jdbcx;

import ch.admin.bar.siard2.jdbc.PostgresConnection;
import org.junit.*;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class PostgresDataSourceTester {

    @Rule
    public PostgreSQLContainer db = new PostgreSQLContainer("postgres:latest");

    private PostgresDataSource datasource = null;
    private Connection connection = null;

    @Before
    public void setUp() {
        datasource = new PostgresDataSource();
    } /* setUp */

    @After
    public void tearDown() throws SQLException {
        if ((connection != null) && (!connection.isClosed())) connection.close();
    }

    @Test
    public void testWrapper() throws SQLException {
        Assert.assertSame("Invalid wrapper!", true, datasource.isWrapperFor(DataSource.class));
        DataSource dsWrapped = datasource.unwrap(DataSource.class);
        assertSame("Invalid wrapped class!", org.postgresql.ds.PGSimpleDataSource.class, dsWrapped.getClass());
    }

    @Test
    public void testGetConnection() throws SQLException {
        datasource.setUrl(db.getJdbcUrl());
        datasource.setUser(db.getUsername());
        datasource.setPassword(db.getPassword());
        connection = datasource.getConnection();
        if (connection.unwrap(Connection.class) instanceof PostgresConnection) fail("Double wrap!");
    }

}
