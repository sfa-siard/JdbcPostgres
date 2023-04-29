package ch.admin.bar.siard2.jdbc;


import ch.admin.bar.siard2.jdbcx.PostgresDataSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.SQLException;

public class PostgresConnectionTest {

    @Test
    public void shouldCreateConnection() throws SQLException {
        PostgreSQLContainer postgres = new PostgreSQLContainer(PostgreSQLContainer.IMAGE);
        postgres.start();

        PostgresDataSource dataSource = new PostgresDataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());

        Assertions.assertNotNull(dataSource.getConnection());
        Assertions.assertEquals(dataSource.getConnection().getClass(), PostgresConnection.class);
    }
}