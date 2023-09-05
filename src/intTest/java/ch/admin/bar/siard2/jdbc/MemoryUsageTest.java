package ch.admin.bar.siard2.jdbc;

import ch.admin.bar.siard2.jdbcx.PostgresDataSource;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.testcontainers.utility.MountableFile.forClasspathResource;

/* this test covers the following issues:
- https://github.com/sfa-siard/SiardGui/issues/60
- https://github.com/sfa-siard/SiardGui/issues/61
- https://github.com/users/sfa-siard/projects/1/views/1?pane=issue&itemId=31404142
*/
public class MemoryUsageTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    // set up a database that is about 5GB in size
    @Rule
    public PostgreSQLContainer postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:9.6.12"))
            .withCopyFileToContainer(forClasspathResource("postgres/duplicate-rows.sql"), "/tmp/duplicate-rows.sql")
            .withInitScript("postgres/memory-usage.sql");

    @Test
    public void shouldExportDatabaseWithLowMemoryFootPrint() throws IOException, InterruptedException, SQLException, ClassNotFoundException {
        // given
        postgres.execInContainer("psql", "-d", postgres.getDatabaseName(), "-U", postgres.getUsername(), "-w", "-f", "/tmp/duplicate-rows.sql");

        PostgresDataSource dataSource = new PostgresDataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        PostgresConnection connection = (PostgresConnection) dataSource.getConnection();
        Statement statement = connection.createStatement();

        PostgresStatement postgresStatement = new PostgresStatement(statement, connection);

        // when
        ResultSet resultSet = postgresStatement.executeQuery("select * from testdata");

        // then
        assertTrue(resultSet.next());
    }
}
