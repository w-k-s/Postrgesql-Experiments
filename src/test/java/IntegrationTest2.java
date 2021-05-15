import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;


@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class IntegrationTest2 {

    private DataSource dataSource;

    @Container
    public JdbcDatabaseContainer postgreSQLContainer = new PostgreSQLContainer("postgres:13")
            .withDatabaseName("integration-tests-db")
            .withUsername("sa")
            .withPassword("sa")
            .withInitScript("init.sql");

    @BeforeEach
    public void setUp() {
        postgreSQLContainer
                .withExposedPorts(5433)
                .start();

        final var props = new Properties();
        props.setProperty("dataSourceClassName", "org.postgresql.ds.PGSimpleDataSource");
        props.setProperty("dataSource.user", postgreSQLContainer.getUsername());
        props.setProperty("dataSource.password", postgreSQLContainer.getPassword());
        props.setProperty("dataSource.url", postgreSQLContainer.getJdbcUrl());

        HikariConfig config = new HikariConfig(props);
        dataSource = new HikariDataSource(config);
    }

    @AfterEach
    public void afterEach() {
        Jdbi.create(dataSource).withHandle(handle -> handle.execute("DELETE FROM destination"));
    }

    @Test
    public void GIVEN_2ProcessorsAreProcessingUsers_WHEN_parallelStrategyIsUsed_THEN_workIsDividedEvenlyBetweenTwoProcessors_AND_rowsCanBeUpdatedAfterCommit() throws ExecutionException, InterruptedException {
        CompletableFuture.allOf(
                new UserProcessor("processor-1", dataSource).processInParallel(10),
                new UserProcessor("processor-2", dataSource).processInParallel(10)
        ).get();

        assertThat(getWorkDoneByProcessorNamed("processor-1")).isEqualTo(10);
        assertThat(getWorkDoneByProcessorNamed("processor-2")).isEqualTo(10);

        var updatedRows = Jdbi.create(dataSource)
                .withHandle(handle -> handle.execute("UPDATE source SET is_processed = FALSE"));
        assertThat(updatedRows).isEqualTo(20);
    }

    @Test
    public void GIVEN_2ProcessorsAreProcessingUsers_WHEN_masterNodeStrategyIsUsed_THEN_oneProcessorDoesAllTheWork_AND_rowsCanBeUpdatedAfterCommit() throws ExecutionException, InterruptedException {
        CompletableFuture.allOf(
                new UserProcessor("processor-1", dataSource).processWithMasterNode(),
                new UserProcessor("processor-2", dataSource).processWithMasterNode()
        ).get();

        var processor1Work = getWorkDoneByProcessorNamed("processor-1");
        var processor2Work = getWorkDoneByProcessorNamed("processor-2");

        assertThat(processor1Work).isNotEqualTo(processor2Work);
        assertThat(processor1Work).satisfiesAnyOf(
                count -> assertThat(count).isEqualTo(0),
                count -> assertThat(count).isEqualTo(20)
        );
        assertThat(processor2Work).satisfiesAnyOf(
                count -> assertThat(count).isEqualTo(0),
                count -> assertThat(count).isEqualTo(20)
        );

        var updatedRows = Jdbi.create(dataSource)
                .withHandle(handle -> handle.execute("UPDATE source SET is_processed = FALSE"));
        assertThat(updatedRows).isEqualTo(20);
    }

    private int getWorkDoneByProcessorNamed(String name) {
        return Jdbi.create(dataSource).withHandle(handle ->
                handle.select("SELECT COUNT(*) FROM destination WHERE processed_by = ?", name)
                        .mapTo(Integer.class)
                        .one()
        );
    }
}
