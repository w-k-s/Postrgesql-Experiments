import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import org.jdbi.v3.core.Jdbi
import java.util.concurrent.CompletableFuture
import javax.sql.DataSource

class UserProcessor(
    private val name: String,
    source: DataSource,
) {
    private val source = Jdbi.create(source)

    fun process(batchSize: Int) : CompletableFuture<Void>{
        return CompletableFuture.runAsync {
            source.inTransaction<Unit,Exception> { handle ->
                handle.select("SELECT user_json FROM source ORDER BY id FOR UPDATE SKIP LOCKED LIMIT ?", batchSize)
                    .mapTo(String::class.java)
                    .list()
                    .map { Json.decodeFromString<User>(it) }
                    .forEach {
                        source.withHandle<Int, Exception> { handle ->
                            handle.execute(
                                "INSERT INTO destination(first_name, last_name, processed_by) VALUES (?, ?, ?)",
                                it.firstName,
                                it.lastName,
                                name
                            )
                        }
                    }
            }
        }
    }
}