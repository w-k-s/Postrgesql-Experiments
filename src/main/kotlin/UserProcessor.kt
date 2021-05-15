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

    fun processInParallel(batchSize: Int): CompletableFuture<Void> {
        return CompletableFuture.runAsync {
            source.inTransaction<Unit, Exception> { handle ->
                handle.select("SELECT id, user_json FROM source WHERE is_processed = FALSE ORDER BY id FOR UPDATE SKIP LOCKED LIMIT ?", batchSize)
                    .mapToMap()
                    .list()
                    .forEach { map ->
                        val id = map["id"] as Int
                        val user = Json.decodeFromString<User>(map["user_json"] as String)

                        handle.execute(
                            "INSERT INTO destination(first_name, last_name, processed_by) VALUES (?, ?, ?)",
                            user.firstName,
                            user.lastName,
                            name
                        )
                        handle.execute("UPDATE source SET is_processed = TRUE WHERE id = ?", id)
                    }
            }
        }
    }

    fun processWithMasterNode(): CompletableFuture<Void> {
        return CompletableFuture.runAsync {
            source.inTransaction<Unit, Exception> { handle ->
                handle.select("SELECT id,user_json FROM source WHERE is_processed = FALSE ORDER BY id FOR UPDATE")
                    .mapToMap()
                    .forEach { map ->
                        val id = map["id"] as Int
                        val user = Json.decodeFromString<User>(map["user_json"] as String)

                        handle.execute(
                            "INSERT INTO destination(first_name, last_name, processed_by) VALUES (?, ?, ?)",
                            user.firstName,
                            user.lastName,
                            name
                        )
                        handle.execute("UPDATE source SET is_processed = TRUE WHERE id = ?", id)
                    }
            }
        }
    }
}
