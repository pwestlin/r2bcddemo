package nu.westlin.r2dbcdemo

import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import nu.westlin.r2dbcdemo.UserRepository.CreateResult
import nu.westlin.r2dbcdemo.UserRepository.DeleteResult
import nu.westlin.r2dbcdemo.UserRepository.UpdateResult
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.r2dbc.core.DatabaseClient
import org.springframework.transaction.reactive.TransactionalOperator
import org.springframework.transaction.reactive.executeAndAwait

// TODO petves: databasetest slice
@SpringBootTest
internal class UserRepositoryTest {

    @Autowired
    private lateinit var client: DatabaseClient

    @Autowired
    private lateinit var txOperator: TransactionalOperator

    private lateinit var repository: UserRepository

    private val mimi = User(1, "Mimi")
    private val mickey = User(2, "Mickey")

    @Suppress("unused")
    @BeforeAll
    private fun init() {
        repository = UserRepository(client)
    }

    @Test
    fun `all users`() {
        executeAndRollBack {
            assertThat(repository.all().getOrThrow().toList()).containsExactlyInAnyOrder(mimi, mickey)
        }
    }

    @Test
    fun `one user`() {
        executeAndRollBack {
            assertThat(repository.byId(mimi.id).getOrThrow()).isEqualTo(mimi)
        }
    }

    @Test
    fun `one user that does not exist`() {
        executeAndRollBack {
            assertThat(repository.byId(-1).getOrThrow()).isNull()
        }
    }

    @Test
    fun `create a user`() {
        executeAndRollBack {
            val user = User(3, "Donald")
            assertThat(repository.create(user)).isEqualTo(CreateResult.CREATED)
            assertThat(repository.byId(user.id).getOrThrow()).isEqualTo(user)
        }
    }

    @Test
    fun `create a user that already exist`() {
        executeAndRollBack {
            assertThat(repository.create(mickey)).isEqualTo(CreateResult.ALREADY_EXIST)
        }
    }

    @Test
    fun `update a user`() {
        executeAndRollBack {
            val user = mimi.copy(name = "Mimiiii")
            assertThat(repository.update(user)).isEqualTo(UpdateResult.UPDATED)
            assertThat(repository.byId(user.id).getOrThrow()).isEqualTo(user)
        }
    }

    @Test
    fun `update a user that does not exist`() {
        executeAndRollBack {
            val user = User(-1, "Foo")
            assertThat(repository.update(user)).isEqualTo(UpdateResult.NOT_FOUND)
            assertThat(repository.byId(user.id).getOrThrow()).isNull()
        }
    }

    @Test
    fun `delete a user`() {
        executeAndRollBack {
            assertThat(repository.delete(mimi.id)).isEqualTo(DeleteResult.DELETED)
            assertThat(repository.byId(mimi.id).getOrThrow()).isNull()
        }
    }

    @Test
    fun `delete a user that does not exist`() {
        executeAndRollBack {
            assertThat(repository.delete(-1)).isEqualTo(DeleteResult.NOT_FOUND)
        }
    }

    /**
     * Runs [block] and rolls back the database transaction.
     */
    private fun executeAndRollBack(block: suspend () -> Unit) {
        runBlocking {
            txOperator.executeAndAwait {
                block()
                it.setRollbackOnly()
            }
        }
    }
}