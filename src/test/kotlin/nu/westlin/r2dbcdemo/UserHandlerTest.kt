package nu.westlin.r2dbcdemo

import io.mockk.coEvery
import io.mockk.mockk
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.runBlocking
import nu.westlin.r2dbcdemo.UserRepository.CreateResult
import nu.westlin.r2dbcdemo.UserRepository.UpdateResult
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.test.web.reactive.server.WebTestClient
import org.springframework.test.web.reactive.server.expectBody
import org.springframework.test.web.reactive.server.expectBodyList

internal class UserHandlerTest {

    private val userRepository = mockk<UserRepository>()
    private val handler = UserHandler(userRepository)

    private lateinit var client: WebTestClient

    private val user1 = User(1, "User 1")
    private val user2 = User(2, "User 2")
    private val user3 = User(3, "User 3")

    @Suppress("unused")
    @BeforeAll
    private fun init() {
        client = WebTestClient.bindToRouterFunction(WebConfiguration().routes(handler)).build()
    }

    @Test
    fun `all users`() {
        runBlocking {
            // TODO petves: Inline classes (I.E. Result) does not work with MockK... :/
            coEvery { userRepository.all() } returns Result.success(listOf(user1, user2).asFlow())

            client.get().uri("/users").accept(MediaType.APPLICATION_JSON)
                .exchange()
                .expectStatus().isOk
                .expectHeader().contentType(MediaType.APPLICATION_JSON)
                .expectBodyList<User>().contains(user1, user2)
        }
    }

    @Test
    fun `a user by id`() {
        runBlocking {
            val user = user3
            coEvery { userRepository.byId(user.id) } returns Result.success(user3)

            client.get().uri("/users/{id}", user.id).accept(MediaType.APPLICATION_JSON)
                .exchange()
                .expectStatus().isOk
                .expectHeader().contentType(MediaType.APPLICATION_JSON)
                .expectBody<User>().isEqualTo(user)
        }
    }

    @Test
    fun `a user by id that does not exist`() {
        runBlocking {
            val user = user3
            coEvery { userRepository.byId(user.id) } returns Result.success(null)

            client.get().uri("/users/{id}", user.id).accept(MediaType.APPLICATION_JSON)
                .exchange()
                .expectStatus().isNotFound
                .expectHeader().doesNotExist("Accept")
                .expectBody().isEmpty
        }
    }

    @Test
    fun `create a user`() {
        runBlocking {
            val user = User(4, "User 4")
            coEvery { userRepository.create(user) } returns CreateResult.CREATED

            client.post().uri("/users").contentType(MediaType.APPLICATION_JSON).bodyValue(user)
                .accept(MediaType.APPLICATION_JSON)
                .exchange()
                .expectStatus().isCreated
                .expectHeader().doesNotExist("Accept")
                .expectBody().isEmpty
        }
    }

    @Test
    fun `create a user that already exist`() {
        runBlocking {
            val user = User(4, "User 4")
            coEvery { userRepository.create(user) } returns CreateResult.ALREADY_EXIST

            client.post().uri("/users").contentType(MediaType.APPLICATION_JSON).bodyValue(user)
                .accept(MediaType.APPLICATION_JSON)
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.CONFLICT)
                .expectHeader().doesNotExist("Accept")
                .expectBody().isEmpty
        }
    }

    @Test
    fun `update a user`() {
        runBlocking {
            val user = User(4, "User 4")
            coEvery { userRepository.update(user) } returns UpdateResult.UPDATED

            client.put().uri("/users").contentType(MediaType.APPLICATION_JSON).bodyValue(user)
                .accept(MediaType.APPLICATION_JSON)
                .exchange()
                .expectStatus().isNoContent
                .expectHeader().doesNotExist("Accept")
                .expectBody().isEmpty
        }
    }

    @Test
    fun `update a user that does not exist`() {
        runBlocking {
            val user = User(4, "User 4")
            coEvery { userRepository.update(user) } returns UpdateResult.NOT_FOUND

            client.put().uri("/users").contentType(MediaType.APPLICATION_JSON).bodyValue(user)
                .accept(MediaType.APPLICATION_JSON)
                .exchange()
                .expectStatus().isNotFound
                .expectHeader().doesNotExist("Accept")
                .expectBody().isEmpty
        }
    }

    @Test
    fun `delete a user`() {
        runBlocking {
            val user = user3
            coEvery { userRepository.delete(user.id) } returns UserRepository.DeleteResult.DELETED

            client.delete().uri("/users/{id}", user.id).accept(MediaType.APPLICATION_JSON)
                .exchange()
                .expectStatus().isOk
                .expectHeader().doesNotExist("Accept")
                .expectBody().isEmpty
        }
    }

    @Test
    fun `delete a user that does not exist`() {
        runBlocking {
            val user = user3
            coEvery { userRepository.delete(user.id) } returns UserRepository.DeleteResult.NOT_FOUND

            client.delete().uri("/users/{id}", user.id).accept(MediaType.APPLICATION_JSON)
                .exchange()
                .expectStatus().isNotFound
                .expectHeader().doesNotExist("Accept")
                .expectBody().isEmpty
        }
    }
}