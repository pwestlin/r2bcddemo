package nu.westlin.r2dbcdemo

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.awaitBody
import org.springframework.web.reactive.function.client.awaitBodyOrNull
import org.springframework.web.reactive.function.client.awaitExchange
import java.time.Instant

fun main() {
    val client = WebClient.create("http://localhost:8080/users")
    runBlocking(Dispatchers.IO) {
        launch { (3L..5L).forEach { createUser(client, User(it, "Foo$it")) } }
        launch { allUsers(client) }
        (1L..6L).forEach { launch { oneUser(client, it) } }
    }
    runBlocking {
        launch { allUsers(client) }
    }
}

private suspend fun createUser(client: WebClient, user: User) {
    val response = client.post().uri("").contentType(MediaType.APPLICATION_JSON).bodyValue(user).awaitExchange()
    log { "response = ${response.statusCode()}" }
    log { "response = ${response.rawStatusCode()}" }
    check(response.statusCode() == HttpStatus.CREATED) { "Wrong status: ${response.statusCode()}" }
    log { "Created user: $user" }
    log { "headers = ${response.headers().header("Location")}" }
    val location = response.headers().header("Location").first()
    log { "Created user lookup by location header: $location" }
    val createdUser = client.get().uri(location).accept(MediaType.APPLICATION_JSON).awaitExchange().awaitBodyOrNull<User>()
    log { "Created ny location: $createdUser" }
}

private suspend fun allUsers(client: WebClient) {
    val users = client.get().uri("").accept(MediaType.APPLICATION_JSON)
        .awaitExchange().awaitBody<List<User>>()
    log { "users = $users" }
}

private suspend fun oneUser(client: WebClient, id: Long) {
    val user = client.get().uri("/$id").accept(MediaType.APPLICATION_JSON)
        .awaitExchange().awaitBodyOrNull<User>()
    log { "user = $user" }
}

private fun log(f: () -> String) {
    println("${Instant.now()}: ${f()} - ${Thread.currentThread().name}")
}