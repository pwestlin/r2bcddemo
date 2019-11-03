package nu.westlin.r2dbcdemo

import io.netty.util.internal.StringUtil
import io.r2dbc.spi.Connection
import io.r2dbc.spi.ConnectionFactories
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.ConnectionFactoryOptions
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.annotation.Id
import org.springframework.data.r2dbc.core.DatabaseClient
import org.springframework.data.r2dbc.core.asType
import org.springframework.data.r2dbc.core.await
import org.springframework.data.r2dbc.core.awaitOneOrNull
import org.springframework.data.r2dbc.core.flow
import org.springframework.data.r2dbc.core.from
import org.springframework.data.r2dbc.core.into
import org.springframework.data.r2dbc.core.table
import org.springframework.data.r2dbc.query.Criteria.where
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType.APPLICATION_JSON
import org.springframework.stereotype.Component
import org.springframework.stereotype.Repository
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.awaitBody
import org.springframework.web.reactive.function.server.bodyAndAwait
import org.springframework.web.reactive.function.server.bodyValueAndAwait
import org.springframework.web.reactive.function.server.buildAndAwait
import org.springframework.web.reactive.function.server.coRouter
import reactor.core.publisher.Flux
import java.net.URI


@SpringBootApplication
class Application

fun main(args: Array<String>) {
    runApplication<Application>(*args)
}

data class User(@Id val id: Long, val name: String)

@Repository
class UserRepository(private val client: DatabaseClient) {

    enum class UpdateResult {
        NOT_FOUND, UPDATED
    }

    enum class DeleteResult {
        NOT_FOUND, DELETED
    }

    enum class CreateResult {
        CREATED, ALREADY_EXIST
    }

    fun all(): Flow<User> {
        return client.select().from("User").asType<User>().fetch().flow()
    }

    suspend fun byId(id: Long): User? {
        return client.execute("SELECT * FROM User WHERE id = :id").bind("id", id).asType<User>().fetch().awaitOneOrNull()
    }

    suspend fun create(user: User): CreateResult {
        // Or should this rather be at the "service level"?
        return when (byId(user.id)) {
            null -> {
                client.insert().into<User>().table("User").using(user).await()
                CreateResult.CREATED
            }
            else -> CreateResult.ALREADY_EXIST
        }
    }

    suspend fun update(user: User): UpdateResult {
        // Or should this rather be at the "service level"?
        return when (val noRows = client.update().table<User>().using(user).fetch().rowsUpdated().awaitFirstOrNull()) {
            1 -> UpdateResult.UPDATED
            0 -> UpdateResult.NOT_FOUND
            else -> throw RuntimeException("update for user $user affected $noRows when exactly 1 row was expected")
        }
    }

    suspend fun delete(id: Long): DeleteResult {
        // Or should this rather be at the "service level"?
        return when (val noRows = client.delete().from<User>().matching(where("id").`is`(id)).fetch().rowsUpdated().awaitFirstOrNull()) {
            1 -> DeleteResult.DELETED
            0 -> DeleteResult.NOT_FOUND
            else -> throw RuntimeException("delete for user $id affected $noRows when exactly 1 row was expected")
        }
    }
}

@Configuration
class WebConfiguration {

    @Bean
    fun routes(userHandler: UserHandler) = coRouter {
        accept(APPLICATION_JSON).nest {
            "/users".nest {
                GET("", userHandler::all)
                GET("/{id}", userHandler::byId)
                POST("", userHandler::create)
                PUT("", userHandler::update)
                DELETE("/{id}", userHandler::delete)
            }
        }
    }
}

@Component
class UserHandler(private val userRepository: UserRepository) {
    suspend fun all(request: ServerRequest) = ServerResponse.ok().bodyAndAwait(userRepository.all())

    suspend fun byId(request: ServerRequest): ServerResponse {
        return when (val user = userRepository.byId(request.pathVariable("id").toLong())) {
            null -> ServerResponse.notFound().buildAndAwait()
            else -> ServerResponse.ok().bodyValueAndAwait(user)
        }
    }

    suspend fun create(request: ServerRequest): ServerResponse {
        val user = request.awaitBody<User>()
        val uri = URI("${request.uri()}/${user.id}")
        return when (userRepository.create(user)) {
            UserRepository.CreateResult.CREATED -> {
                ServerResponse.created(uri).buildAndAwait()
            }
            UserRepository.CreateResult.ALREADY_EXIST -> ServerResponse.status(HttpStatus.CONFLICT).header("Location", uri.toASCIIString()).buildAndAwait()
        }
    }

    suspend fun update(request: ServerRequest): ServerResponse {
        return when (userRepository.update(request.awaitBody())) {
            UserRepository.UpdateResult.UPDATED -> ServerResponse.noContent().buildAndAwait()
            UserRepository.UpdateResult.NOT_FOUND -> ServerResponse.notFound().buildAndAwait()
        }
    }

    suspend fun delete(request: ServerRequest): ServerResponse {
        return when (userRepository.delete(request.pathVariable("id").toLong())) {
            UserRepository.DeleteResult.DELETED -> ServerResponse.ok().buildAndAwait()
            UserRepository.DeleteResult.NOT_FOUND -> ServerResponse.notFound().buildAndAwait()
        }
    }
}

@Configuration
class DatabaseConfiguration {

    @Bean
    fun connectionFactory(properties: R2DBCConfigurationProperties): ConnectionFactory {
        val baseOptions = ConnectionFactoryOptions.parse(properties.url)
        var ob = ConnectionFactoryOptions.builder().from(baseOptions)
        if (!StringUtil.isNullOrEmpty(properties.user)) {
            ob = ob.option(ConnectionFactoryOptions.USER, properties.user!!)
        }
        if (!StringUtil.isNullOrEmpty(properties.password)) {
            ob = ob.option(ConnectionFactoryOptions.PASSWORD, properties.password!!)
        }
        return ConnectionFactories.get(ob.build())
    }

    @Suppress("SpringJavaInjectionPointsAutowiringInspection")
    @Bean
    fun initDatabase2(connectionFactory: ConnectionFactory) = CommandLineRunner {
        Flux.from<Connection>(connectionFactory.create())
            .flatMap<io.r2dbc.spi.Result> { c ->
                Flux.from<io.r2dbc.spi.Result>(c.createBatch()
                    .add("drop table if exists User")
                    .add("create table User(id number(3) primary key,name varchar(80) unique not null)")
                    .add("insert into User(id, name) values(1, 'Mimi')")
                    .add("insert into User(id, name) values(2, 'Mickey')")
                    .execute())
                    .doFinally { c.close() }
            }
            .log()
            .blockLast()
    }
}

@ConfigurationProperties(prefix = "r2dbc")
@ConstructorBinding
data class R2DBCConfigurationProperties(val url: String, val user: String? = null, val password: String? = null)
