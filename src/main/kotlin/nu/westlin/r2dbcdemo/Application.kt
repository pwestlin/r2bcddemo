package nu.westlin.r2dbcdemo

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import io.netty.util.internal.StringUtil
import io.r2dbc.spi.Connection
import io.r2dbc.spi.ConnectionFactories
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.ConnectionFactoryOptions
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.springframework.beans.factory.getBean
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer
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
import org.springframework.http.server.reactive.ServerHttpResponse
import org.springframework.stereotype.Repository
import org.springframework.transaction.reactive.TransactionalOperator
import org.springframework.transaction.reactive.executeAndAwait
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.PutMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Flux


@SpringBootApplication
class Application

fun main(args: Array<String>) {
    val ctx = runApplication<Application>(*args)
    println("ctx.getBean<ObjectMapper>() = ${ctx.getBean<ObjectMapper>()}")
}

data class User(@Id val id: Long, val name: String)

@Repository
class UserRepository(private val client: DatabaseClient, private val txOperator: TransactionalOperator) {

    enum class UpdateResult {
        NOT_FOUND, UPDATED
    }

    enum class DeleteResult {
        NOT_FOUND, DELETED
    }

    fun all(): Flow<User> {
        return client.select().from("User").asType<User>().fetch().flow()
    }

    suspend fun byId(id: Long): User? {
        return client.execute("SELECT * FROM User WHERE id = :id").bind("id", id).asType<User>().fetch().awaitOneOrNull()
    }

    suspend fun add(user: User) {
        // Or should this rather be at the "service level"?
        txOperator.executeAndAwait { client.insert().into<User>().table("User").using(user).await() }
    }

    suspend fun update(user: User): UpdateResult {
        // Or should this rather be at the "service level"?
        val noRows = txOperator.executeAndAwait {
            client.update().table<User>().using(user).fetch().rowsUpdated().awaitFirstOrNull()
        }
        return when (noRows) {
            1 -> UpdateResult.UPDATED
            0 -> UpdateResult.NOT_FOUND
            else -> throw RuntimeException("update for user $user affected $noRows when exactly 1 row was expected")
        }
    }

    suspend fun delete(id: Long): DeleteResult {
        // Or should this rather be at the "service level"?
        val noRows = txOperator.executeAndAwait {
            client.delete().from<User>().matching(where("id").`is`(id)).fetch().rowsUpdated().awaitFirstOrNull()
        }
        return when (noRows) {
            1 -> DeleteResult.DELETED
            0 -> DeleteResult.NOT_FOUND
            else -> throw RuntimeException("delete for user $id affected $noRows when exactly 1 row was expected")
        }
    }
}

@RestController
@RequestMapping("/users")
class UserController(private val userRepository: UserRepository) {

    @GetMapping("")
    fun all(): Flow<User> = userRepository.all()

    @GetMapping("/{id}")
    suspend fun byId(@PathVariable("id") id: Long): User? = userRepository.byId(id)

    @PostMapping("")
    suspend fun add(@RequestBody user: User) = userRepository.add(user)

    @PutMapping("")
    suspend fun update(@RequestBody user: User, response: ServerHttpResponse) {
        when (userRepository.update(user)) {
            UserRepository.UpdateResult.UPDATED -> response.statusCode = HttpStatus.NO_CONTENT
            UserRepository.UpdateResult.NOT_FOUND -> response.statusCode = HttpStatus.NOT_FOUND
        }
    }

    @DeleteMapping("/{id}")
    suspend fun delete(@PathVariable("id") id: Long, response: ServerHttpResponse) {
        when (userRepository.delete(id)) {
            UserRepository.DeleteResult.DELETED -> response.statusCode = HttpStatus.OK
            UserRepository.DeleteResult.NOT_FOUND -> response.statusCode = HttpStatus.NOT_FOUND
        }
    }

}

@Configuration
class WebConfiguration {
    @Bean
    fun jackson2ObjectMapperBuilderCustomizer(): Jackson2ObjectMapperBuilderCustomizer {
        return Jackson2ObjectMapperBuilderCustomizer {
            it.featuresToDisable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
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
