package nu.westlin.r2dbcdemo

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.netty.util.internal.StringUtil
import io.r2dbc.spi.Connection
import io.r2dbc.spi.ConnectionFactories
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.ConnectionFactoryOptions
import kotlinx.coroutines.flow.Flow
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.r2dbc.core.DatabaseClient
import org.springframework.data.r2dbc.core.asType
import org.springframework.data.r2dbc.core.await
import org.springframework.data.r2dbc.core.awaitOneOrNull
import org.springframework.data.r2dbc.core.flow
import org.springframework.data.r2dbc.core.into
import org.springframework.stereotype.Repository
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Flux


@SpringBootApplication
class R2dbcdemoApplication

fun main(args: Array<String>) {
    runApplication<R2dbcdemoApplication>(*args)
}

data class User(val id: Long, val name: String)

@Repository
class UserRepository(private val client: DatabaseClient) {
    fun all(): Flow<User> {
        return client.select().from("User").asType<User>().fetch().flow()
    }

    suspend fun byId(id: Long): User? {
        return client.execute("SELECT * FROM User WHERE id = :id").bind("id", id).asType<User>().fetch().awaitOneOrNull()
    }

    suspend fun add(user: User) {
        client.insert().into<User>().table("User").using(user).await()
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

}

@Configuration
class WebConfiguration {
    @Bean
    fun objectMapper() = jacksonObjectMapper()
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

    @Bean
    fun initDatabase2(cf: ConnectionFactory) = CommandLineRunner {
        Flux.from<Connection>(cf.create())
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
