# flyway-database-duckdb

do migrate after jpa auto-ddl (update)

```java

@Bean
FlywayMigrationInitializer flywayInitializer(Flyway flyway) {
    return new FlywayMigrationInitializer(flyway, (f) -> {
    });
}

@Bean
@DependsOn("entityManagerFactory")
public CommandLineRunner delayedFlywayInitializer(Flyway flyway, FlywayProperties flywayProperties) {
    if (flywayProperties.isEnabled())
        flyway.migrate();
    return args -> {
    };
}

```
