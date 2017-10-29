package rewrite.bible.config;

import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;

@Configuration
public class DatabaseConfiguration {

    @ConfigurationProperties(prefix = "spring.datasource")
    @Bean
    @Primary
    public DataSource dataSource() {
        return DataSourceBuilder.create().build();
    }
}
