package rewrite.bible.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.FormatterLineAggregator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import rewrite.bible.dto.Bible;
import rewrite.bible.dto.BibleBodruk;
import rewrite.bible.processor.BibleBodrukItemProcessor;

import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    @Bean
    public ItemReader<BibleBodruk> dbItemReader(DataSource dataSource) {
        JdbcCursorItemReader<BibleBodruk> databaseReader = new JdbcCursorItemReader<>();

        String QUERY_READ_BIBLE =
                "SELECT " +
                        "id, " +
                        "book, " +
                        "chapter, " +
                        "verse, " +
                        "text, " +
                        "testament, " +
                        "version " +
                        "FROM verses WHERE version = 'ari'" +
                        "ORDER BY id ASC";

        databaseReader.setDataSource(dataSource);
        databaseReader.setSql(QUERY_READ_BIBLE);
        databaseReader.setRowMapper(new BeanPropertyRowMapper<>(BibleBodruk.class));

        return databaseReader;
    }

    @Bean
    public ItemWriter<Bible> writer() {
        FlatFileItemWriter<Bible> writer = new FlatFileItemWriter<>();

        writer.setResource(new FileSystemResource("~/ari.sql"));
        FormatterLineAggregator<Bible> delLineAgg = new FormatterLineAggregator<>();
        delLineAgg.setMaximumLength(2048);
        delLineAgg.setFormat("INSERT INTO `bible` (`book`, `chapter`, `verse`, `citation`, `version_id`) VALUES(%d, %d, %d, '%s', %d);");
        BeanWrapperFieldExtractor<Bible> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[] {"book", "chapter", "verse", "citation", "version"});
        delLineAgg.setFieldExtractor(fieldExtractor);
        writer.setLineAggregator(delLineAgg);
        return writer;
    }

    @Bean
    public ItemProcessor<BibleBodruk, Bible> processor() {
        return new BibleBodrukItemProcessor(9);
    }

    @Bean
    public Job importBibleJob(JobBuilderFactory jobs, Step step) {
        return jobs
                .get("importBibleJob")
                .flow(step)
                .end()
                .build();

    }

    @Bean
    public Step step(StepBuilderFactory stepBuilderFactory, ItemReader<BibleBodruk> reader,
                     ItemWriter<Bible> writer, ItemProcessor<BibleBodruk, Bible> processor) {
        return stepBuilderFactory.get("step")
                .<BibleBodruk, Bible> chunk(100)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

}
