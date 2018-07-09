package rewrite.bible.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import rewrite.bible.dto.Bible;
import rewrite.bible.dto.BibleBodruk;
import rewrite.bible.dto.BibleSource;
import rewrite.bible.processor.BibleBodrukItemProcessor;

import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private DataSource originDataSource;

    @Autowired
    private DataSource destinyDataSource;

//    @Bean
//    public FlatFileItemReader<BibleSource> readerASV() {
//        FlatFileItemReader<BibleSource> reader = buildReader("t_asv.csv");
//        return reader;
//    }

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
                        "FROM verses WHERE version = 'ara'" +
                        "ORDER BY id ASC";

        databaseReader.setDataSource(dataSource);
        databaseReader.setSql(QUERY_READ_BIBLE);
        databaseReader.setRowMapper(new BeanPropertyRowMapper<>(BibleBodruk.class));

        return databaseReader;
    }

//    private FlatFileItemReader<BibleSource> buildReader(String file) {
//        FlatFileItemReader<BibleSource> reader = new FlatFileItemReader<>();
//
//
//        reader.setResource(new ClassPathResource(file));
//        reader.setLineMapper(new DefaultLineMapper<BibleSource>() {{
//            setLineTokenizer(new DelimitedLineTokenizer() {{
//                setNames(new String[]{"personalizedId", "book", "chapter", "verse", "citation"});
//            }});
//            setFieldSetMapper(new BeanWrapperFieldSetMapper<BibleSource>() {{
//                setTargetType(BibleSource.class);
//            }});
//        }});
//        return reader;
//    }

    @Bean
    public BibleBodrukItemProcessor processor() {
        return new BibleBodrukItemProcessor("ari");
    }

    @Bean
    public JdbcBatchItemWriter<Bible> writer() {
        JdbcBatchItemWriter<Bible> writer = new JdbcBatchItemWriter<>();
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
        writer.setSql("INSERT INTO bible (book, chapter, verse, citation, version) VALUES(:book, :chapter, :verse, :citation, :version)");
        writer.setDataSource(destinyDataSource);
        return writer;
    }

    @Bean
    public Job importBibleJob(JobCompletionNotificationListener listener) {
        return jobBuilderFactory
                .get("importBibleJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .start(step())
                .build();
    }


    private Step step() {
        return stepBuilderFactory.get("step")
                .<BibleBodruk, Bible>chunk(10)
                .reader(dbItemReader(originDataSource))
                .processor(processor())
                .writer(writer())
                .build();
    }
//    private Step stepYLT() {
//        return stepBuilderFactory.get("stepYLT")
//                .<BibleSource, Bible>chunk(10)
//                .reader(readerYLT())
//                .processor(processorYLT())
//                .writer(writer())
//                .build();
//    }

}
