package rewrite.bible.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import rewrite.bible.dto.Bible;
import rewrite.bible.dto.BibleSource;
import rewrite.bible.processor.BibleItemASVProcessor;

import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    public DataSource dataSource;

    @Bean
    public FlatFileItemReader<BibleSource> reader() {
        FlatFileItemReader<BibleSource> reader = new FlatFileItemReader<>();

        reader.setResource(new ClassPathResource("t_asv.csv"));
        reader.setLineMapper(new DefaultLineMapper<BibleSource>() {{
            setLineTokenizer(new DelimitedLineTokenizer() {{
                setNames(new String[]{"personalizedId", "book", "chapter", "verse", "citation"});
            }});
            setFieldSetMapper(new BeanWrapperFieldSetMapper<BibleSource>() {{
                setTargetType(BibleSource.class);
            }});
        }});
        return reader;
    }

    @Bean
    public BibleItemASVProcessor processor() {
        return new BibleItemASVProcessor();
    }

    @Bean
    public JdbcBatchItemWriter<Bible> writer() {
        JdbcBatchItemWriter<Bible> writer = new JdbcBatchItemWriter<>();
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
        writer.setSql("INSERT INTO bible (book, chapter, verse, citation, version) VALUES(:book, :chapter, :verse, :citation, :version)");
        writer.setDataSource(dataSource);
        return writer;
    }

    @Bean
    public Job importBibleJob(JobCompletionNotificationListener listener) {
        return jobBuilderFactory
                .get("importBibleJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(step1())
                .end()
                .build();
    }

    private Step step1() {
        return stepBuilderFactory.get("step1")
                .<BibleSource, Bible>chunk(10)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .build();
    }
}
