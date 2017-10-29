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
import rewrite.bible.processor.BibleItemProcessor;

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
    public FlatFileItemReader<BibleSource> reader(String file) {
        FlatFileItemReader<BibleSource> reader = new FlatFileItemReader<>();

        reader.setResource(new ClassPathResource(file));
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
    public BibleItemProcessor processor(String bibleVersion) {
        return new BibleItemProcessor(bibleVersion);
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
                .start(stepASV())
                .next(stepBBE())
                .next(stepDBY())
                .next(stepKJV())
                .next(stepWBT())
                .next(stepWEB())
                .next(stepYLT())
                .build();
    }


    private Step stepASV() {
        return stepBuilderFactory.get("stepASV")
                .<BibleSource, Bible>chunk(10)
                .reader(reader("t_asv.csv"))
                .processor(processor("asv"))
                .writer(writer())
                .build();
    }

    private Step stepBBE() {
        return stepBuilderFactory.get("stepBBE")
                .<BibleSource, Bible>chunk(10)
                .reader(reader("t_bbe.csv"))
                .processor(processor("bbe"))
                .writer(writer())
                .build();
    }

    private Step stepDBY() {
        return stepBuilderFactory.get("stepDBY")
                .<BibleSource, Bible>chunk(10)
                .reader(reader("t_dby.csv"))
                .processor(processor("dby"))
                .writer(writer())
                .build();
    }

    private Step stepKJV() {
        return stepBuilderFactory.get("stepKJV")
                .<BibleSource, Bible>chunk(10)
                .reader(reader("t_kjv.csv"))
                .processor(processor("kjv"))
                .writer(writer())
                .build();
    }

    private Step stepWBT() {
        return stepBuilderFactory.get("stepWBT")
                .<BibleSource, Bible>chunk(10)
                .reader(reader("t_wbt.csv"))
                .processor(processor("wbt"))
                .writer(writer())
                .build();
    }

    private Step stepWEB() {
        return stepBuilderFactory.get("stepWEB")
                .<BibleSource, Bible>chunk(10)
                .reader(reader("t_web.csv"))
                .processor(processor("web"))
                .writer(writer())
                .build();
    }

    private Step stepYLT() {
        return stepBuilderFactory.get("stepYLT")
                .<BibleSource, Bible>chunk(10)
                .reader(reader("t_ytl.csv"))
                .processor(processor("ytl"))
                .writer(writer())
                .build();
    }
}
