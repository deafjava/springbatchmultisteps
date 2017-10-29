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
    public FlatFileItemReader<BibleSource> readerASV() {
        FlatFileItemReader<BibleSource> reader = buildReader("t_asv.csv");
        return reader;
    }

    @Bean
    public FlatFileItemReader<BibleSource> readerBBE() {
        FlatFileItemReader<BibleSource> reader = buildReader("t_bbe.csv");
        return reader;
    }

    @Bean
    public FlatFileItemReader<BibleSource> readerDBY() {
        FlatFileItemReader<BibleSource> reader = buildReader("t_dby.csv");
        return reader;
    }


    @Bean
    public FlatFileItemReader<BibleSource> readerWBT() {
        FlatFileItemReader<BibleSource> reader = buildReader("t_wbt.csv");
        return reader;
    }


    @Bean
    public FlatFileItemReader<BibleSource> readerWEB() {
        FlatFileItemReader<BibleSource> reader = buildReader("t_web.csv");
        return reader;
    }


    @Bean
    public FlatFileItemReader<BibleSource> readerYLT() {
        FlatFileItemReader<BibleSource> reader = buildReader("t_ylt.csv");
        return reader;
    }

    @Bean
    public FlatFileItemReader<BibleSource> readerKJV() {
        FlatFileItemReader<BibleSource> reader = buildReader("t_kjv.csv");
        return reader;
    }


    private FlatFileItemReader<BibleSource> buildReader(String file) {
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
    public BibleItemProcessor processorASV() {
        return new BibleItemProcessor("asv");
    }

    @Bean
    public BibleItemProcessor processorBBE() {
        return new BibleItemProcessor("bbe");
    }

    @Bean
    public BibleItemProcessor processorKJV() {
        return new BibleItemProcessor("kjv");
    }

    @Bean
    public BibleItemProcessor processorWBT() {
        return new BibleItemProcessor("wbt");
    }

    @Bean
    public BibleItemProcessor processorWEB() {
        return new BibleItemProcessor("web");
    }

    @Bean
    public BibleItemProcessor processorYLT() {
        return new BibleItemProcessor("ylt");
    }

    @Bean
    public BibleItemProcessor processorDBY() {
        return new BibleItemProcessor("dby");
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
                .reader(readerASV())
                .processor(processorASV())
                .writer(writer())
                .build();
    }

    private Step stepBBE() {
        return stepBuilderFactory.get("stepBBE")
                .<BibleSource, Bible>chunk(10)
                .reader(readerBBE())
                .processor(processorBBE())
                .writer(writer())
                .build();
    }

    private Step stepDBY() {
        return stepBuilderFactory.get("stepDBY")
                .<BibleSource, Bible>chunk(10)
                .reader(readerDBY())
                .processor(processorDBY())
                .writer(writer())
                .build();
    }

    private Step stepKJV() {
        return stepBuilderFactory.get("stepKJV")
                .<BibleSource, Bible>chunk(10)
                .reader(readerKJV())
                .processor(processorKJV())
                .writer(writer())
                .build();
    }

    private Step stepWBT() {
        return stepBuilderFactory.get("stepWBT")
                .<BibleSource, Bible>chunk(10)
                .reader(readerWBT())
                .processor(processorWBT())
                .writer(writer())
                .build();
    }

    private Step stepWEB() {
        return stepBuilderFactory.get("stepWEB")
                .<BibleSource, Bible>chunk(10)
                .reader(readerWEB())
                .processor(processorWEB())
                .writer(writer())
                .build();
    }

    private Step stepYLT() {
        return stepBuilderFactory.get("stepYLT")
                .<BibleSource, Bible>chunk(10)
                .reader(readerYLT())
                .processor(processorYLT())
                .writer(writer())
                .build();
    }
}
