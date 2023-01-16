package com.springboot.batch.demo.config;

import com.springboot.batch.demo.dto.Teacher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.ItemReadListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Configuration
@RequiredArgsConstructor
@EnableBatchProcessing
public class ExampleJobConfig {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    private final String RESULT_COMPLETED = "COMPLETED";
    private final String RESULT_FAIL = "FAIL";
    private final String RESULT_UNKNOWN = "UNKNOWN";

    @Bean
    public Job ExampleJob() {
        Job exampleJob = jobBuilderFactory.get("exampleJob")
                .start(startStep())
                .next(nextStep())
                .next(lastStep())
                .build();

        return exampleJob;
    }

    @Bean
    public Job ExampleJob2() {
        Job exampleJob2 = jobBuilderFactory.get("exampleJob2")
                .start(startStep2())
                    .on("FAILED")
                    .to(failOverStep())
                    .on("*")
                    .to(writeStep())
                    .on("*")
                    .end()

                .from(startStep2())
                    .on("COMPLETED")
                    .to(processStep())
                    .on("*")
                    .to(writeStep())
                    .on("*")
                    .end()

                .from(startStep2())
                    .on("*")
                    .to(writeStep())
                    .on("*")
                    .end()
                .end()
                .build();

        return exampleJob2;
    }

    @Bean
    public Job ExampleJob3() {
        return jobBuilderFactory.get("exampleJob3")
                .start(processorWriterStep())
                .build();
    }

    @Bean
    public Step processorWriterStep() {
        return stepBuilderFactory.get("processorWriterStep")
                .<Teacher, Teacher>chunk(100)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .faultTolerant()
                .retryLimit(3)
                .retry(RuntimeException.class)
                .build();
    }

    @Bean
    public ItemProcessor<Teacher, Teacher> processor() {
        return teacher -> teacher;
    }

    @Bean
    public ItemReader<Teacher> reader() {
        List<Teacher> teachers = new ArrayList<>();
        teachers.add(new Teacher("name1"));
        teachers.add(new Teacher("name2"));
        teachers.add(new Teacher("name3"));
        teachers.add(new Teacher("name4"));

        return new ListItemReader<>(teachers);
    }

    private ItemWriter<Teacher> writer() {
        return teachers ->
        {
            for (Teacher teacher : teachers) {
                log.info("Teacher name = {}", teacher.toString());
            }
        };
    }

    @Bean
    public Step startStep() {
        return stepBuilderFactory
                .get("startStep")
                .tasklet((contribution, chunkContext) -> {
                    log.info("start step!");
                    return RepeatStatus.FINISHED;
                })
                .build();
    }

    @Bean
    public Step nextStep() {
        return stepBuilderFactory.get("nextStep")
                .tasklet((contribution, chunkContext) -> {
                    log.info("next step!");
                    return RepeatStatus.FINISHED;
                })
                .build();
    }

    @Bean
    public Step lastStep() {
        return stepBuilderFactory.get("lastStep")
                .tasklet((contribution, chunkContext) -> {
                    log.info("last step!");
                    return RepeatStatus.FINISHED;
                })
                .build();
    }

    @Bean
    @JobScope
    public Step startStep2() {
        return stepBuilderFactory.get("startStep2")
                .startLimit(3)
                .tasklet((contribution, chunkContext) -> {
                    log.info("start step2!");

                    String result = RESULT_COMPLETED;

                    if (result.equals("COMPLETED")) contribution.setExitStatus(ExitStatus.COMPLETED);
                    if (result.equals("FAIL")) contribution.setExitStatus(ExitStatus.FAILED);
                    if (result.equals("UNKNOWN")) contribution.setExitStatus(ExitStatus.UNKNOWN);

                    return RepeatStatus.FINISHED;
                })
                .build();
    }

    @Bean
    public Step failOverStep() {
        return stepBuilderFactory.get("nextStep")
                .tasklet((contribution, chunkContext) -> {
                    log.info("FAILOVER step!");
                    return RepeatStatus.FINISHED;
                })
                .build();
    }

    @Bean
    public Step processStep() {
        return stepBuilderFactory.get("processStep")
                .tasklet((contribution, chunkContext) -> {
                    log.info("PROCESS step!");
                    return RepeatStatus.FINISHED;
                })
                .build();
    }

    @Bean
    public Step writeStep() {
        return stepBuilderFactory.get("writeStep")
                .tasklet((contribution, chunkContext) -> {
                    log.info("WRITE step!");
                    return RepeatStatus.FINISHED;
                })
                .build();
    }
}
