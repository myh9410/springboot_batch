package com.springboot.batch.demo.config;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
@EnableBatchProcessing
public class ExampleJobConfig {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;


    @Bean
    public Job ExampleJob() {
        Job exampleJob = jobBuilderFactory.get("exampleJob")
                .start(step())
                .build();

        return exampleJob;
    }

    @Bean
    public Step step() {
        return stepBuilderFactory
                .get("step")
                .tasklet((contribution, chunkContext) -> {
                    System.out.println("step");
                    return RepeatStatus.FINISHED;
                })
                .build();
    }

}
