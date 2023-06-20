package batch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.transaction.PlatformTransactionManager;

@SpringBootApplication
public class BatchProcessingApplication {
    public static void main(String[] args) {
        SpringApplication.run(BatchProcessingApplication.class, args);
    }


    @Bean
    Tasklet tasklet() {
        return (contribution, context) -> {
            System.out.println("Hello, Spring Batch!");
            return RepeatStatus.FINISHED;  // the tasklet has an output variable or a (state) that tells us whether to continue trying the state or whether it's done.
        };
    }

    @Bean
    Job job(JobRepository jobRepository, Step step) {
        return new JobBuilder("job", jobRepository)
                .start(step)    // Job has a step, Step is being defined as a separate bean
                .build();
    }

    @Bean
    Step step1(JobRepository jobRepository, Tasklet tasklet, PlatformTransactionManager transactionManager) {
        return new StepBuilder("step1", jobRepository).tasklet(tasklet, transactionManager).build();  // step can have a tasklet which is basically is just a callback like runnable.
    }
}

