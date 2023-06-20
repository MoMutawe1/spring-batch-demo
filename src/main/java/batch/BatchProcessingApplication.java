package batch;

import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.UUID;

@SpringBootApplication
public class BatchProcessingApplication {
    public static void main(String[] args) {
        SpringApplication.run(BatchProcessingApplication.class, args);
    }

    /*
        by applying spring.batch.job.enabled=false in the application.properties file
        I disabled the job run at startup (now after disable this if we run the app spring batch will not kick up or run the batch job)
        so you will need to run spring batch by yourself by implementing the ApplicationRunner.

        [ Job Launcher ] ———> [ Job ] ———> [ Step ]  ———> [ItemReader] & [ItemProcessor] & [ItemWriter]
             |					|
             |					| --- ———> -m [ Step ]  ———> [ItemReader] & [ItemProcessor] & [ItemWriter]
             |
	         V
      [ Job Repository ] ———> [ Database ]
     */
    @Bean
    ApplicationRunner runner(JobLauncher jobLauncher, Job job) {
        return args ->
        {
            var jobParameters = new JobParametersBuilder()
                .addString("uuid", UUID.randomUUID().toString())
                .toJobParameters();
            var run = jobLauncher.run(job, jobParameters);
            var instanceId = run.getJobInstance().getInstanceId();
            System.out.println("InstanceId: " + instanceId);
        };
    }

    @Bean
    @StepScope // this bean get recreated each time there's a new run of the Job, so you may run this job every midnight or as per requirements.
    Tasklet tasklet(@Value("#{jobParameters['uuid']}") String uuid) {
        return (contribution, context) -> {
            System.out.println("Hello, Spring Batch! your UUID is " + uuid);
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

