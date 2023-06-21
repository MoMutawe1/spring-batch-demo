package batch;

import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Date;
import java.util.UUID;

@SpringBootApplication
public class BatchProcessingApplication {
    public static void main(String[] args) {
        SpringApplication.run(BatchProcessingApplication.class, args);
    }

    /*
        by default when you start the Java application it runs all the batch jobs but in some scenarios this is not what you want,
        sometimes we need to manually kick off jobs when some events happen in our app,
        in order to do that we can apply spring.batch.job.enabled=false in the application.properties file,

        by doing so I disabled the job run at startup (now after disable this if we run the spring batch app it will not kick up or run the batch job)
        so you will need to run spring batch by yourself by implementing the ApplicationRunner and inject the JobLauncher and a pointer to the Job.
        and this comes handy in some scenarios when you have a long-lived web application that's processes data and whenever data comes
        in you need to kick off a batch job whenever you need to.


        [ Job Launcher ] ———> [ Job ] ———> [ Step ]  ———> [ItemReader] & [ItemProcessor] & [ItemWriter]
             |					|
             |					| --- ———> [ Step ]  ———> [ItemReader] & [ItemProcessor] & [ItemWriter]
             |
	         V
      [ Job Repository ] ———> [ Database ]
     */

    // .addDate: by using date as a parameter, I would have the same parameter no matter how many times I run this on the same day,
    // and that will stop it from re-running multiple times, because you don't want to run the same job again especially it has some sort of side effects.
    @Bean
    ApplicationRunner runner(JobLauncher jobLauncher, Job job) {
        return args ->
        {
            var jobParameters = new JobParametersBuilder()
                //.addString("uuid", UUID.randomUUID().toString()) // this will allow you from running re-running same job multiple times by generate and use unique key each time.
                .addDate("date", new Date())
                .toJobParameters();
            var run = jobLauncher.run(job, jobParameters);
            var instanceId = run.getJobInstance().getInstanceId();
            System.out.println("InstanceId: " + instanceId);
        };
    }

    // Job has a Step, Step has a Tasklet, but the real power of a Step is not the Tasklet, Tasklet used whenever you need some sort of callback just to do something generic.
    @Bean
    @StepScope // this bean get recreated each time there's a new run of the Job, so you may run this job every midnight or as per requirements.
    Tasklet tasklet(@Value("#{jobParameters['date']}") Date date) {
        return (contribution, context) -> {
            System.out.println("Hello, Spring Batch! the date is " + date);
            return RepeatStatus.FINISHED;  // the tasklet has an output variable or a (state) that tells us whether to continue trying the state or whether it's done.
        };
    }

    record CsvRow(int i, String s, String readString, int readInt, String string, String s1, float v, float readFloat,
                  float aFloat, float v1, float readFloat1){}

    private static int parseInt(String text){
        if(text!=null && !text.contains("NA") && !text.contains("N/A")) return Integer.parseInt(text);
        return 0;
    }

    @Bean
    FlatFileItemReader<CsvRow> csvRowFlatFileItemReader (
            @Value("file://${HOME}/Desktop/batch/data/vgsales.csv") Resource resource) {
        return new FlatFileItemReaderBuilder<CsvRow>()
                .resource(resource)
                .name("csvFFIR")
                .delimited().delimiter(",")
                .names("rank,name,platform,year,genre,publisher,na_sales,eu_sales,jp_sales,other_sales,global_sales".split(","))
                .linesToSkip(1)
                .fieldSetMapper(fieldSet -> new CsvRow(
                        fieldSet.readInt("rank"),
                        fieldSet.readString("name"),
                        fieldSet.readString("platform"),
                        parseInt(fieldSet.readString("year")),
                        fieldSet.readString("genre"),
                        fieldSet.readString("publisher"),
                        fieldSet.readFloat("na_sales"),
                        fieldSet.readFloat("eu_sales"),
                        fieldSet.readFloat("jp_sales"),
                        fieldSet.readFloat("other_sales"),
                        fieldSet.readFloat("global_sales")
                )).build();
    }

    // if anything goes wrong spring batch will roll back that one chunk, you can configure its policy like if something go wrong just roll back that 1 chunk
    // but keep going, or if something go wrong anywhere then fail the whole job and mark it as incomplete.
    // for every single write you get a chunk of lines (for every 100 rows you'll get a chunk to process or save, for efficiency so that you can do things in 1 transaction, so you can get 100 record at a time)
    //chunk: how many record you think can handle at a time safely without getting invalid records from your csv file, just to avoid rollback a big chunk of data.
    //reader: read data from the csv file, the contract is you return one line from every single read you return (so here we are reading the data one line at a time).
    @Bean
    Step csvToDB(JobRepository jobRepository, PlatformTransactionManager ptm,
                 FlatFileItemReader <CsvRow> csvRowFlatFileItemReader) throws Exception{

        return new StepBuilder("csvToDB", jobRepository)
                .<CsvRow, CsvRow> chunk(100, ptm)
                .reader(csvRowFlatFileItemReader)
                .writer(chunk -> {
                    var oneHundredRows = chunk.getItems();
                    System.out.println("got " + oneHundredRows.size());  // print the size of each chunk
                    System.out.println(oneHundredRows);     // print all the chunks to the console for testing
                })
                .build();
        }

    @Bean
    Job job(JobRepository jobRepository, Step step, Step csvToDB) {
        return new JobBuilder("job", jobRepository)
                .start(step)    // Job has a step, Step is being defined as a separate bean, this is our first step call
                .start(csvToDB) // second step execution call
                .build();
    }

    /*@Bean
    JdbcTemplate template (DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }*/

    // if you have only one step it will get injected by type, otherwise spring will look for step name (so make sure to match the step name in your Job definition).
    @Bean
    Step step(JobRepository jobRepository, Tasklet tasklet, PlatformTransactionManager transactionManager) {
        return new StepBuilder("step1", jobRepository).tasklet(tasklet, transactionManager).build();  // step can have a tasklet which is basically is just a callback like runnable.
    }
}

