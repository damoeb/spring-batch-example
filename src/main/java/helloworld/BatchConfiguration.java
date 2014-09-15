package helloworld;

import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.AbstractJob;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.FlowJob;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.core.repository.dao.*;
import org.springframework.batch.core.repository.support.SimpleJobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;

/**
 * A basic spring batch example for SimpleJob and FlowJob
 *
 * @link http://www.jroller.com/0xcafebabe/entry/spring_batch_hello_world_1
 *
 * Created by damoeb on 9/9/14.
 */
@Configuration
public class BatchConfiguration {

    public static final FlowExecutionStatus BLOCKED = new FlowExecutionStatus("BLOCKED");
    public static final FlowExecutionStatus YES = new FlowExecutionStatus("YES");
    public static final FlowExecutionStatus NO = new FlowExecutionStatus("NO");
    public static final FlowExecutionStatus VITAL = new FlowExecutionStatus("VITAL");


    private boolean vital = true;
    private boolean blocked = false;
    private boolean running = false;


    public static void main(String[] args) throws JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, JobParametersInvalidException {
        ApplicationContext context = new AnnotationConfigApplicationContext(BatchConfiguration.class);

        SimpleJobLauncher launcher = context.getBean(SimpleJobLauncher.class);

//        AbstractJob job = context.getBean(SimpleJob.class);
        AbstractJob job = context.getBean(FlowJob.class);
        JobExecution execution = launcher.run(job, new JobParameters());

    }

   @Bean
   public ResourcelessTransactionManager getTransactionManager() {
       return new ResourcelessTransactionManager();
   }

    @Bean
    public SimpleJobLauncher getJobLauncher() {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(getJobRepository());
        return jobLauncher;
    }

    @Bean
    public Job getFlowJob() {
        return getJobBuilderFactory()
                .get("a")
                .incrementer(new RunIdIncrementer())
                .flow(getInitState())
                .next(decide_environmentVitality())
                .on(c(VITAL))
                .to(decide_hatLaufendeAlarmierungen())
                .on(c(NO))
                .to(decide_hatBlockierteDaten())
                .on(c(YES))
                .end()
                .on(c(NO))
                .to(do_StarteStammdatenlieferung())
                .next(do_DatenSelektieren())
                .next(do_DatenAnreichern())
                .next(do_InDbPersistieren())
//                .split(split_Processing())
                .end()
                .build();
    }

    @Bean
    public TaskExecutor split_Processing() {
        return new TaskExecutor() {
            @Override
            public void execute(Runnable task) {

            }
        };
    }

    private String c(FlowExecutionStatus executionStatus) {
        return executionStatus.getName();
    }

    @Bean
    public JobExecutionDecider decide_environmentVitality() {
        return new JobExecutionDecider() {

            @Override
            public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {

                System.out.println("Vitality? " + vital);

                if(vital) {
                    return VITAL;
                } else {
                    return FlowExecutionStatus.FAILED;
                }
            }
        };
    }

    @Bean
    public JobExecutionDecider decide_hatBlockierteDaten() {
        return new JobExecutionDecider() {

            @Override
            public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {

                System.out.println("Blocked data? " + blocked);

                if(blocked) {
                    return YES;
                } else {
                    return NO;
                }
            }
        };
    }

    @Bean
    public JobExecutionDecider decide_hatLaufendeAlarmierungen() {
        return new JobExecutionDecider() {

            @Override
            public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {

                System.out.println("Any Running? " + running);

//                return running ? FlowExecutionStatus.STOPPED : FlowExecutionStatus.COMPLETED;
                if(running) {
                    return YES;
                } else {
                    return NO;
                }

            }
        };
    }


    @Bean
    public Step getInitState() {
        return getStepBuilderFactory()
                .get("step1")
                .tasklet(new Tasklet() {
                    @Override
                    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                        System.out.println("Initialize");
                        return RepeatStatus.FINISHED;
                    }
                })
                .build();
    }

    @Bean
    public Step do_InDbPersistieren() {
        return getStepBuilderFactory()
                .get("do_InDbPersistieren")
                .tasklet(new Tasklet() {

                    @Override
                    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                        System.out.println("Daten in DB persistieren");
                        return RepeatStatus.FINISHED;
                    }
                })
                .build();
    }

    @Bean
    public Step do_DatenAnreichern() {
        return getStepBuilderFactory()
                .get("do_DatenAnreichern")
                .tasklet(new Tasklet() {

                    @Override
                    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                        System.out.println("Daten anreichern");
                        return RepeatStatus.FINISHED;
                    }
                })
                .build();
    }

    @Bean
    public Step do_DatenSelektieren() {
        return getStepBuilderFactory()
                .get("do_DatenSelektieren")
                .tasklet(new Tasklet() {

                    @Override
                    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                        System.out.println("Daten selektieren");
                        return RepeatStatus.FINISHED;
                    }
                })
                .build();
    }

    @Bean
    public Step do_StarteStammdatenlieferung() {
        return getStepBuilderFactory()
                .get("ftpUpload")
                .tasklet(new Tasklet() {

                    @Override
                    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                        System.out.println("Starte Stammdatenlieferung");
                        return RepeatStatus.FINISHED;
                    }
                })
                .build();
    }

    @Bean
    public SimpleJobRepository getJobRepository() {
        return new SimpleJobRepository(getJobInstanceDao(), getJobExecutionDao(), getStepExecutionDao(), getExecutionContextDao());
    }

    @Bean
    public JobInstanceDao getJobInstanceDao() {
        return new MapJobInstanceDao();
    }

    @Bean
    public JobExecutionDao getJobExecutionDao() {
        return new MapJobExecutionDao();
    }

    @Bean
    public StepExecutionDao getStepExecutionDao() {
        return new MapStepExecutionDao();
    }

    @Bean
    public MapExecutionContextDao getExecutionContextDao() {
        return new MapExecutionContextDao();
    }

    public JobExecutionDecider getDecider() {
        return new JobExecutionDecider() {

            @Override
            public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {
                boolean left = true;

                if(left) {
                    jobExecution.setExitStatus(ExitStatus.COMPLETED);
//                    return FlowExecutionStatus.COMPLETED;
                } else {
                    jobExecution.setExitStatus(ExitStatus.EXECUTING);
                }
                return FlowExecutionStatus.COMPLETED;

            }
        };
    }

    @Bean
    public JobBuilderFactory getJobBuilderFactory() {
        return new JobBuilderFactory(getJobRepository());
    }

    @Bean
    public StepBuilderFactory getStepBuilderFactory() {
        return new StepBuilderFactory(getJobRepository(), getTransactionManager());
    }

}

