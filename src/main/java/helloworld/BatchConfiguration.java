package helloworld;

import org.springframework.batch.core.*;
import org.springframework.batch.core.job.AbstractJob;
import org.springframework.batch.core.job.SimpleJob;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.FlowJob;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.job.flow.support.StateTransition;
import org.springframework.batch.core.job.flow.support.state.DecisionState;
import org.springframework.batch.core.job.flow.support.state.EndState;
import org.springframework.batch.core.job.flow.support.state.StepState;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.core.repository.dao.*;
import org.springframework.batch.core.repository.support.SimpleJobRepository;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.LinkedList;
import java.util.List;

/**
 * A basic spring batch example for SimpleJob and FlowJob
 *
 * @link http://www.jroller.com/0xcafebabe/entry/spring_batch_hello_world_1
 *
 * Created by damoeb on 9/9/14.
 */
@Configuration
public class BatchConfiguration {

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
    public FlowJob getFlowJob() {
        FlowJob job = new FlowJob();

        job.setFlow(getMainFlow());

        job.setJobRepository(getJobRepository());

        return job;
    }


    @Bean
    public SimpleJob getSimpleJob() {
        SimpleJob job = new SimpleJob();

        job.addStep(getStep1());
        job.addStep(getStep2());

        job.setJobRepository(getJobRepository());

        return job;
    }

    @Bean
    public Step getStep1() {
        TaskletStep step = new TaskletStep();
        step.setTasklet(new PrintTasklet("Hello"));
        step.setJobRepository(getJobRepository());
        step.setTransactionManager(getTransactionManager());
        return step;
    }

    @Bean
    public Step getStep2() {
        TaskletStep step = new TaskletStep();
        step.setTasklet(new PrintTasklet("World"));
        step.setJobRepository(getJobRepository());
        step.setTransactionManager(getTransactionManager());
        return step;
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

    public SimpleFlow getMainFlow() {
        SimpleFlow flow = new SimpleFlow("main");
        List<StateTransition> transitions = new LinkedList<StateTransition>();

        transitions.add(StateTransition.createStateTransition(new StepState("1", getStep1()), "2"));
        transitions.add(StateTransition.createStateTransition(new DecisionState(getDecider(), "d"), "2"));
        transitions.add(StateTransition.createStateTransition(new StepState("2", getStep2()), "end"));
        transitions.add(StateTransition.createEndStateTransition(new EndState(FlowExecutionStatus.COMPLETED, "end")));

        flow.setStateTransitions(transitions);

        return flow;
    }

    public JobExecutionDecider getDecider() {
        return new JobExecutionDecider() {

            @Override
            public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {
                stepExecution.getExitStatus();
                return FlowExecutionStatus.STOPPED;
            }
        };
    }
}

