package com.javacodegeeks.snippets.enterprise.quartzexample;

import java.util.Properties;

import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

import com.javacodegeeks.snippets.enterprise.quartzexample.job.Job1;
import com.javacodegeeks.snippets.enterprise.quartzexample.job.Jobload;
import com.javacodegeeks.snippets.enterprise.quartzexample.job.Util;

public class MyApp {

	
	
	public synchronized void calljob(){
		
		try {
			JobDetail job1 = JobBuilder.newJob(Job1.class)
					.withIdentity("Job1", "group1").build();
			 
			Properties prop=Util.loadPropertiesFile();
			 
			Trigger trigger1 = TriggerBuilder.newTrigger()
					.withIdentity("cronTrigger1", "group1")
					.withSchedule(CronScheduleBuilder.cronSchedule(prop.getProperty("CornJoB")))
					.build();
			System.out.println(prop.getProperty("CornJoB") );
			Scheduler scheduler1 = new StdSchedulerFactory().getScheduler();
			scheduler1.start();
			scheduler1.scheduleJob(job1, trigger1);

			
			
			/*obDetail job2 = JobBuilder.newJob(Job2.class)
					.withIdentity("job2", "group2").build();
			
			Trigger trigger2 = TriggerBuilder.newTrigger()
					.withIdentity("cronTrigger2", "group2")
					.withSchedule(CronScheduleBuilder.cronSchedule(new CronExpression("0/7 * * * * ?")))
					.build();
			
			Scheduler scheduler2 = new StdSchedulerFactory().getScheduler();
			scheduler2.start();
			scheduler2.scheduleJob(job2, trigger2);

			JobDetail job3 = JobBuilder.newJob(Job3.class)
					.withIdentity("job3", "group3").build();
			
			Trigger trigger3 = TriggerBuilder.newTrigger()
					.withIdentity("cronTrigger3", "group3")
					.withSchedule(CronScheduleBuilder.dailyAtHourAndMinute(13, 46))
					.build();
			
			Scheduler scheduler3 = new StdSchedulerFactory().getScheduler();
			scheduler3.start();
			scheduler3.scheduleJob(job3, trigger3);

			JobDetail job4 = JobBuilder.newJob(Job4.class)
					.withIdentity("job4", "group4").build();
			
			Trigger trigger4 = TriggerBuilder.newTrigger()
					.withIdentity("cronTrigger4", "group4")
					.withSchedule(CronScheduleBuilder.weeklyOnDayAndHourAndMinute(3, 13, 46))
					.build();
			
			Scheduler scheduler4 = new StdSchedulerFactory().getScheduler();
			scheduler4.start();
			scheduler4.scheduleJob(job4, trigger4);

			JobDetail job5 = JobBuilder.newJob(Job5.class)
					.withIdentity("job5", "group5").build();
			
			Trigger trigger5 = TriggerBuilder0
					.newTrigger().withIdentity("cronTrigger5", "group5")
					.withSchedule(CronScheduleBuilder.monthlyOnDayAndHourAndMinute(28, 13, 46))
					.build();	
			
			Scheduler scheduler5 = new StdSchedulerFactory().getScheduler();
			scheduler5.start();
			scheduler5.scheduleJob(job5, trigger5);*/
			
			Thread.sleep(10000);
			
			//scheduler1.shutdown();
			/*scheduler2.shutdown();
			scheduler3.shutdown();
			scheduler4.shutdown();
			scheduler5.shutdown();*/
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	public static void main(String[] args) {
		
	}

}