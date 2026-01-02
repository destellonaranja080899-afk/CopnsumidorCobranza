package com.gs.atc.bok;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
@EnableScheduling
@SpringBootApplication
public class TaskServiceUsersApplication {
	public static void main(String[] args) {
		SpringApplication.run(TaskServiceUsersApplication.class, args);
	}
}
