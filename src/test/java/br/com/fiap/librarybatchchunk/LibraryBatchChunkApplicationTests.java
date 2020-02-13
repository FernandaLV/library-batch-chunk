package br.com.fiap.librarybatchchunk;

import br.com.fiap.librarybatchchunk.config.BatchConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import javax.sql.DataSource;

import java.sql.ResultSet;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@SpringBootTest
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {LibraryBatchChunkApplication.class,
		BatchConfig.class})
class LibraryBatchChunkApplicationTests {

	@Autowired
	private JobLauncherTestUtils jobLauncherTestUtils;

	@Autowired
	private Job job;

	@Autowired
	private DataSource dataSource;

	@Test
	public void testPessoaJob() throws Exception {
		JobExecution jobExecution = jobLauncherTestUtils.getJobLauncher()
				.run(job, jobLauncherTestUtils.getUniqueJobParameters());

		assertNotNull(jobExecution);
		assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

		ResultSet resultSet = dataSource.getConnection()
				.prepareStatement("select count(*) from TB_PESSOA")
				.executeQuery();

		await().atMost(10, TimeUnit.SECONDS)
				.until(() -> {
					resultSet.last();
					return resultSet.getInt(1) == 3;
				});

		assertEquals(3, resultSet.getInt(1));


	}

}
