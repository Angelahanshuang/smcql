package org.smcql.runner;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.calcite.sql.SqlDialect;
import org.smcql.codegen.QueryCompiler;
import org.smcql.config.SystemConfiguration;
import org.smcql.db.data.QueryTable;
import org.smcql.executor.SMCQLQueryExecutor;
import org.smcql.executor.config.WorkerConfiguration;
import org.smcql.executor.smc.ExecutionSegment;
import org.smcql.parser.SqlStatementParser;
import org.smcql.plan.SecureRelRoot;
import org.smcql.util.Utilities;

public class SMCQLRunner {
	protected SqlDialect dialect = SqlDialect.DatabaseProduct.POSTGRESQL.getDialect();
	protected String codePath = Utilities.getSMCQLRoot() + "/conf/workload/sql";
	protected static WorkerConfiguration honestBroker;
	protected static SqlStatementParser parser;
	private static Logger logger = null;
	
	private static void setUp() throws Exception {
		System.setProperty("smcql.setup", Utilities.getSMCQLRoot() + "/conf/setup.localhost");////系统配置文件位置（诚实代理的地址信息）

		parser = new SqlStatementParser();
		honestBroker = SystemConfiguration.getInstance().getHonestBrokerConfig();
		SystemConfiguration.getInstance().resetCounters();
		logger = SystemConfiguration.getInstance().getLogger();
	}

	public static void main(String[] args) throws Exception {
		setUp();

		/*String sql = "SELECT COUNT(DISTINCT d.patient_id) as rx_cnt FROM \n" +
				"    (SELECT * FROM mi_cohort_diagnoses dt\n" +
				"        WHERE dt.icd9 like '414%') AS d\n" +
				"    INNER JOIN \n" +
				"    (SELECT * FROM mi_cohort_medications mt\n" +
				"        WHERE lower(mt.medication) like '%aspirin%') AS m\n" +
				"    ON d.timestamp_ <= m.timestamp_ AND d.patient_id = m.patient_id";*/
		String sql = args[0];
		System.out.println("\nQuery:\n" + sql);
		
		String aWorkerId = args[1];
		String bWorkerId = args[2];
		
		String testName = "userQuery";
		QueryCompiler qc;
		//todo: test code
		if(Utilities.Isomerism){//支持各方异构
			SecureRelRoot aSecRoot = new SecureRelRoot(testName, sql);
			SecureRelRoot bSecRoot = new SecureRelRoot(testName, sql);
			System.out.println("[CODE]SecureRelRoot:\n" + aSecRoot);
			System.out.println("[CODE]SecureRelRoot:\n" + bSecRoot);
			Map<String, String> workerTable = new HashMap<String, String>();
			Map<String, SecureRelRoot> secRoots = new HashMap<String, SecureRelRoot>();
			workerTable.put(aWorkerId, "mi_cohort_diagnoses");
			workerTable.put(bWorkerId, "mi_cohort_medications");
			secRoots.put(aWorkerId, aSecRoot);
			secRoots.put(bWorkerId, bSecRoot);
			qc = new QueryCompiler(secRoots, sql, workerTable);
		}else{
			SecureRelRoot secRoot = new SecureRelRoot(testName, sql);
			System.out.println("[CODE]SecureRelRoot:\n" + secRoot);
			qc = new QueryCompiler(secRoot, sql);
		}
		for(ExecutionSegment segment: qc.getSegments()){
			System.out.println("[CODE]ExecutionStep(" + segment.exeStep.getWorkerId() + "):\n" + segment.exeStep.printTree());
		}

		SMCQLQueryExecutor exec = new SMCQLQueryExecutor(qc, aWorkerId, bWorkerId, Utilities.Isomerism);
		exec.run();
		
	    QueryTable results = exec.getOutput();
	    System.out.println("\nOutput:\n" + results);
	    System.exit(0);
	}

}
