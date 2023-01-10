package org.smcql.executor;

import org.smcql.codegen.QueryCompiler;
import org.smcql.config.SystemConfiguration;
import org.smcql.db.data.QueryTable;
import org.smcql.executor.smc.ExecutionSegment;
import org.smcql.executor.smc.SecureQueryTable;
import org.smcql.executor.step.ExecutionStep;
import org.smcql.executor.step.PlaintextStep;
import org.smcql.executor.step.SecureStep;
import org.smcql.type.SecureRelDataTypeField;
import org.smcql.type.SecureRelDataTypeField.SecurityPolicy;
import org.smcql.util.SMCUtils;
import org.smcql.type.SecureRelRecordType;
import java.util.logging.Logger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;


// handles only 2 nodes, must contain at least one SecureStep in plan

public class SMCQLQueryExecutor implements Runnable {
	
	SegmentExecutor runner = null;
	QueryCompiler compiledPlan = null;
	private SecureRelRecordType lastSchema;
	private List<SecureQueryTable> lastOutput;
	private QueryTable plainOutput;
	private Logger logger = null;
	private boolean Isomerism = false;
	private String aWorker;
	private String bWorker;
	
	public class ExecutionSegmentWrapper {
		private List<ExecutionSegment> segments = new ArrayList<>();
		int port = 0;

		public void add(ExecutionSegment seg){
			if(port == 0){
				port = seg.runConf.port;
			}else{
				seg.runConf.port = port;
			}
			segments.add(seg);
		}

		public List<ExecutionSegment> get(){
			return segments;
		}

		@Override
		public String toString(){
			String ret = "";
			for(ExecutionSegment segment: segments){
				ret += (segment.toString()) + "\n";
			}
			return ret;
		}
	}

	public SMCQLQueryExecutor(QueryCompiler compiled, String aWorker, String bWorker, boolean Isomerism) throws Exception {
		runner = new SegmentExecutor(aWorker, bWorker);
		compiledPlan = compiled;
		logger = SystemConfiguration.getInstance().getLogger();
		this.Isomerism = Isomerism;
		this.aWorker = aWorker;
		this.bWorker = bWorker;
	}
	
	
	public void run() {
		System.out.println("[CODE]SMCQLQueryExecutor Isomerism:" + Isomerism);
		List<ExecutionSegment> segments = compiledPlan.getSegments();
		
		ExecutionStep root = compiledPlan.getRoot();
		if(Isomerism){
			root = compiledPlan.getRoot(aWorker);
		}
		
		for (SecureRelDataTypeField field : root.getExec().outSchema.getAttributes()) {
			if (field.getSecurityPolicy().equals(SecurityPolicy.Private)) {
				System.out.println("Exception: Private attribute " + field.getName() + " in out schema!");
				return;
			}
		}
		
		if(root instanceof PlaintextStep) {		
			try {
				System.out.println("[CODE]runPlain:" + root.getPackageName());
				plainOutput = runner.runPlain((PlaintextStep) root);
			} catch (Exception e) {
				System.out.println("Exception: No runnable execution step!");
			}
			return;
		}
		
		if(Isomerism){
			// 在相反的顺序迭代自底向上 iterate in reverse order to go bottom up
			List<ExecutionSegmentWrapper> wseg = new ArrayList<>();
			List<ExecutionSegment> aWorkerSegments = new ArrayList<>();
			List<ExecutionSegment> bWorkerSegments = new ArrayList<>();
			//todo: both lists must have same items
			for(ExecutionSegment seg: segments){
				if(seg.workerId.equals(aWorker)){
					aWorkerSegments.add(seg);
				}else{
					bWorkerSegments.add(seg);
				}
			}
			for(int i = 0; i < aWorkerSegments.size(); i++){
				ExecutionSegmentWrapper wrapper = new ExecutionSegmentWrapper();
				wrapper.add(aWorkerSegments.get(i));
				wrapper.add(bWorkerSegments.get(i));
				wseg.add(wrapper);
			}
			ListIterator<ExecutionSegmentWrapper> li = wseg.listIterator(wseg.size());

			try {
				while(li.hasPrevious()) { 
					ExecutionSegmentWrapper segment = li.previous();
					System.out.println("[CODE]异构runSecureSegment:\n" + segment.toString());
					lastOutput = runner.runSecureSegment(segment.get());
					lastSchema = segment.get().get(0).outSchema;
				}
			}
			catch(Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}else{
			// 在相反的顺序迭代自底向上,why？
			ListIterator<ExecutionSegment> li = segments.listIterator(segments.size());
			try {
				while(li.hasPrevious()) { 
					ExecutionSegment segment = li.previous();
					System.out.println("[CODE]非异构runSecureSegment:" + segment.toString());
					lastOutput = runner.runSecureSegment(segment);
					lastSchema = segment.outSchema;
				}
			}
			catch(Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}
	
	public QueryTable getOutput() throws Exception {
		if (lastOutput == null && plainOutput == null)
			return null;
		
		if(compiledPlan.getRoot() instanceof PlaintextStep) {
			return plainOutput;
		}
		
		SecureQueryTable lhs = lastOutput.get(0);
		SecureQueryTable rhs = lastOutput.get(1);
		
		return lhs.declassify(rhs, lastSchema);		
		
	}
	
	
}
