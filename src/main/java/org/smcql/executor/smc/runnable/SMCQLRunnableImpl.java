package org.smcql.executor.smc.runnable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.smcql.codegen.smc.DynamicCompiler;
import org.smcql.config.SystemConfiguration;
import org.smcql.db.data.QueryTable;
import org.smcql.db.data.Tuple;
import org.smcql.executor.config.RunConfig.ExecutionMode;
import org.smcql.executor.plaintext.SqlQueryExecutor;
import org.smcql.executor.smc.BasicSecureQueryTable;
import org.smcql.executor.smc.ExecutionSegment;
import org.smcql.executor.smc.OperatorExecution;
import org.smcql.executor.smc.SecureBufferPool;
import org.smcql.executor.smc.SecureQueryTable;
import org.smcql.executor.smc.SlicedSecureQueryTable;
import org.smcql.executor.smc.io.ArrayManager;
import org.smcql.util.SMCUtils;
import org.smcql.util.Utilities;

import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.flexsc.Party;
import com.oblivm.backend.gc.GCSignal;
import com.oblivm.backend.lang.inter.ISecureRunnable;
import com.oblivm.backend.lang.inter.Util;
import com.oblivm.backend.oram.SecureArray;

// gen和eva完全重复的方法
public class SMCQLRunnableImpl<T> implements Serializable {

	ExecutionSegment runSpec;
	ArrayManager<T> dataManager;
	boolean sliceEnabled = true;
	Map<String, SlicedSecureQueryTable> sliceInputs;
	Map<String, Double> perfReport;
	
	// single slice key / segment
	SlicedSecureQueryTable sliceOutput;
	boolean slicedExecution = true;
	boolean semijoinExecution = true;
	Tuple executingSliceValue = null; // all slices move in lockstep, so we need only one
	SMCRunnable parent;
	SecureQueryTable lastOutput = null;
	Logger logger; 
	List<SecureArray<T>> mergeSecArray = new ArrayList<>();
	
	public SMCQLRunnableImpl(ExecutionSegment segment, SMCRunnable runnable) throws Exception {
		System.out.println("[CODE]SMCQLRunnableImpl <init>");
		runSpec = segment;
		dataManager = new ArrayManager<T>();
		perfReport = new HashMap<String, Double>();
		
		logger = SystemConfiguration.getInstance().getLogger();
		try {
			slicedExecution = SystemConfiguration.getInstance().getProperty("sliced-execution").equals("true");
			semijoinExecution = SystemConfiguration.getInstance().getProperty("semijoin-execution").equals("true");
		} catch (Exception e) {
		}
		
		slicedExecution = slicedExecution && runSpec.rootNode.parentSegment.executionMode == ExecutionMode.Slice;

		if(slicedExecution) {
			sliceInputs = new HashMap<String, SlicedSecureQueryTable>();
		}
		
		//logger.info("Running in slicedExecution=" + slicedExecution + " mode.");

		parent = runnable;
	}
	
	public static String getKey(OperatorExecution opEx, boolean isLhs) {
		String ret = opEx.packageName;
		ret += isLhs ? "-lhs" : "-rhs";
		return ret;
	}

	
	@SuppressWarnings("unchecked")
	public void secureCompute(CompEnv<T> env) throws Exception {
		if(slicedExecution) {
			System.out.println("[CODE]secureCompute slicedExecuting");
			//logger.info("secureCompute slicedExecuting");
			sliceOutput = new SlicedSecureQueryTable(runSpec.rootNode, (CompEnv<GCSignal>) env, parent);
			List<Tuple> sliceVals = runSpec.sliceValues;
			for(Tuple t : sliceVals) {
				runSpec.resetOutput();
				executingSliceValue = t;

				SecureArray<T> output = runOneSliced(runSpec.rootNode, env); 

				if(output != null) {
					GCSignal[] payload = (GCSignal[]) Util.secToIntArray(env, output);
					GCSignal[] nonNulls = (GCSignal[]) output.getNonNullEntries();
					sliceOutput.addSlice(executingSliceValue, payload, nonNulls);
				}
			}
		
			lastOutput = sliceOutput;
			SecureBufferPool.getInstance().addArray(runSpec.rootNode, sliceOutput); // bypass flattening it until we need to
		} else  {
			System.out.println("[CODE]secureCompute nonslicedExecuting");
			//logger.info("secureCompute nonslicedExecuting");
			SecureArray<T> secResult = runOne(runSpec.rootNode, env);
			if(Utilities.SYSCMD_PSI_MPC){
			}else{
				GCSignal[] payload = (GCSignal[]) Util.secToIntArray(env, secResult);
				GCSignal[] nonNulls = (GCSignal[]) secResult.getNonNullEntries();
				
				BasicSecureQueryTable output = new BasicSecureQueryTable(payload, nonNulls, runSpec.rootNode.outSchema, (CompEnv<GCSignal>) env, parent);
				lastOutput = output;
			}
		}
	}
	
	boolean slicesRemain() {
		if(sliceInputs.isEmpty())  // no runs yet
			return true;
		
		SlicedSecureQueryTable firstOne = (SlicedSecureQueryTable) sliceInputs.values().toArray()[0];
		if(firstOne.hasNext())
			return true;
		
		return false;
	}

	private String addSlicePredicate(String query) {
		String key = "";
		try {
			key = runSpec.sliceSpec.getAttributes().get(0).getName();
		} catch (Exception e) {
			return query;
		}
		
		int orderByIndex = query.lastIndexOf("ORDER BY");
		String result = (orderByIndex == -1) ? query: query.substring(0, orderByIndex);
		String orderBy = (orderByIndex == -1) ? "": query.substring(orderByIndex);
	
		String predicate = "(";
		for (int i=0; i < runSpec.sliceValues.size(); i++) {
			Tuple t = runSpec.sliceValues.get(i);
			String val = t.getField(0).toString();
			
			if (i > 0) 
				predicate += ", "; 
			
			predicate += val;
		}
		predicate += ")";
		
		return (predicate.equals("()")) ? null : result + " WHERE " + key + " IN " + predicate + " " + orderBy;
	}

	// returns output of run		
	@SuppressWarnings("unchecked")
	SecureArray<T>   runOne(OperatorExecution op, CompEnv<T> env) throws Exception {
		if(op == null || (op.parentSegment != runSpec)){ // != runspec表示子级是在另一个段中计算的
			System.out.println("[CODE]secureCompute runOne computed");
			return null;//左分支或右分支遍历结束
		}
		// already exec'd cte
		if(op.output != null) {
			System.out.println("[CODE]secureCompute runOne null-op");
			return (SecureArray<T>) op.output;
		}
		if(Utilities.isCTE(op)) { // skip ctes
			System.out.println("[CODE]secureCompute runOne skip cte");
			return runOne(op.lhsChild, env);
		}
		if (semijoinExecution && op.getSourceSQL() != null) {
			op.setSourceSQL(addSlicePredicate(op.getSourceSQL()));
		}
		
		System.out.println("[CODE]secureCompute runOne:" + Utilities.getPackageClassName(op.packageName) + "(" + Utilities.getOperatorChildrenPackageClassName(op) + ") on LHS");
		SecureArray<T> lhs = runOne(op.lhsChild, env);
		if(lhs == null) { // 递归结束。get input from outside execution segment
			long start = System.nanoTime();
			lhs = dataManager.getInput(op, true, env, parent);
			System.out.println("[CODE]secureCompute runOne(" + Utilities.getPackageClassName(op.packageName) + ") had LHS:" + ((lhs != null) ? lhs.length : 0));
			long end = System.nanoTime();
			//logger.info("Loaded lhs data in " + (end - start) / 1e9  + " seconds.");
		}
		System.out.println("[CODE]secureCompute runOne:" + Utilities.getPackageClassName(op.packageName) + "(" + Utilities.getOperatorChildrenPackageClassName(op) + ") on RHS");
		SecureArray<T> rhs = runOne(op.rhsChild, env);
		if(rhs == null)  {
			long start = System.nanoTime();
			rhs = dataManager.getInput(op, false, env, parent);
			System.out.println("[CODE]secureCompute runOne(" + Utilities.getPackageClassName(op.packageName) + ") had RHS:" + ((rhs != null) ? rhs.length : 0));
			long end = System.nanoTime();
			//logger.info("Loaded rhs data in " + (end - start) / 1e9  + " seconds.");
		}
		if(Utilities.SYSCMD_PSI_MPC){
			//cross table PSI to reduce tuple count
			//suppose A has table A1 and A2, while B has table B1 and B2
			//in the case without SYSCMD_PSI_MPC, SMCQL join A1+B1 with A2+B2, 
			// + means merge tuples with garbled circuit
			//in the case with SYSCMD_PSI_MPC, join (A1&B2)+(B1&A2) with (A2&B1)+(B2&A1),
			// & means PSI, (A1&B2) means get PSI result at A
			if(lhs!= null && lhs.isPlain && lhs.joinId != null && rhs != null && rhs.isPlain && rhs.joinId != null){
				// merge operator, save plain table and merge operator
				// merge operator will run after PSI
				if(op.getParty() == Party.Alice){
					System.out.println("[CODE]secureCompute runOne syscmd PSI pending plain table Alice LHS " + mergeSecArray.size());
					mergeSecArray.add(lhs);
				}else{
					System.out.println("[CODE]secureCompute runOne syscmd PSI pending plain table Bob RHS " + mergeSecArray.size());
					mergeSecArray.add(rhs);
				}
				return null;
			}else {
				System.out.println("[CODE]secureCompute runOne syscmd PSI:" + Utilities.getPackageClassName(op.packageName) + " isJoin:" + op.isJoin + " joinId:" + op.joinId);
				if(op.isJoin && op.joinId != null){
					// join operator, do PSI
					Party party = op.getParty();
					if(party == Party.Alice){
						//local(A1)&local(B2)
						SecureArray<T> localA1 = mergeSecArray.get(0);
						SecureArray<T> localA2 = mergeSecArray.get(1);
						//PSI
						parent.sendInt(localA1.length);
						if(localA1.length > 0){
							//(A1&B2)
							SMCUtils.syscmdPSI(localA1);
						}
						parent.sendInt(localA2.length);
						if(localA2.length > 0){
							//(A2&B1)
							SMCUtils.syscmdPSI(localA2);
						}
						//run merge
						lhs = SMCUtils.prepareMpc(localA1);
						rhs = SMCUtils.prepareMpc(localA2);
					}else{
						//local(B2)&local(A1)
						SecureArray<T> localB1 = mergeSecArray.get(0);
						SecureArray<T> localB2 = mergeSecArray.get(1);
						//PSI
						int tupleCountA1 = parent.getInt();
						if(tupleCountA1 > 0){
							//(A1&B2)
							SMCUtils.syscmdPSI(localB2);
						}
						int tupleCountA2 = parent.getInt();
						if(tupleCountA2 > 0){
							//(A2&B1)
							SMCUtils.syscmdPSI(localB1);
						}
						//run merge
						lhs = SMCUtils.prepareMpc(localB1);
						rhs = SMCUtils.prepareMpc(localB2);
					}
					mergeSecArray.clear();
				}else{
					//not join, or join condition does not contain equal, prepare MPC
					if(lhs!= null && lhs.isPlain){
						lhs = SMCUtils.prepareMpc(lhs);
					}
					if(rhs!= null && rhs.isPlain){
						rhs = SMCUtils.prepareMpc(rhs);
					}
				}
			}
		}
		System.out.println("[CODE]secureCompute runOne run:" + Utilities.getPackageClassName(op.packageName));
		return runSecureOperator(op, lhs, rhs, env);
	}
	
	@SuppressWarnings("unchecked")
	SecureArray<T> runSecureOperator(OperatorExecution op, SecureArray<T> lhs, SecureArray<T> rhs, CompEnv<T> env) throws Exception{
		if(Utilities.SYSCMD_PSI_MPC){
			return SMCUtils.syscmdMPC(op, lhs, rhs);
		}else{
			double start = System.nanoTime();
			ISecureRunnable<T> runnable = DynamicCompiler.loadClass(op.packageName, op.byteCode, env);
			int rhsLength = (rhs != null) ? rhs.length : 0;
			int lhsLength = (lhs != null) ? lhs.length : 0;
			String msg =  "Operator " + op.packageName + " started at " + Utilities.getTime() + " on " + lhsLength + "," + rhsLength  + " tuples.";
			//logger.info(msg);
			System.out.println("[CODE]secureCompute runSecureOperator " + msg);
			
			SecureArray<T> secResult = null;
			if(Utilities.isMerge(op) && (lhs == null || rhs == null)) { // applies for both null too
				System.out.println("[CODE]secureCompute runSecureOperator merge leaf(" + lhs + ", " + rhs + ")");
				secResult = (lhs == null) ? rhs : lhs;
			}
			else {
				System.out.println("[CODE]secureCompute runSecureOperator " + Utilities.getPackageClassName(op.packageName) + "(" + lhs + ", " + rhs + ")");
				secResult = runnable.run(lhs, rhs);
				if(secResult == null) 
					throw new Exception("Null result for " + op.packageName);

				if(secResult.getNonNullEntries() == null) {
					T[] prevEntries = lhs.getNonNullEntries();
					secResult.setNonNullEntries(prevEntries);
				}
				System.out.println("[CODE]secureCompute runSecureOperator " + Utilities.getPackageClassName(op.packageName) + "(" + lhs + ", " + rhs + ") result:" + secResult);
			}
			double end = System.nanoTime();
			double elapsed = (end - start) / 1e9;
			
			msg = "Operator ended at " + Utilities.getTime() + " it ran in " + op.packageName + " ran in " + elapsed + " seconds, output=" + secResult;
			//logger.info(msg);

			// sum for slices
			if(perfReport.containsKey(op.packageName)) {
				double oldSum = perfReport.get(op.packageName);
				
				perfReport.put(op.packageName, oldSum + elapsed);
			}
			else 
				perfReport.put(op.packageName, elapsed);
			
			dataManager.registerArray(op, secResult, env, parent);
			op.output = (SecureArray<GCSignal>) secResult;
			return secResult;
		}
	}
	
	@SuppressWarnings("unchecked")
	SecureArray<T> runOneSliced(OperatorExecution op, CompEnv<T> env) throws Exception {
		if(op == null || op.parentSegment != runSpec) // != runspec implies that child was computed in another segment, pull this in with parent
			return null;

		// already exec'd cte
		if(op.output != null) {
			return (SecureArray<T>) op.output;
		}
		
		if(Utilities.isCTE(op)) { // skip ctes
			return runOneSliced(op.lhsChild, env);
		}

		
		SecureArray<T> lhs = runOneSliced(op.lhsChild, env);
		if(lhs == null) { // get input from outside execution segment
				String key = getKey(op, true);
				if(!sliceInputs.containsKey(key))  {
					SlicedSecureQueryTable allSlices = dataManager.getSliceInputs(op, env, parent, true);					
					sliceInputs.put(key, allSlices);
				}
				SlicedSecureQueryTable srcTable = sliceInputs.get(key);
				if (srcTable != null)
					lhs = (SecureArray<T>) srcTable.getSlice(executingSliceValue, (CompEnv<GCSignal>) env);
				 				
		}
	
		
		SecureArray<T> rhs = runOneSliced(op.rhsChild, env);

		if(rhs == null)  {
				String key = getKey(op, false);
				if(!sliceInputs.containsKey(key))  {
				
					SlicedSecureQueryTable allSlices = dataManager.getSliceInputs(op, env, parent, false);
					if(allSlices != null)
						sliceInputs.put(key, allSlices);
				}
				SlicedSecureQueryTable srcTable = sliceInputs.get(key);
				if (srcTable != null) 
					rhs = (SecureArray<T>) srcTable.getSlice(executingSliceValue, (CompEnv<GCSignal>) env);
		}

		if (rhs == null && lhs == null)
			return null;

		double start = System.nanoTime();
		SecureArray<T> secResult = null;
		if(Utilities.isMerge(op)) {
			secResult = mergeRun(op, env, lhs, rhs);
		}
		else
			secResult = slicedRun(op, env, lhs, rhs);
		
		double end = System.nanoTime();
		double elapsed = (end - start) / 1e9;

		//logger.info("Operator ended at " + Utilities.getTime() + " it ran in " + op.packageName + " ran in " + elapsed + " seconds.");

		if(perfReport.containsKey(op.packageName)) {
			double oldSum = perfReport.get(op.packageName);
			
			perfReport.put(op.packageName, oldSum + elapsed);
		}
		else 
			perfReport.put(op.packageName, elapsed);
		
		return secResult;
		

	}
	
	
	private SecureArray<T> mergeRun(OperatorExecution op, CompEnv<T> env, SecureArray<T> lhs, SecureArray<T> rhs) throws Exception {
		if(lhs != null && rhs != null) {
			return slicedRun(op, env, lhs, rhs);
		}
		if(lhs != null) {
			return lhs;
		}
		if(rhs != null) {

			return rhs;
		}
		throw new Exception("Operator " + op.packageName + " has no input!");
	}
	
	@SuppressWarnings("unchecked")
	private SecureArray<T> slicedRun(OperatorExecution op, CompEnv<T> env, SecureArray<T> lhs, SecureArray<T> rhs) throws Exception {
		ISecureRunnable<T> runnable = DynamicCompiler.loadClass(op.packageName, op.byteCode, env);
		
		int lhsLength = lhs != null ? lhs.length: 0;
		int rhsLength = rhs != null ? rhs.length : 0;
		String msg =  "Operator " + op.packageName + " started at " + Utilities.getTime() + " on " + lhsLength + "," + rhsLength  + " tuples.";
		//logger.info(msg);
	
		SecureArray<T> secResult = null;
		if (op.packageName.contains("Join") && (lhsLength == 0 || rhsLength == 0)) {
			secResult = (lhsLength == 0) ? runnable.run(rhs, rhs) : runnable.run(lhs, lhs);
			secResult = null;
		} else {
			secResult = runnable.run(lhs, rhs);
		}
		
		if(secResult != null && secResult.getNonNullEntries() == null) {
			T[] prevEntries = lhs.getNonNullEntries();
			secResult.setNonNullEntries(prevEntries);
		}

		dataManager.registerSlicedArray(op, secResult, env, parent, this.executingSliceValue);
		op.output = (SecureArray<GCSignal>) secResult;
		return secResult;
	}
	
	public void prepareOutput(CompEnv<T> env) throws Exception {		
		if(runSpec.sliceComplementSQL != null && !runSpec.sliceComplementSQL.isEmpty() && semijoinExecution) {
			 QueryTable plainOut = SqlQueryExecutor.query(runSpec.sliceComplementSQL, runSpec.outSchema, runSpec.workerId);
			 lastOutput.setPlaintextOutput(plainOut);
			 SecureBufferPool.getInstance().addArray(runSpec.rootNode, lastOutput);
		}
		
		
		//logger.info("Finished prepare output for  " + runSpec.rootNode.packageName + " at " + Utilities.getTime());
		//logger.info("Perf times " + perfReport);

	}
	
	public SecureQueryTable getOutput() {
		return lastOutput;
	}
}
