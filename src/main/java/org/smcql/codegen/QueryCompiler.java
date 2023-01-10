
package org.smcql.codegen;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.commons.io.FileUtils;
import org.smcql.codegen.plaintext.PlainOperator;
import org.smcql.codegen.smc.operator.SecureOperator;
import org.smcql.codegen.smc.operator.SecureOperatorFactory;
import org.smcql.codegen.smc.operator.support.MergeMethod;
import org.smcql.config.SystemConfiguration;
import org.smcql.executor.config.ConnectionManager;
import org.smcql.executor.config.RunConfig;
import org.smcql.executor.config.RunConfig.ExecutionMode;
import org.smcql.executor.smc.BasicSecureQueryTable;
import org.smcql.executor.smc.ExecutionSegment;
import org.smcql.executor.smc.OperatorExecution;
import org.smcql.executor.smc.SecureBufferPool;
import org.smcql.executor.step.ExecutionStep;
import org.smcql.executor.step.PlaintextStep;
import org.smcql.executor.step.SecureStep;
import org.smcql.plan.SecureRelRoot;
import org.smcql.plan.operator.Aggregate;
import org.smcql.plan.operator.CommonTableExpressionScan;
import org.smcql.plan.operator.Filter;
import org.smcql.plan.operator.Join;
import org.smcql.plan.operator.Operator;
import org.smcql.plan.operator.Project;
import org.smcql.plan.operator.Sort;
import org.smcql.plan.operator.WindowAggregate;
import org.smcql.type.SecureRelDataTypeField;
import org.smcql.type.SecureRelRecordType;
import org.smcql.util.ClassPathUpdater;
import org.smcql.util.CodeGenUtils;
import org.smcql.util.Utilities;

import com.oblivm.backend.flexsc.Mode;



public class QueryCompiler {
	

	Map<ExecutionStep, String> sqlCode;
	Map<ExecutionStep, String> smcCode;
	Map<Operator,ExecutionStep> allSteps;

	//todo: test code
	Map<String, String> workerTable = null;
	Map<String, ExecutionStep> workerCompiledRoot = null;
	
	String queryId;
	
	List<String> smcFiles;
	List<String> sqlFiles;
	List<ExecutionSegment> executionSegments;
	
	String userQuery = null;

	SecureRelRoot queryPlan;
	
	ExecutionStep compiledRoot;
	
	Mode mode = Mode.REAL;
	String generatedClasspath = null;

	public ExecutionStep getRoot() {
		return compiledRoot;
	}
	public ExecutionStep getRoot(String workerId) {
		return workerCompiledRoot.get(workerId);
	}
	public List<ExecutionSegment> getSegments() {
		return executionSegments;
	}

	public QueryCompiler(SecureRelRoot q) throws Exception {
	
		queryPlan = q;
		smcFiles = new ArrayList<String>();
		sqlFiles = new ArrayList<String>();
		sqlCode = new HashMap<ExecutionStep, String>();
		smcCode = new HashMap<ExecutionStep, String>();
		executionSegments = new ArrayList<ExecutionSegment>();

		allSteps = new HashMap<Operator, ExecutionStep>();
	
		queryId = q.getName();
		Operator root = q.getPlanRoot();
		
		// set up space for .class files
		generatedClasspath = Utilities.getSMCQLRoot()+ "/bin/org/smcql/generated/" + queryId;
		Utilities.mkdir(generatedClasspath);
		Utilities.cleanDir(generatedClasspath);
		
		
		// single plaintext executionstep if no secure computation detected
		if(root.getExecutionMode() == ExecutionMode.Plain) {
			compiledRoot = generatePlaintextStep(root);
			ExecutionSegment segment = createSegment(compiledRoot);
			executionSegments.add(segment);
			compiledRoot.getExec().parentSegment = segment;
		}
		else {  // recurse
			compiledRoot = addOperator(root, new ArrayList<Operator>());
		}

		inferExecutionSegment(compiledRoot);
	}
	
	public QueryCompiler(SecureRelRoot q, String sql) throws Exception {
		
		queryPlan = q;
		smcFiles = new ArrayList<String>();
		sqlFiles = new ArrayList<String>();
		sqlCode = new HashMap<ExecutionStep, String>();
		smcCode = new HashMap<ExecutionStep, String>();
		executionSegments = new ArrayList<ExecutionSegment>();
		userQuery = sql;

		allSteps = new HashMap<Operator, ExecutionStep>();
	
		queryId = q.getName();
		Operator root = q.getPlanRoot();
		
		// set up space for .class files
		generatedClasspath = Utilities.getSMCQLRoot()+ "/bin/org/smcql/generated/" + queryId;
		/*File file = new File(generatedClasspath);
		if(!file.exists()){
			file.mkdirs();
		}
		FileUtils.cleanDirectory(file);*/
		Utilities.mkdir(generatedClasspath);//TODO tians 部署打包前放开
		Utilities.cleanDir(generatedClasspath);
		
		
		// 如果未检测到安全计算，则执行单个明文步骤
		if(root.getExecutionMode() == ExecutionMode.Plain) {
			compiledRoot = generatePlaintextStep(root);
			ExecutionSegment segment = createSegment(compiledRoot);
			executionSegments.add(segment);
			compiledRoot.getExec().parentSegment = segment;
		}
		else {  // recurse
			compiledRoot = addOperator(root, new ArrayList<Operator>());
		}

		inferExecutionSegment(compiledRoot);
		//SMCQLQueryExecutor uses executionSegments and compiledRoot to run
	}

	//todo: test code
	public QueryCompiler(Map<String, SecureRelRoot> secRoots, String sql, Map<String, String> workerTable) throws Exception {
		this.workerTable = workerTable;
		String aWorkerId = (String)workerTable.keySet().toArray()[0];
		String bWorkerId = (String)workerTable.keySet().toArray()[1];
		System.out.println("[CODE]QueryCompiler Asymmetric(" + aWorkerId + ", " + bWorkerId + ")");
		workerCompiledRoot = new HashMap<String, ExecutionStep>();
		queryPlan = secRoots.get(aWorkerId);
		smcFiles = new ArrayList<String>();
		sqlFiles = new ArrayList<String>();
		sqlCode = new HashMap<ExecutionStep, String>();
		smcCode = new HashMap<ExecutionStep, String>();
		executionSegments = new ArrayList<ExecutionSegment>();
		userQuery = sql;

		allSteps = new HashMap<Operator, ExecutionStep>();
	
		queryId = queryPlan.getName();
		Operator root = queryPlan.getPlanRoot();
		
		// set up space for .class files
		generatedClasspath = Utilities.getSMCQLRoot()+ "/bin/org/smcql/generated/" + queryId;
		Utilities.mkdir(generatedClasspath);
		Utilities.cleanDir(generatedClasspath);
		
		// single plaintext executionstep if no secure computation detected
		ExecutionStep aWorkerCompiledRoot = null;
		ExecutionStep bWorkerCompiledRoot = null;
		if(root.getExecutionMode() == ExecutionMode.Plain) {
			aWorkerCompiledRoot = generatePlaintextStep(root, aWorkerId);
			ExecutionSegment aSegment = createSegment(aWorkerCompiledRoot);
			if(aSegment != null){
				aSegment.workerId = aWorkerId;
				executionSegments.add(aSegment);
				aWorkerCompiledRoot.getExec().parentSegment = aSegment;
				inferExecutionSegment(aWorkerCompiledRoot, aWorkerId);
				workerCompiledRoot.put(aWorkerId, aWorkerCompiledRoot);
			}

			bWorkerCompiledRoot = generatePlaintextStep(root, bWorkerId);
			ExecutionSegment bSegment = createSegment(bWorkerCompiledRoot);
			if(bSegment != null){
				bSegment.workerId = bWorkerId;
				executionSegments.add(bSegment);
				bWorkerCompiledRoot.getExec().parentSegment = bSegment;
				inferExecutionSegment(bWorkerCompiledRoot, bWorkerId);
				workerCompiledRoot.put(bWorkerId, bWorkerCompiledRoot);
			}
		}
		else {  // recurse
			aWorkerCompiledRoot = addOperator(secRoots.get(aWorkerId).getPlanRoot(), aWorkerId, new ArrayList<Operator>());
			System.out.println("[CODE]QueryCompiler CompiledRoot(" + aWorkerId + "):" + Utilities.getPackageClassName(aWorkerCompiledRoot.getPackageName()));
			inferExecutionSegment(aWorkerCompiledRoot, aWorkerId);
			workerCompiledRoot.put(aWorkerId, aWorkerCompiledRoot);
			bWorkerCompiledRoot = addOperator(secRoots.get(bWorkerId).getPlanRoot(), bWorkerId, new ArrayList<Operator>());
			System.out.println("[CODE]QueryCompiler CompiledRoot(" + bWorkerId + "):" + Utilities.getPackageClassName(bWorkerCompiledRoot.getPackageName()));
			inferExecutionSegment(bWorkerCompiledRoot, bWorkerId);
			workerCompiledRoot.put(bWorkerId, bWorkerCompiledRoot);
		}
		//SMCQLQueryExecutor uses executionSegments and workerCompiledRoot to run
	}

	public QueryCompiler(SecureRelRoot q, Mode m) throws Exception {
		
		queryPlan = q;
		mode = m;
		smcFiles = new ArrayList<String>();
		sqlFiles = new ArrayList<String>();
		sqlCode = new HashMap<ExecutionStep, String>();
		smcCode = new HashMap<ExecutionStep, String>();
		executionSegments = new ArrayList<ExecutionSegment>();

		allSteps = new HashMap<Operator, ExecutionStep>();
	
		queryId = q.getName();
		Operator root = q.getPlanRoot();
		
		// single plaintext executionstep if no secure computation detected
		if(root.getExecutionMode() == ExecutionMode.Plain) {
			compiledRoot = generatePlaintextStep(root);
			ExecutionSegment segment = createSegment(compiledRoot);
			executionSegments.add(segment);
		}
		else {  // recurse
			compiledRoot = addOperator(root, new ArrayList<Operator>());
		}
		
		inferExecutionSegment(compiledRoot);
	}
	
	public void writeToDisk() throws Exception {
		
		String targetPath = Utilities.getCodeGenTarget() + "/" + queryId;
		Utilities.cleanDir(targetPath);


		Utilities.mkdir(targetPath + "/sql");

		Utilities.mkdir(targetPath + "/smc");
		
		
		for(Entry<ExecutionStep, String> e : sqlCode.entrySet()) {
			CodeGenerator cg = e.getKey().getCodeGenerator();
			String targetFile = cg.destFilename(ExecutionMode.Plain);
			sqlFiles.add(targetFile);
			Utilities.writeFile(targetFile, e.getValue());
		}
		
		for(Entry<ExecutionStep, String> e : smcCode.entrySet()) {
			CodeGenerator cg = e.getKey().getCodeGenerator();
			String targetFile = cg.destFilename(ExecutionMode.Secure);
			smcFiles.add(targetFile);
			if(e.getValue() != null) // no ctes
				Utilities.writeFile(targetFile, e.getValue());
		}

	}	
	
	public List<String> getClasses() throws IOException, InterruptedException {
	
		File path = new File(Utilities.getCodeGenTarget() + "/org/smcql/generated/" +queryId);
		String[] extensions = new String[1];
		extensions[0] = "class";
		Collection<File> files = FileUtils.listFiles(path, extensions, true);
		List<String> filenames = new ArrayList<String>();
		
		for(File f : files) {
			filenames.add(f.toString());
		}
		return filenames;
	}
	
	public void loadClasses() throws IOException, InterruptedException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
		List<String> classFiles = getClasses();
		for(String classFile : classFiles) {
			ClassPathUpdater.add(classFile);
		}
	}

	private String getPackageClassName(String fullname){
		return fullname.substring(fullname.lastIndexOf(".") + 1);
	}
	private String getCombineOperatorName(List<Operator> opsToCombine){
		if(opsToCombine.size() == 0){
			return "";
		}else{
			StringBuilder sb = new StringBuilder();
			sb.append(" |+| ");
			for(Operator op: opsToCombine){
				sb.append(RelOptUtil.toString(op.getSecureRelNode().getRelNode()).split("\\n")[0]);
				sb.append(" ");
			}
			return sb.toString();
		}
	}
	private ExecutionStep addOperator(Operator o, String workerId, List<Operator> opsToCombine) throws Exception {
		System.out.println("[CODE]QueryCompiler(" + queryPlan.getName() + ":" + workerId + ") addOperator:[" + RelOptUtil.toString(o.getSecureRelNode().getRelNode()).split("\\n")[0] + getCombineOperatorName(opsToCombine) + "] as [" + getPackageClassName(o.getPackageName()) + "]");
		o.workerId = workerId;
		if(o instanceof CommonTableExpressionScan) {
			System.out.println("[CODE]QueryCompiler(" + queryPlan.getName() + ":" + workerId + ") addOperator CommonTableExpressionScan:" + o.getPackageName());
			Operator child = o.getSources().get(0);
			SecureBufferPool.getInstance().addPointer(o.getPackageName(), child.getPackageName());
		} 
	
		if(allSteps.containsKey(o) && allSteps.get(o).getWorkerId().equals(workerId)) {
			System.out.println("[CODE]QueryCompiler(" + queryPlan.getName() + ":" + workerId + ") addOperator Operator added:" + o.getPackageName());
			return allSteps.get(o);
		}

		if(o.getExecutionMode() == ExecutionMode.Plain) { // child of a secure leaf
			System.out.println("[CODE]QueryCompiler(" + queryPlan.getName() + ":" + workerId + ") addOperator ExecutionMode.Plain:" + o.getPackageName());
			return generatePlaintextStep(o, workerId);
		}
		
		// secure case
		List<ExecutionStep> merges = new ArrayList<ExecutionStep>();
		List<ExecutionStep> localChildren = new ArrayList<ExecutionStep>();
		for(Operator child : o.getSources()) {
			System.out.println("[CODE]QueryCompiler(" + queryPlan.getName() + ":" + workerId + ") addOperator child Operator:[" + RelOptUtil.toString(child.getSecureRelNode().getRelNode()).split("\\n")[0] + "] as [" + getPackageClassName(child.getPackageName()) + "]");
			List<Operator> nextToCombine = new ArrayList<Operator>();
			while (child instanceof Filter || child instanceof Project) {
				if (child instanceof Filter) {
					opsToCombine.add(child);
				} else {
					nextToCombine.add(child);
				}
				child = child.getChild(0);
				child.workerId = workerId;
			}
			
			if(child.getExecutionMode() != o.getExecutionMode()) { // secure leaf 
				System.out.println("[CODE]QueryCompiler(" + queryPlan.getName() + ":" + workerId + ") addOperator merge:" + getPackageClassName(o.getPackageName()) + "(" + o.getExecutionMode() + ")," + getPackageClassName(child.getPackageName()) + "(" + child.getExecutionMode() + ")");
				ExecutionStep childSource = null;
				Operator tmp = o;
				if (child.getExecutionMode() == ExecutionMode.Plain) {
					//splittable:Distinct, Sort, WindowAggregate, Aggregate
					Operator plain = (o.isSplittable() && !(o instanceof WindowAggregate)) ? o : child;
					System.out.println("[CODE]QueryCompiler(" + queryPlan.getName() + ":" + workerId + ") addOperator child plain:" + getPackageClassName(plain.getPackageName()));
					childSource = generatePlaintextStep(plain, workerId);
					if(childSource == null){
						continue;
					}
				} else {
					System.out.println("[CODE]QueryCompiler(" + queryPlan.getName() + ":" + workerId + ") addOperator child recurse:" + getPackageClassName(child.getPackageName()));
					childSource = addOperator(child, workerId, nextToCombine);
					if(childSource == null){
						continue;
					}
				}
				ExecutionStep mergeStep = addMerge(tmp, childSource);
				childSource.setParent(mergeStep);
				localChildren.add(mergeStep);
				merges.add(mergeStep);
			} else {
				System.out.println("[CODE]QueryCompiler(" + queryPlan.getName() + ":" + workerId + ") addOperator non-merge:" + getPackageClassName(o.getPackageName()) + "(" + o.getExecutionMode() + ")," + getPackageClassName(child.getPackageName()) + "(" + child.getExecutionMode() + ")");
				ExecutionStep e = addOperator(child, workerId, nextToCombine);
				if(e == null){
					continue;
				}
				localChildren.add(e);
			}
		}  // end iterating over children

		ExecutionStep secStep = null;
		if(o instanceof Sort) {
			System.out.println("[CODE]QueryCompiler(" + queryPlan.getName() + ":" + workerId + ") addOperator Sort:" + getPackageClassName(o.getPackageName()));
			Operator sortChild = o.getChild(0);
			if(CodeGenUtils.isSecureLeaf(o) || sortChild.sharesComputeOrder(o)) { // implement splittable join
				secStep = localChildren.get(0); 
			}
		}

		if(secStep == null) {
			System.out.println("[CODE]QueryCompiler(" + queryPlan.getName() + ":" + workerId + ") addOperator generateSecureStep:" + getPackageClassName(o.getPackageName()));
			secStep = generateSecureStep(o, localChildren, opsToCombine, merges);
		}
		
		return secStep;
	}
	private ExecutionStep addOperator(Operator o, List<Operator> opsToCombine) throws Exception {
		System.out.println("[CODE]QueryCompiler(" + queryPlan.getName() + ") addOperator:[" + RelOptUtil.toString(o.getSecureRelNode().getRelNode()).split("\\n")[0] + getCombineOperatorName(opsToCombine) + "] as [" + getPackageClassName(o.getPackageName()) + "]");
		o.workerId = "Symmetry";
		if(o instanceof CommonTableExpressionScan) {
			System.out.println("[CODE]QueryCompiler(" + queryPlan.getName() + ") addOperator CommonTableExpressionScan:" + o.getPackageName());
			Operator child = o.getSources().get(0);
			SecureBufferPool.getInstance().addPointer(o.getPackageName(), child.getPackageName());
		} 
	
		if(allSteps.containsKey(o)) {
			System.out.println("[CODE]QueryCompiler(" + queryPlan.getName() + ") addOperator Operator added:" + o.getPackageName());
			return allSteps.get(o);
		}

		if(o.getExecutionMode() == ExecutionMode.Plain) { // 安全叶子的孩子
			System.out.println("[CODE]QueryCompiler(" + queryPlan.getName() + ") addOperator ExecutionMode.Plain:" + o.getPackageName());
			return generatePlaintextStep(o);
		}
		
		// secure case
		List<ExecutionStep> merges = new ArrayList<ExecutionStep>();
		List<ExecutionStep> localChildren = new ArrayList<ExecutionStep>();
		for(Operator child : o.getSources()) {
			System.out.println("[CODE]QueryCompiler(" + queryPlan.getName() + ") addOperator child Operator:[" + RelOptUtil.toString(child.getSecureRelNode().getRelNode()).split("\\n")[0] + "] as [" + getPackageClassName(child.getPackageName()) + "]");
			List<Operator> nextToCombine = new ArrayList<Operator>();
			while (child instanceof Filter || child instanceof Project) {
				if (child instanceof Filter) {
					opsToCombine.add(child);
				} else {
					nextToCombine.add(child);//准备合并的
				}
				child = child.getChild(0);//向下继续遍历
				child.workerId = "Symmetry";
			}
			
			if(child.getExecutionMode() != o.getExecutionMode()) { // secure leaf 
				System.out.println("[CODE]QueryCompiler(" + queryPlan.getName() + ") addOperator merge: o-" + getPackageClassName(o.getPackageName()) + "(" + o.getExecutionMode() + "), child-" + getPackageClassName(child.getPackageName()) + "(" + child.getExecutionMode() + ")");
				ExecutionStep childSource = null;
				Operator tmp = o;
				if (child.getExecutionMode() == ExecutionMode.Plain) {
					Operator plain = (o.isSplittable() && !(o instanceof WindowAggregate)) ? o : child;
					System.out.println("[CODE]QueryCompiler(" + queryPlan.getName() + ") addOperator child ExecutionMode.plain:" + getPackageClassName(plain.getPackageName()));
					childSource = generatePlaintextStep(plain);//递归结束点
				} else {
					System.out.println("[CODE]QueryCompiler(" + queryPlan.getName() + ") addOperator child 递归:" + getPackageClassName(child.getPackageName()));
					childSource = addOperator(child, nextToCombine);
				}
				ExecutionStep mergeStep = addMerge(tmp, childSource);
				childSource.setParent(mergeStep);
				localChildren.add(mergeStep);
				merges.add(mergeStep);
			} else {
				System.out.println("[CODE]QueryCompiler(" + queryPlan.getName() + ") addOperator non-merge: o-" + getPackageClassName(o.getPackageName()) + "(" + o.getExecutionMode() + "), child-" + getPackageClassName(child.getPackageName()) + "(" + child.getExecutionMode() + ")");
				ExecutionStep e = addOperator(child, nextToCombine);
				localChildren.add(e);
			}
		}  // 结束对子级的迭代

		ExecutionStep secStep = null;
		if(o instanceof Sort) {
			System.out.println("[CODE]QueryCompiler(" + queryPlan.getName() + ") addOperator Sort: o-" + getPackageClassName(o.getPackageName()));
			Operator sortChild = o.getChild(0);
			if(CodeGenUtils.isSecureLeaf(o) || sortChild.sharesComputeOrder(o)) { // implement splittable join
				secStep = localChildren.get(0); 
			}
		}

		if(secStep == null) {
			secStep = generateSecureStep(o, localChildren, opsToCombine, merges);
		}
		
		return secStep;
	}
	
	
	//将给定步骤添加到全局集合，并将执行信息添加到步骤，以供触发执行
	private void processStep(PlaintextStep step) throws Exception {
		System.out.println("[CODE]QueryCompiler(" + queryPlan.getName() + ") processStep 将给定步骤添加到全局集合，并将执行信息添加到步骤:" + getPackageClassName(step.getPackageName()));
		Operator op = step.getSourceOperator();
		String sql = op.generate();
		allSteps.put(op, step);
		sqlCode.put(step, sql);

		//get parent join operator
		Operator srcOperator = step.getSourceOperator();
		Join join = null;
		while(join == null && srcOperator != null){
			if(srcOperator.getSecureRelNode().getRelNode() instanceof LogicalJoin){
				join = (Join)srcOperator;
			}else{
				srcOperator = srcOperator.getParent();
			}
		}
		if(join != null){
			step.setJoinId(join.joinId);
		}
	}

	private PlaintextStep generatePlaintextStep(Operator op, PlaintextStep prevStep, String workerId) throws Exception {
		System.out.println("[CODE]QueryCompiler (" + queryPlan.getName() + ") generatePlaintextStep(" + op.toString() + "," + prevStep + ")");
		op.workerId = workerId;
		RunConfig pRunConf = new RunConfig();
		pRunConf.port = 54321; // does not matter for plaintext
		pRunConf.smcMode = mode;
				
		if (prevStep == null) {
			PlaintextStep result = new PlaintextStep(op, pRunConf, null); 
			//todo: brute force
			if(!result.getExec().getSourceSQL().contains(workerTable.get(workerId))){
				System.out.println("[CODE]QueryCompiler(" + queryPlan.getName() + ":" + workerId + ") generatePlaintextStep non executable");
				result.setExecutable(false);
			}else{
				System.out.println("[CODE]QueryCompiler PlainStepSQL:\n" + result.getExec().getSourceSQL());
			}
			result.setWorkerId(workerId);
			processStep(result);
			return result;
		}
		//todo: should not get here
		System.out.println("[CODE]QueryCompiler(" + queryPlan.getName() + ":" + workerId + ") generatePlaintextStep PlaintextStep root has prevStep");
		PlaintextStep curStep = prevStep;
		if (op.isBlocking() || op.getParent() == null) {
			curStep = new PlaintextStep(op, pRunConf, null);
			curStep.setParent(prevStep);
			if (prevStep != null)
				prevStep.addChild(curStep);
			processStep(curStep);
		}	
		
		for (Operator child : op.getChildren()) {	
			PlaintextStep nextStep = generatePlaintextStep(child, curStep, workerId);		
			if (nextStep != null) {
				curStep.addChild(nextStep);
				nextStep.setParent(curStep);
			}
		}
		return  curStep;
	}
	private ExecutionStep generatePlaintextStep(Operator op, String workerId) throws Exception {
		return generatePlaintextStep(op, null, workerId);
	}

	// 从给定的操作符和父步骤创建 PlaintextStep 计划
	private PlaintextStep generatePlaintextStep(Operator op, PlaintextStep prevStep) throws Exception {
		System.out.println("[CODE]QueryCompiler (" + queryPlan.getName() + ") generatePlaintextStep(" + op.toString() + "，" + prevStep + ")");
		RunConfig pRunConf = new RunConfig();
		pRunConf.port = 54321; // 对于明文无关紧要
		pRunConf.smcMode = mode;

		if (prevStep == null) {
			System.out.println("[CODE]QueryCompiler(" + queryPlan.getName() + ") generatePlaintextStep PlaintextStep");
			PlaintextStep result = new PlaintextStep(op, pRunConf, null); 
			processStep(result);
			return result;
		}
		
		PlaintextStep curStep = prevStep;
		if (op.isBlocking() || op.getParent() == null || prevStep == null) {
			curStep = new PlaintextStep(op, pRunConf, null);
			curStep.setParent(prevStep);
			if (prevStep != null)
				prevStep.addChild(curStep);
			processStep(curStep);
		}	
		
		for (Operator child : op.getChildren()) {	
			PlaintextStep nextStep = generatePlaintextStep(child, curStep);		
			if (nextStep != null) {
				curStep.addChild(nextStep);
				nextStep.setParent(curStep);
			}
		}
		
		return  curStep;
	}
	private ExecutionStep generatePlaintextStep(Operator op) throws Exception {
		return generatePlaintextStep(op, (PlaintextStep)null);
	}
	
	// 尚未为拆分执行实现join, child.getSourceOp可能等于拆分执行的op。 join not yet implemented for split execution child.getSourceOp may be equal to op for split execution
	private ExecutionStep addMerge(Operator op, ExecutionStep child) throws Exception {
		// 将输入元组与另一方合并
		MergeMethod merge = null;
		if(op instanceof Join) { // inserts merge for specified child
			System.out.println("[CODE]QueryCompiler(" + queryPlan.getName() + ") addMerge Join child executable:" + child.getExecutable());
			Operator childOp = child.getSourceOperator();
			Join joinOp = (Join) op;
			Operator leftChild = joinOp.getChild(0).getNextValidChild();
			Operator rightChild = joinOp.getChild(1).getNextValidChild();
			
			boolean isLhs = (leftChild == childOp); 
			List<SecureRelDataTypeField> orderBy = (isLhs) ? leftChild.secureComputeOrder() : rightChild.secureComputeOrder();
			merge = new MergeMethod(op, child, orderBy);	
		} else {
			System.out.println("[CODE]QueryCompiler(" + queryPlan.getName() + ") addMerge NonJoin");
			 merge = new MergeMethod(op, child, op.secureComputeOrder());
		}
		merge.compileIt();
		
		RunConfig mRunConf = new RunConfig();
		mRunConf.port = (SystemConfiguration.getInstance()).readAndIncrementPortCounter();
		mRunConf.smcMode = mode;
		
		 if(child.getSourceOperator() instanceof CommonTableExpressionScan) {
			 String src = child.getPackageName();
			 String dst = merge.getPackageName();
			 SecureBufferPool.getInstance().addPointer(src, dst);
		 }

		SecureStep mergeStep = new SecureStep(merge, op, mRunConf, child, null);
		child.setParent(mergeStep);
		smcCode.put(mergeStep, merge.generate());
		return mergeStep;
	}
	
	
	private ExecutionStep generateSecureStep(Operator op, List<ExecutionStep> children, List<Operator> opsToCombine, List<ExecutionStep> merges) throws Exception {
		SecureOperator secOp = SecureOperatorFactory.get(op);
		if (!merges.isEmpty())
			secOp.setMerges(merges);
		
		for (Operator cur : opsToCombine) {
			if (cur instanceof Filter) {
				secOp.addFilter((Filter) cur);
			} else if (cur instanceof Project) {
				secOp.addProject((Project) cur);
			}
		}
		secOp.compileIt();
	
		RunConfig sRunConf = new RunConfig();
		
		sRunConf.port = (SystemConfiguration.getInstance()).readAndIncrementPortCounter();
		sRunConf.smcMode = mode;
		sRunConf.host = getAliceHostname();
		
		SecureStep smcStep = null;

		if(children.size() == 1) {
			ExecutionStep child = children.get(0);
			smcStep = new SecureStep(secOp, op, sRunConf, child, null);
			child.setParent(smcStep);
		}
		else if(children.size() == 2) {// join
			ExecutionStep lhsChild = children.get(0);
			ExecutionStep rhsChild = children.get(1);
			smcStep = new SecureStep(secOp, op, sRunConf, lhsChild, rhsChild);
			lhsChild.setParent(smcStep);
			rhsChild.setParent(smcStep);
		}
		else {
			throw new Exception("Operator cannot have >2 children.");
		}
		smcStep.setWorkerId(op.workerId);
		allSteps.put(op, smcStep);
		String code = secOp.generate();
		smcCode.put(smcStep, code);
		return smcStep;
	}

	public Map<ExecutionStep, String> getSMCCode() {
		return smcCode;
	}
	
	public Map<ExecutionStep, String> getSQLCode() {
		return sqlCode;
	}
	
	private String getAliceHostname() throws Exception {
		ConnectionManager cm = ConnectionManager.getInstance(); 
		String alice = cm.getAlice();
		return cm.getWorker(alice).hostname;
	}
	
	private void inferExecutionSegment(ExecutionStep step, String workerId) throws Exception  {
		step.setWorkerId(workerId);
		if(step instanceof PlaintextStep) 
			return;
		
		SecureStep secStep = (SecureStep) step;
		
		if(secStep.getExec().parentSegment != null) {
			return;
		}
		
		// if root node
		if(secStep.getParent() == null) {
			System.out.println("[CODE]inferExecutionSegment(" + queryPlan.getName() + ") createSegment root segment");
			ExecutionSegment segment = createSegment(secStep);
			segment.workerId = workerId;
			System.out.println("[CODE]inferExecutionSegment(" + queryPlan.getName() + ") add root segment:" + segment.toString());
			executionSegments.add(segment);
			secStep.getExec().parentSegment = segment;
		}
		else { // non-root
			System.out.println("[CODE]inferExecutionSegment(" + queryPlan.getName() + ") createSegment non-root segment");
			Operator parentOp = secStep.getParent().getSourceOperator();
			Operator localOp = secStep.getSourceOperator();
			SecureStep parent = (SecureStep) step.getParent();
	
			if(localOp.sharesExecutionProperties(parentOp)) { // same segment
				secStep.getExec().parentSegment = parent.getExec().parentSegment;
			}
			else { // create new segment
				ExecutionSegment current = createSegment(secStep);
				current.workerId = workerId;
				System.out.println("[CODE]inferExecutionSegment(" + queryPlan.getName() + ") add segment:" + current.toString());
				executionSegments.add(current);	
				secStep.getExec().parentSegment = current;
			}
		}
		
		List<ExecutionStep> sources = secStep.getChildren();
		for(ExecutionStep s : sources) 
			inferExecutionSegment(s, workerId);
	}	
	private void inferExecutionSegment(ExecutionStep step) throws Exception  {//推断执行部分
		step.setWorkerId("Symmetry");
		if(step instanceof PlaintextStep) 
			return;
		
		SecureStep secStep = (SecureStep) step;
		
		if(secStep.getExec().parentSegment != null) {
			return;
		}
		
		// if root node
		if(secStep.getParent() == null) {
			System.out.println("[CODE]inferExecutionSegment(" + queryPlan.getName() + ") createSegment root segment");
			ExecutionSegment segment = createSegment(secStep);
			System.out.println("[CODE]inferExecutionSegment(" + queryPlan.getName() + ") add root segment:" + segment.toString());
			executionSegments.add(segment);
			secStep.getExec().parentSegment = segment;
		}
		else { // non-root
			System.out.println("[CODE]inferExecutionSegment(" + queryPlan.getName() + ") createSegment non-root segment");
			Operator parentOp = secStep.getParent().getSourceOperator();
			Operator localOp = secStep.getSourceOperator();
			SecureStep parent = (SecureStep) step.getParent();
	
			if(localOp.sharesExecutionProperties(parentOp)) { // same segment
				secStep.getExec().parentSegment = parent.getExec().parentSegment;
			}
			else { // create new segment
				ExecutionSegment current = createSegment(secStep);
				System.out.println("[CODE]inferExecutionSegment(" + queryPlan.getName() + ") add segment:" + current.toString());
				executionSegments.add(current);	
				secStep.getExec().parentSegment = current;
			}
		}
		
		List<ExecutionStep> sources = secStep.getChildren();
		for(ExecutionStep s : sources) 
			inferExecutionSegment(s);
	}

	public SecureRelRoot getPlan() {
		return queryPlan;
	}
	
	private ExecutionSegment createSegment(ExecutionStep secStep) throws Exception {
		System.out.println("[CODE]QueryCompiler createSegment " + secStep.getSourceOperator().getExecutionMode());
		ExecutionSegment current = new ExecutionSegment();
		current.rootNode = secStep.getExec();
		current.exeStep = secStep;
		current.runConf = secStep.getRunConfig();
		current.outSchema = new SecureRelRecordType(secStep.getSchema());
		current.executionMode = secStep.getSourceOperator().getExecutionMode();
		
		/*if(secStep.getSourceOperator().getExecutionMode() == ExecutionMode.Slice && userQuery != null) {
			current.sliceSpec = secStep.getSourceOperator().getSliceKey();
			PlainOperator sqlGenRoot = secStep.getSourceOperator().getPlainOperator();		
			sqlGenRoot.inferSlicePredicates(current.sliceSpec);
			current.sliceValues = sqlGenRoot.getSliceValues();
			current.complementValues = sqlGenRoot.getComplementValues();
			current.sliceComplementSQL = sqlGenRoot.generatePlaintextForSliceComplement(userQuery); //plaintext query for single site values
		}*/

		return current;
		
	}
	
	Byte[] toByteObject(byte[] primitive) {
	    Byte[] bytes = new Byte[primitive.length];
	    int i = 0;
	    for(byte b: primitive)
	    	bytes[i++] = Byte.valueOf(b); 
	    return bytes;
	}
	
}
