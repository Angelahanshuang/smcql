package org.smcql.executor.step;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.util.ImmutableIntList;
import org.smcql.codegen.CodeGenerator;
import org.smcql.codegen.smc.operator.support.MergeMethod;
import org.smcql.type.SecureRelRecordType;
import org.smcql.executor.config.RunConfig;
import org.smcql.executor.smc.OperatorExecution;
import org.smcql.plan.operator.Join;
import org.smcql.plan.operator.Operator;

public class SecureStep implements ExecutionStep, Serializable {

	transient CodeGenerator codeGenerator;
	transient Operator srcOperator;
	ExecutionStep parent;
	List<ExecutionStep> children;
	RunConfig runConf;
	boolean visited = false;
	String workerId;
	
	// hold on to state for serialization 
	boolean isMerge = false;
	OperatorExecution exec;
	boolean executable = true;
	
	public SecureStep(CodeGenerator cg, Operator op, RunConfig r, ExecutionStep lhsChild, ExecutionStep rhsChild) throws Exception {
		codeGenerator = cg;
		srcOperator = op;
		runConf = r;
		executable = lhsChild.getExecutable();
		
		children = new ArrayList<ExecutionStep>();
		children.add(lhsChild);
		if(rhsChild != null)
			children.add(rhsChild);
		
		exec = new OperatorExecution(this);
		
		// needed to avoid serializing whole code generator
		if(cg instanceof MergeMethod)  {
			isMerge = true;
			exec.setSourceSQL(((MergeMethod) cg).getSourceSQL());

			MergeMethod merge = (MergeMethod)cg;
			exec.joinId = merge.childStep.getJoinId();
		}
		if(op instanceof Join){
			exec.isJoin = true;
			exec.joinId = ((Join)op).joinId;
		}
	}
	
	@Override
	public void setJoinId(List<String> joinId) {
		exec.joinId = joinId;
		System.out.println("[CODE]PlaintextStep joinId:" + exec.joinId);
	}
	@Override
	public List<String> getJoinId() {
		return exec.joinId;
	}
	
	@Override
	public String getWorkerId(){
		return workerId;
	}
	@Override
	public void setWorkerId(String workerId){
		this.workerId = workerId;
	}
	@Override
	public boolean getExecutable(){
		return executable;
	}
	public void setExecutable(boolean executable){
		this.executable = executable;
	}

	@Override
	public String generate() throws Exception {
		return codeGenerator.generate();
	}

	@Override
	public OperatorExecution getExec() {
		return exec;
	}
	
	@Override
	public String getPackageName() {
		return codeGenerator.getPackageName();
	}

	@Override
	public ExecutionStep getParent() {
		return parent;
	}
	
	public void setParent(ExecutionStep p) {
		parent = p;
	}

	@Override
	public ExecutionStep getChild(int idx) {
		return (children.size() > idx) ? children.get(idx) : null;
		
	}

	@Override
	public SecureRelRecordType getInSchema() {
		return codeGenerator.getInSchema();
	}

	@Override
	public SecureRelRecordType getSchema() {
		return codeGenerator.getSchema();
	}

	@Override
	public RunConfig getRunConfig() {
		return runConf;
	}

	@Override
	public Operator getSourceOperator() {
		return srcOperator;
	}

	
		

	@Override
	public CodeGenerator getCodeGenerator() {
		return codeGenerator;
	}

	@Override
	public List<ExecutionStep> getChildren() {
		return children;
	}

	@Override
	public boolean visited() {
		return visited;
	}

	@Override
	public void visit() {
		 visited = true;
	}

	@Override
	public void setHostname(String host) {
		runConf.host = host;
		
	}


	@Override
	public SecureRelRecordType getSchema(boolean forSecureLeaf) {
		return getSchema();
	}


	@Override
	public String printTree() {
		return appendOperator(this, new String(), "");
	}

	private String getPackageClassName(String fullname){
		return fullname.substring(fullname.lastIndexOf(".") + 1);
	}	
	private String appendOperator(ExecutionStep step, String src, String linePrefix) {
		String opString = step.getSourceOperator().toString();
		if (step instanceof SecureStep && ((SecureStep) step).isMerge)
			opString = "LogicalMerge(Secure, " + getPackageClassName(step.getSourceOperator().getPackageName()) + ")";
		src += linePrefix + opString + ", Executable:" + step.getExecutable() + "\n";
		linePrefix += "    ";
		for(ExecutionStep child : step.getChildren()) {
			src = appendOperator(child, src, linePrefix);
		}
		return src;
	}
}
