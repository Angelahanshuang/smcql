package org.smcql.plan.operator;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableIntList;
import org.smcql.executor.config.RunConfig.ExecutionMode;
import org.smcql.plan.SecureRelNode;
import org.smcql.type.SecureRelDataTypeField;
import org.smcql.type.SecureRelDataTypeField.SecurityPolicy;
import org.smcql.type.SecureRelRecordType;
import org.smcql.util.Utilities;

public class Join extends Operator {

	public List<String> joinId = null;
	public Join(String name, SecureRelNode src, Operator ...children ) throws Exception {
		super(name, src, children);
		LogicalJoin join = (LogicalJoin)src.getRelNode();
		System.out.println("[CODE]OperatorFactory::get node join condition===[" + join.getCondition() +"]===");
		System.out.println("[CODE]OperatorFactory::get node join schema:" + src.getSchema());
		JoinInfo ji = join.analyzeCondition();
		if(ji != null){
			List<ImmutableIntList> joins = ji.keys();
			if(ji.isEqui()){
				System.out.println("[CODE]OperatorFactory::get node equi-join keys:" + joins);
			}else{
				System.out.println("[CODE]OperatorFactory::get node join keys:" + joins);
			}
			if(joins != null && joins.size() == 2){
				System.out.println("[CODE]OperatorFactory::get node join LHS keys:" + joins.get(0));
				System.out.println("[CODE]OperatorFactory::get node join RHS keys:" + joins.get(1));
				joinId = new ArrayList<>();
				joinId.add(joins.get(0).toString());
				joinId.add(joins.get(1).toString());
			}
		}
	}
	
	

	@Override
	public void inferExecutionMode() {
		SecurityPolicy maxAccess = maxAccessLevel();

		super.inferExecutionMode();
		
		if(children.get(0).executionMode.compareTo(ExecutionMode.Plain) <= 0 && 
				children.get(1).executionMode.compareTo(ExecutionMode.Plain) <= 0 && maxAccess == SecurityPolicy.Public) { // results are replicated
				executionMode = ExecutionMode.Plain;
		}
	}
	

	public List<SecureRelDataTypeField> getSliceAttributes() {
		
		List<SecureRelDataTypeField> sliceKey = new ArrayList<SecureRelDataTypeField>();
		
		LogicalJoin join = (LogicalJoin) this.getSecureRelNode().getRelNode();
		
		assert(join.getJoinType() == JoinRelType.INNER || join.getJoinType() == JoinRelType.FULL);
		
		RexNode joinOn = join.getCondition();
		
		if(joinOn == null)
			return sliceKey; // no keys
		
		
		RexBuilder rexBuilder = join.getCluster().getRexBuilder();
		
		joinOn = RexUtil.toCnf(rexBuilder, joinOn);  // get it to conjunctive normal form for easier optimization
		
		if(joinOn.getKind() == SqlKind.AND) { 
			List<RexNode> operands = new ArrayList<RexNode>(((RexCall) joinOn).operands);
			for(RexNode op : operands) {
				sliceKey = checkForSliceField(op, sliceKey);
								
			}
		}
		else // single comparison
			sliceKey = checkForSliceField(joinOn, sliceKey);
		
		assert(sliceKey.size() <= 2); // multiple, composite slice keys not implemented
		return sliceKey;
	}

	
	// equality predicates only
	List<SecureRelDataTypeField> checkForSliceField(RexNode rex, List<SecureRelDataTypeField> sliceKey) {

		SecureRelRecordType inSchema = getInSchema();
		
		if(rex.getKind() == SqlKind.EQUALS) { // equality predicate
			RexCall cOp = (RexCall) rex;
			RexNode lhs = cOp.getOperands().get(0);
			RexNode rhs = cOp.getOperands().get(1);
		
			if(lhs.getKind() == SqlKind.INPUT_REF && rhs.getKind() == SqlKind.INPUT_REF) {
				RexInputRef lhsRef = (RexInputRef) lhs;
				RexInputRef rhsRef = (RexInputRef) rhs;
				
				int lOrdinal = lhsRef.getIndex();
				int rOrdinal = rhsRef.getIndex();
				
				SecureRelDataTypeField lField = inSchema.getSecureField(lOrdinal);
				SecureRelDataTypeField rField = inSchema.getSecureField(rOrdinal);
				
				
					if(lField.isSliceAble() && rField.isSliceAble())  {							
						if(!sliceKey.contains(lField))
								sliceKey.add(lField);
						if(!sliceKey.contains(rField))
								sliceKey.add(rField);
					}
						
	
				}
	
		}
		return sliceKey;
	}
	
	public SecureRelRecordType getInSchema() {
		
		return  getSchema();
	}
	
	public List<SecureRelDataTypeField> computesOn() {

		LogicalJoin join = (LogicalJoin) this.getSecureRelNode().getRelNode();
		RexNode joinOn = join.getCondition();
		
		return AttributeResolver.getAttributes(joinOn, getSchema());
		
	}



	public RexNode getCondition() {
		LogicalJoin join = (LogicalJoin) this.getSecureRelNode().getRelNode();
		return join.getCondition();
	}
		
};
