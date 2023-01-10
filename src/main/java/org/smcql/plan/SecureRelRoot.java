package org.smcql.plan;

import java.util.logging.Level;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlExplainLevel;
import org.smcql.config.SystemConfiguration;
import org.smcql.parser.SqlStatementParser;
import org.smcql.parser.TreeBuilder;
import org.smcql.plan.operator.Operator;

public class SecureRelRoot {
	RelRoot baseRoot;
	Operator treeRoot;
	String name;
	
	public SecureRelRoot(String queryName, String sql) throws Exception {
		SqlStatementParser parser = new SqlStatementParser();

		baseRoot = parser.convertSqlToRelMinFields(sql);
		baseRoot = parser.optimize(baseRoot); // 优化为以细粒度表示，以避免更多的smc optimized to represent in a fine granularity for more smc avoidance
		
		baseRoot = parser.trimFields(baseRoot); // 使用最小的字段集以避免触发不必要的SMC use minimal set of fields to avoid triggering unnecessary SMC
		
		baseRoot = parser.mergeProjects(baseRoot); // 放弃计划中任何不必要的步骤 drop any unnecessary steps in the plan
		
		name = (queryName == null) ? SystemConfiguration.getInstance().getQueryName() : queryName.replaceAll("-", "_");
		treeRoot = TreeBuilder.create(name, baseRoot);//轻版本的查询规划器,采用逻辑RelNode树生成物理操作符树
	}
	
	
	public RelRoot getRelRoot() {
		return baseRoot;
	}
	
	public Operator getPlanRoot() {
		return treeRoot;
	}
	
	public String toString() {
		return appendOperator(treeRoot, new String(), "");
	}
	
	public String getName() {
		return name;
	}
	
	private void addSqlGenerationNode(RelRoot node) {
		treeRoot.addSqlGenerationNode(node.project());
	}
	
	String appendOperator(Operator op, String src, String linePrefix) {
		src += linePrefix + op + "\n";
		linePrefix += "    ";
		for(Operator child : op.getChildren()) {
			src = appendOperator(child, src, linePrefix);
		}
		return src;
	}
	
}
