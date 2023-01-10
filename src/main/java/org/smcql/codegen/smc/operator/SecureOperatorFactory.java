package org.smcql.codegen.smc.operator;


import org.smcql.plan.operator.Operator;
import org.smcql.util.CodeGenUtils;

public class SecureOperatorFactory {
	
	public static SecureOperator get(Operator o) throws Exception {
		switch(o.getOpName()) {
			case "Aggregate":
				System.out.println("[CODE]SecureOperatorFactory get Aggregate");
				return new SecureAggregate(o);
			case "Sort":
				System.out.println("[CODE]SecureOperatorFactory get Sort");
				return new SecureSort(o);
			case "Distinct":
				System.out.println("[CODE]SecureOperatorFactory get Distinct");
				return new SecureDistinct(o);
			case "WindowAggregate":
				System.out.println("[CODE]SecureOperatorFactory get WindowAggregate");
				return new SecureWindowAggregate(o);
			case "Join":
				System.out.println("[CODE]SecureOperatorFactory get Join");
				return new SecureJoin(o);
			case "Filter":
				System.out.println("[CODE]SecureOperatorFactory get Filter");
				return new SecureFilter(o);
			case "Merge":
				System.out.println("[CODE]SecureOperatorFactory get Merge");
				return new SecureMerge(o);
			default:
				System.out.println("[CODE]SecureOperatorFactory get " + o.getOpName() + " is NULL");
				return null;
		}
	}
}
