package org.smcql.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Timestamp;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.smcql.config.SystemConfiguration;
import org.smcql.db.data.QueryTable;
import org.smcql.db.data.Tuple;
import org.smcql.executor.smc.BasicSecureQueryTable;
import org.smcql.executor.smc.OperatorExecution;
import org.smcql.executor.smc.runnable.SMCRunnable;
import org.smcql.type.SecureRelDataTypeField;
import org.smcql.type.SecureRelRecordType;

import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.flexsc.Party;
import com.oblivm.backend.gc.GCGenComp;
import com.oblivm.backend.gc.GCSignal;
import com.oblivm.backend.lang.inter.Util;
import com.oblivm.backend.oram.SecureArray;
import com.oblivm.backend.util.Utils;
import org.smcql.db.data.field.BooleanField;
import org.smcql.db.data.field.CharField;
import org.smcql.db.data.field.IntField;
import org.smcql.db.data.field.TimestampField;

public class SMCUtils {

	
	public static String toString(GCSignal[] tArray) {
		String ret = tArray[0].toHexStr();
		for(int i = 1; i < tArray.length; ++i) {
			ret += " " + tArray[1].toHexStr();
		}
		
		return ret;
	}
	
	public static String toString(GCSignal[][] tArray) {
		String ret = new String();
		for(int i = 0; i < tArray.length; ++i) {
			ret += i + ": " + toString(tArray[i]) + "\n";
		}
		
		return ret;
	}
	
	
	// decoupled version of CircuitLib::toSignals()
	public static GCSignal[] toSignals(long v, int width) {
		GCSignal[] result = new GCSignal[width];
		for (int i = 0; i < width; ++i) {
			if ((v & 1) == 1)
				result[i] = new GCSignal(true);
			else
				result[i] = new GCSignal(false);
			v >>= 1;
		}
		return result;
		
	}
	
	// standalone version of Util::secArrayToInt
	@SuppressWarnings("unchecked")
	public static<T> T[] flattenSecArray(CompEnv<T> env, SecureArray<T> secInput) throws Exception {
		T[] intArray = null;
		int arraySize = secInput.length;
		
		for(long i=0; i<arraySize; ++i) {
			if(i==0) {
				T[] idx = (T[]) SMCUtils.toSignals(i, 64);
				intArray = secInput.read(idx);
			} else {
				intArray = concat(intArray, secInput.read((T[]) SMCUtils.toSignals(i, 64)));
			}
		}
		
		return intArray;
	}

	public static<T> byte[] secArrayToBytes(CompEnv<T> env, SecureArray<T> secInput) throws Exception {
		byte[] output = new byte[secInput.dataSize * secInput.length * 10];
		int arraySize = secInput.length;
		T[] intArray = null;
		int dstIdx = 0;
		
		for(long i=0; i<arraySize; ++i) {
				@SuppressWarnings("unchecked")
				T[] idx = (T[]) SMCUtils.toSignals(i, 64);
				intArray = secInput.read(idx);
				for(int j = 0; j < intArray.length; ++j) {
					GCSignal signal = (GCSignal) intArray[j];
					
					System.arraycopy(signal.bytes, 0, output, dstIdx, 10);
					dstIdx += 10;
				}
		}
		
		return output;
	}
	
	private static <T> T[] concat(T[] first, T[] second) {
		  T[] result = Arrays.copyOf(first, first.length + second.length);
		  System.arraycopy(second, 0, result, first.length, second.length);
		  return result;
	}
	
	public static <T> SecureArray<T> prepareLocalPlainArray(QueryTable table, CompEnv<T> env, SMCRunnable parent) throws Exception {
		if(table == null || table.tupleCount() == 0){
			parent.sendInt(0);
			return null;
		}
		boolean[] srcData = table.toBinary();
		int len = (srcData != null) ? srcData.length : 0;
		Party party = env.party;
		int tupleSize = table.getSchema().size(); 

		assert(len % tupleSize == 0);
		System.out.println("[CODE]prepareLocalPlainArray send " + table.tupleCount() + " tuples");
		parent.sendInt(len);

		if(len > 0) {
			parent.sendInt(tupleSize);
			boolean[] tupleCount = Utils.fromInt(table.tupleCount(), 32);
			T[] nonNullLength = (party == Party.Bob) ?  env.inputOfBob(tupleCount) : env.inputOfAlice(tupleCount);
			T[] tArray = (party == Party.Alice)  ? env.inputOfAlice(srcData) : env.inputOfBob(srcData);
			env.flush();

			SecureArray<T> input = Util.intToSecArray(env, tArray, tupleSize, len / tupleSize);
			input.setNonNullEntries(nonNullLength);
			env.flush();
		
			return input;
		}
		return null;
	}
	public static<T> SecureArray<T> prepareRemotePlainArray(CompEnv<T> env, SMCRunnable parent) throws Exception {
		Party party = env.party;
		int len = parent.getInt();
		if(len > 0) {
			int tupleSize = parent.getInt();
			T[] nonNullLength = (party == Party.Bob) ? env.inputOfAlice(new boolean[32]) : env.inputOfBob(new boolean[32]);
			T[] tArray = (party == Party.Bob)  ? env.inputOfAlice(new boolean[len]) : env.inputOfBob(new boolean[len]);
			env.flush();
		
			int tupleCount = len / tupleSize;
			System.out.println("[CODE]prepareRemotePlainArray recv " + tupleCount + " tuples");
			SecureArray<T> input =  Util.intToSecArray(env, tArray, tupleSize, tupleCount);
			input.setNonNullEntries(nonNullLength);
			env.flush();
			return input;
		}
		System.out.println("[CODE]prepareRemotePlainArray recv 0 tuples");
		return null;
	}

	/* MPC 
	  PrepareMpc 函数将普通数据作为输入，并生成共享的秘密数据输出。
	  SyscmdMPC 以共享秘密数据作为输入进行 MPC，并生成共享秘密数据输出。
	*/
	public static<T> SecureArray<T> prepareMpc(SecureArray<T> secArray) throws Exception {
		//input: plain data, may be null
		//output: secret shared data
		String input = secArray.fileName;
		secArray.operatorName = "merge";
		getFileName(secArray);
		System.out.println("[CODE]prepareMpc from[" + input + "] to[" + secArray.fileName + "]");
		Utilities.executeSh(String.format("cp -f %s %s", input, secArray.fileName));//todo:change cp to MPC
		return secArray;
	}
	public static<T> SecureArray<T> syscmdMPC(OperatorExecution op, SecureArray<T> lhs, SecureArray<T> rhs) throws Exception{
		if(lhs == null && rhs == null){
			return null;
		}
		String lhsInput = lhs != null ? lhs.fileName : "";
		String rhsInput = rhs != null ? rhs.fileName : "";
		if(lhsInput.isEmpty() && rhsInput.isEmpty()){
			return null;
		}
		SecureArray<T> secArray = new SecureArray<>();
		String operatorName = Utilities.getSubPackageClassName(op.packageName);
		if(!lhsInput.isEmpty() && !rhsInput.isEmpty()){
			secArray.fileName = lhsInput + "." + operatorName  + "." + Utilities.getFileName(rhsInput);
		}else if(!lhsInput.isEmpty()){
			secArray.fileName = secArray.fileName = lhsInput + "." + operatorName;
		}else if(!rhsInput.isEmpty()){
			secArray.fileName = secArray.fileName = rhsInput + "." + operatorName;
		}
		int rhsLength = (rhs != null) ? rhs.length : 0;
		int lhsLength = (lhs != null) ? lhs.length : 0;
		System.out.println("[CODE]syscmdMPC " + operatorName + " tuples[" + lhsLength + "," + rhsLength + "] LHS[" + lhsInput + "] RHS[" + rhsInput + "] schema[" + op.outSchema + "]");
		saveAsEmptyFile(secArray.fileName);//todo:change cp to MPC
		return secArray;
	}
	public static<T> void getFileName(SecureArray<T> secArray) throws Exception{
		if(secArray.fileName == null){
			if(secArray.tableName == null){
				throw new Exception("Error: empty table name");
			}
			String remotePath = null;
			try {
				remotePath = SystemConfiguration.getInstance().getProperty("remote-path");
			} catch (Exception e) {
				e.printStackTrace();
			}
			if(remotePath == null)
				remotePath = "/tmp/smcql";

			secArray.fileName = remotePath + "/" + secArray.tableName + (secArray.isLhs ? ".lsh" : ".rsh");
			return;
		}
		if(secArray.operatorName != null){
			if(!secArray.fileName.contains(secArray.operatorName))
				secArray.fileName = secArray.fileName + "." + secArray.operatorName;
			return;
		}else{
			throw new Exception("Error: empty operator name");
		}
	}

	public static void saveAsEmptyFile(String filename){
		Utilities.executeSh(String.format("cd > %s", filename));
	}
	
	//PSI
	public static<T> void syscmdPSI(SecureArray<T> secArray) throws Exception{
		String input = secArray.fileName;
		String joinId = secArray.isLhs ? secArray.joinId.get(0) : secArray.joinId.get(1);
		secArray.operatorName = "PSI";
		getFileName(secArray);
		//secArray.fileName is output file name now
		System.out.println("[CODE]syscmdPSI:" + secArray.fileName + joinId);
		Utilities.executeSh(String.format("cp -f %s %s", input, secArray.fileName));//todo:change cp to psi
	}
	public static boolean saveTable(QueryTable table, String filename){
		if(table == null){
			return false;
		}
		System.out.println("[CODE]save plain table to:" + filename);
		try {
			//todo:dedup
			String[] tuples = table.toString().split("\\n");
			PrintWriter writer = new PrintWriter(filename, "UTF-8");
			for(int i = 1; i < tuples.length; i++){
				writer.write(tuples[i].replaceAll("^\\[", "").replaceAll("\\]$", "").replaceAll(" ", "") + "\n");
			}
			writer.close();
		} catch (FileNotFoundException | UnsupportedEncodingException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	public static QueryTable loadTable(SecureRelRecordType outSchema, String filename){
		System.out.println("[CODE]load plain table from:" + filename);
		QueryTable table = new QueryTable(outSchema);
		try {
			List<String> lines = Utilities.readFile(filename);
			for(String line: lines){
				String[] tupleValues = line.split(",");
				Tuple t = new Tuple();
				int i = 0;
				for(SecureRelDataTypeField r : outSchema.getAttributes()) {
					RelDataType type = r.getBaseField().getType();
					SqlTypeName sqlType = type.getSqlTypeName();
					if(SqlTypeName.CHAR_TYPES.contains(sqlType)){
						t.addField(new CharField(r, tupleValues[i]));
					}else if(sqlType == SqlTypeName.INTEGER || sqlType == SqlTypeName.BIGINT){
						t.addField(new IntField(r, Integer.valueOf(tupleValues[i])));
					}else if(SqlTypeName.DATETIME_TYPES.contains(sqlType)) {
						t.addField(new TimestampField(r, new Timestamp(Long.valueOf(tupleValues[i]))));
					}else if(SqlTypeName.BOOLEAN_TYPES.contains(sqlType)){
						t.addField(new BooleanField(r, tupleValues[i].equalsIgnoreCase("true")));
					}else{
						System.out.println("[CODE]loadTable unknown field " + sqlType);
						break;
					}
					i++;
				}
				if(i != outSchema.getFieldCount()){
					System.out.println("[CODE]loadTable fields mismatch " + i + ":" + outSchema.getFieldCount());
					return null;
				}
				table.addTuple(t);
			}
			return table;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	public static<T> QueryTable loadTable(SecureArray<T> secArray){
		if(secArray.schema == null){
			return null;
		}
		return loadTable(secArray.schema, secArray.fileName);
	}

	public static<T> BasicSecureQueryTable prepareLocalPlaintext(QueryTable table, CompEnv<T> env, SMCRunnable parent) throws Exception {
		return prepareLocalPlaintext(null, table.tuples(), env, parent);
	}
	
	public static<T> BasicSecureQueryTable prepareLocalPlaintext(Tuple key, List<Tuple> tuples, CompEnv<T> env, SMCRunnable parent) throws Exception {
		boolean[] srcData = QueryTable.toBinary(tuples); 
		int tupleSize = (tuples.size() == 0) ? 0 : tuples.get(0).size(); // for merge ops input schema == output schema
		SecureRelRecordType schema = (tuples.size() == 0) ? null : tuples.get(0).getSchema();
		return prepareLocalPlaintext(key, srcData, tupleSize, schema, env, parent);
		
	}
	
	public static<T> BasicSecureQueryTable prepareLocalPlaintext(Tuple key, boolean[] srcData, int tupleSize, SecureRelRecordType schema, CompEnv<T> env, SMCRunnable parent) throws Exception {

		Party party = env.party;

		int len = (srcData == null) ? 0 : srcData.length;
		srcData = (srcData == null) ? new boolean[0] : srcData; 
		int tupleCount = (tupleSize == 0) ? 0 : len / tupleSize;

		assert(len % tupleSize == 0);

		parent.sendInt(len);
		if(len > 0) {
			parent.sendInt(tupleSize);
			if(key != null) {// slice mode
				parent.sendTuple(key);
			}
			
			T[] tArray;
			T[] nonNullLength;
			
			if(party == Party.Alice) {
				nonNullLength = env.inputOfAlice(Utils.fromInt(tupleCount, 32));
				env.flush();
				tArray = env.inputOfAlice(srcData);
			}
			else {
				nonNullLength = env.inputOfBob(Utils.fromInt(tupleCount, 32));
				env.flush();
				tArray = env.inputOfBob(srcData);
			}
			env.flush();
			
			@SuppressWarnings("unchecked")
			BasicSecureQueryTable output = new BasicSecureQueryTable((GCSignal[]) tArray, tupleSize, (CompEnv<GCSignal>) env, parent);
			
			output.nonNullLength = (GCSignal[]) nonNullLength;
			output.R = GCGenComp.R;
			output.schema = schema;
			
			return output;
			
		}
		
		
		return null;
	}
	
	

	@SuppressWarnings("unchecked")
	public static<T> BasicSecureQueryTable prepareRemotePlaintext(CompEnv<T> env, SMCRunnable parent) {
		Party party = env.party;

		int len = parent.getInt();
		if(len > 0) {
			int tupleSize = parent.getInt();
			T[] nonNullLength = (party == Party.Bob) ? env.inputOfAlice(new boolean[32]) : env.inputOfBob(new boolean[32]);
			env.flush();
			T[] tArray = (party == Party.Bob)  ? env.inputOfAlice(new boolean[len]) : env.inputOfBob(new boolean[len]);		
			env.flush();
			
			BasicSecureQueryTable output = new BasicSecureQueryTable((GCSignal[]) tArray, tupleSize, (CompEnv<GCSignal>) env, parent);
			output.nonNullLength = (GCSignal[]) nonNullLength;
			
			return output;
		}		
		
		return null;
	}

	@SuppressWarnings("unchecked")
	public static<T> Pair<Tuple, BasicSecureQueryTable> prepareRemoteSlicedPlaintext(CompEnv<T> env, SMCRunnable parent) {
		Party party = env.party;

		int len = parent.getInt();		

		if(len > 0) {
			int tupleSize = parent.getInt();
			Tuple slice = parent.getTuple();
			T[] nonNullLength = (party == Party.Bob) ? env.inputOfAlice(new boolean[32]) : env.inputOfBob(new boolean[32]);	
			T[] tArray = (party == Party.Bob)  ? env.inputOfAlice(new boolean[len]) : env.inputOfBob(new boolean[len]);		

			env.flush();
			BasicSecureQueryTable output = new BasicSecureQueryTable((GCSignal[]) tArray, tupleSize, (CompEnv<GCSignal>) env, parent);
			output.nonNullLength = (GCSignal[]) nonNullLength;

			return new ImmutablePair<Tuple, BasicSecureQueryTable>(slice, output);
		}			
		return null;
	}

	
}
