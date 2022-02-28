
public class Operation {
	int transaction_id;
	String type;
	int value;
	int variable;
	int age;
	boolean blocked;

	Operation(String type, int tid, int variable, int age) {
		this.type = type;
		this.transaction_id = tid;
		this.variable = variable;
		this.age = age;
	}

	Operation(String type, int tid, int variable, int value, int time) {
		this.type = type;
		this.transaction_id = tid;
		this.variable = variable;
		this.value = value;
		this.age = time;
	}

}