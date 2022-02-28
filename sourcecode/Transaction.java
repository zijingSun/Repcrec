import java.util.*;

public class Transaction{  
    int age;
    String state;
    int id;
    String type;//RO RW
    List<Operation> operations = new ArrayList<Operation>();
	List<Operation> waited_operations = new ArrayList<Operation>();
    int[] snapshot = new int[20];
    
    
    Transaction(){
    	
    }
    
    Transaction(int tid, String type, int age){
    	this.id = tid;
    	this.type = type;
    	this.age =age;
    }
    
    public void addOperation(Operation operation) {
    	operations.add(operation);
    }
    
    public void addWaitedOperation(Operation operation) {
    	waited_operations.add(operation);
    }

    public void print() {
		System.out.print("ID:"+id+" Type:"+type);
    	for(int i = 0; i<operations.size();i++) {
    		Operation op = operations.get(i);
    		System.out.print(" "+op.type+" "+op.variable+" ");
    	}
    }



} 