import java.util.*;

public class Site {
	int site_num;
	ArrayList<ArrayList<Variable>> committed_value_age = new ArrayList<ArrayList<Variable>>(20);
	int[] committed_value = new int[20];
	ArrayList<ArrayList<Integer>> read_lock_table = new ArrayList<ArrayList<Integer>>(20);
	int[] write_lock_table = new int[20];
	String type;

	Site() {
		for (int i = 0; i < 20; i++) {
			ArrayList<Integer> list = new ArrayList<>();
			read_lock_table.add(list);
		}
		for (int i = 1; i < 20; i++) {
			committed_value[i] = -1;
		}
		for (int i = 1; i < 20; i += 2) {
			committed_value[i] = (i + 1) * 10;
		}
		for (int i = 0; i < 20; i++) {
			write_lock_table[i] = 0;
		}
		for (int i = 0; i < 20; i++) {
			ArrayList<Variable> list = new ArrayList<>();
			committed_value_age.add(list);
		}
		this.type = " ";

	}

    /**
     * check whether the variable has write lock
     * @param int variable_id
     * @return  boolean
     * @author Hanwei Peng
     */
	boolean is_write_lock(int variable_id) {
		return write_lock_table[variable_id - 1] != 0;
	}

    /**
     * check whether the variable has read lock
     * @param int variable_id
     * @return  boolean
     * @author Hanwei Peng
     */
	boolean is_read_lock(int variable_id) {
		return read_lock_table.get(variable_id - 1).size() != 0;
	}

    /**
     * add write lock to the variable 
     * @param int variable_id, int transaction_id
     * @return 
     * @author Hanwei Peng
     */
	void add_write_lock(int variable_id, int transaction_id) {
		write_lock_table[variable_id - 1] = transaction_id;
	}

    /**
     * add read lock to the variable
     * @param int variable_id, int transaction_id
     * @return 
     * @author Hanwei Peng
     */
	void add_read_lock(int variable_id, int transaction_id) {
		ArrayList<Integer> a = read_lock_table.get(variable_id - 1);
		a.add(transaction_id);
		read_lock_table.set(variable_id - 1, a);
	}

    /**
     * unlock the read lock for the variable 
     * @param int variable_id, int transaction_id
     * @return 
     * @author Hanwei Peng
     */
	void unlock_read(int variable_id, int transaction_id) {
		if (read_lock_table.get(variable_id - 1).contains(transaction_id))
			read_lock_table.get(variable_id - 1).remove(new Integer(transaction_id));
	}

    /**
     * unlock the write lock for the variable 
     * @param int variable_id, int transaction_id
     * @return 
     * @author Hanwei Peng
     */
	void unlock_write(int variable_id) {
		write_lock_table[variable_id - 1] = 0;
	}

    /**
     * change the type of the specific Site to “fail”
     * @param 
     * @return 
     * @author Hanwei Peng
     */
	void fail() {
		this.type = "fail";
	}

    /**
     * change the type of the specific Site to “recover”
     * @param 
     * @return 
     * @author Hanwei Peng
     */
	void recover() {
		this.type = "recover";
	}

}