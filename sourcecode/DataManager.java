import java.util.ArrayList;

public class DataManager {
	Site site_table[] = new Site[10];

	DataManager() {
		for (int i = 0; i < 10; i++) {
			site_table[i] = new Site();
		}
		for (int i = 0; i < 20; i += 2) {
			int variable_id = i + 1;
			int index = (variable_id % 10) + 1;
			site_table[index - 1].committed_value[variable_id - 1] = variable_id * 10;
		}

	}
    /**
     * check whether the variable at specific site has write lock
     * @param int site_num, int variable_id
     * @return  boolean
     * @author Hanwei Peng
     */
	boolean is_write_lock(int site_num, int variable_id) {
		return site_table[site_num - 1].is_write_lock(variable_id);
	}

    /**
     * check whether the variable at specific site has read lock
     * @param int site_num, int variable_id
     * @return  boolean
     * @author Hanwei Peng
     */
	boolean is_read_lock(int site_num, int variable_id) {
		return site_table[site_num - 1].is_read_lock(variable_id);
	}

    /**
     * add write lock to the variable at specific site
     * @param int site_num, int variable_id, int transaction_id
     * @return 
     * @author Hanwei Peng
     */
	void add_write_lock(int site_num, int variable_id, int transaction_id) {
		site_table[site_num - 1].add_write_lock(variable_id, transaction_id);
	}

    /**
     * add read lock to the variable at specific site
     * @param int site_num, int variable_id, int transaction_id
     * @return 
     * @author Hanwei Peng
     */
	void add_read_lock(int site_num, int variable_id, int transaction_id) {
		site_table[site_num - 1].add_read_lock(variable_id, transaction_id);
	}

    /**
     * check whether the specific site has failed or not
     * @param int site_num
     * @return boolean
     * @author Hanwei Peng
     */
	boolean check_fail(int site_num) {
		return site_table[site_num - 1].type.equals("fail");
	}

    /**
     * check whether the specific site has recovered or not
     * @param int site_num
     * @return boolean
     * @author Hanwei Peng
     */
	boolean check_recover(int site_num) {
		return site_table[site_num - 1].type.equals("recover");
	}

    /**
     * release all of the locks held by the specific transaction
     * @param int site_num, Transaction t
     * @return 
     * @author Hanwei Peng
     */
	void release_lock(Transaction t, int site_num) {
		for (Operation o : t.operations) {
			if (o.type.equals("R")) {
				for (int j = 0; j < site_table[site_num - 1].read_lock_table.get(o.variable - 1).size(); j++) {
					if (site_table[site_num - 1].read_lock_table.get(o.variable - 1).get(j) == t.id) {
						site_table[site_num - 1].read_lock_table.get(o.variable - 1).remove(new Integer(t.id));
						break;
					}
				}
			}
			if (o.type.equals("W")) {
				site_table[site_num - 1].write_lock_table[o.variable - 1] = 0;
			}
		}
	}

    /**
     * release all of the locks for a specific site
     * @param int site_num
     * @return 
     * @author Hanwei Peng
     */
	void release_all_lock(int site_num) {
		for (int i = 0; i < 20; i++) {
			site_table[site_num - 1].read_lock_table.get(i).clear();
		}
		for (int i = 0; i < 20; i++) {
			site_table[site_num - 1].write_lock_table[i] = 0;
		}
	}

    /**
     * get all the transactions that hold the read lock for a variable at specific site
     * @param int site_num, int variable_id
     * @return ArrayList<Integer>
     * @author Hanwei Peng
     */
	ArrayList<Integer> get_read_dependency(int variable_id, int site_num) {
		return site_table[site_num - 1].read_lock_table.get(variable_id - 1);
	}

    /**
     * get the transaction that hold the write lock for a variable at specific site
     * @param int site_num, int variable_id
     * @return int
     * @author Hanwei Peng
     */
	int get_write_dependency(int variable_id, int site_num) {
		return site_table[site_num - 1].write_lock_table[variable_id - 1];
	}

    /**
     * get the committed time for a variable at specific site
     * @param int site_num, int variable_id
     * @return int
     * @author Hanwei Peng
     */
	int get_committed_time(int variable_id, int site_num) {
		int size = site_table[site_num - 1].committed_value_age.get(variable_id - 1).size();
		if (size == 0)
			return -1;
		return site_table[site_num - 1].committed_value_age.get(variable_id - 1).get(size - 1).age;
	}

    /**
     * assign value to a specific variable in a specific site
     * @param int site_num, int time, int variable_id, int value, int transaction_id
     * @return 
     * @author Hanwei Peng
     */
	void write(int site_num, int time, int variable_id, int value, int transaction_id) {
		site_table[site_num - 1].committed_value[variable_id - 1] = value;
		Variable v = new Variable();
		v.value = value;
		v.transaction_id = transaction_id;
		v.age = time;

		ArrayList<Variable> vl = site_table[site_num - 1].committed_value_age.get(variable_id - 1);
		vl.add(v);
		site_table[site_num - 1].committed_value_age.set(variable_id - 1, vl);

	}

    /**
     * take snapshot for a specific site
     * @param Transaction t, int site_num
     * @return 
     * @author Hanwei Peng
     */
	void snapshot(Transaction t, int site_num) {
		t.snapshot = site_table[site_num - 1].committed_value;
	}

    /**
     * read the value of the specific variable in the specific site
     * @param int site_num, int variable_id
     * @return int
     * @author Hanwei Peng
     */
	int read(int site_num, int variable_id) {
		return site_table[site_num - 1].committed_value[variable_id - 1];
	}

    /**
     * change the type of the specific Site to “fail”
     * @param int site_num
     * @return 
     * @author Hanwei Peng
     */
	void fail(int site_num) {
		site_table[site_num - 1].type = "fail";
	}

    /**
     * change the type of the specific Site to “recover”
     * @param int site_num
     * @return 
     * @author Hanwei Peng
     */
	void recover(int site_num) {
		site_table[site_num - 1].type = "recover";
	}

    /**
     * print the site table
     * @param 
     * @return 
     * @author Hanwei Peng
     */
	void print() {
		System.out.println();
		for (int i = 0; i < 10; i++) {
			System.out.print("Site " + (i + 1) + ":   ");
			for (int j = 0; j < 20; j++) {
				if (site_table[i].committed_value[j] == -1) {
					System.out.print(" *    ");
				} else {
					System.out.print(site_table[i].committed_value[j] + "    ");
				}

			}
			System.out.println();
		}
	}

}