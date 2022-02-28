import java.util.Map.Entry;
import java.util.*;

public class TransactionManager {
	DataManager dm = new DataManager();
	Map<Integer, Transaction> transactions = new HashMap<Integer, Transaction>();//
	Queue<Transaction> wait_transactions = new LinkedList<Transaction>();
	Map<Integer, List<Integer>> failure_history = new HashMap<Integer, List<Integer>>();
	Map<Integer, List<Integer>> recover_history = new HashMap<Integer, List<Integer>>();
	Map<Integer, List<Integer>> dependency = new HashMap<Integer, List<Integer>>();

	TransactionManager() {
		for (int i = 1; i <= 10; i++) {
			List<Integer> l = new ArrayList<Integer>();
			List<Integer> s = new ArrayList<Integer>();
			failure_history.put(i, l);
			recover_history.put(i, s);
		}
	}
	
	 /**
     * add new transaction to transaction list
     * @param transaction_id
     * @param Transaciotn
     * @return
     * @author Zijing Sun
     */

	public void createTransaction(Integer tid, Transaction t) {
		transactions.put(tid, t);
	}
	
	 /**
     * add new operation to its transaction
     * @param transaction_id
     * @param Operation
     * @return
     * @author Zijing Sun
     */

	public void addOperationToTransaction(Integer tid, Operation operation) {
		if (transactions.containsKey(tid)) {
			Transaction newt = transactions.get(tid);
			newt.addOperation(operation);
			transactions.put(tid, newt);
		}
	}
	
	 /**
     * add operation that need to wait to its transaction
     * @param transaction_id
     * @param Operation
     * @return
     * @author Zijing Sun
     */

	public void addWaitOperationToTransaction(Integer tid, Operation operation) {
		if (transactions.containsKey(tid)) {
			Transaction newt = transactions.get(tid);
			newt.addWaitedOperation(operation);
			transactions.put(tid, newt);
		}
	}
	
	 /**
     * add transaction that need to wait to waitlist
     * @param Transaction
     * @return
     * @author Zijing Sun
     */

	public void addTransactionToWait(Transaction t) {
		wait_transactions.add(t);
	}
	
	 /**
     * update the state of a transaction
     * @param transaction_id
     * @param STATE
     * @return
     * @author Zijing Sun
     */

	public void updateTransactionState(int tid, String state) {
		Transaction t = transactions.get(tid);
		t.state = state;
		transactions.put(tid, t);
	}
	
	 /**
     * remove a transaction from waitlist
     * @param Transaction
     * @return
     * @author Zijing Sun
     */

	public void popTransactionFromWait(Transaction t) {
		Queue<Transaction> temp = new LinkedList<Transaction>();
		for (Transaction transaction : wait_transactions) {
			if (transaction.id == t.id)
				continue;
			temp.add(transaction);
		}
		wait_transactions = temp;
	}
	
	 /**
     * clear the all the dependency of a transaction
     * @param transaction_id
     * @return
     * @author Zijing Sun
     */

	public void clearDependency(int t) {
		HashSet<Integer> remove = new HashSet<Integer>();
		for (Entry<Integer, List<Integer>> entity : dependency.entrySet()) {
			if (entity.getKey() == t || entity.getValue().contains(t)) {
				int key = entity.getKey();
				remove.add(key);
			}
		}
		for (Integer i : remove) {
			if (i == t) {
				dependency.remove(i);
			} else {
				List<Integer> temp = dependency.get(i);
				temp.remove(new Integer(t));
				dependency.put(i, temp);
			}
		}
	}
	
	 /**
     * try to write a value to site, need to consider lock, site state, failure recovery
     * replicate and non-replicate case, finally decided which site to write, also 
     * update dependency
     * @param transaction_id
     * @param variable_id
     * @param value
     * @param time
     * @return
     * @author Zijing Sun
     */

	public void write(int transaction_id, int variable_id, int value, int time) {
		if (dependency.get(transaction_id) == null)
			dependency.put(transaction_id, new ArrayList<Integer>());
		if (variable_id % 2 == 1) {
			Integer site_num = 1 + (variable_id % 10);
			if (checkWriteWaitList(variable_id, transaction_id)
					&& dm.get_write_dependency(variable_id, site_num) != transaction_id) {

				int parent = checkWriteWaitListTransaction(variable_id);
				dependency.get(transaction_id).add(parent);
				addWaitOperationToTransaction(transaction_id,
						new Operation("W", transaction_id, variable_id, value, time));
				Transaction t = transactions.get(transaction_id);
				wait_transactions.add(t);

			} else if (dm.check_fail(site_num)) {
				Transaction t = transactions.get(transaction_id);
				t.state = "abort";
				transactions.put(transaction_id, t);
				// abort(t);
			} else if ((dm.get_read_dependency(variable_id, site_num).contains(transaction_id)
					|| !dm.is_read_lock(site_num, variable_id))
					&& (dm.get_write_dependency(variable_id, site_num) == transaction_id
							|| !dm.is_write_lock(site_num, variable_id))) {
				// write success
				dm.add_write_lock(site_num, variable_id, transaction_id);
				addOperationToTransaction(transaction_id, new Operation("W", transaction_id, variable_id, value, time));
				System.out.println("T" + transaction_id + " Write " + value + " to x" + variable_id);
			} else {
				// wait case
				addWaitOperationToTransaction(transaction_id,
						new Operation("W", transaction_id, variable_id, value, time));
				Transaction t = transactions.get(transaction_id);
				wait_transactions.add(t);
				if (dm.is_write_lock(site_num, variable_id)) {
					int parent = dm.get_write_dependency(variable_id, site_num);
					dependency.get(transaction_id).add(parent);
				} else {
					ArrayList<Integer> parent1 = dm.get_read_dependency(variable_id, site_num);
					for (Integer j : parent1) {
						dependency.get(transaction_id).add(j);
					}
				}

			}
		} else if (variable_id % 2 == 0) {
			for (int i = 1; i <= 10; i++) {
				if (dm.check_fail(i))
					continue;
				else if (checkWriteWaitList(variable_id, transaction_id)
						&& dm.get_write_dependency(variable_id, i) != transaction_id
						&& (recover_history.get(i).size() == 0 || recover_history.get(i)
								.get(recover_history.get(i).size() - 1) < dm.get_committed_time(variable_id, i))) {

					int parent = checkWriteWaitListTransaction(variable_id);
					dependency.get(transaction_id).add(parent);
					addWaitOperationToTransaction(transaction_id,
							new Operation("W", transaction_id, variable_id, value, time));
					Transaction t = transactions.get(transaction_id);
					wait_transactions.add(t);

				} else if ((dm.get_read_dependency(variable_id, i).contains(transaction_id)
						|| !dm.is_read_lock(i, variable_id))
						&& (dm.get_write_dependency(variable_id, i) == transaction_id
								|| !dm.is_write_lock(i, variable_id))) {
					// write success
					addOperationToTransaction(transaction_id,
							new Operation("W", transaction_id, variable_id, value, time));
					System.out.println("T" + transaction_id + " Write " + value + " to x" + variable_id);
					for (int j = 1; j <= 10; j++) {
						if (!dm.check_fail(i)) {
							dm.add_write_lock(i, variable_id, transaction_id);
						}
					}
					break;
				} else {
					// wait case
					addWaitOperationToTransaction(transaction_id,
							new Operation("W", transaction_id, variable_id, value, time));
					Transaction t = transactions.get(transaction_id);
					wait_transactions.add(t);
					if (dm.is_write_lock(i, variable_id)) {
						int parent = dm.get_write_dependency(variable_id, i);
						dependency.get(transaction_id).add(parent);
					} else {
						ArrayList<Integer> parent1 = dm.get_read_dependency(variable_id, i);
						for (Integer j : parent1) {
							dependency.get(transaction_id).add(j);
							;
						}
					}

				}
				break;
			}

		}
	}
	
	 /**
     * check whether there is a read type transaction in waitlist waiting for a specific variable
     * @param transaction_id
     * @param variable_id
     * @return boolean
     * @author Zijing Sun
     */

	public boolean checkReadWaitList(int variable_id, int transaction_id) {
		for (Transaction t : wait_transactions) {
			if (t.id == transaction_id)
				break;
			if (variable_id == t.waited_operations.get(0).variable && t.waited_operations.get(0).type.equals("W"))
				return true;
		}
		return false;
	}
	
	 /**
     * check whether there is a transaction in waitlist waiting for a specific variable
     * @param transaction_id
     * @param variable_id
     * @return boolean
     * @author Zijing Sun
     */

	public boolean checkWriteWaitList(int variable_id, int transaction_id) {
		for (Transaction t : wait_transactions) {
			if (t.id == transaction_id)
				break;
			if (variable_id == t.waited_operations.get(0).variable)
				return true;
		}
		return false;
	}
	
	 /**
     * return the id of a read type transaction in waitlist waiting for a specific variable
     * @param variable_id
     * @return integer
     * @author Zijing Sun
     */

	public int checkReadWaitListTransaction(int variable_id) {
		int result = 0;
		for (Transaction t : wait_transactions) {
			if (variable_id == t.waited_operations.get(0).variable && t.waited_operations.get(0).type.equals("W"))
				result = t.waited_operations.get(0).transaction_id;
		}

		return result;
	}
	
	 /**
     * return the id of a transaction in waitlist waiting for a specific variable
     * @param variable_id
     * @return integer
     * @author Zijing Sun
     */

	public int checkWriteWaitListTransaction(int variable_id) {
		int result = 0;
		for (Transaction t : wait_transactions) {
			if (variable_id == t.waited_operations.get(0).variable)
				result = t.waited_operations.get(0).transaction_id;
		}

		return result;
	}

	 /**
     * try to read, need to consider lock, site state, failure recovery
     * replicate and non-replicate case, finally decided which site to write, also 
     * update dependency
     * @param transaction_id
     * @param variable_id
     * @param time
     * @return result of read
     * @author Zijing Sun
     */
	
	public Integer read(int transaction_id, int variable_id, int time) {
		if (dependency.get(transaction_id) == null)
			dependency.put(transaction_id, new ArrayList<Integer>());
		int result = -1;
		if (variable_id % 2 == 1) {
			Integer site_num = 1 + (variable_id % 10);
			if (checkReadWaitList(variable_id, transaction_id)
					&& dm.get_write_dependency(variable_id, site_num) != transaction_id) {

				int parent = checkReadWaitListTransaction(variable_id);
				// System.out.println("Wait1"+parent);
				dependency.get(transaction_id).add(parent);
				addWaitOperationToTransaction(transaction_id, new Operation("R", transaction_id, variable_id, time));
				Transaction t = transactions.get(transaction_id);
				wait_transactions.add(t);

			} else if ((dm.is_write_lock(site_num, variable_id)
					&& dm.get_write_dependency(variable_id, site_num) != transaction_id)) {
				// wait case
				addWaitOperationToTransaction(transaction_id, new Operation("R", transaction_id, variable_id, time));
				Transaction t = transactions.get(transaction_id);
				wait_transactions.add(t);
				int parent = dm.get_write_dependency(variable_id, site_num);
				// System.out.println("Wait2"+parent);
				dependency.get(transaction_id).add(parent);
			} else if (dm.check_fail(site_num)) {
				Transaction t = transactions.get(transaction_id);
//				t.state = "abort";
//				transactions.put(transaction_id, t);
				abort(t);
			} else if (!dm.is_write_lock(site_num, variable_id)
					|| dm.get_write_dependency(variable_id, site_num) == transaction_id) {
				// read success, update the read lock at 10 sites
				addOperationToTransaction(transaction_id, new Operation("R", transaction_id, variable_id, time));
				result = dm.read(site_num, variable_id);
				for (int i = 1; i <= 10; i++) {
					// System.out.println("add lock to site"+i);
					dm.add_read_lock(i, variable_id, transaction_id);
				}
				System.out.println("T" + transaction_id + " Read x" + variable_id + ":" + result);

			}
		} else if (variable_id % 2 == 0) {
			for (int i = 1; i <= 10; i++) {

				if (!dm.check_fail(i) && (recover_history.get(i).size() == 0 || recover_history.get(i)
						.get(recover_history.get(i).size() - 1) < dm.get_committed_time(variable_id, i))) {
					if (checkReadWaitList(variable_id, transaction_id)
							&& dm.get_write_dependency(variable_id, i) != transaction_id) {

						int parent = checkReadWaitListTransaction(variable_id);
						// System.out.println("Waiteven1"+parent);
						dependency.get(transaction_id).add(parent);
						addWaitOperationToTransaction(transaction_id,
								new Operation("R", transaction_id, variable_id, time));
						Transaction t = transactions.get(transaction_id);
						wait_transactions.add(t);

					} else if (dm.is_write_lock(i, variable_id)
							&& dm.get_write_dependency(variable_id, i) != transaction_id) {
						// wait case
						addWaitOperationToTransaction(transaction_id,
								new Operation("R", transaction_id, variable_id, time));
						Transaction t = transactions.get(transaction_id);
						wait_transactions.add(t);
						int parent = dm.get_write_dependency(variable_id, i);
						dependency.get(transaction_id).add(parent);
					} else {
						addOperationToTransaction(transaction_id,
								new Operation("R", transaction_id, variable_id, time));
						result = dm.read(i, variable_id);
						for (int j = 1; j <= 10; j++) {
							if (!dm.check_fail(j) && (recover_history.get(i).size() == 0 || recover_history.get(i)
									.get(recover_history.get(i).size() - 1) < dm.get_committed_time(variable_id, i))) {
								dm.add_read_lock(j, variable_id, transaction_id);
							}

						}
						System.out.println("T" + transaction_id + " Read x" + variable_id + ":" + result);
					}
					break;
				} else if (!dm.check_fail(i) && recover_history.get(i).size() != 0 && recover_history.get(i)
						.get(recover_history.get(i).size() - 1) > dm.get_committed_time(variable_id, i)) {
					// System.out.println("Waiteven333");
					addWaitOperationToTransaction(transaction_id,
							new Operation("R", transaction_id, variable_id, time));
					Transaction t = transactions.get(transaction_id);
					wait_transactions.add(t);
				}
			}
		}
		return result;
	}
	
	 /**
     * do readonly
     * update dependency
     * @param transaction_id
     * @param variable_id
     * @return result of readonly
     * @author Zijing Sun
     */

	public Integer readRO(int transaction_id, int variable_id) {
		Transaction t = transactions.get(transaction_id);
		int result = t.snapshot[variable_id - 1];
		System.out.println("T" + transaction_id + " Read x" + variable_id + ":" + result);
		return result;
	}
	
	 /**
     * abort a transaction
     * update dependency
     * @param Transaction
     * @return 
     * @author Zijing Sun
     */

	public void abort(Transaction t) {
		updateTransactionState(t.id, "abort");
		// release lock
		for (int i = 1; i <= 10; i++) {
			dm.release_lock(t, i);
		}
//		//clear transaction map and wait list
//		popTransactionFromWait(t);
//		transactions.remove(t.id);
//		//clear dependency
//		clearDependency(t.id);
//		System.out.println(t.id+" abort");

	}
	
	 /**
     * commit a transaction
     * update dependency
     * @param Transaction
     * @param time
     * @return 
     * @author Zijing Sun
     */

	public void commit(Transaction t, int time) {
		updateTransactionState(t.id, "commit");
		List<Operation> operations = t.operations;
		for (Operation o : operations) {
			int transaction_id = o.transaction_id;
			int variable_id = o.variable;
			if (o.type.equals("W")) {
				int site_num = 1 + (variable_id % 10);
				int value = o.value;
				if (variable_id % 2 == 1) {
					if (!dm.check_fail(site_num) && (failure_history.get(site_num).size() == 0
							|| failure_history.get(site_num).get(failure_history.get(site_num).size() - 1) < o.age)) {
						dm.write(site_num, time, variable_id, value, transaction_id);
					} else {
						abort(t);
						return;
					}
				} else if (variable_id % 2 == 0) {
					for (int i = 1; i <= 10; i++) {
						// System.out.println(i);
						if (!dm.check_fail(i)
								&& (failure_history.get(i).size() == 0
										|| failure_history.get(i).get(failure_history.get(i).size() - 1) < o.age)
								&& i != 10) {
							continue;
						}
						if (failure_history.get(i).size() != 0
								&& failure_history.get(i).get(failure_history.get(i).size() - 1) > o.age) {
							// System.out.println(t.id+" "+ o.type+" "+o.age+" "+
							// failure_history.get(i).get(failure_history.get(i).size()-1)+" "+i);
							abort(t);
							return;
						}
						if (i == 10) {
							for (int j = 1; j <= 10; j++) {
								if (dm.check_fail(j) || (failure_history.get(j).size() != 0
										&& failure_history.get(j).get(failure_history.get(j).size() - 1) < o.age
										&& (recover_history.size() != 0 && recover_history.get(j)
												.get(recover_history.get(j).size() - 1) > o.age)))
									continue;
								dm.write(j, time, variable_id, value, transaction_id);
							}
						}
					}
				}
			} else if (o.type.equals("R")) {
				int site_num = 1 + (variable_id % 10);
				if (variable_id % 2 == 1) {
					if (!dm.check_fail(site_num) && (failure_history.get(site_num).size() == 0
							|| failure_history.get(site_num).get(failure_history.get(site_num).size() - 1) < o.age)) {
					} else {
						abort(t);
						return;
					}
				} else if (variable_id % 2 == 0) {
					for (int i = 1; i <= 10; i++) {
						// System.out.println(i);
//						if(!dm.check_fail(i)&& (failure_history.get(i).size()==0 || failure_history.get(i).get(failure_history.get(i).size()-1)<o.age)&& i!=10) {
//							continue;
//						}
						if (failure_history.get(i).size() != 0
								&& failure_history.get(i).get(failure_history.get(i).size() - 1) > o.age) {
							abort(t);
							return;
						}

//						if(recover_history.get(i).size()!=0 ) {
//							//System.out.println("+====================================");
//							if(recover_history.get(i).get(recover_history.get(i).size()-1)>dm.get_committed_time(variable_id, i)) {
//								System.out.println("+====================================");
//								abort(t);
//								return;
//							}
//							
//						}
						if (i == 10) {
							for (int j = 1; j <= 10; j++) {
								if (dm.check_fail(j) || (failure_history.get(j).size() != 0
										&& failure_history.get(j).get(failure_history.get(j).size() - 1) < o.age
										&& (recover_history.size() != 0 && recover_history.get(j)
												.get(recover_history.get(j).size() - 1) > o.age)))
									continue;
							}
						}
					}
				}
			}
		}
		for (int i = 1; i <= 10; i++) {
			dm.release_lock(t, i);
		}
//		//clear transaction map and wait list
//		popTransactionFromWait(t);
//		transactions.remove(t.id);
//		//clear dependency
//		clearDependency(t.id);
//		System.out.println(t.id+" commit");
	}
	
	 /**
     * end a transaction, abort or commit
     * update dependency
     * @param transaction_id
     * @param time
     * @return 
     * @author Zijing Sun
     */

	public void end(int transaction_id, int time) {
		Transaction t = transactions.get(transaction_id);
		if (t.state == "abort") {
			abort(t);
			// clear transaction map and wait list
			popTransactionFromWait(t);
			transactions.remove(t.id);
			// clear dependency
			clearDependency(t.id);
			System.out.println(t.id + " abort");
		} else {
			commit(t, time);
			t = transactions.get(transaction_id);
			if (t.state == "abort") {
				System.out.println("T" + t.id + " Abort");
			} else {
				System.out.println("T" + t.id + " Commit");
			}
			// clear transaction map and wait list
			popTransactionFromWait(t);
			transactions.remove(t.id);
			// clear dependency
			clearDependency(t.id);

		}
		if (wait_transactions.size() != 0) {
			runWaitList(time);

		}

	}
	
	 /**
     * run a transaction from waitlist
     * @param time
     * @return 
     * @author Zijing Sun
     */

	public void runWaitList(int time) {
		Transaction next = new Transaction();
		List<Transaction> temp = new ArrayList<Transaction>();
		int next_id = -1;
		for (Transaction transaction : wait_transactions) {
			int variable_id = transaction.waited_operations.get(0).variable;
			int tid = transaction.waited_operations.get(0).transaction_id;
			String type = transaction.waited_operations.get(0).type;
			if (variable_id % 2 == 1) {
				Integer site_num = 1 + (variable_id % 10);
				if (type.equals("W")) {
					int value = transaction.waited_operations.get(0).value;
					if ((!dm.is_read_lock(site_num, variable_id) && !dm.is_write_lock(site_num, variable_id)
							&& !dm.check_fail(site_num)) && !checkWriteWaitList(variable_id, transaction.id)) {
						write(tid, variable_id, value, time);
						next = transaction;
						next_id = tid;
						temp.add(transaction);

					}
				} else if (type.equals("R")) {
					if ((!dm.is_write_lock(site_num, variable_id) && !dm.check_fail(site_num))
							&& !checkReadWaitList(variable_id, transaction.id)) {
						read(tid, variable_id, time);
						next = transaction;
						next_id = tid;
						temp.add(transaction);

					}
				}
			} else if (variable_id % 2 == 0) {
				if (type.equals("W")) {
					int value = transaction.waited_operations.get(0).value;
					for (int i = 1; i <= 10; i++) {
						if (!dm.is_read_lock(i, variable_id) && !dm.is_write_lock(i, variable_id) && !dm.check_fail(i)
								&& !checkWriteWaitList(variable_id, transaction.id)) {
							write(tid, variable_id, value, time);
							next = transaction;
							next_id = tid;
							if (!temp.contains(transaction))
								temp.add(transaction);

							break;
						}

					}

				} else if (type.equals("R")) {
					for (int i = 1; i <= 10; i++) {
						if (!dm.check_fail(i)
								&& (recover_history.get(i).size() == 0 || recover_history.get(i)
										.get(recover_history.get(i).size() - 1) < dm.get_committed_time(variable_id, i))
								&& !checkReadWaitList(variable_id, transaction.id)) {
							read(tid, variable_id, time);
							next = transaction;
							next_id = tid;
							if (!temp.contains(transaction))
								temp.add(transaction);
							break;
						}
					}

				}

			}

		}
		for (Transaction t : temp) {
			popTransactionFromWait(t);
			clearDependency(t.id);
		}
	}
	
	 /**
     * fail a site
     * @param site_num
     * @param time
     * @return 
     * @author Zijing Sun
     */

	public void fail(int site_num, int time) {
		dm.fail(site_num);
		dm.release_all_lock(site_num);
		List<Integer> l = failure_history.get(site_num);
		l.add(time);
		failure_history.put(site_num, l);
		System.out.println("Site" + site_num + " :fail");

	}
	
	 /**
     * recover a site
     * @param site_num
     * @param time
     * @return 
     * @author Zijing Sun
     */

	public void recover(int site_num, int time) {
		dm.recover(site_num);
		List<Integer> l = recover_history.get(site_num);
		l.add(time);
		recover_history.put(site_num, l);
		System.out.println("Site" + site_num + " :recover");

	}
	
	 /**
     * create a read write transaction
     * @param transaction_id
     * @param time
     * @return 
     * @author Zijing Sun
     */


	public void begin(int transaction_id, int time) {
		transactions.put(transaction_id, new Transaction(transaction_id, "RW", time));
		System.out.println("create T" + transaction_id);
	}
	
	 /**
     * create a readonly transaction
     * @param transaction_id
     * @param time
     * @return 
     * @author Zijing Sun
     */

	public void beginRO(int transaction_id, int time) {
		int[] snapshot = new int[20];
		transactions.put(transaction_id, new Transaction(transaction_id, "RO", time));
		for (int variable_id = 1; variable_id <= 20; variable_id++) {
			if (variable_id % 2 == 1) {
				Integer site_num = 1 + (variable_id % 10);
				if (dm.check_fail(site_num)) {
					Transaction t = transactions.get(transaction_id);
					abort(t);
				} else {
					snapshot[variable_id - 1] = dm.read(site_num, variable_id);
				}

			} else if (variable_id % 2 == 0) {
				for (int i = 1; i <= 10; i++) {
					if (!dm.check_fail(i) && (recover_history.get(i).size() == 0 || recover_history.get(i)
							.get(recover_history.get(i).size() - 1) < dm.get_committed_time(variable_id, i))) {
						snapshot[variable_id - 1] = dm.read(i, variable_id);
						break;
					}
				}
			}
		}
		Transaction t = transactions.get(transaction_id);
		t.snapshot = snapshot;
		transactions.put(transaction_id, t);
		System.out.println("Create T" + transaction_id);
	}
	
    /**
     * perform the read operation
     * @param int tid, int variable_id, int time
     * @return 
     * @author Hanwei Peng
     */

	public void Read(int tid, int variable_id, int time) {
		String type = transactions.get(tid).type;
		if (type.equals("RW")) {
			read(tid, variable_id, time);
		} else {
			readRO(tid, variable_id);
		}
		// if (result != -1) System.out.println("x"+variable_id+": "+ result);
	}
	
    /**
     * check whether the current program has deadlock and abort the youngest transation if detected
     * @param int time
     * @return 
     * @author Hanwei Peng
     */

	public void deadlock_detect(int time) {
		// printdenpendency();
		int minage = Integer.MIN_VALUE;
		int youngest = -1;
		int curr = -1;
		boolean hasCycle = false;
		HashSet<Integer> set = new HashSet<>();// Class A{int parent, int child} List<A>
		List<Wait> waitList = new ArrayList<Wait>();
		for (int tid : transactions.keySet()) {
			if (dependency.containsKey(tid)) {
				for (int child : dependency.get(tid)) {
					waitList.add(new Wait(tid, child));
				}
			}
		}

		for (int tid : dependency.keySet()) {
			int count = 0;
			curr = -1;
			for (Wait w : waitList) {
				if (w.parent == tid) {
					curr = w.child;
					break;
				}
			}
			set.add(tid);
			while (true) {
				set.add(curr);
				if (tid == curr) {
					hasCycle = true;
					break;
				}
				if (!dependency.containsKey(curr)) {
					break;
				}
				if (dependency.get(curr).size() == 0)
					break;
				for (Wait w : waitList) {
					if (w.parent == curr) {
						curr = w.child;
						break;
					}

				}
				count++;
				if (count == dependency.size() + 1)
					break;
			}

			if (hasCycle) {
				break;
			}
			set = new HashSet<>();
		}
		if (hasCycle) {
			for (int k : set) {
				Transaction curr_t = transactions.get(k);
				if (curr_t.age > minage) {
					minage = curr_t.age;
					youngest = k;
				}
			}
			System.out.println("Deadlock Detect!");

			Transaction t = transactions.get(youngest);
			for (int i = 1; i <= 10; i++) {
				dm.release_lock(t, i);
			}
			clearDependency(youngest);
			popTransactionFromWait(t);
			System.out.println("T" + youngest + " Abort");
			runWaitList(time);

		}
	}
	
    /**
     * print the site status
     * @param 
     * @return 
     * @author Hanwei Peng
     */

	public void dump() {
		dm.print();
	}
	

    /**
     * print current transation
     * @param 
     * @return 
     * @author Hanwei Peng
     */

	public void print() {
		for (Entry<Integer, Transaction> entity : transactions.entrySet()) {
			entity.getValue().print();
			System.out.println();
		}
	}
	
    /**
     * print the dependency graph
     * @param 
     * @return 
     * @author Hanwei Peng
     */

	public void printdenpendency() {
		dependency.entrySet().forEach(entry -> {
			System.out.println(entry.getKey() + " " + entry.getValue());
		});
	}

	public class Wait {
		int parent;
		int child;

		Wait(int p, int c) {
			this.parent = p;
			this.child = c;
		}
	}
}
