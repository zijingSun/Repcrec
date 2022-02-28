import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;

public class main {
	public static void main(String[] args) throws FileNotFoundException {
		String fileName = args[0];
		String output = args[1];

		// fileName = "C:/Users/13129/Desktop/test1.txt";
		BufferedReader reader = null;
		try {

			PrintStream fileOut = new PrintStream(output);
			System.setOut(fileOut);

			reader = new BufferedReader(new FileReader(fileName));
			String tempString = null;
			TransactionManager tm = new TransactionManager();
			Integer time = 0;
			while ((tempString = reader.readLine()) != null) {
				if (tempString.equals(("")))
					continue;
				if (tempString.indexOf("//") != -1)
					continue;
				String input = tempString;
				input = input.replaceAll(" ", "");
				String command = input.substring(0, devideInput(input));
				String[] var = input.substring(devideInput(input) + 1, input.length() - 1).split(",");
				if (command.equals("begin")) {
					int Tid = Integer.valueOf(var[0].substring(1, var[0].length()));
					tm.begin(Tid, time);

				} else if (command.equals("beginRO")) {
					int Tid = Integer.valueOf(var[0].substring(1, var[0].length()));
					tm.beginRO(Tid, time);

				} else if (command.equals("W")) {
					int Tid = Integer.parseInt(var[0].substring(1, var[0].length()));
					int variableid = Integer.parseInt(var[1].substring(1, var[1].length()));
					int writevalue = Integer.parseInt(var[2].substring(0, var[2].length()));
					tm.write(Tid, variableid, writevalue, time);

				} else if (command.equals("R")) {
					int Tid = Integer.parseInt(var[0].substring(1, var[0].length()));
					int variableid = Integer.parseInt(var[1].substring(1, var[1].length()));
					tm.Read(Tid, variableid, time);

				} else if (command.equals("end")) {
					int Tid = Integer.parseInt(var[0].substring(1, var[0].length()));
					tm.end(Tid, time);

				} else if (command.equals("fail")) {
					int Sid = Integer.valueOf(var[0].substring(0, var[0].length()));
					tm.fail(Sid, time);

				} else if (command.equals("recover")) {
					int Sid = Integer.valueOf(var[0].substring(0, var[0].length()));
					tm.recover(Sid, time);
				} else if (command.equals("dump")) {
					tm.dump();
				}

				tm.deadlock_detect(time);

				time++;

			}

			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e1) {
				}
			}
		}

	}

	public static Integer devideInput(String input) {
		char[] chars = input.toCharArray();
		int i = 0;
		while (i < chars.length) {
			if (chars[i] == '(') {
				break;
			}
			i++;
		}
		return i;
	}

}
