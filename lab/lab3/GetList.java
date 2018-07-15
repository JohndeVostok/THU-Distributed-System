import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class GetList {

	public static void main(String[] args) throws Exception {
		Map map = new HashMap <Integer, Integer>();

		File fileuser = new File("cluster/userlist");
		BufferedReader userreader = new BufferedReader(new FileReader(fileuser));
		String tmp = null;
		for (int i = 0; i < 100000; i++) {
			tmp = userreader.readLine();
			int userId = Integer.valueOf(tmp);
			map.put(userId, i);
		}

		File fileout = new File("cluster/data/data");
		BufferedWriter writer = new BufferedWriter(new FileWriter(fileout));
		String path = "netflix/training_set";
		File file = new File(path);
		File[] files = file.listFiles();
		int cnt = 0;
		for (File filein : files) {
			System.out.println(filein.length());
			BufferedReader reader = new BufferedReader(new FileReader(filein));
			tmp = reader.readLine();
			StringBuffer str = new StringBuffer(tmp);
			while ((tmp = reader.readLine()) != null) {
				int idx = tmp.indexOf(",");
				int userId = Integer.valueOf(tmp.substring(0, idx));
				int idy = tmp.indexOf(",", idx + 1);
				int rank = Integer.valueOf(tmp.substring(idx + 1, idy));
				if (map.containsKey(userId)) {
					str.append(String.valueOf(map.get(userId)));
					str.append(",");
					str.append(String.valueOf(rank));
					str.append(";");
				}
			}
			reader.close();
			writer.write(str.toString());
			writer.newLine();
			System.out.println("File" + String.valueOf(cnt) + " finish.");
			cnt++;
		}
		writer.close();
	}
}
