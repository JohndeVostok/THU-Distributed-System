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

public class GetUser {

	public static void main(String[] args) throws Exception {
		Map map = new HashMap <Integer, Integer>();
		
		String path = "netflix/training_set";
		File file = new File(path);
		File[] files = file.listFiles();
		int cnt = 0;
		for (File filein : files) {
			BufferedReader reader = new BufferedReader(new FileReader(filein));
			String tmp = reader.readLine();
			while ((tmp = reader.readLine()) != null) {
				int idx = tmp.indexOf(",");
				int userId = Integer.valueOf(tmp.substring(0, idx));
				if (!map.containsKey(userId)) {
					map.put(userId, 0);
				}
				map.put(userId, (int) map.get(userId) + 1);
			}
			reader.close();
			System.out.println("File" + String.valueOf(cnt) + " finish.\n");
			cnt++;
		}
		List <Map.Entry <Integer, Integer>> list = new ArrayList <Map.Entry <Integer, Integer>> (map.entrySet());
		Collections.sort(list, new Comparator <Map.Entry <Integer, Integer>> () {
			@Override
			public int compare(Map.Entry <Integer, Integer> o1, Map.Entry <Integer, Integer> o2) {
				return o2.getValue().compareTo(o1.getValue());
			}
		});
		File fileout = new File("cluster/userlist");
		BufferedWriter writer = new BufferedWriter(new FileWriter(fileout));
		for (int i = 0; i < 100000; i++) {
			Map.Entry entry = list.get(i);
			writer.write(String.valueOf(entry.getKey()) + "\n");
		}
		writer.close();
	}
}
