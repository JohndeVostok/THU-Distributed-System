import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class PageCount {
	public static String parse(String value) throws Exception {
		String keyInfo, valueInfo;
		Pattern linkPattern = Pattern.compile("\\[\\[.+?\\]\\]");
		String[] strs = parseText(value);
		String buf = "";
		if (!validPage(strs[0])) {
			return "";
		}
		keyInfo = strs[0].replace(" ", "_");
		buf = keyInfo + "\n";
		return buf;
	}
		
	private static String[] parseText(String value) throws CharacterCodingException {
		String[] strs = new String[2];
		int l = value.indexOf("<title>");
		int r = value.indexOf("</title>", l);
		if (l < 0 || r < 0) {
			return new String[] {"", ""};
		}
		l += 7;
		strs[0] = value.substring(l, r);
		l = value.indexOf("<text");
		l = value.indexOf(">", l) + 1;
		r = value.indexOf("</text>", l);
		if (l < 0 || r < 0) {
			return new String[] {"", ""};
		}
		l += 1;
		strs[1] = value.substring(l, r);
		return strs;
	}

	public static boolean validPage(String str) {
		if (str == null || str.isEmpty()) {
			return false;
		}
		return !str.contains(":");
	}

	public static void main(String[] args) throws Exception {
		String path = "wiki";
		File file = new File(path);
		File[] files = file.listFiles();
		int cnt = 0;
		File fileout = new File("wiki-num/pagelist");
		BufferedWriter writer = new BufferedWriter(new FileWriter(fileout));
		for (File filein : files) {
			BufferedReader reader = new BufferedReader(new FileReader(filein));
			String tmp = null;
			while ((tmp = reader.readLine()) != null) {
				writer.write(parse(tmp));
			}
			reader.close();
			cnt++;
			System.out.println("finish " + ". Index: " + String.valueOf(cnt) + " of " + String.valueOf(files.length) + ".");
		}
		writer.close();
	}
}
