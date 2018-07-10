import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class XMLParser {
	public static String parse(String value) throws Exception {
		String keyInfo, valueInfo;
		Pattern linkPattern = Pattern.compile("\\[\\[.+?\\]\\]");
		String[] strs = parseText(value);
		String buf = "";
		if (!validPage(strs[0])) {
			return "";
		}
		keyInfo = strs[0].replace(" ", "_");
		buf += keyInfo + "\t1.0;";
		Matcher matcher = linkPattern.matcher(strs[1]);
		while (matcher.find()) {
			String page = matcher.group();
			page = formatPage(page);
			if (page == null || page.isEmpty()) {
				continue;
			}
			valueInfo = page;
			buf += valueInfo + ",";
		}
		buf = buf.substring(0, buf.length() - 1) + "\n";
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
	
	private static String formatPage(String str) {
		int l = 1;
		if (str.startsWith("[[")); {
			l = 2;
		}
		if (str.length() < l + 2 || str.length() > 100) {
			return null;
		}
		char ch = str.charAt(l);
		if (ch == '#' || ch == '.' || ch == '\'' || ch == '-' || ch == '{') {
			return null;
		}

		if (str.contains(":") || str.contains(",") || str.contains("&")) {
			return null;
		}
		int r = str.indexOf("]");
		int t = str.indexOf("|");
		if (t > 0) {
			r = t;
		}
		t = str.indexOf("#");
		if (t > 0) {
			r = t;
		}
		str = str.substring(l, r);
		str = str.replace(" ", "_");
		return str;
	}

	public static void main(String[] args) throws Exception {
		String path = "wiki";
		File file = new File(path);
		File[] files = file.listFiles();
		int cnt = 0;
		for (File filein : files) {
			String name = filein.getName().substring(0, filein.getName().indexOf("."));
			File fileout = new File("wiki-tmp/iter0/" + name);
			BufferedReader reader = new BufferedReader(new FileReader(filein));
			BufferedWriter writer = new BufferedWriter(new FileWriter(fileout));
			String tmp = null;
			while ((tmp = reader.readLine()) != null) {
				writer.write(parse(tmp));
			}
			reader.close();
			writer.close();
			cnt++;
			System.out.println("finish " + name + ". Index: " + String.valueOf(cnt) + " of " + String.valueOf(files.length) + ".");
		}
	}
}
