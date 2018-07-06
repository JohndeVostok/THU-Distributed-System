import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class Test {
	public static void main(String[] args) {
		String str = "GONZALO	I have great comfort from this fellow: methinks he";
		System.out.println(str.length());
		Pattern pattern = Pattern.compile("[^a-z|A-Z]");
		Matcher matcher = pattern.matcher(str);
		int l = 0, r = 0;
		
		while (matcher.find()) {
			r = matcher.start();
			if (r - l > 0) {
				System.out.println(str.substring(l, r));
			}
			l = r + 1;
		}
	}
}
