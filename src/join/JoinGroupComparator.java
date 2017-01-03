package join;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.Text;

public class JoinGroupComparator extends WritableComparator {

	protected JoinGroupComparator() {
		super(Text.class,true);

	}

	public int compare(WritableComparable w1, WritableComparable w2) {
		Text tip1 = (Text) w1;
		Text tip2 = (Text) w2;
		return tip1.compareTo(tip2);
	}
	
}

