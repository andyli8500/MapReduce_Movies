package join;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.IntWritable;
import java.nio.ByteBuffer;

public class FreqComparator extends WritableComparator {

	protected FreqComparator() {
		super(IntWritable.class, true);

	}
//	@Override
//	public int compare(WritableComparable w1, WritableComparable w2) {
///		IntWritable tip1 = (IntWritable) w1;
//		IntWritable tip2 = (IntWritable) w2;
//		//int result = tip1.get() < tip2.get() ? 0 : 1;
//		return tip1.compareTo(tip2);
//	}

	@Override
    	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

        	Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
        	Integer v2 = ByteBuffer.wrap(b2, s2, l2).getInt();

        	return v1.compareTo(v2) * (-1);
    	}
	
}

