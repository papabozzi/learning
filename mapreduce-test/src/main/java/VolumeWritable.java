import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class VolumeWritable implements Writable{
    public IntWritable up;
    public IntWritable down;
    public IntWritable total;

    public VolumeWritable (IntWritable up, IntWritable down, IntWritable total){
        this.up = up;
        this.down = down;
        this.total = total;
    }

    public VolumeWritable (){
        this.up = new IntWritable();
        this.down = new IntWritable();
        this.total = new IntWritable();
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        up.write(dataOutput);
        down.write(dataOutput);
        total.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        up.readFields(dataInput);
        down.readFields(dataInput);
        total.readFields(dataInput);
    }

    @Override
    public String toString() {
        return up.toString() + "\t" + down.toString() + "\t" + total.toString();
    }


}
