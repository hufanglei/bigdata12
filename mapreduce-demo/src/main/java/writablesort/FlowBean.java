package writablesort;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Getter
@Setter
public class FlowBean implements WritableComparable<FlowBean>{
    private long upflow;
    private long downflow;
    private long sumflow;


    //反序列化调用
    public FlowBean(){

    }

    //比较器
    public int compareTo(FlowBean o) {

        //-1不交换位置（负数）   1交换位置(正数)

        return this.sumflow>o.sumflow? -1:1;
    }

    //序列化
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeLong(this.upflow);
        dataOutput.writeLong(this.downflow);
        dataOutput.writeLong(this.sumflow);
    }

    //反序列化
    public void readFields(DataInput dataInput) throws IOException {

        this.upflow=dataInput.readLong();
        this.downflow=dataInput.readLong();
        this.sumflow=dataInput.readLong();
    }

    @Override
    public String toString() {
        return this.upflow+"\t"+this.downflow+"\t"+this.sumflow;
    }
}
