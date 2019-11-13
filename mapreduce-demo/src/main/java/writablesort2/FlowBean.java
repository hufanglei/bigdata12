package writablesort2;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Getter
@Setter
public class FlowBean implements WritableComparable<FlowBean>{
    private int id;
    private double price;


    //反序列化调用
    public FlowBean(){

    }

    //比较器
    public int compareTo(FlowBean o) {
        //-1不交换位置（负数）   1交换位置(正数)
        if(this.id>o.id){
            return 1;
        }else if(this.id<o.id){
            return -1;
        }else {
            return this.price>o.price? -1:1;
        }
    }

    //序列化
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.id);
        dataOutput.writeDouble(this.price);
    }

    //反序列化
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.price = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return this.id+"\t"+this.price;
    }
}
