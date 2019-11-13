package join.join2.reducejoin;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Getter
@Setter
public class TableBean implements Writable{

    //订单id
    private String order_id;
    //产品id
    private String p_id;
    //产品数量
    private int amonut;
    //产品名称
    private String pname;
    //标记
    private String flag;


    public TableBean(){

    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(order_id);
        dataOutput.writeUTF(p_id);
        dataOutput.writeInt(amonut);
        dataOutput.writeUTF(pname);
        dataOutput.writeUTF(flag);

    }

    public void readFields(DataInput dataInput) throws IOException {

        this.order_id = dataInput.readUTF();
        this.p_id = dataInput.readUTF();
        this.amonut = dataInput.readInt();
        this.pname = dataInput.readUTF();
        this.flag = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return this.order_id+"\t"+this.pname+"\t"+this.amonut;
    }
}
