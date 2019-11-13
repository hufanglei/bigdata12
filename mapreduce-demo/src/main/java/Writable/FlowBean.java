package Writable;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable {
    private long upflow;
    private long downflow;
    private long sumflow;

    //无参数的构造方法，在反序列化的时候调用
    public FlowBean(){

    }
    //构造方法赋值
    public FlowBean(long upflow,long downdlow){
        this.upflow=upflow;
        this.downflow=downdlow;
        this.sumflow=upflow+downdlow;
    }

    //序列化 属性的顺序是一致
    public void write(DataOutput output) throws IOException {

        output.writeLong(upflow);
        output.writeLong(downflow);
        output.writeLong(sumflow);

    }

    //反序列化
    public void readFields(DataInput input) throws IOException {

        this.upflow=input.readLong();
        this.downflow=input.readLong();
        this.sumflow=input.readLong();

    }

    public long getUpflow() {
        return upflow;
    }

    public void setUpflow(long upflow) {
        this.upflow = upflow;
    }

    public long getDownflow() {
        return downflow;
    }

    public void setDownflow(long downflow) {
        this.downflow = downflow;
    }

    public long getSumflow() {
        return sumflow;
    }

    public void setSumflow(long sumflow) {
        this.sumflow = sumflow;
    }

    @Override
    public String toString() {
        return this.upflow+"\t"+this.downflow+"\t"+this.sumflow;
    }
}
