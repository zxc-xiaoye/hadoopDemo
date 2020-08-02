package com.zxc.mr.reducerjoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TableBean implements Writable {

    private String id;
    private String pid;
    private int amount;
    private String pname;
    private String flag;

    public TableBean(){
        super();
    }


    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(pid);
        out.writeInt(amount);
        out.writeUTF(pname);
        out.writeUTF(flag);
    }

    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.pid = in.readUTF();
        this.amount = in.readInt();
        this.pname = in.readUTF();
        this.flag = in.readUTF();
    }

    public String getId() {
        return id;
    }

    public String getPid() {
        return pid;
    }

    public int getAmount() {
        return amount;
    }

    public String getPname() {
        return pname;
    }

    public String getFlag() {
        return flag;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        return id + "\t" + amount + "\t" + pname;
    }
}
