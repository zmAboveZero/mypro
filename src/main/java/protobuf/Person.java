package protobuf;

import java.io.Serializable;

public class Person implements Serializable {

    private static final long serialVersionUID = 2690623243047210668L;

    //    基本类型不会被序列化，但会被自动封装为包装类
//    所有数据类型包装类都是可序列化的
    private Integer pid;
    private String pname;
    private Double score;

    public Integer getPid() {
        return pid;
    }

    public void setPid(Integer pid) {
        this.pid = pid;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public Double getScore() {
        return score;
    }

    public void setScore(Double score) {
        this.score = score;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Person{");
        sb.append("pid=").append(pid);
        sb.append(", pname='").append(pname).append('\'');
        sb.append(", score=").append(score);
        sb.append('}');
        return sb.toString();
    }
}