package edu.hhu.stonk.dao.datafile;

import java.io.Serializable;
import java.util.List;

/**
 * 数据集
 *
 * @author hayes, @create 2017-12-11 19:00
 **/
public class DataFile implements Serializable {

    private static final long serialVersionUID = 6926160742414606834L;
    /**
     * 数据集文件路径
     */
    private String path;

    /**
     * 所有者
     */
    private String uname;

    /**
     * 数据集文件名
     */
    private String name;

    /**
     * 分隔符
     */
    private String delim = ",";

    /**
     * 是否有头部
     */
    private boolean header = false;

    /**
     * 数据集类型，默认CSV
     */
    private DataFileType dataFileType = DataFileType.CSV;

    /**
     * 列描述
     */
    private List<FieldInfo> fieldInfos;

    public boolean isHeader() {
        return header;
    }

    public void setHeader(boolean header) {
        this.header = header;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<FieldInfo> getFieldInfos() {
        return fieldInfos;
    }

    public void setFieldInfos(List<FieldInfo> fieldInfos) {
        this.fieldInfos = fieldInfos;
    }

    public String getDelim() {
        return delim;
    }

    public void setDelim(String delim) {
        this.delim = delim;
    }

    public DataFileType getDataFileType() {
        return dataFileType;
    }

    public void setDataFileType(DataFileType dataFileType) {
        this.dataFileType = dataFileType;
    }

    public String getUname() {
        return uname;
    }

    public void setUname(String uname) {
        this.uname = uname;
    }
}


