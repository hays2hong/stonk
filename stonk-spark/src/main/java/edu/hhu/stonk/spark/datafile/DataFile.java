package edu.hhu.stonk.spark.datafile;

import com.fasterxml.jackson.annotation.JsonIgnore;
import edu.hhu.stonk.spark.exception.CantConverException;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

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

    /**
     * 将数据集文件转换为DataFrame
     *
     * @param context
     * @return
     * @throws CantConverException
     */
    public Dataset<Row> convertToDataFrame(JavaSparkContext context) throws CantConverException {
        SparkSession sparkSession = SparkSession.builder()
                .sparkContext(context.sc())
                .getOrCreate();

        SQLContext sqlContext = new SQLContext(sparkSession);

        switch (dataFileType) {
            case CSV:
                return csvToDataFrame(context, sqlContext);
            case LIBSVM:
                return libsvmToDataFrame(sqlContext);
            default:
                throw new CantConverException("不支持的数据集格式");
        }
    }

    private Dataset<Row> libsvmToDataFrame(SQLContext sqlContext) {
        return sqlContext.read()
                .format("libsvm")
                .load(path);
    }

    private Dataset<Row> csvToDataFrame(JavaSparkContext context, SQLContext sqlContext) throws CantConverException {
        StructType schema = this.getStructType();

        JavaRDD<Row> rdd = context.textFile(path)
                .map(new LineParse(this));
        return sqlContext.createDataFrame(rdd, schema);
//        return sqlContext.read()
//                .format("csv")
//                .option("header", header ? "true" : "false")
//                .option("delimiter", delim)
//                .option("inferSchema", "false")
//                .schema(getStructType())
//                .load(path);
    }

    /**
     * Spark StructType
     *
     * @return
     * @throws CantConverException
     */
    @JsonIgnore
    public StructType getStructType() throws CantConverException {
        //按照 Index 排序
        fieldInfos.sort((FieldInfo f1, FieldInfo f2) -> f1.getIndex() > f2.getIndex() ? -1 : 1);

        StructField[] fields = new StructField[fieldInfos.size()];
        for (int i = 0; i < fieldInfos.size(); i++) {
            fields[i] = fieldInfos.get(i).convertToStructField();
        }

        return new StructType(fields);
    }


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
}


/**
 * 行->Row
 */
class LineParse implements Function<String, Row> {
    private static final long serialVersionUID = -1481954080127428634L;

    private DataFile dataFile;

    public LineParse(DataFile dataFile) {
        this.dataFile = dataFile;
    }

    @Override
    public Row call(String line) throws Exception {
        String[] strArr;
        if (StringUtils.isEmpty(dataFile.getDelim())) {
            strArr = new String[]{line};
        } else {
            strArr = line.split(dataFile.getDelim());
        }

        List<FieldInfo> fieldInfos = dataFile.getFieldInfos();
        Object[] objs = new Object[fieldInfos.size()];
        for (int i = 0; i < fieldInfos.size(); i++) {
            FieldInfo fieldInfo = fieldInfos.get(i);
            //单列
            if (fieldInfo.getIndex() != -1) {
                objs[i] = fieldInfo.call(strArr[i]);
                //多列
            } else {
                int tmpSize = fieldInfo.getEndIndex() - fieldInfo.getStartIndex() + 1;
                String[] tmp = new String[tmpSize];
                System.arraycopy(strArr, fieldInfo.getStartIndex(), tmp, 0, tmpSize);
                objs[i] = fieldInfo.call(tmp);
            }
        }
        return RowFactory.create(objs);
    }
}