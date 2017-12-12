package edu.hhu.stonk.spark.datafile;

import edu.hhu.stonk.spark.exception.CantConverException;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

/**
 * 列属性描述
 *
 * @author hayes, @create 2017-12-11 19:02
 **/
public class FieldInfo {

    private static final String DOUBLE_DATATYPE = "double";
    private static final String BOOLEAN_DATATYPE = "boolean";
    private static final String INTEGER_DATATYPE = "int";
    private static final String STRING_DATATYPE = "string";
    private static final String TIMESTAMP_DATATYPE = "timestamp";
    private static final String LONG_DATATYPE = "long";
    private static final String NULL_DATATYPE = "null";

    /**
     * 数据类型
     */
    public String dataType;

    /**
     * 字段名称
     */
    public String name;

    /**
     * 是否可以为空
     */
    public boolean nullable;

    /**
     * index
     */
    public int index;

    /**
     * TODO: 支持向量类型 VectorUDT
     * StructField，
     *
     * @return
     * @throws CantConverException
     */
    public StructField convertToStructField() throws CantConverException {
        DataType type = getSparkDataType();
        return DataTypes.createStructField(name, type, nullable);
    }

    /**
     * Spark SQL DataType
     *
     * @return
     */
    public DataType getSparkDataType() throws CantConverException {
        switch (dataType) {
            case DOUBLE_DATATYPE: {
                return DataTypes.DoubleType;
            }
            case BOOLEAN_DATATYPE: {
                return DataTypes.BooleanType;
            }
            case INTEGER_DATATYPE: {
                return DataTypes.IntegerType;
            }
            case STRING_DATATYPE: {
                return DataTypes.StringType;
            }
            case TIMESTAMP_DATATYPE: {
                return DataTypes.TimestampType;
            }
            case LONG_DATATYPE: {
                return DataTypes.LongType;
            }
            case NULL_DATATYPE: {
                return DataTypes.NullType;
            }
            default: {
                throw new CantConverException("不支持的类型");
            }
        }
    }


    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }
}
