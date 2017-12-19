package edu.hhu.stonk.spark.datafile;

import edu.hhu.stonk.spark.exception.CantConverException;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;

import java.io.Serializable;


/**
 * 列属性描述
 *
 * @author hayes, @create 2017-12-11 19:02
 **/
public class FieldInfo implements Serializable {

    private static final long serialVersionUID = -7123058551214352633L;

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
    private String dataType;

    /**
     * 字段名称
     */
    private String name;

    /**
     * 是否可以为空
     */
    private boolean nullable;

    /**
     * index
     */
    private int index = -1;

    /**
     * Start index
     */
    private int startIndex;

    /**
     * end index
     */
    private int endIndex;

    /**
     * StructField，
     *
     * @return
     * @throws CantConverException
     */
    public StructField convertToStructField() throws CantConverException {
        if (index != -1) {
            return DataTypes.createStructField(name, sparkDataType(), nullable);
        } else {
            switch (dataType) {
                case STRING_DATATYPE: {
                    return new StructField(name, DataTypes.createArrayType(DataTypes.StringType), nullable, Metadata.empty());
                }
                case DOUBLE_DATATYPE:
                case INTEGER_DATATYPE:
                case LONG_DATATYPE: {
                    return new StructField(name, new VectorUDT(), nullable, Metadata.empty());
                }
                default:
                    throw new CantConverException("不合法类型");
            }
        }
    }

    /**
     * String[] -> Obj
     *
     * @param value
     * @return
     * @throws Exception
     */
    public Object call(String[] value) throws Exception {
        switch (dataType) {
            case STRING_DATATYPE: {
                return value;
            }
            case DOUBLE_DATATYPE:
            case INTEGER_DATATYPE:
            case LONG_DATATYPE: {
                double[] vect = new double[value.length];
                try {
                    for (int i = 0; i < value.length; i++) {
                        vect[i] = Double.valueOf(value[i]);
                    }
                } catch (Exception e) {
                    throw new CantConverException(e.getMessage());
                }
                return Vectors.dense(vect);
            }
            default:
                throw new CantConverException("不合法类型");
        }
    }

    /**
     * String -> obj
     *
     * @param value
     * @return
     * @throws Exception
     */
    public Object call(String value) throws Exception {
        if (StringUtils.isEmpty(value) && !nullable) {
            throw new Exception(name + "列为空");
        } else if (StringUtils.isEmpty(value)) {
            return null;
        }

        try {
            switch (dataType) {
                case BOOLEAN_DATATYPE: {
                    return Boolean.valueOf(value);
                }
                case STRING_DATATYPE: {
                    return value;
                }
                case DOUBLE_DATATYPE: {
                    return Double.valueOf(value);
                }
                case INTEGER_DATATYPE: {
                    return Integer.valueOf(value);
                }
                case LONG_DATATYPE: {
                    return Long.valueOf(value);
                }
                case TIMESTAMP_DATATYPE: {
                    return Long.valueOf(value);
                }
                case NULL_DATATYPE: {
                    return null;
                }
                default: {
                    throw new CantConverException("dataType不支持");
                }
            }
        } catch (Exception e) {
            throw new Exception(value + "->" + dataType + " error");
        }

    }

    /**
     * Spark SQL DataType
     *
     * @return
     */
    public DataType sparkDataType() throws CantConverException {
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

    public int getStartIndex() {
        return startIndex;
    }

    public void setStartIndex(int startIndex) {
        this.startIndex = startIndex;
    }

    public int getEndIndex() {
        return endIndex;
    }

    public void setEndIndex(int endIndex) {
        this.endIndex = endIndex;
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

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
