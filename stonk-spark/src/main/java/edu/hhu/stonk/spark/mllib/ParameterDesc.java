package edu.hhu.stonk.spark.mllib;

/**
 * 算法参数描述
 *
 * @author hayes, @create 2017-12-11 18:34
 **/
public class ParameterDesc {

    /**
     * 在算法中的属性名
     */
    public String name;

    /**
     * 展示的名字（可中文）
     */
    public String showName;

    /**
     * 参数类型（int、String、...）
     */
    public ParameterValueType valueType;

    public Class getJavaTypeClass() {
        switch (valueType) {
            case BOOLEAN: {
                return Boolean.class;
            }
            case STRING: {
                return String.class;
            }
            case INT: {
                return Integer.class;
            }
            case DOUBLE: {
                return Double.class;
            }
        }
        return String.class;
    }

    public Object valueOf(String value) {
        switch (valueType) {
            case BOOLEAN: {
                return Boolean.valueOf(value);
            }
            case STRING: {
                return value;
            }
            case INT: {
                return Integer.valueOf(value);
            }
            case DOUBLE: {
                return Double.valueOf(value);
            }
        }
        return value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getShowName() {
        return showName;
    }

    public void setShowName(String showName) {
        this.showName = showName;
    }

    public ParameterValueType getValueType() {
        return valueType;
    }

    public void setValueType(ParameterValueType valueType) {
        this.valueType = valueType;
    }
}
