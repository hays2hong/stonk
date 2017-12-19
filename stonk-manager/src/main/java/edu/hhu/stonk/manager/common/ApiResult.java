package edu.hhu.stonk.manager.common;

/**
 * api result
 *
 * @author hayes, @create 2017-12-19 15:06
 **/
public class ApiResult<T> {

    private T data;

    private boolean sucess;

    private String msg;

    private String errMsg;

    private ApiResult() {
    }

    public static <T> ApiResult buildSucessWithData(String msg, T data) {
        ApiResult rst = new ApiResult();
        rst.setSucess(true);
        rst.setMsg(msg);
        rst.setData(data);
        return rst;
    }

    public static ApiResult buildSucess(String msg) {
        ApiResult rst = new ApiResult();
        rst.setSucess(true);
        rst.setMsg(msg);
        return rst;
    }

    public static ApiResult buildFail(String errMsg) {
        ApiResult rst = new ApiResult();
        rst.setSucess(false);
        rst.setErrMsg(errMsg);
        return rst;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public boolean isSucess() {
        return sucess;
    }

    public void setSucess(boolean sucess) {
        this.sucess = sucess;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public String getErrMsg() {
        return errMsg;
    }

    public void setErrMsg(String errMsg) {
        this.errMsg = errMsg;
    }
}
