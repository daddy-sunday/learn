package com.zhiyuan.zm.raft.dto.monitor;

/**
 * 统一 API 响应 DTO
 * @author zhouzhiyuan
 * @date 2026/03/06
 */
public class MonitorResponseDTO {

    /**
     * 响应码
     */
    private int code;

    /**
     * 响应消息
     */
    private String message;

    /**
     * 响应数据
     */
    private Object data;

    public MonitorResponseDTO() {
    }

    public MonitorResponseDTO(int code, String message, Object data) {
        this.code = code;
        this.code = code;
        this.message = message;
        this.data = data;
    }

    public static MonitorResponseDTO success(Object data) {
        return new MonitorResponseDTO(200, "success", data);
    }

    public static MonitorResponseDTO error(int code, String message) {
        return new MonitorResponseDTO(code, message, null);
    }

    public static MonitorResponseDTO error(String message) {
        return new MonitorResponseDTO(500, message, null);
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }
}
