package com.zhiyuan.zm.raft.monitor;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.zhiyuan.zm.raft.dto.monitor.MonitorResponseDTO;
import com.zhiyuan.zm.raft.monitor.handler.ApiHandler;

import fi.iki.elonen.NanoHTTPD;

/**
 * 监控 HTTP 服务器
 * 基于 NanoHTTPD 实现
 *
 * @author zhouzhiyuan
 * @date 2026/03/06
 */
public class MonitorServer extends NanoHTTPD {

    private final MonitorService monitorService;
    private final ApiHandler apiHandler;

    private static final String CONTENT_TYPE_JSON = "application/json";
    private static final String CONTENT_TYPE_HTML = "text/html; charset=utf-8";
    private static final String CONTENT_TYPE_CSS = "text/css";
    private static final String CONTENT_TYPE_JS = "application/javascript";

    public MonitorServer(int port, MonitorService monitorService) {
        super(port);
        this.monitorService = monitorService;
        this.apiHandler = new ApiHandler(monitorService);
    }

    @Override
    public Response serve(IHTTPSession session) {
        String uri = session.getUri();
        Method method = session.getMethod();

        try {
            // API 路由
            if (uri.startsWith("/api/")) {
                return handleApiRequest(session, uri, method);
            }

            // 静态资源路由
            if (uri.equals("/") || uri.equals("/monitor") || uri.equals("/monitor/")) {
                return serveStaticFile("/web/index.html", CONTENT_TYPE_HTML);
            }
            if (uri.equals("/monitor")) {
                return serveStaticFile("/web/index.html", CONTENT_TYPE_HTML);
            }
            if (uri.startsWith("/css/")) {
                return serveStaticFile("/web" + uri, CONTENT_TYPE_CSS);
            }
            if (uri.startsWith("/js/")) {
                return serveStaticFile("/web" + uri, CONTENT_TYPE_JS);
            }

            // 默认返回 404
            return newFixedLengthResponse(Response.Status.NOT_FOUND, CONTENT_TYPE_JSON,
                JSON.toJSONString(MonitorResponseDTO.error(404, "Not Found")));

        } catch (Exception e) {
            return newFixedLengthResponse(Response.Status.INTERNAL_ERROR, CONTENT_TYPE_JSON,
                JSON.toJSONString(MonitorResponseDTO.error(500, "Internal Server Error: " + e.getMessage())));
        }
    }

    /**
     * 处理 API 请求
     */
    private Response handleApiRequest(IHTTPSession session, String uri, Method method) throws IOException {
        if (!Method.GET.equals(method)) {
            return newFixedLengthResponse(Response.Status.METHOD_NOT_ALLOWED, CONTENT_TYPE_JSON,
                JSON.toJSONString(MonitorResponseDTO.error(405, "Method Not Allowed")));
        }

        Map<String, String> params = session.getParms();
        String responseJson;

        // 路由分发
        if (uri.equals("/api/cluster")) {
            responseJson = apiHandler.handleClusterStatus();
        } else if (uri.equals("/api/node/status")) {
            responseJson = apiHandler.handleNodeStatus();
        } else if (uri.equals("/api/node/metrics")) {
            responseJson = apiHandler.handleNodeMetrics();
        } else if (uri.equals("/api/transactions")) {
            responseJson = apiHandler.handleTransactionStatus();
        } else if (uri.equals("/api/storage")) {
            responseJson = apiHandler.handleStorageStats();
        } else if (uri.equals("/api/health")) {
            responseJson = JSON.toJSONString(MonitorResponseDTO.success("OK"));
        } else {
            return newFixedLengthResponse(Response.Status.NOT_FOUND, CONTENT_TYPE_JSON,
                JSON.toJSONString(MonitorResponseDTO.error(404, "API Not Found")));
        }

        return newFixedLengthResponse(Response.Status.OK, CONTENT_TYPE_JSON, responseJson);
    }

    /**
     * 提供静态文件服务
     */
    private Response serveStaticFile(String path, String contentType) {
        try (InputStream inputStream = getClass().getResourceAsStream(path)) {
            if (inputStream == null) {
                // 如果资源不存在，返回 index.html（用于前端路由）
                if (path.startsWith("/web/") && !path.endsWith(".html")) {
                    return serveStaticFile("/web/index.html", CONTENT_TYPE_HTML);
                }
                return newFixedLengthResponse(Response.Status.NOT_FOUND, CONTENT_TYPE_HTML,
                    "<html><body><h1>404 - Resource Not Found</h1></body></html>");
            }

            // Java 8 兼容：使用 Apache Commons IO 或手动读取
            byte[] bytes = readAllBytes(inputStream);
            return newFixedLengthResponse(Response.Status.OK, contentType,
                new String(bytes, StandardCharsets.UTF_8));
        } catch (IOException e) {
            return newFixedLengthResponse(Response.Status.INTERNAL_ERROR, CONTENT_TYPE_HTML,
                "<html><body><h1>500 - Internal Server Error</h1></body></html>");
        }
    }

    /**
     * 兼容 Java 8 的读取方法
     */
    private byte[] readAllBytes(InputStream inputStream) throws IOException {
        java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream();
        byte[] data = new byte[4096];
        int nRead;
        while ((nRead = inputStream.read(data, 0, data.length)) != -1) {
            buffer.write(data, 0, nRead);
        }
        buffer.flush();
        return buffer.toByteArray();
    }

    /**
     * 启动服务器
     */
    public void start() throws IOException {
        start(NanoHTTPD.SOCKET_READ_TIMEOUT, false);
    }

    /**
     * 停止服务器
     */
    @Override
    public void stop() {
        super.stop();
    }
}
