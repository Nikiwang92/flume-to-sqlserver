package flume.sqlserver.sink;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.sql.*;
import java.util.List;
import org.json.JSONObject;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.gson.Gson;

/**
 * A Source to read data from a SQL database. This source ask for new data in a table each configured time.<p>
 *
 * @author <a href="mailto:riccosir@qq.com">Ricco</a>
 */
public class SQLSink extends AbstractSink implements Configurable {

    private static final Logger LOG = LoggerFactory.getLogger(SQLSink.class);

    private String hostname;
    private String port;
    private String databaseName;
    private String sqltext;
    private String user;
    private String password;
    private int batchSize;      //每次提交的批次大小
    private SinkCounter sinkCounter; //自定义监控counter
    private MetricsProxyServer metricsProxy; //自定义发送监控数据到http
    private HttpClient httpClient;

    // 连接池
    private HikariDataSource dataSource; // 使用 HikariDataSource
    private int connectionPoolSize = 10; // 默认连接池大小

    public SQLSink() {
        LOG.info("Sqlserver Sink start...");
    }

    /**实现Configurable接口中的方法：可获取配置文件中的属性*/
    @Override
    public void configure(Context context) {
        hostname = context.getString("hostname");
        Preconditions.checkNotNull(hostname, "hostname must be set!!");
        port = context.getString("port");
        Preconditions.checkNotNull(port, "port must be set!!");
        databaseName = context.getString("databaseName");
        Preconditions.checkNotNull(databaseName, "databaseName must be set!!");
        sqltext = context.getString("sqltext");
        Preconditions.checkNotNull(sqltext, "sqltext must be set!!");
        user = context.getString("user");
        Preconditions.checkNotNull(user, "user must be set!!");
        password = context.getString("password");
        Preconditions.checkNotNull(password, "password must be set!!");
        batchSize = context.getInteger("batchSize", 100);       //设置了batchSize的默认值
        Preconditions.checkNotNull(batchSize > 0, "batchSize must be a positive number!!");
        connectionPoolSize = context.getInteger("connectionPoolSize", 14); // 获取连接池大小配置

        // 初始化 HTTP 客户端
        this.httpClient = new HttpClient();

        // 初始化连接池
        HikariConfig config = new HikariConfig();
        config.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        String url = "jdbc:sqlserver://" + hostname + ":" + port + ";databaseName=" + databaseName + ";encrypt=true;trustServerCertificate=true;;ConnectRetryCount=10;ConnectRetryInterval=10;loginTimeout=180;";
        config.setJdbcUrl(url);
        config.setUsername(user);
        config.setPassword(password);
        config.setMaximumPoolSize(connectionPoolSize); // 最大连接数
        config.setMinimumIdle(5);                     // 最小空闲连接数
        config.setMaxLifetime(1800000); // 连接最大生存时间，毫秒，30分钟
        config.setConnectionTimeout(30000); // 连接超时时间，毫秒，30秒
        config.setIdleTimeout(600000); // 空闲连接超时时间，毫秒，10分钟
        config.setValidationTimeout(3000); //验证连接超时时间
        config.setPoolName("Flume-HikariCP");

        dataSource = new HikariDataSource(config);

        try {
            dataSource.getConnection().close(); // 验证连接池是否正常工作
            LOG.info("SQLSink 配置完成，连接池初始化成功");
        } catch (SQLException e) {
            LOG.error("SQLSink 配置失败，连接池初始化失败: " + e.getMessage(), e);
            throw new RuntimeException("SQLSink 配置失败，连接池初始化失败", e); // 使用 RuntimeException
        }
    }

    /**
     * 服务启动时执行的代码，这里做准备工作
     */
    @Override
    public void start() {
        sinkCounter = new SinkCounter(getName());
        metricsProxy = new MetricsProxyServer();
        super.start();
    }

    /**
     * 服务关闭时执行
     */
    @Override
    public void stop() {
        sinkCounter.stop();
        super.stop();

        try {
            if (dataSource != null) {
                dataSource.close(); // 关闭连接池
                LOG.info("SQLSink 关闭，连接池已关闭");
            }
        } catch (Exception e) { // HikariCP 的 close() 方法抛出 Exception
            LOG.error("关闭连接池失败: " + e.getMessage(), e);
        }

        // 这里可以释放占用的资源
        if (httpClient != null) {
            httpClient.close();
        }
    }

    /**
     *  执行的事情：<br/>
     *  （1）持续不断的从channel中获取event放到batchSize大小的数组中<br/>
     *  （2）event可以获取到则进行event处理，否则返回Status.BACKOFF标识没有数据提交<br/>
     *  （3）batchSize中有内容则进行jdbc提交<br/>
     */
    @Override
    public Status process() throws EventDeliveryException {
        Status result = Status.READY;
        Channel channel = getChannel();
        String channelName = channel.getName(); // 获取 Channel 的名称
        Transaction transaction = channel.getTransaction();
        Event event;
        String content;
        String filename;

        List<JsonFile> jsons = Lists.newArrayList();

        Connection conn = null;
        PreparedStatement preparedStatement = null;

        try {
            transaction.begin();
            //sinkCounter.incrementEventCount();
            //LOG.info("开始执行transaction...");

            conn = dataSource.getConnection(); // 从连接池获取连接
            preparedStatement = conn.prepareStatement(sqltext);
            conn.setAutoCommit(false); // 禁用自动提交      

            for (int i = 0; i < batchSize; i++) {
                event = channel.take();
                if (event != null) {
                    content = new String(event.getBody());
                    filename = event.getHeaders().get("fileName");

                    //LOG.warn("batchsize={},filename={},sql text={}.",batchSize,filename, content);

                    if (isValidJson(content)) {
                        JsonFile json = new JsonFile();
                        json.setjson(content);
                        json.setfilename(filename);
                        jsons.add(json);
                        //sinkCounter.incrementEventCount();
                    } 
                    else {
                        LOG.error("Invalid JSON data detected and skipped,content text={},filename={} ",content,filename);
                        sinkCounter.incrementFailedEventCount();
                    }

                    //LOG.error("batchsize={},filename={},sql text={}.",batchSize,filename, content);

                    //JsonFile json = new JsonFile();
                    //json.setjson(content);
                    //json.setfilename(filename);
                    //jsons.add(json);
                    //sinkCounter.incrementEventCount();
                } else {
                    result = Status.BACKOFF;
                    break;
                }
            }

            if (jsons.size() > 0) {

                preparedStatement.clearBatch();
                for (JsonFile temp : jsons) {
                    preparedStatement.setString(1, temp.getjson());
                    preparedStatement.setString(2, temp.getfilename());
                    preparedStatement.addBatch();
                }

                preparedStatement.executeBatch();
                conn.commit(); // 手动提交
            } else {
                LOG.info("当前channel: {} 数据塞入已完成...",channelName);
            }

            transaction.commit();
            //sinkCounter.incrementSuccessfulEventCount();

        } catch (ChannelException e) {
            transaction.rollback();
            LOG.error("Unable to get event from channel. Exception follows.", e);
            Throwables.propagate(e);
            result = Status.BACKOFF;

        } catch (BatchUpdateException e) {
            transaction.rollback();
            LOG.error("批处理执行异常：SQLState={}, ErrorCode={}, Message={}", e.getSQLState(), e.getErrorCode(), e.getMessage());
            Throwables.propagate(e);
            result = Status.BACKOFF;

        } catch (SQLException se) {
            transaction.rollback();
            LOG.error("SQL异常：SQLState={}, ErrorCode={}, Message={}", se.getSQLState(), se.getErrorCode(), se.getMessage());
            Throwables.propagate(se);
            result = Status.BACKOFF;

        } catch (Exception e) {
            transaction.rollback();
            //sinkCounter.incrementFailedEventCount();
            result = Status.BACKOFF;
            LOG.error("Failed to commit flume transaction. Flume transaction rolled back.", e);
            Throwables.propagate(e);

        } finally {
            // 确保连接和 PreparedStatement 被关闭
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    LOG.error("关闭 PreparedStatement 失败: " + e.getMessage(), e);
                }
            }
            if (conn != null) {
                try {
                    conn.close(); // 将连接返回到连接池
                } catch (SQLException e) {
                    LOG.error("将连接返回到连接池失败: " + e.getMessage(), e);
                }
            }
            transaction.close();
        }

        return result;
    }

    // 添加辅助方法
    private boolean isValidJson(String content) {
        try {
                // 检查是否包含过多null字符
                if (content.contains("\u0000")) {
                    return false;
                }
            
                // 简单检查JSON格式
                if (!content.trim().startsWith("{") || !content.trim().endsWith("}")) {
                    return false;
                }
            
                // 尝试解析JSON
                new JSONObject(content);
                return true;
        } catch (Exception e) {
            return false;
        }
    }

}