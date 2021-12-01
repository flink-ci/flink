package org.apache.flink.mongodb.internal.options;

import org.apache.flink.annotation.Internal;
import org.apache.flink.mongodb.MongodbConnectionOptions;

import javax.annotation.Nullable;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Options for the Mongodb connector. */
@Internal
public class MongodbConnectorOptions extends MongodbConnectionOptions {
    private static final long serialVersionUID = 1L;

    private final String tableName;

    private final @Nullable Integer parallelism;

    private MongodbConnectorOptions(
            String dbURL,
            String tableName,
            @Nullable String username,
            @Nullable String password,
            @Nullable Integer parallelism,
            int connectionCheckTimeoutSeconds) {
        super(dbURL, username, password, connectionCheckTimeoutSeconds);
        this.tableName = tableName;
        this.parallelism = parallelism;
    }

    public String getTableName() {
        return tableName;
    }

    public Integer getParallelism() {
        return parallelism;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof MongodbConnectorOptions) {
            MongodbConnectorOptions options = (MongodbConnectorOptions) o;
            return Objects.equals(url, options.url)
                    && Objects.equals(tableName, options.tableName)
                    && Objects.equals(username, options.username)
                    && Objects.equals(password, options.password)
                    && Objects.equals(parallelism, options.parallelism)
                    && Objects.equals(
                            connectionCheckTimeoutSeconds, options.connectionCheckTimeoutSeconds);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                url, tableName, username, password, parallelism, connectionCheckTimeoutSeconds);
    }

    /** Builder of {@link MongodbConnectorOptions}. */
    public static class Builder {
        private String dbURL;
        private String tableName;
        private String driverName;
        private String username;
        private String password;
        private Integer parallelism;
        private int connectionCheckTimeoutSeconds = 60;

        /** required, table name. */
        public Builder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        /** optional, user name. */
        public Builder setUsername(String username) {
            this.username = username;
            return this;
        }

        /** optional, password. */
        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        /** optional, connectionCheckTimeoutSeconds. */
        public Builder setConnectionCheckTimeoutSeconds(int connectionCheckTimeoutSeconds) {
            this.connectionCheckTimeoutSeconds = connectionCheckTimeoutSeconds;
            return this;
        }

        /** required, JDBC DB url. */
        public Builder setDBUrl(String dbURL) {
            this.dbURL = dbURL;
            return this;
        }

        public Builder setParallelism(Integer parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        public MongodbConnectorOptions build() {
            checkNotNull(dbURL, "No dbURL supplied.");
            checkNotNull(tableName, "No tableName supplied.");
            return new MongodbConnectorOptions(
                    dbURL,
                    tableName,
                    username,
                    password,
                    parallelism,
                    connectionCheckTimeoutSeconds);
        }
    }
}
