/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.gateway.service.result;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.results.FetchOrientation;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.api.results.ResultSetImpl;
import org.apache.flink.table.gateway.service.utils.SqlExecutionException;
import org.apache.flink.table.utils.print.RowDataToStringConverter;
import org.apache.flink.util.CloseableIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.api.internal.StaticResultProvider.SIMPLE_ROW_DATA_TO_STRING_CONVERTER;

/**
 * A fetcher to fetch result from submitted statement.
 *
 * <p>The fetcher uses the {@link Iterator} model. It means every time fetch the result with the
 * current token, the fetcher will move forward and retire the old data.
 *
 * <p>After closes, the fetcher will not fetch the results from the remote but is able to return all
 * data in the local cache.
 */
public class ResultFetcher {

    private static final Logger LOG = LoggerFactory.getLogger(ResultFetcher.class);
    private static final int TABLE_RESULT_MAX_INITIAL_CAPACITY = 5000;
    private static final RowDataToStringConverter DEFAULT_CONVERTER =
            SIMPLE_ROW_DATA_TO_STRING_CONVERTER;

    private final OperationHandle operationHandle;

    private final ResolvedSchema resultSchema;
    private final ResultStore resultStore;
    private final LinkedList<RowData> bufferedResults = new LinkedList<>();
    private final LinkedList<RowData> bufferedPrevResults = new LinkedList<>();
    private final RowDataToStringConverter converter;

    private final boolean isQueryResult;

    @Nullable private final JobID jobID;

    private final ResultKind resultKind;

    private long currentToken = 0;
    private boolean noMoreResults = false;

    private ResultFetcher(
            OperationHandle operationHandle,
            ResolvedSchema resultSchema,
            CloseableIterator<RowData> resultRows,
            RowDataToStringConverter converter,
            boolean isQueryResult,
            @Nullable JobID jobID,
            ResultKind resultKind) {
        this(
                operationHandle,
                resultSchema,
                resultRows,
                converter,
                isQueryResult,
                jobID,
                resultKind,
                TABLE_RESULT_MAX_INITIAL_CAPACITY);
    }

    @VisibleForTesting
    ResultFetcher(
            OperationHandle operationHandle,
            ResolvedSchema resultSchema,
            CloseableIterator<RowData> resultRows,
            RowDataToStringConverter converter,
            boolean isQueryResult,
            @Nullable JobID jobID,
            ResultKind resultKind,
            int maxBufferSize) {
        this.operationHandle = operationHandle;
        this.resultSchema = resultSchema;
        this.resultStore = new ResultStore(resultRows, maxBufferSize);
        this.converter = converter;
        this.isQueryResult = isQueryResult;
        this.jobID = jobID;
        this.resultKind = resultKind;
    }

    private ResultFetcher(
            OperationHandle operationHandle,
            ResolvedSchema resultSchema,
            List<RowData> rows,
            @Nullable JobID jobID) {
        this.operationHandle = operationHandle;
        this.resultSchema = resultSchema;
        this.bufferedResults.addAll(rows);
        this.resultStore = ResultStore.DUMMY_RESULT_STORE;
        this.converter = DEFAULT_CONVERTER;
        this.isQueryResult = false;
        this.jobID = jobID;
        this.resultKind = ResultKind.SUCCESS_WITH_CONTENT;
    }

    public static ResultFetcher fromTableResult(
            OperationHandle operationHandle,
            TableResultInternal tableResult,
            boolean isQueryResult,
            @Nullable JobID jobID) {
        return new ResultFetcher(
                operationHandle,
                tableResult.getResolvedSchema(),
                tableResult.collectInternal(),
                tableResult.getRowDataToStringConverter(),
                isQueryResult,
                jobID,
                tableResult.getResultKind());
    }

    public static ResultFetcher fromResults(
            OperationHandle operationHandle, ResolvedSchema resultSchema, List<RowData> results) {
        return fromResults(operationHandle, resultSchema, results, null);
    }

    public static ResultFetcher fromResults(
            OperationHandle operationHandle,
            ResolvedSchema resultSchema,
            List<RowData> results,
            @Nullable JobID jobID) {
        return new ResultFetcher(operationHandle, resultSchema, results, jobID);
    }

    public void close() {
        resultStore.close();
    }

    public ResolvedSchema getResultSchema() {
        return resultSchema;
    }

    public synchronized ResultSet fetchResults(FetchOrientation orientation, int maxFetchSize) {
        long token;
        switch (orientation) {
            case FETCH_NEXT:
                token = currentToken;
                break;
            case FETCH_PRIOR:
                token = currentToken - 1;
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format("Unknown fetch orientation: %s.", orientation));
        }

        if (orientation == FetchOrientation.FETCH_NEXT && bufferedResults.isEmpty()) {
            // make sure data is available in the buffer
            resultStore.waitUntilHasData();
        }

        return fetchResults(token, maxFetchSize);
    }

    /**
     * Fetch results from the result store. It tries to return the data cached in the buffer first.
     * If the buffer is empty, then fetch results from the {@link ResultStore}. It's possible
     * multiple threads try to fetch results in parallel. To keep the data integration, use the
     * synchronized to allow only one thread can fetch the result at any time.
     */
    public synchronized ResultSet fetchResults(long token, int maxFetchSize) {
        if (maxFetchSize <= 0) {
            throw new IllegalArgumentException("The max rows should be larger than 0.");
        }

        if (token == currentToken) {
            // equal to the Iterator.next()
            if (noMoreResults) {
                LOG.debug("There is no more result for operation: {}.", operationHandle);
                return buildEOSResultSet();
            }

            // a new token arrives, move the current buffer data into the prev buffered results.
            bufferedPrevResults.clear();
            if (bufferedResults.isEmpty()) {
                // buffered results have been totally consumed,
                // so try to fetch new results
                Optional<List<RowData>> newResults = resultStore.retrieveRecords();

                if (newResults.isPresent()) {
                    bufferedResults.addAll(newResults.get());
                } else {
                    noMoreResults = true;
                    return buildEOSResultSet();
                }
            }

            int resultSize = Math.min(bufferedResults.size(), maxFetchSize);
            LOG.debug(
                    "Fetching current result for operation: {}, token: {}, maxFetchSize: {}, resultSize: {}.",
                    operationHandle,
                    token,
                    maxFetchSize,
                    resultSize);

            // move forward
            currentToken++;
            // move result to buffer
            for (int i = 0; i < resultSize; i++) {
                bufferedPrevResults.add(bufferedResults.removeFirst());
            }
            return buildPayloadResultSet();
        } else if (token == currentToken - 1 && token >= 0) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Fetching previous result for operation: {}, token: {}, maxFetchSize: {}",
                        operationHandle,
                        token,
                        maxFetchSize);
            }
            if (maxFetchSize < bufferedPrevResults.size()) {
                String msg =
                        String.format(
                                "As the same token is provided, fetch size must be not less than the previous returned buffer size."
                                        + " Previous returned result size is %s, current max_fetch_size to be %s.",
                                bufferedPrevResults.size(), maxFetchSize);
                if (LOG.isDebugEnabled()) {
                    LOG.error(msg);
                }
                throw new SqlExecutionException(msg);
            }
            return buildPayloadResultSet();
        } else {
            String msg;
            if (currentToken == 0) {
                msg = "Expecting token to be 0, but found " + token + ".";
            } else {
                msg =
                        "Expecting token to be "
                                + currentToken
                                + " or "
                                + (currentToken - 1)
                                + ", but found "
                                + token
                                + ".";
            }
            if (LOG.isDebugEnabled()) {
                LOG.error(msg);
            }
            throw new SqlExecutionException(msg);
        }
    }

    @VisibleForTesting
    public ResultStore getResultStore() {
        return resultStore;
    }

    private ResultSet buildEOSResultSet() {
        return ResultSetImpl.newBuilder()
                .resultType(ResultSet.ResultType.EOS)
                .nextToken(null)
                .resolvedSchema(resultSchema)
                .data(Collections.emptyList())
                .build();
    }

    private ResultSet buildPayloadResultSet() {
        return ResultSetImpl.newBuilder()
                .resultType(ResultSet.ResultType.PAYLOAD)
                .nextToken(currentToken)
                .resolvedSchema(resultSchema)
                .data(new ArrayList<>(bufferedPrevResults))
                .converter(converter)
                .isQueryResult(isQueryResult)
                .jobID(jobID)
                .resultKind(resultKind)
                .build();
    }
}
