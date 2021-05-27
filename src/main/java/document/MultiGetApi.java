package document;

import client.LocalhostClient;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class MultiGetApi {

    public MultiGetResponse multiGet(List<Map<String, String>> documents) {
        try (final RestHighLevelClient client = LocalhostClient.create()) {
            final MultiGetRequest request = new MultiGetRequest();
            documents.forEach(document -> request.add(new MultiGetRequest.Item(
                    document.get("index"),
                    document.get("id")
            )));

            return client.mget(request, RequestOptions.DEFAULT);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    public SearchResponse search(String queryString) {
        try (final RestHighLevelClient client = LocalhostClient.create()) {

            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.query(QueryBuilders.queryStringQuery(queryString));
            sourceBuilder.highlighter(SearchSourceBuilder.highlight().preTags("<pre>").postTags("</pre>").field("*"));
            return client.search(new SearchRequest().source(sourceBuilder), RequestOptions.DEFAULT);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }
}
