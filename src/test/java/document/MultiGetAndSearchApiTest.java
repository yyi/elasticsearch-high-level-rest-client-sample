package document;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class MultiGetAndSearchApiTest {

    private Logger log = LoggerFactory.getLogger(MultiGetAndSearchApiTest.class);

    @Test
    public void multiGetTest() {
        final MultiGetApi api = new MultiGetApi();

        SearchResponse searchResponse = api.search("jvm");
        searchResponse.getHits().forEach(
                hit -> log.info(hit.getIndex() + " : " + hit.getSourceAsMap().toString()+"\n"+hit.getHighlightFields().toString())
        );

        final MultiGetResponse responses = api.multiGet(List.of(
                Map.of("index", "my_index", "id", "532c96ba-b689-4a5d-95bd-b4509dc4d252"),
                Map.of("index", "my_index", "id", "nbAxp3kBQb85oxf6CcIS")
        ));

        responses.forEach(response -> {
            final GetResponse getResponse = response.getResponse();

            if (getResponse != null) {
                log.info("Index: {}", getResponse.getIndex());
                log.info("Id: {}", getResponse.getId());

                if (getResponse.isExists()) {
                    log.info("Version: {}", getResponse.getVersion());
                    log.info("Document: {}", getResponse.getSourceAsMap());
                } else {
                    log.info("The document doesn't exists.");
                }
            } else {
                log.info("The index doesn't exists.");
            }
        });
    }
}
