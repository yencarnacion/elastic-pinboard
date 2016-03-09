package com.example
/**
 * Created by yamir on 3/9/16.
 */

import groovy.json.JsonSlurper
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse
import org.elasticsearch.action.get.GetRequestBuilder
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.index.IndexRequestBuilder
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.transport.InetSocketTransportAddress

public class PinboardElasticsearch {

    Client client;
    final byte[] ipAddr = [192, 168,33, 10]; // Your ElasticSearch node ip goes here
    final String indexName = "pinboard";
    final String documentType = "bookmark";

    public PinboardElasticsearch() {
        client = TransportClient.builder().build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByAddress(ipAddr), 9300));
    }

    public void index(){

        // Create Mapping
        def jsonSlurper = new JsonSlurper()
        def mapping = jsonSlurper.parseText '''
 {
                    "bookmark": {
                        "properties": {
                            "description": {
                                "type": "string"
                            },
                            "extended": {
                                "type": "string"
                            },
                            "hash": {
                                "type": "string"
                            },
                            "href": {
                                "type": "string"
                            },
                            "meta": {
                                "type": "string"
                            },
                            "shared": {
                                "type": "string"
                            },
                            "tags": {
                                "type": "string",
                                "analyzer": "lowercasespaceanalyzer"
                            },
                            "time": {
                                "type": "date",
                                "format": "strict_date_optional_time||epoch_millis"
                            },
                            "toread": {
                                "type": "string"
                            }
                        }
                    }
                }'''
        def settings = jsonSlurper.parseText '''
                    "analysis": {
                        "analyzer": {
                            "lowercasespaceanalyzer": {
                                "type": "custom",
                                "tokenizer": "whitespace",
                                "filter": [
                                        "lowercase"
                                ]
                            }
                        }
                    }
'''

        final IndicesExistsResponse res = client.admin().indices().prepareExists(indexName).execute().actionGet();
        if (res.isExists()) {
            final DeleteIndexRequestBuilder delIdx = client.admin().indices().prepareDelete(indexName);
            delIdx.execute().actionGet();
        }

        final CreateIndexRequestBuilder createIndexRequestBuilder;

        if(!res.isExists()){
            createIndexRequestBuilder = client.admin().indices().prepareCreate(indexName);
            System.out.println(mapping.toString());
            createIndexRequestBuilder.setSettings(settings)
            createIndexRequestBuilder.execute().actionGet();

            createIndexRequestBuilder.addMapping(documentType, mapping);

            // MAPPING DONE
            createIndexRequestBuilder.execute().actionGet();
        }


        addDocuments();
    }

    private void addDocuments(){
        def inputFile = new File("pinboard.json")
        def InputJSON = new JsonSlurper().parseText(inputFile.text)
        int count = 1;
        // Add documents
        InputJSON.each{
            System.out.println("${count}: ${it.description}");
            //it["date"] = new Date().clearTime().format("yyyy-MM-dd").toString();
            it["id"] = count;
            //it["title_na"] = new String(it["title"]);
            IndexRequestBuilder indexRequestBuilder = client.prepareIndex(indexName, documentType, java.util.UUID.randomUUID() as String);
            indexRequestBuilder.setSource(it);
            indexRequestBuilder.execute().actionGet();
            count = count +1;
        }
    }

    private String getValue2(final String indexName, final String documentType,
                             final String documentId, final String fieldName) {
        GetRequestBuilder getRequestBuilder = client.prepareGet(indexName, documentType, documentId);
        getRequestBuilder.setFields([ fieldName ] as String[]);
        GetResponse response2 = getRequestBuilder.execute().actionGet();
        String name = response2.getField(fieldName).getValue().toString();
        return name;
    }

    public String getValue(final String documentId, final String fieldName){
        getValue2(indexName, documentType, documentId, fieldName )
    }

    public void close() {
        client.close()
    }

    public static void main (String[] Args){
        PinboardElasticsearch so =  new PinboardElasticsearch();

        so.index();
        //Thread.sleep(5000L);
        //System.out.println(so.getValue("1", "title"));
        so.close();
    }
}
