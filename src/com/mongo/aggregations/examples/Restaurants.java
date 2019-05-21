/**
 * 
 */
package com.mongo.aggregations.examples;

import java.util.Arrays;

import org.bson.Document;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;

/**
 * @author Abin K. Antony 
 * 21-May-2019
 * @version 1.0
 */
public class Restaurants {
    public static void main(String[] args) {
	MongoClient mongoClient = new MongoClient("localhost", 27017);
	MongoDatabase database = mongoClient.getDatabase("test");
	MongoCollection<Document> mongoCollection = database
		.getCollection("restaurants");
	Block<Document> printBlock = new Block<Document>() {

	    @Override
	    public void apply(Document document) {
		System.out.println(document.toJson());
	    }
	};
	mongoCollection
		.aggregate(Arrays.asList(
			Aggregates.match(Filters.eq("categories", "Bakery")),
			Aggregates.group("$stars",
				Accumulators.sum("count", 1))))
		.forEach(printBlock);
	mongoCollection
		.aggregate(Arrays.asList(Aggregates.project(Projections.fields(
			Projections.excludeId(), Projections.include("name"),
			Projections.computed("firstCategory",
				new Document("$arrayElemAt",
					Arrays.asList("$categories", 0)))))))
		.forEach(printBlock);
	mongoClient.close();
    }
}
