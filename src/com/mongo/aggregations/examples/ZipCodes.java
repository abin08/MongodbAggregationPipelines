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
import com.mongodb.client.model.Sorts;

/**
 * @author Abin K. Antony
 * 21-May-2019
 * @version 1.0
 */
public class ZipCodes {
    public static void main(String[] args) {
	MongoClient mongoClient = new MongoClient("localhost",27017);
	MongoDatabase database = mongoClient.getDatabase("zips");
	MongoCollection<Document> mongoCollection = database.getCollection("zipcodes");
	
	Block<Document> printBlock = new Block<Document>() {
	    
	    @Override
	    public void apply(Document document) {
		System.out.println(document.toJson());
	    }
	};
	
//	print states with population above 10 million
	System.out.println("States with population above 10 million");
	mongoCollection.aggregate(
		Arrays.asList(
			Aggregates.group("$state", Accumulators.sum("totalPop","$pop")),
			Aggregates.match(Filters.gte("totalPop",10000000))
		)
	).forEach(printBlock);
	
//	print average city population by state
	System.out.println("Average city population by state");
	mongoCollection.aggregate(
		Arrays.asList(
			Aggregates.group(new Document("state","$state").append("city", "$city"), 
				Accumulators.sum("pop", "$pop")),
			Aggregates.group("$_id.state", Accumulators.avg("avgCityPop", "$pop"))
		)
	).forEach(printBlock);
	
//	print largest and smallest cities by state
	System.out.println("Largest and smallest cities by state");
	mongoCollection.aggregate(
		Arrays.asList(
			Aggregates.group(new Document("state", "$state").append("city", "$city")
				, Accumulators.sum("pop", "$pop")),
			Aggregates.sort(Sorts.ascending("pop")),
			Aggregates.group("$_id.state", Accumulators.last("biggestCity", "$_id.city"),
				Accumulators.last("biggestPop", "$pop"),
				Accumulators.first("smallestCity", "$_id.city"),
				Accumulators.first("smallestPop", "$pop")
			),
			Aggregates.project(
				new Document("_id",0)
				.append("state", "$_id")
				.append("biggestCity", new Document("name","$biggestCity").append("pop", "$biggestPop"))
				.append("smallestCity", new Document("name", "$smallestCity").append("pop", "$smallestPop"))
			)
		)
	).forEach(printBlock);
	mongoClient.close();
    }
}
