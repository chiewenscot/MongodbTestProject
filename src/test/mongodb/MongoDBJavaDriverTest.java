package test.mongodb;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

/*
 * @author Jimmy Tai
 * 
 * This class implemented example codes of accessing MongoDB using JavaDriver. A sample document downloaded from official site has attached with this class as example document.
 * 
 * Environment:
 * MacBook Pro with OSX
 * MongoDB 3.2.3
 * JDK 8
 * Eclipse 4.5.1
 * 
 * Management UI for verifying data:
 * MongoBooster 1.3.1
 * 
 * Libraries required:
 * Bson 3.0.4
 * MongoDB Driver 3.0.4
 * 
 * sample data for importing into MongoDB:
 * dataset.json in the same folder (file downloaded from MongoDB web-site)
 * **/
public class MongoDBJavaDriverTest {

	public MongoDBJavaDriverTest() {
	}

	public static void main(String[] args) {
		//example 1: create Mongo DB connection, the MongoClient constructor can be initialized with argument if connecting to a MongoDB in local machine.
		try(MongoClient mongoClient = new MongoClient("localhost", 27017)){
			MongoDatabase db = mongoClient.getDatabase("local");
			
			MongoDBJavaDriverTest test = new MongoDBJavaDriverTest();
		
			//example 2: drop a collection
			db.getCollection("test").drop();
			
			//example 3: insert an entry to test collection
			System.out.println("Example 3:");
			test.insertOne(db, "test", new Document("greeting", "Hello"));
			test.insertOne(db, "test", new Document("greeting", "Bonjour le monde"));

			//example 4: update geeting value
			System.out.println("Example 4:");
			test.updateOne(db, "test", Filters.eq("greeting", "Hello"), new Document("$set", new Document("greeting", "Hello World")));
			
			//example 5: delete Hello World
			System.out.println("Example 5");
			test.deleteOne(db, "test", Filters.eq("greeting", "Hello World"));
			
			//example 6: print all records of the test collection
			System.out.println("Example 6:");
			test.printAll(db, "test");
			
			/**
			 * example 7: find a restaurant by name and print to standard output
			 * 
			 * Example script:
			 * db.restaurants.find({"name":"Dj Reynolds Pub And Restaurant"})
			 * 
			 */
			System.out.println("Example 7:");
			test.findByBson(db, "restaurants", new Document("name", "Dj Reynolds Pub And Restaurant"));
			
			/**
			 * example 8: find restaurants by a given zipcode and print to standard output
			 * 
			 * Example script:
			 * db.restaurants.find({"address.zipcode":"11223"})
			 * 
			 */
			System.out.println("Example 8:");
			test.findByBson(db, "restaurants", Filters.eq("address.zipcode", "11223"));
			
			/**
			 * example 9: find restaurants by multiple conditions, sort by restantant_id, and print to standard output
			 * 
			 * Example script:
			 * db.restaurants.find({$or:[{"cuisine":"Hamburgers", "borough":"Manhattan","address.zipcode":"10019"},{"cuisine":"Irish", "borough":"Manhattan","address.zipcode":"10019"}]}).sort({ "restaurant_id":1 })
			 * 
			 */
			System.out.println("Example 9:");
			test.findByBsonAndSort(db, "restaurants", Filters.or(Filters.and(Filters.eq("cuisine", "Hamburgers"), Filters.eq("borough","Manhattan"), Filters.eq("address.zipcode","10019")), Filters.and(Filters.eq("cuisine","Irish"), Filters.eq("borough","Manhattan"), Filters.eq("address.zipcode","10019"))), new Document("restaurant_id", 1));
		}
		
	}
		
	
	/**
	 * delete one document
	 * 
	 * @param	db	the MongoDB database proxy
	 * @param	docName	the name of the collection to filter
	 * @param	filter	select to delete
	 * */
	public void deleteOne(MongoDatabase db, String docName, Bson filter){
		DeleteResult dr = db.getCollection(docName).deleteOne(filter);
		System.out.println(dr);
	}
	
	/**
	 * update one document
	 * 
	 * @param	db	the MongoDB database proxy
	 * @param	docName	the name of the collection to filter
	 * @param	filter	is like the where condition of update statement
	 * @param	update	is the fields to be updated 
	 * */
	public void updateOne(MongoDatabase db, String docName, Bson filter, Bson update){
		UpdateResult ur = db.getCollection(docName).updateOne(filter, update);
		System.out.println(ur);
	}
	
	/**
	 * find by a given query condition
	 * 
	 * @param	db	the MongoDB database proxy
	 * @param	docName	the name of the collection to filter
	 * @param	doc	the query condition
	 * @param	sort	the sorting condition 
	 * */
	public void findByBsonAndSort(MongoDatabase db, String docName, Bson doc, Bson sort){
		FindIterable<Document> iterable = db.getCollection(docName).find(doc).sort(sort);
		
		iterable.forEach(new Block<Document>() {
		    @Override
		    public void apply(final Document document) {
		        System.out.println(document);
		    }
		});
	}
	
	/**
	 * find by a given query condition
	 * 
	 * @param	db	the MongoDB database proxy
	 * @param	docName	the name of the collection to filter
	 * @param	doc	the query condition
	 * */
	public void findByBson(MongoDatabase db, String docName, Bson doc){
		FindIterable<Document> iterable = db.getCollection(docName).find(doc);
		
		iterable.forEach(new Block<Document>() {
		    @Override
		    public void apply(final Document document) {
		        System.out.println(document);
		    }
		});
	}
		
	/**
	 * print all entries of a given collection to standard output
	 * 
	 * @param	db	the MongoDB database proxy
	 * @param	docName the name of collection
	 * */
	public void printAll(MongoDatabase db, String docName){
		FindIterable<Document> iterable = db.getCollection(docName).find();
		
		iterable.forEach(new Block<Document>() {
		    @Override
		    public void apply(final Document document) {
		        System.out.println(document);
		    }
		});
	}
	
	/**
	 * insert a document entry to the MongoDB
	 * @param	db	the MongoDB database proxy
	 * @param	docName	the name of document collection to operate
	 * @param	doc	the document to insert
	 * */
	public void insertOne(MongoDatabase db, String docName, Document doc){
		db.getCollection(docName).insertOne(doc);
	}

}
