package edu.sjsu.cmpe273.CRDTClient;

import java.util.Map;
import java.util.List;
import java.util.Map.Entry;
import com.mashape.unirest.http.HttpResponse;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import com.mashape.unirest.http.JsonNode;
import java.util.concurrent.CountDownLatch;
import com.mashape.unirest.http.Unirest;
import java.util.concurrent.Future;
import java.io.IOException;
import com.mashape.unirest.http.exceptions.UnirestException;
import java.util.HashMap;
import com.mashape.unirest.http.async.Callback;
import java.util.ArrayList;




public class CRDTClient {
	private List<DistributedCacheService> serverList;
	private CountDownLatch countDownLatch;
//////////////////////////////////// Client CRDT //////////////////////////////////////////////////////////////////////////////
//                                  Creating Server List 
	public CRDTClient() {
		DistributedCacheService cacheServer3000 = new DistributedCacheService(
				"http://localhost:3000");
		DistributedCacheService cacheServer3001 = new DistributedCacheService(
				"http://localhost:3001");
		DistributedCacheService cacheServer3002 = new DistributedCacheService(
				"http://localhost:3002");

		this.serverList = new ArrayList<DistributedCacheService>();

		serverList.add(cacheServer3000);
		serverList.add(cacheServer3001);
		serverList.add(cacheServer3002);
	}

	// --------------------------------------------------- Asyncronous PUT  ------------------------------------------------- //
	
	public boolean put(long key, String value) throws InterruptedException, IOException {
		final AtomicInteger counter = new AtomicInteger(0);
		this.countDownLatch = new CountDownLatch(serverList.size());
		final ArrayList<DistributedCacheService> serverWrittenList = new ArrayList<DistributedCacheService>(3);
		for (final DistributedCacheService cacheServer : serverList) {
			Future<HttpResponse<JsonNode>> future = Unirest.put(cacheServer.getCacheServerUrl()+ "/cache/{key}/{value}")
					.header("accept", "application/json")
					.routeParam("key", Long.toString(key))
					.routeParam("value", value)
					.asJsonAsync(new Callback<JsonNode>() {

						public void failed(UnirestException e) {
							System.out.println("The request is failed.. "+cacheServer.getCacheServerUrl());
							countDownLatch.countDown();
						}

						public void completed(HttpResponse<JsonNode> response) {
							int count = counter.incrementAndGet();
							serverWrittenList.add(cacheServer);
							System.out.println("The request is successful.. "+cacheServer.getCacheServerUrl());
							countDownLatch.countDown();
						}

						public void cancelled() {
							System.out.println("The request has been cancelled..");
							countDownLatch.countDown();
						}

					});
		}
		this.countDownLatch.await();
		if (counter.intValue() > 1) {
			return true;
		} else {
			System.out.println("Deleting...");
			this.countDownLatch = new CountDownLatch(serverWrittenList.size());
			for (final DistributedCacheService cacheServer : serverWrittenList) {

                cacheServer.remove(key);
                System.out.println("Deleted resouce with key: "+key+" from : "+cacheServer.getCacheServerUrl());

			}
			this.countDownLatch.await(3, TimeUnit.SECONDS);
			Unirest.shutdown();
			return false;
		}
	}

	// --------------------------------------------------- Asyncronous GET  ------------------------------------------------- //
	
	public String get(long key) throws InterruptedException, UnirestException, IOException {
		this.countDownLatch = new CountDownLatch(serverList.size());
		final Map<DistributedCacheService, String> resultMap = new HashMap<DistributedCacheService, String>();
		for (final DistributedCacheService cacheServer : serverList) {
			Future<HttpResponse<JsonNode>> future = Unirest.get(cacheServer.getCacheServerUrl() + "/cache/{key}")
					.header("accept", "application/json")
					.routeParam("key", Long.toString(key))
					.asJsonAsync(new Callback<JsonNode>() {

						public void failed(UnirestException e) {
							System.out.println("The request is failed...");
							countDownLatch.countDown();
						}

						public void completed(HttpResponse<JsonNode> response) {
							resultMap.put(cacheServer, response.getBody().getObject().getString("value"));
							System.out.println("The request is successful on sever... "+cacheServer.getCacheServerUrl());
							countDownLatch.countDown();
						}

						public void cancelled() {
							System.out.println("The request is cancelled...");
							countDownLatch.countDown();
						}
				});
		}
		this.countDownLatch.await(3, TimeUnit.SECONDS);
		final Map<String, Integer> counterMap = new HashMap<String, Integer>();
		int maximumCounter = 0;
		for (String value : resultMap.values()) {
			int count = 1;
			if (counterMap.containsKey(value)) {
				count = counterMap.get(value);
				count++;
			}
			if (maximumCounter < count)
				maximumCounter = count;
			counterMap.put(value, count);
		}
		System.out.println("maximumCounter value "+maximumCounter);
		String value = this.getKeyByValue(counterMap, maximumCounter);
		System.out.println("maximumCounter value "+value);
		if (maximumCounter != this.serverList.size()) {
			for (Entry<DistributedCacheService, String> cacheServerData : resultMap.entrySet()) {
				if (!value.equals(cacheServerData.getValue())) {
					System.out.println("Repairing the server "+cacheServerData.getKey());
					HttpResponse<JsonNode> response = Unirest.put(cacheServerData.getKey() + "/cache/{key}/{value}")
							.header("accept", "application/json")
							.routeParam("key", Long.toString(key))
							.routeParam("value", value)
							.asJson();
				}
			}
			for (DistributedCacheService cacheServer : this.serverList) {
				if (resultMap.containsKey(cacheServer)) continue;
				System.out.println("Repairing the server "+cacheServer.getCacheServerUrl());
				HttpResponse<JsonNode> response = Unirest.put(cacheServer.getCacheServerUrl() + "/cache/{key}/{value}")
						.header("accept", "application/json")
						.routeParam("key", Long.toString(key))
						.routeParam("value", value)
						.asJson();
			}
		} else {
			System.out.println("Repair is not needed");
		}
		Unirest.shutdown();
		return value;
	}

	// --------------------------------------------------- getting KEY - VALUE ------------------------------------------------- //
	public String getKeyByValue(Map<String, Integer> map, int value) {
		for (Entry<String, Integer> entry : map.entrySet()) {
			if (value == entry.getValue()) return entry.getKey();
		}
		return null;
	}
}