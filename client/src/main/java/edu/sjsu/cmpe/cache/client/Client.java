package edu.sjsu.cmpe.cache.client;

import java.util.List;
import com.google.common.hash.Hashing;
import java.util.ArrayList;


public class Client {

    public static void main(String[] args) throws Exception {
        System.out.println("Running Cache Client ... ");

        //Creating pool of cache servers
        List<CacheServiceInterface> serverPool = new ArrayList<CacheServiceInterface>();

        serverPool.add(new DistributedCacheService("http://localhost:3000"));
        serverPool.add(new DistributedCacheService("http://localhost:3001"));
        serverPool.add(new DistributedCacheService("http://localhost:3002"));

        char[] keyValues ={' ','a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j'};

        System.out.println("Adding data to cache server pool");

        int keyValue = 0;
        for(int k=1;k<=keyValues.length-1;k++)
        {
            keyValue = Hashing.consistentHash(Hashing.md5().hashString(Integer.toString(k)), serverPool.size());
            CacheServiceInterface serverToPutData = serverPool.get(keyValue);
            serverToPutData.put(k,Character.toString(keyValues[k]));
            System.out.println("Putting (PUT) => Key=" + k + " and Value="+ keyValues[k] + " routing to Cache server running at localhost://300"+ keyValue);
        }

        System.out.println(" ");

        System.out.println("Getting data from Cache servers");

        for(int k=1;k<=keyValues.length-1;k++){
            keyValue = Hashing.consistentHash(Hashing.md5().hashString(Integer.toString(k)), serverPool.size());
            CacheServiceInterface serverToGetData =serverPool.get(keyValue);
            String val = serverToGetData.get(k);
            System.out.println("Getting (GET) <= Received Key=" + k + " , Value="+ val + " from Cache server running at localhost://300"+ keyValue);
        }


        System.out.println("Exit ..");

    }
}