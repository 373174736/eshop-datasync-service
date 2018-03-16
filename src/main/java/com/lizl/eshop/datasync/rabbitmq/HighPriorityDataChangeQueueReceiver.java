package com.lizl.eshop.datasync.rabbitmq;

import com.alibaba.fastjson.JSONObject;
import com.lizl.eshop.datasync.service.EshopProductService;
import org.apache.log4j.Logger;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * 1）接收到增删改消息
 *
 *（2）直接基于Fegion调用依赖服务接口，拉取数据，对redis原子数据进行增删改操作
 *
 *（3）再将数据变更消息按照维度发送到rabbitmq
 * Created by lizhaoliang on 18/2/24.
 */
@Component
@RabbitListener(queues = "high-priority-data-change-queue")
public class HighPriorityDataChangeQueueReceiver {

    Logger logger = Logger.getLogger(HighPriorityDataChangeQueueReceiver.class);

    @Autowired
    private EshopProductService eshopProductService;
    @Autowired
    private JedisPool jedisPool;
    @Autowired
    private RabbitMQSender rabbitMQSender;

    private Set<String> dimDataChangeMessageSet = Collections.synchronizedSet(new HashSet<>());

    public HighPriorityDataChangeQueueReceiver(){
        new SendThread().start();
    }

    @RabbitHandler
    public void process(String message){
        try {
            JSONObject jsonObject = JSONObject.parseObject(message);
            //先去data-type
            String dataType = jsonObject.getString("data_type");

            if("brand".equals(dataType)){
                processBrandDataChangeMessage(jsonObject);
            }else if("category".equals(dataType)){
                processCategoryDataChangeMessage(jsonObject);
            }else if("product_intro".equals(dataType)){
                processProductIntroDataChangeMessage(jsonObject);
            }else if("product_property".equals(dataType)){
                processProductPropertyDataChangeMessage(jsonObject);
            }else if ("product".equals(dataType)){
                processProductDataChangeMessage(jsonObject);
            }else if("product_specification".equals(dataType)){
                processProductSpecificationDataChangeMessage(jsonObject);
            }
        }catch (Exception e){
            e.printStackTrace();
            return;
        }


    }

    private void processBrandDataChangeMessage(JSONObject messageJSONObject){

        Integer id = messageJSONObject.getInteger("id");
        String eventType = messageJSONObject.getString("event_type");

        if("add".equals(eventType) || "update".equals(eventType)){
            JSONObject dataJsonObject = JSONObject.parseObject(eshopProductService.findBrandById(id));
            Jedis jedis = jedisPool.getResource();
            jedis.set("brand_" + dataJsonObject.getInteger("id"), dataJsonObject.toJSONString());
        }else if("delete".equals(eventType)){
            Jedis jedis = jedisPool.getResource();
            jedis.del("brand_" + id);
        }

        dimDataChangeMessageSet.add("{\"dim_type\":\"brand\",\"id\":"+  id +"}");
        logger.info("【品牌维度数据变更消息被放入内存set中】，brandId=" + id);
    }

    private void processCategoryDataChangeMessage(JSONObject messageJSONObject){

        Integer id = messageJSONObject.getInteger("id");
        String eventType = messageJSONObject.getString("event_type");

        if("add".equals(eventType) || "update".equals(eventType)){
            JSONObject dataJsonObject = JSONObject.parseObject(eshopProductService.findCategoryById(id));
            Jedis jedis = jedisPool.getResource();
            jedis.set("category_" + dataJsonObject.getInteger("id"), dataJsonObject.toJSONString());
        }else if("delete".equals(eventType)){
            Jedis jedis = jedisPool.getResource();
            jedis.del("category" + id);
        }

        dimDataChangeMessageSet.add("{\"dim_type\":\"category\",\"id\":" + id + "}");
    }

    private void processProductIntroDataChangeMessage(JSONObject messageJSONObject){

        Integer id = messageJSONObject.getInteger("id");
        Integer productId = messageJSONObject.getInteger("product_id");
        String eventType = messageJSONObject.getString("event_type");

        if("add".equals(eventType) || "update".equals(eventType)){
            JSONObject dataJsonObject = JSONObject.parseObject(eshopProductService.findProductIntroById(id));
            Jedis jedis = jedisPool.getResource();
            jedis.set("product_intro_" + productId, dataJsonObject.toJSONString());
        }else if("delete".equals(eventType)){
            Jedis jedis = jedisPool.getResource();
            jedis.del("product_intro_" + productId);
        }

        dimDataChangeMessageSet.add("{\"dim_type\":\"product_intro\",\"id\":" + productId + "}");
    }

    private void processProductPropertyDataChangeMessage(JSONObject messageJSONObject){

        Integer id = messageJSONObject.getInteger("id");
        Integer productId = messageJSONObject.getInteger("product_id");
        String eventType = messageJSONObject.getString("event_type");

        if("add".equals(eventType) || "update".equals(eventType)){
            JSONObject dataJsonObject = JSONObject.parseObject(eshopProductService.findProductPropertyById(id));
            Jedis jedis = jedisPool.getResource();
            jedis.set("product_property_" + productId, dataJsonObject.toJSONString());
        }else if("delete".equals(eventType)){
            Jedis jedis = jedisPool.getResource();
            jedis.del("product_property_" + productId);
        }

        dimDataChangeMessageSet.add("{\"dim_type\":\"product\",\"id\":" + productId + "}");
    }

    private void processProductDataChangeMessage(JSONObject messageJSONObject){

        Integer id = messageJSONObject.getInteger("id");
        String eventType = messageJSONObject.getString("event_type");

        if("add".equals(eventType) || "update".equals(eventType)){
            JSONObject dataJsonObject = JSONObject.parseObject(eshopProductService.findProductById(id));
            Jedis jedis = jedisPool.getResource();
            jedis.set("product_" + dataJsonObject.getInteger("id"), dataJsonObject.toJSONString());
        }else if("delete".equals(eventType)){
            Jedis jedis = jedisPool.getResource();
            jedis.del("product_" + id);
        }

        dimDataChangeMessageSet.add("{\"dim_type\":\"product\",\"id\":" + id + "}");
    }

    private void processProductSpecificationDataChangeMessage(JSONObject messageJSONObject){

        Integer id = messageJSONObject.getInteger("id");
        Integer productId = messageJSONObject.getInteger("product_id");
        String eventType = messageJSONObject.getString("event_type");

        if("add".equals(eventType) || "update".equals(eventType)){
            JSONObject dataJsonObject = JSONObject.parseObject(eshopProductService.findProductSpecificationById(id));
            Jedis jedis = jedisPool.getResource();
            jedis.set("product_specification_" + productId, dataJsonObject.toJSONString());
        }else if("delete".equals(eventType)){
            Jedis jedis = jedisPool.getResource();
            jedis.del("product_specification_" + productId);
        }

        dimDataChangeMessageSet.add("{\"dim_type\":\"product\",\"id\":" + productId + "}");
    }

    private class SendThread extends Thread{

        @Override
        public void run(){
            while (true){
                if(! dimDataChangeMessageSet.isEmpty()){
                    for (String messageData : dimDataChangeMessageSet){
                        rabbitMQSender.send("high-priority-aggr-data-change-queue", messageData);
                        logger.info("【将去重后的数据变更消息发送到下一个 高优先级 queue】，messageData=" + messageData);
                    }
                    dimDataChangeMessageSet.clear();
                }else {
//                    logger.info("dimDataChangeMessageSet 为空 暂停5s");
                }
                try {
                    Thread.sleep(5 * 100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}



