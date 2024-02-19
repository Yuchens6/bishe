#ifndef SOCIAL_NETWORK_MICROSERVICES_REDISCLIENT_H
#define SOCIAL_NETWORK_MICROSERVICES_REDISCLIENT_H
#include <sw/redis++/redis++.h>
#include "RedisClient.h"
#include <cpp_redis/cpp_redis>
    
#include <iostream>
#include "hiredis/hiredis.h"

using namespace sw::redis;
namespace social_network {
  void lru(cpp_redis::client client){
  redisContext* redis = redisConnect("127.0.0.1", 6379); // 这里的IP地址和端口号根据实际情况修改
if (redis == NULL || redis->err) {
    std::cout << "无法连接到 Redis 服务器！错误信息：" << redis->errstr << std::endl;
    return -1;
} else {
    std::cout << "成功连接到 Redis 服务器！" << std::endl;
}
    const char* command = "INFO MEMORY";
redisReply* reply = static_cast<redisReply*>(redisCommand(redis, command));
if (!reply) {
    std::cerr << "无法执行命令！" << std::endl;
    return -1;
}
std::string infoStr((char*)reply->str, reply->len);
size_t startPos = infoStr.find("used_memory:");
size_t endPos = infoStr.find("\n", startPos + sizeof("used_memory:"));
std::string usedMemoryStr = infoStr.substr(startPos + sizeof("used_memory:"), endPos - startPos - sizeof("used_memory:")).trim();
int usedMemory = atoi(usedMemoryStr.c_str());
 
startPos = infoStr.find("maxmemory:");
endPos = infoStr.find("\n", startPos + sizeof("maxmemory:"));
std::string maxMemoryStr = infoStr.substr(startPos + sizeof("maxmemory:"), endPos - startPos - sizeof("maxmemory:")).trim();
int maxMemory = atoi(maxMemoryStr.c_str());
 
double percentageUsed = ((double)(usedMemory * 100)) / maxMemory;
std::cout << "已使用内存占总内存的百分比为：" << percentageUsed << "%" << std::endl;
    if(percentageUsed>=90){
       mongoc_client_t *mongodb_client =
      mongoc_client_pool_pop(_mongodb_client_pool);
  if (!mongodb_client) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MONGODB_ERROR;
    se.message = "Failed to pop a client from MongoDB pool";
    throw se;
  }
      auto collection = mongoc_client_get_collection(
      mongodb_client, "user-timeline", "user-timeline");
  if (!collection) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MONGODB_ERROR;
    se.message = "Failed to create collection user-timeline from MongoDB";
    mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
    throw se;
  }
  bson_t *query = bson_new();

        int n = 10;
        std::vector<std::pair<std::string, std::string>> sortedKeysWithScores;
        client.zrevrangebyscore("sorted-keys", "+inf", "-inf", [&](cpp_redis::reply& reply){
            if (!reply.is_array()) return false;
            for(auto& element : reply.as_array().elements()) {
                if(!element.is_array()) continue;
                double score = element[0].as_double();
                std::string key = element[1].str();
                sortedKeysWithScores.push_back({score, key});
            }
        });
        
        // 输出最近最少使用的前n条记录
        for (size_t i=0; i<n && i<sortedKeysWithScores.size(); ++i) {
            const auto& pair = sortedKeysWithScores[i];
            std::cout << "Key: " << pair.second << ", Score: " << pair.first << std::endl;
          BSON_APPEND_INT64(query, "user_id", pair.second);
  bson_t *update =
      BCON_NEW("$push", "{", "posts", "{", "$each", "[", "{", "post_id",
               BCON_INT64(pair.first), "timestamp", BCON_INT64(timestamp), "}",
               "]", "$position", BCON_INT32(0), "}", "}");
          bson_error_t error;
  bson_t reply;
  auto update_span = opentracing::Tracer::Global()->StartSpan(
      "write_user_timeline_mongo_insert_client",
      {opentracing::ChildOf(&span->context())});
  bool updated = mongoc_collection_find_and_modify(collection, query, nullptr,
                                                   update, nullptr, false, true,
                                                   true, &reply, &error);

  if (!updated) {
    // update the newly inserted document (upsert: false)
    updated = mongoc_collection_find_and_modify(collection, query, nullptr,
                                                update, nullptr, false, false,
                                                true, &reply, &error);
    if (!updated) {
      LOG(error) << "Failed to update user-timeline for user " << user_id
                 << " to MongoDB: " << error.message;
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = error.message;
      bson_destroy(update);
      bson_destroy(query);
      bson_destroy(&reply);
      mongoc_collection_destroy(collection);
      mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
      throw se;
    }
  }

  bson_destroy(update);
  bson_destroy(&reply);
  bson_destroy(query);
  mongoc_collection_destroy(collection);
  mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
        }
  }
  }     
}


    
