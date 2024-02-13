#ifndef SOCIAL_NETWORK_MICROSERVICES_REDISCLIENT_H
#define SOCIAL_NETWORK_MICROSERVICES_REDISCLIENT_H

#include "RedisClient.h"
#include <cpp_redis/cpp_redis>

namespace social_network {
  void lru(cpp_redis::client client){
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
        }
  }
    

}
        int n = 2;
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
            
        }
        

    
