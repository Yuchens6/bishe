#ifndef SOCIAL_NETWORK_MICROSERVICES_SRC_HOMETIMELINESERVICE_HOMETIMELINEHANDLER_H_
#define SOCIAL_NETWORK_MICROSERVICES_SRC_HOMETIMELINESERVICE_HOMETIMELINEHANDLER_H_

#include <sw/redis++/redis++.h>

#include <future>
#include <iostream>
#include <string>
#include <libmemcached/memcached.h>
#include <libmemcached/util.h>

#include "../../gen-cpp/HomeTimelineService.h"
#include "../../gen-cpp/PostStorageService.h"
#include "../../gen-cpp/SocialGraphService.h"
#include "../ClientPool.h"
#include "../ThriftClient.h"
#include "../logger.h"
#include "../tracing.h"

using namespace sw::redis;
namespace social_network {
class HomeTimelineHandler : public HomeTimelineServiceIf {
 public:
  HomeTimelineHandler(Redis *,memcached_pool_st *,
                      ClientPool<ThriftClient<PostStorageServiceClient>> *,
                      ClientPool<ThriftClient<SocialGraphServiceClient>> *);


  HomeTimelineHandler(Redis *,Redis *,memcached_pool_st *,
      ClientPool<ThriftClient<PostStorageServiceClient>>*,
      ClientPool<ThriftClient<SocialGraphServiceClient>>*);


  HomeTimelineHandler(RedisCluster *,memcached_pool_st *,
                      ClientPool<ThriftClient<PostStorageServiceClient>> *,
                      ClientPool<ThriftClient<SocialGraphServiceClient>> *);
  ~HomeTimelineHandler() override = default;

  bool IsRedisReplicationEnabled();

  void ReadHomeTimeline(std::vector<Post> &, int64_t, int64_t, int, int,
                        const std::map<std::string, std::string> &) override;

  void WriteHomeTimeline(int64_t, int64_t, int64_t, int64_t,
                         const std::vector<int64_t> &,
                         const std::map<std::string, std::string> &) override;

 private:
     Redis *_redis_replica_pool;
     Redis *_redis_primary_pool;
     Redis *_redis_client_pool;
     RedisCluster *_redis_cluster_client_pool;
     memcached_pool_st *_memcached_client_pool;
     ClientPool<ThriftClient<PostStorageServiceClient>> *_post_client_pool;
     ClientPool<ThriftClient<SocialGraphServiceClient>> *_social_graph_client_pool;
};

HomeTimelineHandler::HomeTimelineHandler(
    Redis *redis_pool,memcached_pool_st *memcached_client_pool,
    ClientPool<ThriftClient<PostStorageServiceClient>> *post_client_pool,
    ClientPool<ThriftClient<SocialGraphServiceClient>>
        *social_graph_client_pool) {
    _redis_primary_pool = nullptr;
    _redis_replica_pool = nullptr;
    _redis_client_pool = redis_pool;
    _redis_cluster_client_pool = nullptr;
    _post_client_pool = post_client_pool;
   _memcached_client_pool = memcached_client_pool;
    _social_graph_client_pool = social_graph_client_pool;
}

HomeTimelineHandler::HomeTimelineHandler(
    RedisCluster *redis_pool,memcached_pool_st *memcached_client_pool,
    ClientPool<ThriftClient<PostStorageServiceClient>> *post_client_pool,
    ClientPool<ThriftClient<SocialGraphServiceClient>>
        *social_graph_client_pool) {
    _redis_primary_pool = nullptr;
    _redis_replica_pool = nullptr;
    _redis_client_pool = nullptr;
    _redis_cluster_client_pool = redis_pool; 
   _memcached_client_pool = memcached_client_pool;
    _post_client_pool = post_client_pool;
    _social_graph_client_pool = social_graph_client_pool;
}

HomeTimelineHandler::HomeTimelineHandler(
    Redis *redis_replica_pool,
    Redis *redis_primary_pool,memcached_pool_st *memcached_client_pool,
    ClientPool<ThriftClient<PostStorageServiceClient>>* post_client_pool,
    ClientPool<ThriftClient<SocialGraphServiceClient>>
    * social_graph_client_pool) {
    _redis_primary_pool = redis_primary_pool;
    _redis_replica_pool = redis_replica_pool;
    _redis_client_pool = nullptr;
    _redis_cluster_client_pool = nullptr;
 _memcached_client_pool = memcached_client_pool;
    _post_client_pool = post_client_pool;
    _social_graph_client_pool = social_graph_client_pool;
}

bool HomeTimelineHandler::IsRedisReplicationEnabled() {
    return (_redis_primary_pool || _redis_replica_pool);
}

void HomeTimelineHandler::WriteHomeTimeline(
    int64_t req_id, int64_t post_id, int64_t user_id, int64_t timestamp,
    const std::vector<int64_t> &user_mentions_id,
    const std::map<std::string, std::string> &carrier) {
  // Initialize a span
  TextMapReader reader(carrier);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "write_home_timeline_server", {opentracing::ChildOf(parent_span->get())});

  // Find followers of the user
  auto followers_span = opentracing::Tracer::Global()->StartSpan(
      "get_followers_client", {opentracing::ChildOf(&span->context())});
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  opentracing::Tracer::Global()->Inject(followers_span->context(), writer);

  auto social_graph_client_wrapper = _social_graph_client_pool->Pop();
  if (!social_graph_client_wrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
    se.message = "Failed to connect to social-graph-service";
    throw se;
  }
  auto social_graph_client = social_graph_client_wrapper->GetClient();
  std::vector<int64_t> followers_id;
  try {
    social_graph_client->GetFollowers(followers_id, req_id, user_id,
                                      writer_text_map);
  } catch (...) {
    LOG(error) << "Failed to get followers from social-network-service";
    _social_graph_client_pool->Remove(social_graph_client_wrapper);
    throw;
  }
  _social_graph_client_pool->Keepalive(social_graph_client_wrapper);
  followers_span->Finish();

  std::set<int64_t> followers_id_set(followers_id.begin(), followers_id.end());
  followers_id_set.insert(user_mentions_id.begin(), user_mentions_id.end());

  // Update Redis ZSet
  // Zset key: follower_id, Zset value: post_id_str, Zset score: timestamp_str
  auto redis_span = opentracing::Tracer::Global()->StartSpan(
      "write_home_timeline_redis_update_client",
      {opentracing::ChildOf(&span->context())});
  std::string post_id_str = std::to_string(post_id);

  {
    if (_redis_client_pool) {
      auto pipe = _redis_client_pool->pipeline(false);
      for (auto &follower_id : followers_id_set) {
        pipe.zadd(std::to_string(follower_id), post_id_str, timestamp,
                  UpdateType::NOT_EXIST);
      }
      try {
        auto replies = pipe.exec();
      } catch (const Error &err) {
        LOG(error) << err.what();
        throw err;
      }
    }
    
    else if (IsRedisReplicationEnabled()) {
        auto pipe = _redis_primary_pool->pipeline(false);
        for (auto& follower_id : followers_id_set) {
            pipe.zadd(std::to_string(follower_id), post_id_str, timestamp,
                UpdateType::NOT_EXIST);
        }
        try {
            auto replies = pipe.exec();
        }
        catch (const Error& err) {
            LOG(error) << err.what();
            throw err;
        }
    }
    
    else {
      // Create multi-pipeline that match with shards pool
      std::map<std::shared_ptr<ConnectionPool>, std::shared_ptr<Pipeline>> pipe_map;
      auto *shards_pool = _redis_cluster_client_pool->get_shards_pool();

      for (auto &follower_id : followers_id_set) {
        auto conn = shards_pool->fetch(std::to_string(follower_id));
        auto pipe = pipe_map.find(conn);
        if(pipe == pipe_map.end()) {//Not found, create new pipeline and insert
          auto new_pipe = std::make_shared<Pipeline>(_redis_cluster_client_pool->pipeline(std::to_string(follower_id), false));
          pipe_map.insert(make_pair(conn, new_pipe));
          auto *_pipe = new_pipe.get();
          _pipe->zadd(std::to_string(follower_id), post_id_str, timestamp,
                  UpdateType::NOT_EXIST);
        }else{//Found, use exist pipeline
          std::pair<std::shared_ptr<ConnectionPool>, std::shared_ptr<Pipeline>> found = *pipe;
          auto *_pipe = found.second.get();
          _pipe->zadd(std::to_string(follower_id), post_id_str, timestamp,
                  UpdateType::NOT_EXIST);
        }
      }
      // LOG(info) <<"followers_id_set items:" << followers_id_set.size()<<"; pipeline items:" << pipe_map.size();
      try {
        for(auto const &it : pipe_map) {
          auto _pipe = it.second.get();
          _pipe->exec();
        }

      } catch (const Error &err) {
        LOG(error) << err.what();
        throw err;
      }
    }
  }
  redis_span->Finish();
}


void HomeTimelineHandler::ReadHomeTimeline(
    std::vector<Post> &_return, int64_t req_id, int64_t user_id, int start_idx,
    int stop_idx, const std::map<std::string, std::string> &carrier) {
  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "read_home_timeline_server", {opentracing::ChildOf(parent_span->get())});
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  if (stop_idx <= start_idx || start_idx < 0) {
    return;
  }

  auto redis_span = opentracing::Tracer::Global()->StartSpan(
      "read_home_timeline_redis_find_client",
      {opentracing::ChildOf(&span->context())});

  std::vector<std::string> post_ids_str;
  try {
    if (_redis_client_pool) {
      _redis_client_pool->zrevrange(std::to_string(user_id), start_idx,
                                    stop_idx - 1,
                                    std::back_inserter(post_ids_str));
    }
    else if (IsRedisReplicationEnabled()) {
        _redis_replica_pool->zrevrange(std::to_string(user_id), start_idx,
                                       stop_idx - 1,
                                       std::back_inserter(post_ids_str));
    }
    
    else {
      _redis_cluster_client_pool->zrevrange(std::to_string(user_id), start_idx,
                                            stop_idx - 1,
                                            std::back_inserter(post_ids_str));
    }
  } catch (const Error &err) {
    LOG(error) << err.what();
    throw err;
  }
  redis_span->Finish();

  std::vector<int64_t> post_ids;
  for (auto &post_id_str : post_ids_str) {
    post_ids.emplace_back(std::stoul(post_id_str));
  }

  auto post_client_wrapper = _post_client_pool->Pop();
  if (!post_client_wrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
    se.message = "Failed to connect to post-storage-service";
    throw se;
  }
  auto post_client = post_client_wrapper->GetClient();
  try {

    if (post_ids.empty()) {
           return;
         }
       
         std::set<int64_t> post_ids_not_cached(post_ids.begin(), post_ids.end());
         if (post_ids_not_cached.size() != post_ids.size()) {
           LOG(error)<< "Post_ids are duplicated";
           ServiceException se;
           se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
           se.message = "Post_ids are duplicated";
           throw se;
         }
         std::map<int64_t, Post> return_map;
         memcached_return_t memcached_rc;
         auto memcached_client =
             memcached_pool_pop(_memcached_client_pool, true, &memcached_rc);
         if (!memcached_client) {
           ServiceException se;
           se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
           se.message = "Failed to pop a client from memcached pool";
           throw se;
         }
       
         char **keys;
         size_t *key_sizes;
         keys = new char *[post_ids.size()];
         key_sizes = new size_t[post_ids.size()];
         int idx = 0;
         for (auto &post_id : post_ids) {
           std::string key_str = std::to_string(post_id);
           keys[idx] = new char[key_str.length() + 1];
           strcpy(keys[idx], key_str.c_str());
           key_sizes[idx] = key_str.length();
           idx++;
         }
         memcached_rc =
             memcached_mget(memcached_client, keys, key_sizes, post_ids.size());
         if (memcached_rc != MEMCACHED_SUCCESS) {
           LOG(error) << "Cannot get post_ids of request " << req_id << ": "
                      << memcached_strerror(memcached_client, memcached_rc);
           ServiceException se;
           se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
           se.message = memcached_strerror(memcached_client, memcached_rc);
           memcached_pool_push(_memcached_client_pool, memcached_client);
           throw se;
         }
       
         char return_key[MEMCACHED_MAX_KEY];
         size_t return_key_length;
         char *return_value;
         size_t return_value_length;
         uint32_t flags;
         auto get_span = opentracing::Tracer::Global()->StartSpan(
             "post_storage_mmc_mget_client", {opentracing::ChildOf(&span->context())});
       
         while (true) {
           return_value =
               memcached_fetch(memcached_client, return_key, &return_key_length,
                               &return_value_length, &flags, &memcached_rc);
           if (return_value == nullptr) {
             LOG(debug) << "Memcached mget finished";
             break;
           }
           if (memcached_rc != MEMCACHED_SUCCESS) {
             free(return_value);
             memcached_quit(memcached_client);
             memcached_pool_push(_memcached_client_pool, memcached_client);
             LOG(error) << "Cannot get posts of request " << req_id;
             ServiceException se;
             se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
             se.message = "Cannot get posts of request " + std::to_string(req_id);
             throw se;
           }
           Post new_post;
           json post_json = json::parse(
               std::string(return_value, return_value + return_value_length));
           new_post.req_id = post_json["req_id"];
           new_post.timestamp = post_json["timestamp"];
           new_post.post_id = post_json["post_id"];
           new_post.creator.user_id = post_json["creator"]["user_id"];
           new_post.creator.username = post_json["creator"]["username"];
           new_post.post_type = post_json["post_type"];
           new_post.text = post_json["text"];
           for (auto &item : post_json["media"]) {
             Media media;
             media.media_id = item["media_id"];
             media.media_type = item["media_type"];
             new_post.media.emplace_back(media);
           }
           for (auto &item : post_json["user_mentions"]) {
             UserMention user_mention;
             user_mention.username = item["username"];
             user_mention.user_id = item["user_id"];
             new_post.user_mentions.emplace_back(user_mention);
           }
           for (auto &item : post_json["urls"]) {
             Url url;
             url.shortened_url = item["shortened_url"];
             url.expanded_url = item["expanded_url"];
             new_post.urls.emplace_back(url);
           }
           return_map.insert(std::make_pair(new_post.post_id, new_post));
           post_ids_not_cached.erase(new_post.post_id);
           free(return_value);
         }
         get_span->Finish();
         memcached_quit(memcached_client);
         memcached_pool_push(_memcached_client_pool, memcached_client);
         for (int i = 0; i < post_ids.size(); ++i) {
           delete keys[i];
         }
         delete[] keys;
         delete[] key_sizes;

    for (auto &post_id : post_ids) {
         _return.emplace_back(return_map[post_id]);
       }
   
    post_client->ReadPosts(_return, req_id, post_ids_not_cached, writer_text_map);
  } catch (...) {
    _post_client_pool->Remove(post_client_wrapper);
    LOG(error) << "Failed to read posts from post-storage-service";
    throw;
  }
  _post_client_pool->Keepalive(post_client_wrapper);
  span->Finish();
}

}  // namespace social_network

#endif  // SOCIAL_NETWORK_MICROSERVICES_SRC_HOMETIMELINESERVICE_HOMETIMELINEHANDLER_H_
