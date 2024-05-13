#ifndef SOCIAL_NETWORK_MICROSERVICES_SRC_USERTIMELINESERVICE_USERTIMELINEHANDLER_H_
#define SOCIAL_NETWORK_MICROSERVICES_SRC_USERTIMELINESERVICE_USERTIMELINEHANDLER_H_

#include <bson/bson.h>
#include <mongoc.h>
#include <sw/redis++/redis++.h>
#include <libmemcached/memcached.h>
#include <libmemcached/util.h>
#include <future>
#include <iostream>
#include <string>

#include "../../gen-cpp/PostStorageService.h"
#include "../../gen-cpp/UserTimelineService.h"
#include "../ClientPool.h"
#include "../ThriftClient.h"
#include "../logger.h"
#include "../tracing.h"

using namespace sw::redis;

namespace social_network
{

  class UserTimelineHandler : public UserTimelineServiceIf
  {
  public:
    UserTimelineHandler(Redis *, mongoc_client_pool_t *, memcached_pool_st *,
                        ClientPool<ThriftClient<PostStorageServiceClient>> *);

    UserTimelineHandler(Redis *, Redis *, mongoc_client_pool_t *, memcached_pool_st *,
                        ClientPool<ThriftClient<PostStorageServiceClient>> *);

    UserTimelineHandler(RedisCluster *, mongoc_client_pool_t *, memcached_pool_st *,
                        ClientPool<ThriftClient<PostStorageServiceClient>> *);
    ~UserTimelineHandler() override = default;

    bool IsRedisReplicationEnabled();

    void WriteUserTimeline(
        int64_t req_id, int64_t post_id, int64_t user_id, int64_t timestamp,
        const std::map<std::string, std::string> &carrier) override;

    void ReadUserTimeline(std::vector<Post> &, int64_t, int64_t, int, int,
                          const std::map<std::string, std::string> &) override;

  private:
    Redis *_redis_client_pool;
    Redis *_redis_replica_pool;
    Redis *_redis_primary_pool;
    RedisCluster *_redis_cluster_client_pool;
    memcached_pool_st *_memcached_client_pool;
    mongoc_client_pool_t *_mongodb_client_pool;
    ClientPool<ThriftClient<PostStorageServiceClient>> *_post_client_pool;
  };

  UserTimelineHandler::UserTimelineHandler(
      Redis *redis_pool, mongoc_client_pool_t *mongodb_pool, memcached_pool_st *memcached_client_pool,
      ClientPool<ThriftClient<PostStorageServiceClient>> *post_client_pool)
  {
    _redis_client_pool = redis_pool;
    _redis_replica_pool = nullptr;
    _redis_primary_pool = nullptr;
    _redis_cluster_client_pool = nullptr;
    _memcached_client_pool = memcached_client_pool;
    _mongodb_client_pool = mongodb_pool;
    _post_client_pool = post_client_pool;
  }

  UserTimelineHandler::UserTimelineHandler(
      Redis *redis_replica_pool, Redis *redis_primary_pool, mongoc_client_pool_t *mongodb_pool, memcached_pool_st *memcached_client_pool,
      ClientPool<ThriftClient<PostStorageServiceClient>> *post_client_pool)
  {
    _redis_client_pool = nullptr;
    _redis_replica_pool = redis_replica_pool;
    _redis_primary_pool = redis_primary_pool;
    _redis_cluster_client_pool = nullptr;
    _memcached_client_pool = memcached_client_pool;
    _mongodb_client_pool = mongodb_pool;
    _post_client_pool = post_client_pool;
  }

  UserTimelineHandler::UserTimelineHandler(
      RedisCluster *redis_pool, mongoc_client_pool_t *mongodb_pool, memcached_pool_st *memcached_client_pool,
      ClientPool<ThriftClient<PostStorageServiceClient>> *post_client_pool)
  {
    _redis_cluster_client_pool = redis_pool;
    _redis_replica_pool = nullptr;
    _redis_primary_pool = nullptr;
    _redis_client_pool = nullptr;
    _memcached_client_pool = memcached_client_pool;
    _mongodb_client_pool = mongodb_pool;
    _post_client_pool = post_client_pool;
  }

  bool UserTimelineHandler::IsRedisReplicationEnabled()
  {
    return (_redis_primary_pool || _redis_replica_pool);
  }

  void UserTimelineHandler::WriteUserTimeline(
      int64_t req_id, int64_t post_id, int64_t user_id, int64_t timestamp,
      const std::map<std::string, std::string> &carrier)
  {
    // Initialize a span
    TextMapReader reader(carrier);
    std::map<std::string, std::string> writer_text_map;
    TextMapWriter writer(writer_text_map);
    auto parent_span = opentracing::Tracer::Global()->Extract(reader);
    auto span = opentracing::Tracer::Global()->StartSpan(
        "write_user_timeline_server", {opentracing::ChildOf(parent_span->get())});
    opentracing::Tracer::Global()->Inject(span->context(), writer);

    mongoc_client_t *mongodb_client =
        mongoc_client_pool_pop(_mongodb_client_pool);
    if (!mongodb_client)
    {
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = "Failed to pop a client from MongoDB pool";
      throw se;
    }
    auto collection = mongoc_client_get_collection(
        mongodb_client, "user-timeline", "user-timeline");
    if (!collection)
    {
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = "Failed to create collection user-timeline from MongoDB";
      mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
      throw se;
    }
    bson_t *query = bson_new();

    BSON_APPEND_INT64(query, "user_id", user_id);
    bson_t *update =
        BCON_NEW("$push", "{", "posts", "{", "$each", "[", "{", "post_id",
                 BCON_INT64(post_id), "timestamp", BCON_INT64(timestamp), "}",
                 "]", "$position", BCON_INT32(0), "}", "}");
    bson_error_t error;
    bson_t reply;
    auto update_span = opentracing::Tracer::Global()->StartSpan(
        "write_user_timeline_mongo_insert_client",
        {opentracing::ChildOf(&span->context())});
    bool updated = mongoc_collection_find_and_modify(collection, query, nullptr,
                                                     update, nullptr, false, true,
                                                     true, &reply, &error);
    update_span->Finish();

    if (!updated)
    {
      // update the newly inserted document (upsert: false)
      updated = mongoc_collection_find_and_modify(collection, query, nullptr,
                                                  update, nullptr, false, false,
                                                  true, &reply, &error);
      if (!updated)
      {
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

    // Update user's timeline in redis
    auto redis_span = opentracing::Tracer::Global()->StartSpan(
        "write_user_timeline_redis_update_client",
        {opentracing::ChildOf(&span->context())});
    try
    {
      if (_redis_client_pool)
        _redis_client_pool->zadd(std::to_string(user_id), std::to_string(post_id),
                                 timestamp, UpdateType::NOT_EXIST);
      else if (IsRedisReplicationEnabled())
      {
        _redis_primary_pool->zadd(std::to_string(user_id), std::to_string(post_id),
                                  timestamp, UpdateType::NOT_EXIST);
      }
      else
        _redis_cluster_client_pool->zadd(std::to_string(user_id), std::to_string(post_id),
                                         timestamp, UpdateType::NOT_EXIST);
    }
    catch (const Error &err)
    {
      LOG(error) << err.what();
      throw err;
    }
    redis_span->Finish();
    span->Finish();
  }

  void UserTimelineHandler::ReadUserTimeline(
      std::vector<Post> &_return, int64_t req_id, int64_t user_id, int start,
      int stop, const std::map<std::string, std::string> &carrier)
  {
    // Initialize a span
    TextMapReader reader(carrier);
    std::map<std::string, std::string> writer_text_map;
    TextMapWriter writer(writer_text_map);
    auto parent_span = opentracing::Tracer::Global()->Extract(reader);
    auto span = opentracing::Tracer::Global()->StartSpan(
        "read_user_timeline_server", {opentracing::ChildOf(parent_span->get())});
    opentracing::Tracer::Global()->Inject(span->context(), writer);

    if (stop <= start || start < 0)
    {
      return;
    }

    auto redis_span = opentracing::Tracer::Global()->StartSpan(
        "read_user_timeline_redis_find_client",
        {opentracing::ChildOf(&span->context())});

    std::vector<std::string> post_ids_str;
    try
    {
      if (_redis_client_pool)
        _redis_client_pool->zrevrange(std::to_string(user_id), start, stop - 1,
                                      std::back_inserter(post_ids_str));
      else if (IsRedisReplicationEnabled())
      {
        _redis_replica_pool->zrevrange(std::to_string(user_id), start, stop - 1,
                                       std::back_inserter(post_ids_str));
      }
      else
        _redis_cluster_client_pool->zrevrange(std::to_string(user_id), start, stop - 1,
                                              std::back_inserter(post_ids_str));
    }
    catch (const Error &err)
    {
      LOG(error) << err.what();
      throw err;
    }
    redis_span->Finish();

    std::vector<int64_t> post_ids;
    for (auto &post_id_str : post_ids_str)
    {
      post_ids.emplace_back(std::stoul(post_id_str));
    }

    // find in mongodb
    int mongo_start = start + post_ids.size();
    std::unordered_map<std::string, double> redis_update_map;
    if (mongo_start < stop)
    {
      // Instead find post_ids from mongodb
      mongoc_client_t *mongodb_client =
          mongoc_client_pool_pop(_mongodb_client_pool);
      if (!mongodb_client)
      {
        ServiceException se;
        se.errorCode = ErrorCode::SE_MONGODB_ERROR;
        se.message = "Failed to pop a client from MongoDB pool";
        throw se;
      }
      auto collection = mongoc_client_get_collection(
          mongodb_client, "user-timeline", "user-timeline");
      if (!collection)
      {
        ServiceException se;
        se.errorCode = ErrorCode::SE_MONGODB_ERROR;
        se.message = "Failed to create collection user-timeline from MongoDB";
        throw se;
      }

      bson_t *query = BCON_NEW("user_id", BCON_INT64(user_id));
      bson_t *opts = BCON_NEW("projection", "{", "posts", "{", "$slice", "[",
                              BCON_INT32(0), BCON_INT32(stop), "]", "}", "}");

      auto find_span = opentracing::Tracer::Global()->StartSpan(
          "user_timeline_mongo_find_client",
          {opentracing::ChildOf(&span->context())});
      mongoc_cursor_t *cursor =
          mongoc_collection_find_with_opts(collection, query, opts, nullptr);
      find_span->Finish();
      const bson_t *doc;
      bool found = mongoc_cursor_next(cursor, &doc);
      if (found)
      {
        bson_iter_t iter_0;
        bson_iter_t iter_1;
        bson_iter_t post_id_child;
        bson_iter_t timestamp_child;
        int idx = 0;
        bson_iter_init(&iter_0, doc);
        bson_iter_init(&iter_1, doc);
        while (bson_iter_find_descendant(
                   &iter_0, ("posts." + std::to_string(idx) + ".post_id").c_str(),
                   &post_id_child) &&
               BSON_ITER_HOLDS_INT64(&post_id_child) &&
               bson_iter_find_descendant(
                   &iter_1,
                   ("posts." + std::to_string(idx) + ".timestamp").c_str(),
                   &timestamp_child) &&
               BSON_ITER_HOLDS_INT64(&timestamp_child))
        {
          auto curr_post_id = bson_iter_int64(&post_id_child);
          auto curr_timestamp = bson_iter_int64(&timestamp_child);
          if (idx >= mongo_start)
          {
            // In mixed workload condition, post may composed between redis and mongo read
            // mongodb index will shift and duplicate post_id occurs
            if (std::find(post_ids.begin(), post_ids.end(), curr_post_id) == post_ids.end())
            {
              post_ids.emplace_back(curr_post_id);
            }
          }
          redis_update_map.insert(std::make_pair(std::to_string(curr_post_id),
                                                 (double)curr_timestamp));
          bson_iter_init(&iter_0, doc);
          bson_iter_init(&iter_1, doc);
          idx++;
        }
      }
      bson_destroy(opts);
      bson_destroy(query);
      mongoc_cursor_destroy(cursor);
      mongoc_collection_destroy(collection);
      mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
    }

    if (redis_update_map.size() > 0)
    {
      auto redis_update_span = opentracing::Tracer::Global()->StartSpan(
          "user_timeline_redis_update_client",
          {opentracing::ChildOf(&span->context())});
      try
      {
        if (_redis_client_pool)
          _redis_client_pool->zadd(std::to_string(user_id),
                                   redis_update_map.begin(),
                                   redis_update_map.end());
        else if (IsRedisReplicationEnabled())
        {
          _redis_primary_pool->zadd(std::to_string(user_id),
                                    redis_update_map.begin(),
                                    redis_update_map.end());
        }
        else
          _redis_cluster_client_pool->zadd(std::to_string(user_id),
                                           redis_update_map.begin(),
                                           redis_update_map.end());
      }
      catch (const Error &err)
      {
        LOG(error) << err.what();
        throw err;
      }
      redis_update_span->Finish();
    }

    std::future<std::vector<Post>> post_future =
        std::async(std::launch::async, [&]()
                   {
        auto post_client_wrapper = _post_client_pool->Pop();
        if (!post_client_wrapper) {
          ServiceException se;
          se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
          se.message = "Failed to connect to post-storage-service";
          throw se;
        }
        std::vector<Post> _return_posts;
        auto post_client = post_client_wrapper->GetClient();
        try {
          std::set<int64_t> post_ids_not_cached(post_ids.begin(), post_ids.end());
      if (post_ids_not_cached.size() != post_ids.size())
      {
        LOG(error) << "Post_ids are duplicated";
        ServiceException se;
        se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
        se.message = "Post_ids are duplicated";
        throw se;
      }
      std::map<int64_t, Post> return_map;
      memcached_return_t memcached_rc;
      auto memcached_client =
          memcached_pool_pop(_memcached_client_pool, true, &memcached_rc);
      if (!memcached_client)
      {
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
      for (auto &post_id : post_ids)
      {
        std::string key_str = std::to_string(post_id);
        keys[idx] = new char[key_str.length() + 1];
        strcpy(keys[idx], key_str.c_str());
        key_sizes[idx] = key_str.length();
        idx++;
      }
      memcached_rc =
          memcached_mget(memcached_client, keys, key_sizes, post_ids.size());
      if (memcached_rc != MEMCACHED_SUCCESS)
      {
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
          "user_post_mmc_mget_client", {opentracing::ChildOf(&span->context())});

      while (true)
      {
        return_value =
            memcached_fetch(memcached_client, return_key, &return_key_length,
                            &return_value_length, &flags, &memcached_rc);
        if (return_value == nullptr)
        {
          LOG(debug) << "Memcached mget finished";
          break;
        }
        if (memcached_rc != MEMCACHED_SUCCESS)
        {
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
        for (auto &item : post_json["media"])
        {
          Media media;
          media.media_id = item["media_id"];
          media.media_type = item["media_type"];
          new_post.media.emplace_back(media);
        }
        for (auto &item : post_json["user_mentions"])
        {
          UserMention user_mention;
          user_mention.username = item["username"];
          user_mention.user_id = item["user_id"];
          new_post.user_mentions.emplace_back(user_mention);
        }
        for (auto &item : post_json["urls"])
        {
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

      for (int i = 0; i < post_ids.size(); ++i)
      {
        delete keys[i];
      }
      delete[] keys;
      delete[] key_sizes;

      for (auto &post_id : post_ids)
      {
        _return_posts.emplace_back(return_map[post_id]);
      }
          if (!post_ids_not_cached.empty())
      {
        std::vector<int64_t> other_post_ids;
        for (auto &post_id : post_ids_not_cached)
        {
          other_post_ids.emplace_back(post_id);
        }
        std::vector<Post> new_return;
        post_client->ReadPosts(new_return, req_id, other_post_ids, writer_text_map);
        for (auto &post : new_return)
        {
          _return_posts.emplace_back(post);
          std::string post_id_str = std::to_string(post.post_id);
          bson_t *new_doc = bson_new();
          BSON_APPEND_INT64(new_doc, "post_id", post.post_id);
          BSON_APPEND_INT64(new_doc, "timestamp", post.timestamp);
          BSON_APPEND_UTF8(new_doc, "text", post.text.c_str());
          BSON_APPEND_INT64(new_doc, "req_id", post.req_id);
          BSON_APPEND_INT32(new_doc, "post_type", post.post_type);

          bson_t creator_doc;
          BSON_APPEND_DOCUMENT_BEGIN(new_doc, "creator", &creator_doc);
          BSON_APPEND_INT64(&creator_doc, "user_id", post.creator.user_id);
          BSON_APPEND_UTF8(&creator_doc, "username", post.creator.username.c_str());
          bson_append_document_end(new_doc, &creator_doc);

          const char *key;
          int idx = 0;
          char buf[16];
          bson_t url_list;
          BSON_APPEND_ARRAY_BEGIN(new_doc, "urls", &url_list);
          for (auto &url : post.urls)
          {
            bson_uint32_to_string(idx, &key, buf, sizeof buf);
            bson_t url_doc;
            BSON_APPEND_DOCUMENT_BEGIN(&url_list, key, &url_doc);
            BSON_APPEND_UTF8(&url_doc, "shortened_url", url.shortened_url.c_str());
            BSON_APPEND_UTF8(&url_doc, "expanded_url", url.expanded_url.c_str());
            bson_append_document_end(&url_list, &url_doc);
            idx++;
          }
          bson_append_array_end(new_doc, &url_list);

          bson_t user_mention_list;
          idx = 0;
          BSON_APPEND_ARRAY_BEGIN(new_doc, "user_mentions", &user_mention_list);
          for (auto &user_mention : post.user_mentions)
          {
            bson_uint32_to_string(idx, &key, buf, sizeof buf);
            bson_t user_mention_doc;
            BSON_APPEND_DOCUMENT_BEGIN(&user_mention_list, key, &user_mention_doc);
            BSON_APPEND_INT64(&user_mention_doc, "user_id", user_mention.user_id);
            BSON_APPEND_UTF8(&user_mention_doc, "username",
                             user_mention.username.c_str());
            bson_append_document_end(&user_mention_list, &user_mention_doc);
            idx++;
          }
          bson_append_array_end(new_doc, &user_mention_list);

          bson_t media_list;
          idx = 0;
          BSON_APPEND_ARRAY_BEGIN(new_doc, "media", &media_list);
          for (auto &media : post.media)
          {
            bson_uint32_to_string(idx, &key, buf, sizeof buf);
            bson_t media_doc;
            BSON_APPEND_DOCUMENT_BEGIN(&media_list, key, &media_doc);
            BSON_APPEND_INT64(&media_doc, "media_id", media.media_id);
            BSON_APPEND_UTF8(&media_doc, "media_type", media.media_type.c_str());
            bson_append_document_end(&media_list, &media_doc);
            idx++;
          }
          bson_append_array_end(new_doc, &media_list);
          auto post_json_char = bson_as_json(new_doc, nullptr);
          bson_destroy(new_doc);
          memcached_rc = memcached_set(
              memcached_client, post_id_str.c_str(), post_id_str.length(),
              post_json_char, std::strlen(post_json_char), static_cast<time_t>(0),
              static_cast<uint32_t>(0));
        }
        
      }
      memcached_pool_push(_memcached_client_pool, memcached_client);
        } catch (...) {
          _post_client_pool->Remove(post_client_wrapper);
          LOG(error) << "Failed to read posts from post-storage-service";
          throw;
        }
        _post_client_pool->Keepalive(post_client_wrapper);
        return _return_posts; });

    if (redis_update_map.size() > 0)
    {
      auto redis_update_span = opentracing::Tracer::Global()->StartSpan(
          "user_timeline_redis_update_client",
          {opentracing::ChildOf(&span->context())});
      try
      {
        if (_redis_client_pool)
          _redis_client_pool->zadd(std::to_string(user_id),
                                   redis_update_map.begin(),
                                   redis_update_map.end());
        else if (IsRedisReplicationEnabled())
        {
          _redis_primary_pool->zadd(std::to_string(user_id),
                                    redis_update_map.begin(),
                                    redis_update_map.end());
        }
        else
          _redis_cluster_client_pool->zadd(std::to_string(user_id),
                                           redis_update_map.begin(),
                                           redis_update_map.end());
      }
      catch (const Error &err)
      {
        LOG(error) << err.what();
        throw err;
      }
      redis_update_span->Finish();
    }

    try
    {
      _return = post_future.get();
    }
    catch (...)
    {
      LOG(error) << "Failed to get post from post-storage-service";
      throw;
    }
    span->Finish();
  }

} // namespace social_network

#endif // SOCIAL_NETWORK_MICROSERVICES_SRC_USERTIMELINESERVICE_USERTIMELINEHANDLER_H_
