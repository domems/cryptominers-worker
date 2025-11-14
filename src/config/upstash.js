// src/config/upstash.js
import { Redis } from "@upstash/redis";
import "dotenv/config";

/**
 * Cliente Redis (REST) – para locks, caches, etc.
 * Requer:
 *  - UPSTASH_REDIS_REST_URL
 *  - UPSTASH_REDIS_REST_TOKEN
 */
export const redis = Redis.fromEnv();
