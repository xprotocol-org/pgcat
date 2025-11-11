use arc_swap::ArcSwap;
use bb8::{ManageConnection, Pool, PooledConnection, QueueStrategy};
use chrono::naive::NaiveDateTime;
use log::{debug, error, info, warn};
use lru::LruCache;
use once_cell::sync::Lazy;
use parking_lot::{Mutex, RwLock};
use rand::seq::SliceRandom;
use rand::thread_rng;
use regex::Regex;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::num::NonZeroUsize;
use std::sync::atomic::AtomicU64;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Instant;
use tokio::sync::Notify;

use crate::config::{
    get_config, Address, DefaultShard, General, LoadBalancingMode, Plugins, PoolMode, Role, User,
};
use crate::errors::Error;

use crate::auth_passthrough::AuthPassthrough;
use crate::messages::Parse;
use crate::plugins::prewarmer;
use crate::server::{Server, ServerParameters};
use crate::sharding::ShardingFunction;
use crate::stats::{AddressStats, ClientStats, ServerStats};

pub type ProcessId = i32;
pub type SecretKey = i32;
pub type ServerHost = String;
pub type ServerPort = u16;

pub type BanList = Arc<RwLock<Vec<HashMap<Address, (BanReason, NaiveDateTime)>>>>;
pub type ClientServerMap =
    Arc<Mutex<HashMap<(ProcessId, SecretKey), (ProcessId, SecretKey, ServerHost, ServerPort)>>>;
pub type PoolMap = HashMap<PoolIdentifier, ConnectionPool>;

pub static POOLS: Lazy<ArcSwap<PoolMap>> = Lazy::new(|| ArcSwap::from_pointee(HashMap::default()));
static POOLS_WRITE_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

struct ConnectionTimeouts {
    connect: u64,
    idle: u64,
    lifetime: u64,
}

// Reasons for banning a server.
#[derive(Debug, PartialEq, Clone)]
pub enum BanReason {
    FailedHealthCheck,
    MessageSendFailed,
    MessageReceiveFailed,
    FailedCheckout,
    StatementTimeout,
    AdminBan(i64),
}

pub type PreparedStatementCacheType = Arc<Mutex<PreparedStatementCache>>;

// TODO: Add stats the this cache
// TODO: Add application name to the cache value to help identify which application is using the cache
// TODO: Create admin command to show which statements are in the cache
#[derive(Debug)]
pub struct PreparedStatementCache {
    cache: LruCache<u64, Arc<Parse>>,
}

impl PreparedStatementCache {
    pub fn new(mut size: usize) -> Self {
        // Cannot be zeros
        if size == 0 {
            size = 1;
        }

        PreparedStatementCache {
            cache: LruCache::new(NonZeroUsize::new(size).unwrap()),
        }
    }

    /// Adds the prepared statement to the cache if it doesn't exist with a new name
    /// if it already exists will give you the existing parse
    ///
    /// Pass the hash to this so that we can do the compute before acquiring the lock
    pub fn get_or_insert(&mut self, parse: &Parse, hash: u64) -> Arc<Parse> {
        match self.cache.get(&hash) {
            Some(rewritten_parse) => rewritten_parse.clone(),
            None => {
                let new_parse = Arc::new(parse.clone().rewrite(hash));
                let evicted = self.cache.push(hash, new_parse.clone());

                if let Some((_, evicted_parse)) = evicted {
                    debug!(
                        "Evicted prepared statement {} from cache",
                        evicted_parse.name
                    );
                }

                new_parse
            }
        }
    }

    /// Marks the hash as most recently used if it exists
    pub fn promote(&mut self, hash: &u64) {
        self.cache.promote(hash);
    }
}

/// An identifier for a PgCat pool,
/// a database visible to clients.
#[derive(Hash, Debug, Clone, PartialEq, Eq, Default)]
pub struct PoolIdentifier {
    // The name of the database clients want to connect to.
    pub db: String,

    /// The username the client connects with. Each user gets its own pool.
    pub user: String,
}

static POOL_REAPER_RATE: u64 = 30_000; // 30 seconds by default

impl PoolIdentifier {
    /// Create a new user/pool identifier.
    pub fn new(db: &str, user: &str) -> PoolIdentifier {
        PoolIdentifier {
            db: db.to_string(),
            user: user.to_string(),
        }
    }
}

impl Display for PoolIdentifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{}", self.user, self.db)
    }
}

impl From<&Address> for PoolIdentifier {
    fn from(address: &Address) -> PoolIdentifier {
        PoolIdentifier::new(&address.database, &address.username)
    }
}

/// Pool settings.
#[derive(Clone, Debug)]
pub struct PoolSettings {
    /// Transaction or Session.
    pub pool_mode: PoolMode,

    /// Random or LeastOutstandingConnections.
    pub load_balancing_mode: LoadBalancingMode,

    /// Maximum number of checkout failures a client is allowed before it
    /// gets disconnected. This is needed to prevent persistent client/server
    /// imbalance in high availability setups where multiple PgCat instances are placed
    /// behind a single load balancer. If for any reason a client lands on a PgCat instance that has
    /// a large number of connected clients, it might get stuck in perpetual checkout failure loop especially
    /// in session mode
    pub checkout_failure_limit: Option<u64>,

    // Number of shards.
    pub shards: usize,

    // Connecting user.
    pub user: User,
    pub db: String,

    // Default server role to connect to.
    pub default_role: Option<Role>,

    // Enable/disable query parser.
    pub query_parser_enabled: bool,

    // Max length of query the parser will parse.
    pub query_parser_max_length: Option<usize>,

    // Infer role
    pub query_parser_read_write_splitting: bool,

    // Read from the primary as well or not.
    pub primary_reads_enabled: bool,

    // Automatic primary/replica selection based on recent activity.
    pub db_activity_based_routing: bool,

    // DB activity init delay
    pub db_activity_init_delay: u64,

    // DB activity TTL
    pub db_activity_ttl: u64,

    // Table mutation cache TTL
    pub table_mutation_cache_ms_ttl: u64,

    // Sharding function.
    pub sharding_function: ShardingFunction,

    // Sharding key
    pub automatic_sharding_key: Option<String>,

    // Health check timeout
    pub healthcheck_timeout: u64,

    // Health check delay
    pub healthcheck_delay: u64,

    // Ban time
    pub ban_time: i64,

    // Regex for searching for the sharding key in SQL statements
    pub sharding_key_regex: Option<Regex>,

    // Regex for searching for the shard id in SQL statements
    pub shard_id_regex: Option<Regex>,

    // What to do when no shard is selected in a sharded system
    pub default_shard: DefaultShard,

    // Limit how much of each query is searched for a potential shard regex match
    pub regex_search_limit: usize,

    // Auth query parameters
    pub auth_query: Option<String>,
    pub auth_query_user: Option<String>,
    pub auth_query_password: Option<String>,

    /// Plugins
    pub plugins: Option<Plugins>,
}

impl Default for PoolSettings {
    fn default() -> PoolSettings {
        PoolSettings {
            pool_mode: PoolMode::Transaction,
            load_balancing_mode: LoadBalancingMode::Random,
            checkout_failure_limit: None,
            shards: 1,
            user: User::default(),
            db: String::default(),
            default_role: None,
            query_parser_enabled: false,
            query_parser_max_length: None,
            query_parser_read_write_splitting: false,
            primary_reads_enabled: true,
            db_activity_based_routing: false,
            db_activity_init_delay: 100,
            db_activity_ttl: 15 * 60,
            table_mutation_cache_ms_ttl: 50,
            sharding_function: ShardingFunction::PgBigintHash,
            automatic_sharding_key: None,
            healthcheck_delay: General::default_healthcheck_delay(),
            healthcheck_timeout: General::default_healthcheck_timeout(),
            ban_time: General::default_ban_time(),
            sharding_key_regex: None,
            shard_id_regex: None,
            regex_search_limit: 1000,
            default_shard: DefaultShard::Shard(0),
            auth_query: None,
            auth_query_user: None,
            auth_query_password: None,
            plugins: None,
        }
    }
}

/// The globally accessible connection pool.
#[derive(Clone, Debug, Default)]
pub struct ConnectionPool {
    /// The pools handled internally by bb8.
    databases: Arc<Vec<Vec<Pool<ServerPool>>>>,

    /// The addresses (host, port, role) to handle
    /// failover and load balancing deterministically.
    addresses: Arc<Vec<Vec<Address>>>,

    /// List of banned addresses (see above)
    /// that should not be queried.
    banlist: BanList,

    /// The server information has to be passed to the
    /// clients on startup. We pre-connect to all shards and replicas
    /// on pool creation and save the startup parameters here.
    original_server_parameters: Arc<RwLock<ServerParameters>>,

    /// Pool configuration.
    pub settings: Arc<PoolSettings>,

    /// If not validated, we need to double check the pool is available before allowing a client
    /// to use it.
    validated: Arc<AtomicBool>,

    /// Hash value for the pool configs. It is used to compare new configs
    /// against current config to decide whether or not we need to recreate
    /// the pool after a RELOAD command
    pub config_hash: u64,

    /// If the pool has been paused or not.
    paused: Arc<AtomicBool>,
    paused_waiter: Arc<Notify>,

    /// AuthInfo
    pub auth_hash: Arc<RwLock<Option<String>>>,

    /// Cache
    pub prepared_statement_cache: Option<PreparedStatementCacheType>,
}

impl ConnectionPool {
    pub async fn from_config(client_server_map: ClientServerMap) -> Result<(), Error> {
        let config = get_config();
        let mut new_pools = HashMap::new();
        let mut address_id: usize = 0;

        for (pool_name, pool_config) in &config.pools {
            for user in pool_config.users.values() {
                let identifier = PoolIdentifier::new(pool_name, &user.username);

                if let Some(pool) = Self::try_reuse_pool(pool_name, user, pool_config)? {
                    new_pools.insert(identifier, pool);
                    continue;
                }

                info!("[pool: {}][user: {}] creating new pool", pool_name, user.username);

                let pool = Self::build_pool(
                    pool_name,
                    user,
                    pool_config,
                    &config,
                    &client_server_map,
                    &mut address_id,
                )
                .await?;

                Self::spawn_validation_if_needed(&pool, &config);
                new_pools.insert(identifier, pool);
            }
        }

        POOLS.store(Arc::new(new_pools));
        Ok(())
    }

    fn try_reuse_pool(
        pool_name: &str,
        user: &User,
        pool_config: &crate::config::Pool,
    ) -> Result<Option<ConnectionPool>, Error> {
        let new_hash = pool_config.hash_value();

        if let Some(existing_pool) = get_pool(pool_name, &user.username) {
            if existing_pool.config_hash == new_hash {
                info!("[pool: {}][user: {}] has not changed", pool_name, user.username);
                return Ok(Some(existing_pool));
            }
        }

        Ok(None)
    }

    async fn build_pool(
        pool_name: &str,
        user: &User,
        pool_config: &crate::config::Pool,
        config: &crate::config::Config,
        client_server_map: &ClientServerMap,
        address_id: &mut usize,
    ) -> Result<ConnectionPool, Error> {
        let shard_ids = Self::extract_sorted_shard_ids(pool_config)?;
        let pool_auth_hash = Arc::new(RwLock::new(None));

        let (shards, addresses, banlist) = Self::build_shards(
            &shard_ids,
            pool_name,
            user,
            pool_config,
            config,
            client_server_map,
            &pool_auth_hash,
            address_id,
        )
        .await?;

        if shards.len() != addresses.len() {
            error!(
                "Pool configuration error: shard count ({}) does not match address count ({})",
                shards.len(),
                addresses.len()
            );
            return Err(Error::BadConfig);
        }

        Self::log_auth_status(&pool_auth_hash, pool_name, &user.username);

        Ok(ConnectionPool {
            databases: Arc::new(shards),
            addresses: Arc::new(addresses),
            banlist: Arc::new(RwLock::new(banlist)),
            config_hash: pool_config.hash_value(),
            original_server_parameters: Arc::new(RwLock::new(ServerParameters::new())),
            auth_hash: pool_auth_hash,
            settings: Arc::new(Self::build_pool_settings(
                pool_name,
                user,
                pool_config,
                config,
                shard_ids.len(),
            )),
            validated: Arc::new(AtomicBool::new(false)),
            paused: Arc::new(AtomicBool::new(false)),
            paused_waiter: Arc::new(Notify::new()),
            prepared_statement_cache: Self::create_prepared_statement_cache(pool_config),
        })
    }

    fn extract_sorted_shard_ids(pool_config: &crate::config::Pool) -> Result<Vec<String>, Error> {
        let mut shard_ids: Vec<String> = pool_config.shards.keys().cloned().collect();
        shard_ids.sort_by_key(|k| {
            k.parse::<i64>()
                .unwrap_or_else(|_| {
                    error!("Invalid shard id '{}': must be a valid integer", k);
                    i64::MAX
                })
        });

        for id in &shard_ids {
            if id.parse::<i64>().is_err() {
                error!("Invalid shard id '{}': must be a valid integer", id);
                return Err(Error::BadConfig);
            }
        }

        Ok(shard_ids)
    }

    async fn build_shards(
        shard_ids: &[String],
        pool_name: &str,
        user: &User,
        pool_config: &crate::config::Pool,
        config: &crate::config::Config,
        client_server_map: &ClientServerMap,
        pool_auth_hash: &Arc<RwLock<Option<String>>>,
        address_id: &mut usize,
    ) -> Result<(Vec<Vec<Pool<ServerPool>>>, Vec<Vec<Address>>, Vec<HashMap<Address, (BanReason, NaiveDateTime)>>), Error> {
        let mut shards = Vec::new();
        let mut addresses = Vec::new();
        let mut banlist = Vec::new();

        for shard_idx in shard_ids {
            let shard_config = &pool_config.shards[shard_idx];

            let (shard_pools, shard_addresses) = Self::build_shard(
                shard_idx,
                shard_config,
                pool_name,
                user,
                pool_config,
                config,
                client_server_map,
                pool_auth_hash,
                address_id,
            )
            .await?;

            shards.push(shard_pools);
            addresses.push(shard_addresses);
            banlist.push(HashMap::new());
        }

        Ok((shards, addresses, banlist))
    }

    async fn build_shard(
        shard_idx: &str,
        shard_config: &crate::config::Shard,
        pool_name: &str,
        user: &User,
        pool_config: &crate::config::Pool,
        config: &crate::config::Config,
        client_server_map: &ClientServerMap,
        pool_auth_hash: &Arc<RwLock<Option<String>>>,
        address_id: &mut usize,
    ) -> Result<(Vec<Pool<ServerPool>>, Vec<Address>), Error> {
        if shard_config.servers.is_empty() {
            error!("Shard '{}' has no servers configured", shard_idx);
            return Err(Error::BadConfig);
        }

        let mut pools = Vec::new();
        let mut addresses = Vec::new();
        let mut replica_number = 0;

        for (address_index, server_config) in shard_config.servers.iter().enumerate() {
            let address = Self::build_address(
                shard_idx,
                shard_config,
                server_config,
                address_index,
                replica_number,
                pool_name,
                user,
                address_id,
            );

            if server_config.role == Role::Replica {
                replica_number += 1;
            }

            Self::fetch_auth_hash_if_needed(pool_config, &address, pool_auth_hash, shard_config).await;

            let pool = Self::build_bb8_pool(
                &address,
                user,
                pool_config,
                config,
                shard_config,
                client_server_map,
                pool_auth_hash,
                pool_name,
            )
            .await?;

            pools.push(pool);
            addresses.push(address);
        }

        Ok((pools, addresses))
    }

    fn build_address(
        shard_idx: &str,
        shard_config: &crate::config::Shard,
        server_config: &crate::config::ServerConfig,
        address_index: usize,
        replica_number: usize,
        pool_name: &str,
        user: &User,
        address_id: &mut usize,
    ) -> Address {
        let mirror_addresses = Self::build_mirror_addresses(
            shard_idx,
            shard_config,
            server_config,
            address_index,
            replica_number,
            pool_name,
            user,
            address_id,
        );

        let shard_number = shard_idx.parse::<usize>().unwrap_or_else(|e| {
            error!("Failed to parse shard index '{}': {}", shard_idx, e);
            0
        });

        let address = Address {
            id: *address_id,
            database: shard_config.database.clone(),
            host: server_config.host.clone(),
            port: server_config.port,
            role: server_config.role,
            address_index,
            replica_number,
            shard: shard_number,
            username: user.username.clone(),
            pool_name: pool_name.to_string(),
            mirrors: mirror_addresses,
            stats: Arc::new(AddressStats::default()),
            error_count: Arc::new(AtomicU64::new(0)),
        };

        *address_id += 1;
        address
    }

    fn build_mirror_addresses(
        shard_idx: &str,
        shard_config: &crate::config::Shard,
        server_config: &crate::config::ServerConfig,
        address_index: usize,
        replica_number: usize,
        pool_name: &str,
        user: &User,
        address_id: &mut usize,
    ) -> Vec<Address> {
        let Some(mirrors) = &shard_config.mirrors else {
            return Vec::new();
        };

        let shard_number = shard_idx.parse::<usize>().unwrap_or_else(|e| {
            error!("Failed to parse shard index '{}' for mirror: {}", shard_idx, e);
            0
        });

        mirrors
            .iter()
            .enumerate()
            .filter(|(_, mirror)| mirror.mirroring_target_index == address_index)
            .map(|(mirror_idx, mirror)| {
                let addr = Address {
                    id: *address_id,
                    database: shard_config.database.clone(),
                    host: mirror.host.clone(),
                    port: mirror.port,
                    role: server_config.role,
                    address_index: mirror_idx,
                    replica_number,
                    shard: shard_number,
                    username: user.username.clone(),
                    pool_name: pool_name.to_string(),
                    mirrors: vec![],
                    stats: Arc::new(AddressStats::default()),
                    error_count: Arc::new(AtomicU64::new(0)),
                };
                *address_id += 1;
                addr
            })
            .collect()
    }

    async fn fetch_auth_hash_if_needed(
        pool_config: &crate::config::Pool,
        address: &Address,
        pool_auth_hash: &Arc<RwLock<Option<String>>>,
        shard_config: &crate::config::Shard,
    ) {
        let Some(auth_passthrough) = AuthPassthrough::from_pool_config(pool_config) else {
            return;
        };

        match auth_passthrough.fetch_hash(address).await {
            Ok(hash) => {
                if let Some(existing) = &*pool_auth_hash.read() {
                    if hash != *existing {
                        warn!(
                            "Hash is not the same across shards of the same pool, \
                            client auth will be done using last obtained hash. \
                            Server: {}:{}, Database: {}",
                            address.host, address.port, shard_config.database,
                        );
                    }
                }

                debug!("Hash obtained for {:?}", address);
                *pool_auth_hash.write() = Some(hash);
            }
            Err(err) => {
                warn!(
                    "Could not obtain password hashes using auth_query config, ignoring. Error: {:?}",
                    err
                );
            }
        }
    }

    async fn build_bb8_pool(
        address: &Address,
        user: &User,
        pool_config: &crate::config::Pool,
        config: &crate::config::Config,
        shard_config: &crate::config::Shard,
        client_server_map: &ClientServerMap,
        pool_auth_hash: &Arc<RwLock<Option<String>>>,
        pool_name: &str,
    ) -> Result<Pool<ServerPool>, Error> {
        if let Some(min_size) = user.min_pool_size {
            if min_size > user.pool_size {
                error!(
                    "Invalid pool configuration for user '{}': min_pool_size ({}) cannot exceed pool_size ({})",
                    user.username, min_size, user.pool_size
                );
                return Err(Error::BadConfig);
            }
        }

        let plugins = pool_config.plugins.clone().or_else(|| config.plugins.clone());

        let manager = ServerPool::new(
            address.clone(),
            user.clone(),
            &shard_config.database,
            client_server_map.clone(),
            pool_auth_hash.clone(),
            plugins,
            pool_config.cleanup_server_connections,
            pool_config.log_client_parameter_status_changes,
            pool_config.prepared_statements_cache_size,
        );

        let timeouts = Self::resolve_timeouts(user, pool_config, config);
        let reaper_rate = Self::calculate_reaper_rate(&timeouts);
        let queue_strategy = Self::select_queue_strategy(config);

        debug!("[pool: {}][user: {}] Pool reaper rate: {}ms", pool_name, user.username, reaper_rate);

        let builder = Pool::builder()
            .max_size(user.pool_size)
            .min_idle(user.min_pool_size)
            .connection_timeout(std::time::Duration::from_millis(timeouts.connect))
            .idle_timeout(Some(std::time::Duration::from_millis(timeouts.idle)))
            .max_lifetime(Some(std::time::Duration::from_millis(timeouts.lifetime)))
            .reaper_rate(std::time::Duration::from_millis(reaper_rate))
            .queue_strategy(queue_strategy)
            .test_on_check_out(false);

        if config.general.validate_config {
            builder.build(manager).await.map_err(|e| {
                error!(
                    "Failed to build connection pool for {}:{} (shard: {}, database: {}): {}",
                    address.host, address.port, address.shard, shard_config.database, e
                );
                Error::ServerError
            })
        } else {
            Ok(builder.build_unchecked(manager))
        }
    }

    fn resolve_timeouts(
        user: &User,
        pool_config: &crate::config::Pool,
        config: &crate::config::Config,
    ) -> ConnectionTimeouts {
        ConnectionTimeouts {
            connect: user.connect_timeout
                .or(pool_config.connect_timeout)
                .unwrap_or(config.general.connect_timeout),
            idle: user.idle_timeout
                .or(pool_config.idle_timeout)
                .unwrap_or(config.general.idle_timeout),
            lifetime: user.server_lifetime
                .or(pool_config.server_lifetime)
                .unwrap_or(config.general.server_lifetime),
        }
    }

    fn calculate_reaper_rate(timeouts: &ConnectionTimeouts) -> u64 {
        *[timeouts.idle, timeouts.lifetime, POOL_REAPER_RATE]
            .iter()
            .min()
            .unwrap()
    }

    fn select_queue_strategy(config: &crate::config::Config) -> QueueStrategy {
        if config.general.server_round_robin {
            QueueStrategy::Fifo
        } else {
            QueueStrategy::Lifo
        }
    }

    fn build_pool_settings(
        pool_name: &str,
        user: &User,
        pool_config: &crate::config::Pool,
        config: &crate::config::Config,
        shard_count: usize,
    ) -> PoolSettings {
        PoolSettings {
            pool_mode: user.pool_mode.unwrap_or(pool_config.pool_mode),
            load_balancing_mode: pool_config.load_balancing_mode,
            checkout_failure_limit: pool_config.checkout_failure_limit,
            shards: shard_count,
            user: user.clone(),
            db: pool_name.to_string(),
            default_role: Self::parse_default_role(&pool_config.default_role),
            query_parser_enabled: pool_config.query_parser_enabled,
            query_parser_max_length: pool_config.query_parser_max_length,
            query_parser_read_write_splitting: pool_config.query_parser_read_write_splitting,
            primary_reads_enabled: pool_config.primary_reads_enabled,
            sharding_function: pool_config.sharding_function,
            db_activity_based_routing: pool_config.db_activity_based_routing,
            db_activity_init_delay: pool_config.db_activity_init_delay,
            db_activity_ttl: pool_config.db_activity_ttl,
            table_mutation_cache_ms_ttl: pool_config.table_mutation_cache_ms_ttl,
            automatic_sharding_key: pool_config.automatic_sharding_key.clone(),
            healthcheck_delay: config.general.healthcheck_delay,
            healthcheck_timeout: config.general.healthcheck_timeout,
            ban_time: config.general.ban_time,
            sharding_key_regex: pool_config
                .sharding_key_regex
                .as_ref()
                .map(|r| Regex::new(r).unwrap()),
            shard_id_regex: pool_config
                .shard_id_regex
                .as_ref()
                .map(|r| Regex::new(r).unwrap()),
            regex_search_limit: pool_config.regex_search_limit.unwrap_or(1000),
            default_shard: pool_config.default_shard,
            auth_query: pool_config.auth_query.clone(),
            auth_query_user: pool_config.auth_query_user.clone(),
            auth_query_password: pool_config.auth_query_password.clone(),
            plugins: pool_config.plugins.clone().or_else(|| config.plugins.clone()),
        }
    }

    fn parse_default_role(role_str: &str) -> Option<Role> {
        match role_str {
            "any" => None,
            "replica" => Some(Role::Replica),
            "primary" => Some(Role::Primary),
            _ => unreachable!(),
        }
    }

    fn create_prepared_statement_cache(
        pool_config: &crate::config::Pool,
    ) -> Option<PreparedStatementCacheType> {
        match pool_config.prepared_statements_cache_size {
            0 => None,
            size => Some(Arc::new(Mutex::new(PreparedStatementCache::new(size)))),
        }
    }

    fn log_auth_status(pool_auth_hash: &Arc<RwLock<Option<String>>>, pool_name: &str, username: &str) {
        if pool_auth_hash.read().is_some() {
            info!(
                "Auth hash obtained from query_auth for pool {{ name: {}, user: {} }}",
                pool_name, username
            );
        }
    }

    fn spawn_validation_if_needed(pool: &ConnectionPool, config: &crate::config::Config) {
        if config.general.validate_config {
            let validate_pool = pool.clone();
            tokio::task::spawn(async move {
                let _ = validate_pool.validate().await;
            });
        }
    }

    pub async fn create_dynamic_pool(
        db_name: &str,
        username: &str,
        _password_hash: &[u8],
        client_server_map: ClientServerMap,
    ) -> Result<ConnectionPool, Error> {
        let config = get_config();

        let default_pool_config = match config.pools.get("default") {
            Some(pool_config) => pool_config,
            None => return Err(Error::ClientError("No default pool configured".into())),
        };

        let default_user = match default_pool_config
            .users
            .values()
            .find(|user| user.username == username)
        {
            Some(user) => user,
            None => {
                return Err(Error::ClientError(format!(
                    "User '{}' not found in default pool configuration",
                    username
                )))
            }
        };

        let dynamic_user = default_user.clone();

        let mut dynamic_pool_config = default_pool_config.clone();

        for shard in dynamic_pool_config.shards.values_mut() {
            shard.database = db_name.to_string();
        }

        dynamic_pool_config.idle_timeout = Some(5000);
        dynamic_pool_config.server_lifetime = Some(30000);

        let mut address_id: usize = 0;

        info!(
            "[pool: {}][user: {}] creating dynamic pool",
            db_name, username
        );

        let pool = Self::build_pool(
            db_name,
            &dynamic_user,
            &dynamic_pool_config,
            &config,
            &client_server_map,
            &mut address_id,
        )
        .await?;

        Self::spawn_validation_if_needed(&pool, &config);

        Ok(pool)
    }

    pub fn get_default_pool_user() -> Option<User> {
        let config = get_config();

        let default_pool_config = config.pools.get("default")?;
        let default_user = default_pool_config.users.values().next()?;

        Some(default_user.clone())
    }

    /// Connect to all shards, grab server information, and possibly
    /// passwords to use in client auth.
    /// Return server information we will pass to the clients
    /// when they connect.
    /// This also warms up the pool for clients that connect when
    /// the pooler starts up.
    pub async fn validate(&self) -> Result<(), Error> {
        let mut futures = Vec::new();
        let validated = Arc::clone(&self.validated);

        for shard in 0..self.shards() {
            for server in 0..self.servers(shard) {
                let databases = self.databases.clone();
                let validated = Arc::clone(&validated);
                let pool_server_parameters = Arc::clone(&self.original_server_parameters);

                let task = tokio::task::spawn(async move {
                    let connection = match databases[shard][server].get().await {
                        Ok(conn) => conn,
                        Err(err) => {
                            error!("Shard {} down or misconfigured: {:?}", shard, err);
                            return;
                        }
                    };

                    let proxy = connection;
                    let server = &*proxy;
                    let server_parameters: ServerParameters = server.server_parameters();

                    let mut guard = pool_server_parameters.write();
                    *guard = server_parameters;
                    validated.store(true, Ordering::Relaxed);
                });

                futures.push(task);
            }
        }

        futures::future::join_all(futures).await;

        // TODO: compare server information to make sure
        // all shards are running identical configurations.
        if !self.validated() {
            error!("Could not validate connection pool");
            return Err(Error::AllServersDown);
        }

        Ok(())
    }

    /// The pool can be used by clients.
    ///
    /// If not, we need to validate it first by connecting to servers.
    /// Call `validate()` to do so.
    pub fn validated(&self) -> bool {
        self.validated.load(Ordering::Relaxed)
    }

    /// Pause the pool, allowing no more queries and make clients wait.
    pub fn pause(&self) {
        self.paused.store(true, Ordering::Relaxed);
    }

    /// Resume the pool, allowing queries and resuming any pending queries.
    pub fn resume(&self) {
        self.paused.store(false, Ordering::Relaxed);
        self.paused_waiter.notify_waiters();
    }

    /// Check if the pool is paused.
    pub fn paused(&self) -> bool {
        self.paused.load(Ordering::Relaxed)
    }

    /// Check if the pool is paused and wait until it's resumed.
    pub async fn wait_paused(&self) -> bool {
        let waiter = self.paused_waiter.notified();
        let paused = self.paused.load(Ordering::Relaxed);

        if paused {
            waiter.await;
        }

        paused
    }

    /// Get a connection from the pool.
    pub async fn get(
        &self,
        shard: Option<usize>,       // shard number
        role: Option<Role>,         // primary or replica
        client_stats: &ClientStats, // client id
    ) -> Result<(PooledConnection<'_, ServerPool>, Address), Error> {
        if !self.validated() {
            self.validate().await?;
        }

        let effective_shard_id = if self.shards() == 1 {
            // The base, unsharded case
            Some(0)
        } else {
            if !self.valid_shard_id(shard) {
                // None is valid shard ID so it is safe to unwrap here
                return Err(Error::InvalidShardId(shard.unwrap()));
            }
            shard
        };

        let mut candidates = self
            .addresses
            .iter()
            .flatten()
            .filter(|address| address.role == role)
            .collect::<Vec<&Address>>();

        // We start with a shuffled list of addresses even if we end up resorting
        // this is meant to avoid hitting instance 0 everytime if the sorting metric
        // ends up being the same for all instances
        candidates.shuffle(&mut thread_rng());

        match effective_shard_id {
            Some(shard_id) => candidates.retain(|address| address.shard == shard_id),
            None => match self.settings.default_shard {
                DefaultShard::Shard(shard_id) => {
                    candidates.retain(|address| address.shard == shard_id)
                }
                DefaultShard::Random => (),
                DefaultShard::RandomHealthy => {
                    candidates.sort_by(|a, b| {
                        b.error_count
                            .load(Ordering::Relaxed)
                            .partial_cmp(&a.error_count.load(Ordering::Relaxed))
                            .unwrap()
                    });
                }
            },
        };

        if self.settings.load_balancing_mode == LoadBalancingMode::LeastOutstandingConnections {
            candidates.sort_by(|a, b| {
                self.busy_connection_count(b)
                    .partial_cmp(&self.busy_connection_count(a))
                    .unwrap()
            });
        }

        // Indicate we're waiting on a server connection from a pool.
        let now = Instant::now();
        client_stats.waiting();

        while !candidates.is_empty() {
            // Get the next candidate
            let address = match candidates.pop() {
                Some(address) => address,
                None => break,
            };

            let mut force_healthcheck = false;

            if self.is_banned(address) {
                if self.try_unban(address).await {
                    force_healthcheck = true;
                } else {
                    debug!("Address {:?} is banned", address);
                    continue;
                }
            }

            // Check if we can connect
            let mut conn = match self.databases[address.shard][address.address_index]
                .get()
                .await
            {
                Ok(conn) => {
                    address.reset_error_count();
                    conn
                }
                Err(err) => {
                    error!(
                        "Connection checkout error for instance {:?}, error: {:?}",
                        address, err
                    );
                    self.ban(address, BanReason::FailedCheckout, Some(client_stats));
                    address.stats.error();
                    client_stats.checkout_error();
                    continue;
                }
            };

            // // Check if this server is alive with a health check.
            let server = &mut *conn;

            // Will return error if timestamp is greater than current system time, which it should never be set to
            let require_healthcheck = force_healthcheck
                || server.last_activity().elapsed().unwrap().as_millis()
                    > self.settings.healthcheck_delay as u128;

            // Do not issue a health check unless it's been a little while
            // since we last checked the server is ok.
            // Health checks are pretty expensive.
            if !require_healthcheck {
                let checkout_time = now.elapsed().as_micros() as u64;
                client_stats.checkout_success();
                server
                    .stats()
                    .checkout_time(checkout_time, client_stats.application_name());
                server.stats().active(client_stats.application_name());
                client_stats.active();
                return Ok((conn, address.clone()));
            }

            if self
                .run_health_check(address, server, now, client_stats)
                .await
            {
                let checkout_time = now.elapsed().as_micros() as u64;
                client_stats.checkout_success();
                server
                    .stats()
                    .checkout_time(checkout_time, client_stats.application_name());
                server.stats().active(client_stats.application_name());
                client_stats.active();
                return Ok((conn, address.clone()));
            } else {
                continue;
            }
        }

        client_stats.checkout_error();

        Err(Error::AllServersDown)
    }

    async fn run_health_check(
        &self,
        address: &Address,
        server: &mut Server,
        start: Instant,
        client_info: &ClientStats,
    ) -> bool {
        debug!("Running health check on server {:?}", address);

        server.stats().tested();

        match tokio::time::timeout(
            tokio::time::Duration::from_millis(self.settings.healthcheck_timeout),
            server.query(";"), // Cheap query as it skips the query planner
        )
        .await
        {
            // Check if health check succeeded.
            Ok(res) => match res {
                Ok(_) => {
                    let checkout_time: u64 = start.elapsed().as_micros() as u64;
                    client_info.checkout_success();
                    server
                        .stats()
                        .checkout_time(checkout_time, client_info.application_name());
                    server.stats().active(client_info.application_name());

                    return true;
                }

                // Health check failed.
                Err(err) => {
                    error!(
                        "Failed health check on instance {:?}, error: {:?}",
                        address, err
                    );
                }
            },

            // Health check timed out.
            Err(err) => {
                error!(
                    "Health check timeout on instance {:?}, error: {:?}",
                    address, err
                );
            }
        }

        // Don't leave a bad connection in the pool.
        server.mark_bad("failed health check");

        self.ban(address, BanReason::FailedHealthCheck, Some(client_info));
        false
    }

    /// Ban an address (i.e. replica). It no longer will serve
    /// traffic for any new transactions. Existing transactions on that replica
    /// will finish successfully or error out to the clients.
    pub fn ban(&self, address: &Address, reason: BanReason, client_info: Option<&ClientStats>) {
        // Count the number of errors since the last successful checkout
        // This is used to determine if the shard is down
        match reason {
            BanReason::FailedHealthCheck
            | BanReason::FailedCheckout
            | BanReason::MessageSendFailed
            | BanReason::MessageReceiveFailed => {
                address.increment_error_count();
            }
            _ => (),
        };

        // Primary can never be banned
        if address.role == Role::Primary {
            return;
        }

        error!("Banning instance {:?}, reason: {:?}", address, reason);

        let now = chrono::offset::Utc::now().naive_utc();
        let mut guard = self.banlist.write();

        if let Some(client_info) = client_info {
            client_info.ban_error();
            address.stats.error();
        }

        guard[address.shard].insert(address.clone(), (reason, now));
    }

    /// Clear the replica to receive traffic again. Takes effect immediately
    /// for all new transactions.
    pub fn unban(&self, address: &Address) {
        let mut guard = self.banlist.write();
        guard[address.shard].remove(address);
    }

    /// Check if address is banned
    /// true if banned, false otherwise
    pub fn is_banned(&self, address: &Address) -> bool {
        let guard = self.banlist.read();

        match guard[address.shard].get(address) {
            Some(_) => true,
            None => {
                debug!("{:?} is ok", address);
                false
            }
        }
    }

    /// Determines trying to unban this server was successful
    pub async fn try_unban(&self, address: &Address) -> bool {
        // If somehow primary ends up being banned we should return true here
        if address.role == Role::Primary {
            return true;
        }

        // Check if all replicas are banned, in that case unban all of them
        let replicas_available = self.addresses[address.shard]
            .iter()
            .filter(|addr| addr.role == Role::Replica)
            .count();

        debug!("Available targets: {}", replicas_available);

        let read_guard = self.banlist.read();
        let all_replicas_banned = read_guard[address.shard].len() == replicas_available;
        drop(read_guard);

        if all_replicas_banned {
            let mut write_guard = self.banlist.write();
            warn!("Unbanning all replicas.");
            write_guard[address.shard].clear();

            return true;
        }

        // Check if ban time is expired
        let read_guard = self.banlist.read();
        let exceeded_ban_time = match read_guard[address.shard].get(address) {
            Some((ban_reason, timestamp)) => {
                let now = chrono::offset::Utc::now().naive_utc();
                match ban_reason {
                    BanReason::AdminBan(duration) => {
                        now.and_utc().timestamp() - timestamp.and_utc().timestamp() > *duration
                    }
                    _ => now.and_utc().timestamp() - timestamp.and_utc().timestamp() > self.settings.ban_time,
                }
            }
            None => return true,
        };
        drop(read_guard);

        if exceeded_ban_time {
            warn!("Unbanning {:?}", address);
            let mut write_guard = self.banlist.write();
            write_guard[address.shard].remove(address);
            drop(write_guard);

            true
        } else {
            debug!("{:?} is banned", address);
            false
        }
    }

    /// Get the number of configured shards.
    pub fn shards(&self) -> usize {
        self.databases.len()
    }

    pub fn get_bans(&self) -> Vec<(Address, (BanReason, NaiveDateTime))> {
        let mut bans: Vec<(Address, (BanReason, NaiveDateTime))> = Vec::new();
        let guard = self.banlist.read();
        for banlist in guard.iter() {
            for (address, (reason, timestamp)) in banlist.iter() {
                bans.push((address.clone(), (reason.clone(), *timestamp)));
            }
        }
        bans
    }

    /// Get the address from the host url
    pub fn get_addresses_from_host(&self, host: &str) -> Vec<Address> {
        let mut addresses = Vec::new();
        for shard in 0..self.shards() {
            for server in 0..self.servers(shard) {
                let address = self.address(shard, server);
                if address.host == host {
                    addresses.push(address.clone());
                }
            }
        }
        addresses
    }

    /// Get the number of servers (primary and replicas)
    /// configured for a shard.
    pub fn servers(&self, shard: usize) -> usize {
        self.addresses[shard].len()
    }

    /// Get the total number of servers (databases) we are connected to.
    pub fn databases(&self) -> usize {
        let mut databases = 0;
        for shard in 0..self.shards() {
            databases += self.servers(shard);
        }
        databases
    }

    /// Get pool state for a particular shard server as reported by bb8.
    pub fn pool_state(&self, shard: usize, server: usize) -> bb8::State {
        self.databases[shard][server].state()
    }

    /// Get the address information for a shard server.
    pub fn address(&self, shard: usize, server: usize) -> &Address {
        &self.addresses[shard][server]
    }

    pub fn server_parameters(&self) -> ServerParameters {
        self.original_server_parameters.read().clone()
    }

    /// Get the number of checked out connection for an address
    fn busy_connection_count(&self, address: &Address) -> u32 {
        let state = self.pool_state(address.shard, address.address_index);
        let idle = state.idle_connections;
        let provisioned = state.connections;

        if idle > provisioned {
            // Unlikely but avoids an overflow panic if this ever happens
            return 0;
        }
        let busy = provisioned - idle;
        debug!("{:?} has {:?} busy connections", address, busy);
        busy
    }

    fn valid_shard_id(&self, shard: Option<usize>) -> bool {
        match shard {
            None => true,
            Some(shard) => shard < self.shards(),
        }
    }

    /// Register a parse statement to the pool's cache and return the rewritten parse
    ///
    /// Do not pass an anonymous parse statement to this function
    pub fn register_parse_to_cache(&self, hash: u64, parse: &Parse) -> Option<Arc<Parse>> {
        // We should only be calling this function if the cache is enabled
        match self.prepared_statement_cache {
            Some(ref prepared_statement_cache) => {
                let mut cache = prepared_statement_cache.lock();
                Some(cache.get_or_insert(parse, hash))
            }
            None => None,
        }
    }

    /// Promote a prepared statement hash in the LRU
    pub fn promote_prepared_statement_hash(&self, hash: &u64) {
        // We should only be calling this function if the cache is enabled
        if let Some(ref prepared_statement_cache) = self.prepared_statement_cache {
            let mut cache = prepared_statement_cache.lock();
            cache.promote(hash);
        }
    }
}

/// Wrapper for the bb8 connection pool.
pub struct ServerPool {
    /// Server address.
    address: Address,

    /// Server Postgres user.
    user: User,

    /// Server database.
    database: String,

    /// Client/server mapping.
    client_server_map: ClientServerMap,

    /// Server auth hash (for auth passthrough).
    auth_hash: Arc<RwLock<Option<String>>>,

    /// Server plugins.
    plugins: Option<Plugins>,

    /// Should we clean up dirty connections before putting them into the pool?
    cleanup_connections: bool,

    /// Log client parameter status changes
    log_client_parameter_status_changes: bool,

    /// Prepared statement cache size
    prepared_statement_cache_size: usize,
}

impl ServerPool {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        address: Address,
        user: User,
        database: &str,
        client_server_map: ClientServerMap,
        auth_hash: Arc<RwLock<Option<String>>>,
        plugins: Option<Plugins>,
        cleanup_connections: bool,
        log_client_parameter_status_changes: bool,
        prepared_statement_cache_size: usize,
    ) -> ServerPool {
        ServerPool {
            address,
            user,
            database: database.to_string(),
            client_server_map,
            auth_hash,
            plugins,
            cleanup_connections,
            log_client_parameter_status_changes,
            prepared_statement_cache_size,
        }
    }
}

impl ManageConnection for ServerPool {
    type Connection = Server;
    type Error = Error;

    fn connect(&self) -> impl std::future::Future<Output = Result<Self::Connection, Self::Error>> + Send {
        let address = self.address.clone();
        let user = self.user.clone();
        let database = self.database.clone();
        let client_server_map = self.client_server_map.clone();
        let auth_hash = self.auth_hash.clone();
        let cleanup_connections = self.cleanup_connections;
        let log_client_parameter_status_changes = self.log_client_parameter_status_changes;
        let prepared_statement_cache_size = self.prepared_statement_cache_size;
        let plugins = self.plugins.clone();

        async move {
            info!("Creating a new server connection {:?}", address);

            let stats = Arc::new(ServerStats::new(
                address.clone(),
                tokio::time::Instant::now(),
            ));

            stats.register(stats.clone());

            match Server::startup(
                &address,
                &user,
                &database,
                client_server_map,
                stats.clone(),
                auth_hash,
                cleanup_connections,
                log_client_parameter_status_changes,
                prepared_statement_cache_size,
            )
            .await
            {
                Ok(mut conn) => {
                    if let Some(ref plugins) = plugins {
                        if let Some(ref prewarmer) = plugins.prewarmer {
                            let mut prewarmer = prewarmer::Prewarmer {
                                enabled: prewarmer.enabled,
                                server: &mut conn,
                                queries: &prewarmer.queries,
                            };

                            prewarmer.run().await?;
                        }
                    }

                    stats.idle();
                    Ok(conn)
                }
                Err(err) => {
                    stats.disconnect();
                    Err(err)
                }
            }
        }
    }

    fn is_valid(
        &self,
        _conn: &mut Self::Connection,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        async { Ok(()) }
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        conn.is_bad()
    }
}

/// Get the connection pool
pub fn get_pool(db: &str, user: &str) -> Option<ConnectionPool> {
    (*(*POOLS.load()))
        .get(&PoolIdentifier::new(db, user))
        .cloned()
}

/// Get or create a pool dynamically if it doesn't exist and default pool is configured
pub async fn get_or_create_pool(
    db: &str,
    user: &str,
    client_server_map: ClientServerMap,
) -> Result<Option<ConnectionPool>, Error> {
    // Check if pool already exists (fast path without lock)
    if let Some(pool) = get_pool(db, user) {
        return Ok(Some(pool));
    }

    if !has_default_pool() {
        return Ok(None);
    }

    // Acquire lock for double-check
    {
        let _guard = POOLS_WRITE_LOCK.lock();

        // Double-check inside the lock in case another thread created it
        if let Some(pool) = get_pool(db, user) {
            return Ok(Some(pool));
        }
    }

    info!(
        "Creating dynamic pool for database: {:?}, user: {:?}",
        db, user
    );

    let pool = ConnectionPool::create_dynamic_pool(db, user, &[], client_server_map).await?;

    // Final check and add - use block to limit lock scope
    {
        let _guard = POOLS_WRITE_LOCK.lock();

        // Check again - another thread might have created it while we were creating
        if let Some(existing_pool) = get_pool(db, user) {
            return Ok(Some(existing_pool));
        }

        // Insert the pool directly since we already hold the lock
        add_dynamic_pool(db, user, pool.clone());
    }

    Ok(Some(pool))
}

/// Check if default pool exists (used to determine if dynamic pool creation is available)
fn has_default_pool() -> bool {
    let config = get_config();
    config.pools.get("default").is_some()
}

/// Add a dynamic pool to the pool map
fn add_dynamic_pool(db: &str, user: &str, pool: ConnectionPool) {
    let mut pools = (*(*POOLS.load())).clone();
    pools.insert(PoolIdentifier::new(db, user), pool);
    POOLS.store(Arc::new(pools));
}

/// Get a pointer to all configured pools.
pub fn get_all_pools() -> HashMap<PoolIdentifier, ConnectionPool> {
    (*(*POOLS.load())).clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{General, Shard, ServerConfig};
    use std::collections::BTreeMap;

    fn create_test_user() -> User {
        User {
            username: "test_user".to_string(),
            password: Some("test_password".to_string()),
            auth_type: crate::config::AuthType::MD5,
            server_username: None,
            server_password: None,
            pool_size: 10,
            min_pool_size: Some(2),
            statement_timeout: 0,
            pool_mode: None,
            connect_timeout: None,
            idle_timeout: None,
            server_lifetime: None,
        }
    }

    fn create_test_pool_config() -> crate::config::Pool {
        let mut shards = BTreeMap::new();
        shards.insert(
            "0".to_string(),
            Shard {
                database: "test_db".to_string(),
                servers: vec![
                    ServerConfig {
                        host: "localhost".to_string(),
                        port: 5432,
                        role: Role::Primary,
                    },
                    ServerConfig {
                        host: "localhost".to_string(),
                        port: 5433,
                        role: Role::Replica,
                    },
                ],
                mirrors: None,
            },
        );

        let mut users = BTreeMap::new();
        users.insert("0".to_string(), create_test_user());

        crate::config::Pool {
            pool_mode: PoolMode::Transaction,
            load_balancing_mode: LoadBalancingMode::Random,
            default_role: "any".to_string(),
            query_parser_enabled: false,
            query_parser_max_length: None,
            query_parser_read_write_splitting: false,
            primary_reads_enabled: true,
            connect_timeout: Some(5000),
            idle_timeout: Some(30000),
            checkout_failure_limit: None,
            server_lifetime: Some(3600000),
            sharding_function: ShardingFunction::PgBigintHash,
            automatic_sharding_key: None,
            sharding_key_regex: None,
            shard_id_regex: None,
            regex_search_limit: Some(1000),
            default_shard: DefaultShard::Shard(0),
            auth_query: None,
            auth_query_user: None,
            auth_query_password: None,
            cleanup_server_connections: true,
            log_client_parameter_status_changes: false,
            prepared_statements_cache_size: 0,
            db_activity_based_routing: false,
            db_activity_init_delay: 100,
            db_activity_ttl: 900,
            table_mutation_cache_ms_ttl: 50,
            plugins: None,
            shards,
            users,
        }
    }

    fn create_test_config() -> crate::config::Config {
        let mut pools = HashMap::new();
        pools.insert("test_pool".to_string(), create_test_pool_config());

        crate::config::Config {
            path: "test.toml".to_string(),
            general: General {
                host: "0.0.0.0".to_string(),
                port: 6432,
                enable_prometheus_exporter: Some(false),
                prometheus_exporter_port: 9930,
                admin_username: "admin".to_string(),
                admin_password: "admin".to_string(),
                connect_timeout: 5000,
                idle_timeout: 30000,
                server_lifetime: 3600000,
                idle_client_in_transaction_timeout: 0,
                healthcheck_timeout: 1000,
                healthcheck_delay: 30000,
                server_recv_timeout: 0,
                ban_time: 60,
                log_client_connections: false,
                log_client_disconnections: false,
                autoreload: None,
                shutdown_timeout: 60000,
                worker_threads: 4,
                tcp_keepalives_idle: 5,
                tcp_keepalives_count: 5,
                tcp_keepalives_interval: 5,
                tcp_user_timeout: 120000,
                validate_config: false,
                server_round_robin: false,
                server_tls: false,
                verify_server_certificate: false,
                tls_certificate: None,
                tls_private_key: None,
                dns_cache_enabled: false,
                dns_max_ttl: 30,
                admin_auth_type: crate::config::AuthType::MD5,
                auth_query: None,
                auth_query_user: None,
                auth_query_password: None,
            },
            plugins: None,
            pools,
        }
    }

    #[test]
    fn test_extract_sorted_shard_ids_multiple() {
        let mut pool_config = create_test_pool_config();
        pool_config.shards.insert(
            "2".to_string(),
            Shard {
                database: "test_db_2".to_string(),
                servers: vec![],
                mirrors: None,
            },
        );
        pool_config.shards.insert(
            "1".to_string(),
            Shard {
                database: "test_db_1".to_string(),
                servers: vec![],
                mirrors: None,
            },
        );

        let shard_ids = ConnectionPool::extract_sorted_shard_ids(&pool_config).unwrap();

        assert_eq!(shard_ids.len(), 3);
        assert_eq!(shard_ids, vec!["0", "1", "2"]);
    }

    #[test]
    fn test_extract_sorted_shard_ids_invalid() {
        let mut pool_config = create_test_pool_config();
        pool_config.shards.insert(
            "invalid".to_string(),
            Shard {
                database: "test_db".to_string(),
                servers: vec![],
                mirrors: None,
            },
        );

        let result = ConnectionPool::extract_sorted_shard_ids(&pool_config);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), Error::BadConfig);
    }

    #[test]
    fn test_resolve_timeouts_user_override() {
        let mut user = create_test_user();
        user.connect_timeout = Some(1000);
        user.idle_timeout = Some(2000);
        user.server_lifetime = Some(3000);

        let pool_config = create_test_pool_config();
        let config = create_test_config();

        let timeouts = ConnectionPool::resolve_timeouts(&user, &pool_config, &config);

        assert_eq!(timeouts.connect, 1000);
        assert_eq!(timeouts.idle, 2000);
        assert_eq!(timeouts.lifetime, 3000);
    }

    #[test]
    fn test_resolve_timeouts_pool_override() {
        let user = create_test_user();
        let pool_config = create_test_pool_config();
        let config = create_test_config();

        let timeouts = ConnectionPool::resolve_timeouts(&user, &pool_config, &config);

        assert_eq!(timeouts.connect, 5000);
        assert_eq!(timeouts.idle, 30000);
        assert_eq!(timeouts.lifetime, 3600000);
    }

    #[test]
    fn test_resolve_timeouts_default() {
        let mut user = create_test_user();
        user.connect_timeout = None;
        user.idle_timeout = None;
        user.server_lifetime = None;

        let mut pool_config = create_test_pool_config();
        pool_config.connect_timeout = None;
        pool_config.idle_timeout = None;
        pool_config.server_lifetime = None;

        let config = create_test_config();

        let timeouts = ConnectionPool::resolve_timeouts(&user, &pool_config, &config);

        assert_eq!(timeouts.connect, config.general.connect_timeout);
        assert_eq!(timeouts.idle, config.general.idle_timeout);
        assert_eq!(timeouts.lifetime, config.general.server_lifetime);
    }

    #[test]
    fn test_calculate_reaper_rate() {
        let timeouts = ConnectionTimeouts {
            connect: 5000,
            idle: 30000,
            lifetime: 60000,
        };

        let rate = ConnectionPool::calculate_reaper_rate(&timeouts);
        assert_eq!(rate, POOL_REAPER_RATE);
    }

    #[test]
    fn test_calculate_reaper_rate_uses_minimum() {
        let timeouts = ConnectionTimeouts {
            connect: 5000,
            idle: 10000,
            lifetime: 60000,
        };

        let rate = ConnectionPool::calculate_reaper_rate(&timeouts);
        assert_eq!(rate, 10000);
    }

    #[test]
    fn test_select_queue_strategy_fifo() {
        let mut config = create_test_config();
        config.general.server_round_robin = true;

        let strategy = ConnectionPool::select_queue_strategy(&config);
        assert!(matches!(strategy, QueueStrategy::Fifo));
    }

    #[test]
    fn test_select_queue_strategy_lifo() {
        let mut config = create_test_config();
        config.general.server_round_robin = false;

        let strategy = ConnectionPool::select_queue_strategy(&config);
        assert!(matches!(strategy, QueueStrategy::Lifo));
    }

    #[test]
    fn test_parse_default_role_any() {
        let role = ConnectionPool::parse_default_role("any");
        assert_eq!(role, None);
    }

    #[test]
    fn test_parse_default_role_replica() {
        let role = ConnectionPool::parse_default_role("replica");
        assert_eq!(role, Some(Role::Replica));
    }

    #[test]
    fn test_parse_default_role_primary() {
        let role = ConnectionPool::parse_default_role("primary");
        assert_eq!(role, Some(Role::Primary));
    }

    #[test]
    fn test_create_prepared_statement_cache_disabled() {
        let pool_config = create_test_pool_config();
        let cache = ConnectionPool::create_prepared_statement_cache(&pool_config);
        assert!(cache.is_none());
    }

    #[test]
    fn test_create_prepared_statement_cache_enabled() {
        let mut pool_config = create_test_pool_config();
        pool_config.prepared_statements_cache_size = 100;

        let cache = ConnectionPool::create_prepared_statement_cache(&pool_config);
        assert!(cache.is_some());
    }

    #[test]
    fn test_build_mirror_addresses_no_mirrors() {
        let shard_config = Shard {
            database: "test_db".to_string(),
            servers: vec![],
            mirrors: None,
        };

        let server_config = ServerConfig {
            host: "localhost".to_string(),
            port: 5432,
            role: Role::Primary,
        };

        let mut address_id = 0;
        let mirrors = ConnectionPool::build_mirror_addresses(
            "0",
            &shard_config,
            &server_config,
            0,
            0,
            "test_pool",
            &create_test_user(),
            &mut address_id,
        );

        assert_eq!(mirrors.len(), 0);
        assert_eq!(address_id, 0);
    }

    #[test]
    fn test_build_address() {
        let shard_config = Shard {
            database: "test_db".to_string(),
            servers: vec![],
            mirrors: None,
        };

        let server_config = ServerConfig {
            host: "localhost".to_string(),
            port: 5432,
            role: Role::Primary,
        };

        let mut address_id = 42;
        let address = ConnectionPool::build_address(
            "1",
            &shard_config,
            &server_config,
            0,
            0,
            "test_pool",
            &create_test_user(),
            &mut address_id,
        );

        assert_eq!(address.id, 42);
        assert_eq!(address.host, "localhost");
        assert_eq!(address.port, 5432);
        assert_eq!(address.role, Role::Primary);
        assert_eq!(address.shard, 1);
        assert_eq!(address.address_index, 0);
        assert_eq!(address.replica_number, 0);
        assert_eq!(address.username, "test_user");
        assert_eq!(address.pool_name, "test_pool");
        assert_eq!(address.database, "test_db");
        assert_eq!(address_id, 43);
    }

    #[test]
    fn test_build_pool_settings() {
        let user = create_test_user();
        let pool_config = create_test_pool_config();
        let config = create_test_config();

        let settings = ConnectionPool::build_pool_settings(
            "test_pool",
            &user,
            &pool_config,
            &config,
            2,
        );

        assert_eq!(settings.pool_mode, PoolMode::Transaction);
        assert_eq!(settings.load_balancing_mode, LoadBalancingMode::Random);
        assert_eq!(settings.shards, 2);
        assert_eq!(settings.user.username, "test_user");
        assert_eq!(settings.db, "test_pool");
        assert_eq!(settings.default_role, None);
        assert!(!settings.query_parser_enabled);
        assert!(settings.primary_reads_enabled);
        assert_eq!(settings.healthcheck_delay, config.general.healthcheck_delay);
        assert_eq!(settings.healthcheck_timeout, config.general.healthcheck_timeout);
        assert_eq!(settings.ban_time, config.general.ban_time);
    }

    #[test]
    fn test_build_pool_settings_with_role_override() {
        let user = create_test_user();
        let mut pool_config = create_test_pool_config();
        pool_config.default_role = "replica".to_string();
        let config = create_test_config();

        let settings = ConnectionPool::build_pool_settings(
            "test_pool",
            &user,
            &pool_config,
            &config,
            1,
        );

        assert_eq!(settings.default_role, Some(Role::Replica));
    }

    #[test]
    fn test_pool_identifier_display() {
        let identifier = PoolIdentifier::new("my_db", "my_user");
        assert_eq!(format!("{}", identifier), "my_user@my_db");
    }

    #[test]
    fn test_pool_identifier_equality() {
        let id1 = PoolIdentifier::new("db", "user");
        let id2 = PoolIdentifier::new("db", "user");
        let id3 = PoolIdentifier::new("other_db", "user");

        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_prepared_statement_cache_new_zero_size() {
        let cache = PreparedStatementCache::new(0);
        assert!(cache.cache.cap().get() == 1);
    }
}
