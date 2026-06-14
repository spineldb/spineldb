// src/core/commands/vector/mod.rs

//! This module implements the SpinelVector commands, including VS.RESERVE, VS.ADD, VS.GET,
//! VS.DEL, VS.SEARCH, VS.MSEARCH, VS.UPDATE, VS.INFO, VS.CARD, VS.EXISTS, VS.REBUILD,
//! VS.OPTIMIZE, VS.STATS, VS.EXPIRE, VS.TTL, VS.QUANTIZE, VS.HYBRIDSEARCH, VS.TRAINPQ,
//! and VS.FEDERATEDSEARCH.

pub mod command;
pub mod vs_add;
pub mod vs_card;
pub mod vs_del;
pub mod vs_exists;
pub mod vs_expire;
pub mod vs_federatedsearch;
pub mod vs_get;
pub mod vs_hybridsearch;
pub mod vs_info;
pub mod vs_madd;
pub mod vs_msearch;
pub mod vs_optimize;
pub mod vs_quantize;
pub mod vs_rebuild;
pub mod vs_reserve;
pub mod vs_search;
pub mod vs_stats;
pub mod vs_trainpq;
pub mod vs_ttl;
pub mod vs_update;

pub use self::command::{Vector, VectorSubcommand};
pub use self::vs_add::VsAdd;
pub use self::vs_card::VsCard;
pub use self::vs_del::VsDel;
pub use self::vs_exists::VsExists;
pub use self::vs_expire::VsExpire;
pub use self::vs_federatedsearch::VsFederatedSearch;
pub use self::vs_get::VsGet;
pub use self::vs_hybridsearch::VsHybridSearch;
pub use self::vs_info::VsInfo;
pub use self::vs_madd::VsMAdd;
pub use self::vs_msearch::VsMSearch;
pub use self::vs_optimize::VsOptimize;
pub use self::vs_quantize::VsQuantize;
pub use self::vs_rebuild::VsRebuild;
pub use self::vs_reserve::VsReserve;
pub use self::vs_search::VsSearch;
pub use self::vs_stats::VsStats;
pub use self::vs_trainpq::VsTrainPq;
pub use self::vs_ttl::VsTtl;
pub use self::vs_update::VsUpdate;
