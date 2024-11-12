package redis

// The curly braces are used to force keys with simiar tags to go the same cluster slot, which is useful for sharding.
const STREAM_META_DATA_PREFIX = "{streamweaver_stream_metadata}:"
const STREAM_CLEANUP_BUCKET_DELETE = "stream_cleanup_bucket:delete"
const STREAM_CLEANUP_BUCKET_ARCHIVE = "stream_cleanup_bucket:archive"
const STREAM_CLEANUP_BUCKET_DELETE_ARCHIVE = "stream_cleanup_bucket:delete_archive"
const STREAM_REGISTRY_KEY = "stream_registry"
