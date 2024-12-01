package config

var VALID_LOG_LEVELS = []string{"DEBUG", "INFO", "WARN", "ERROR"}
var VALID_LOG_OUTPUTS = []string{"console", "file"}
var VALID_LOG_FORMATS = []string{"text", "json"}

var VALID_STORAGE_PROVIDERS = []string{"local", "s3"}

var VALID_CLEANUP_POLICIES = []string{"delete", "archive", "delete,archive"}
