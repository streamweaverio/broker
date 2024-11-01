package config

type StreamWeaverConfig struct {
	// Port for rpc server to listen on
	Port int `yaml:"port"`
	// Logging configuration
	Logging   *LoggingConfig   `yaml:"logging"`
	Redis     *RedisConfig     `yaml:"redis"`
	Storage   *StorageConfig   `yaml:"storage"`
	Retention *RetentionConfig `yaml:"retention"`
}

type RedisConfig struct {
	// list of Redis hosts to connect to
	Hosts []*RedisHostConfig `yaml:"hosts"`
	// database to use within Redis
	DB int `yaml:"db"`
	// password to use when connecting to Redis
	Password string `yaml:"password"`
}

type RedisHostConfig struct {
	// host of the Redis instance
	Host string `yaml:"host"`
	// port of the Redis instance
	Port int `yaml:"port"`
}

type StorageConfig struct {
	// provider of the storage; either "local" or "aws_s3"
	Provider string                      `yaml:"provider"`
	Local    *LocalStorageProviderConfig `yaml:"local"`
	S3       *AWSS3StorageProviderConfig `yaml:"s3"`
}

// represents the configuration for the local storage provider
type LocalStorageProviderConfig struct {
	// directory where the local storage files are stored
	Directory string `yaml:"directory"`
}

// represents the configuration for the AWS S3 storage provider
type AWSS3StorageProviderConfig struct {
	// AWS region where the S3 bucket is located
	Region string `yaml:"region"`
	// name of the S3 bucket
	Bucket string `yaml:"bucket"`
	// access key ID for the AWS IAM user
	AccessKeyId string `yaml:"access_key"`
	// secret access key for the AWS IAM user
	SecretAccessKey string `yaml:"secret_access_key"`
}

// global retention policy for the broker, which applies to all streams by default unless overridden by the stream configuration
type RetentionConfig struct {
	// retention policy to use; either "time" or "size"
	Policy string `yaml:"policy"`
	// maximum age of a message before its moved to storage
	MaxAge string `yaml:"max_age"`
	// maximum size of a stream before messages are moved to storage (in bytes)
	MaxSize int64 `yaml:"max_size"`
}

type LoggingConfig struct {
	LogLevel string `yaml:"log_level"`
	// where to send log output; either "console" or "file"
	LogOutput string `yaml:"log_output"`
	// format of the log messages; either "json" or "text"
	LogFormat string `yaml:"log_format"`
	// prefix of the log file
	LogFilePrefix string `yaml:"log_file_prefix"`
	// directory where the log file(s) are stored
	LogDirectory string `yaml:"log_directory"`
	// maximum size in bytes of the log file before it is rotated
	MaxFileSize int `yaml:"max_file_size"`
}
