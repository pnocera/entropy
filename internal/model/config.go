package model

type Config struct {
	NATS_URL           string `mapstructure:"NATS_URL"`
	NATS_SERVER_ROOTFS string `mapstructure:"NATS_SERVER_ROOTFS"`
}
