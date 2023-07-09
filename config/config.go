package config

import (
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

type Config struct {
	Service   *service  `yaml:"service"`
	SelfPerr  *selfPerr `yaml:"self_peer,omitempty"`
	Followers []string  `yaml:"followers"`
	IDs       []string  `yaml:"flowwer_ids"`
	Dir       string    `yaml:"dir"`
}

type service struct {
	DBPath       string `yaml:"default_db_path"`
	Host         string `yaml:"default_host"`
	Port         string `yaml:"default_port"`
	DatabasesNum uint   `yaml:"default_databases_num"`
}

type selfPerr struct {
	Host string `yaml:"host"`
	ID   string `yaml:"id"`
}

func ReadConfig(fpath string) (*Config, error) {
	data, err := ioutil.ReadFile(fpath)
	if err != nil {
		return nil, err
	}

	cfg := new(Config)
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}
