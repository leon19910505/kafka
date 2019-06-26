<?php
/**
 * Created by PhpStorm.
 * User: Zhizhong
 * Date: 2019/6/17
 * Time: 上午10:24
 */

namespace Zhizhong\Kafka;

use Exception;

class Producer
{
    private $config;
    private $producer;
    static private $instance;

    private function __construct($config)
    {
        $this->config = $config;
        $this->producer = new \RdKafka\Producer();
        $this->producer->addBrokers($this->config);
    }

    public static function getInstance()
    {
        if (!self::$instance) {
            $brokers = env("KAFKA_BROKERS");
            if (!$brokers) {
                throw new Exception('please configure KAFKA_BROKERS param in env first');
            }
            self::$instance = new self($brokers);
        }
        return self::$instance;
    }

    public function produce($topic, $msg)
    {
        $topicProducer = $this->producer->newTopic($topic);
        $topicProducer->produce(RD_KAFKA_PARTITION_UA, 0, json_encode($msg));
    }


}